# Databricks notebook source
# MAGIC   %sql
# MAGIC CREATE OR REPLACE TEMP VIEW trips_enriched_tmp AS
# MAGIC SELECT * FROM workspace.bde.trips_final;

# COMMAND ----------

import os
os.environ["OPENBLAS_NUM_THREADS"] = "1"
os.environ["MKL_NUM_THREADS"] = "1"
os.environ["NUMEXPR_NUM_THREADS"] = "1"
os.environ["OMP_NUM_THREADS"] = "1"

# COMMAND ----------

# DBTITLE 1,l
from threadpoolctl import threadpool_limits
threadpool_limits(limits=1)

# COMMAND ----------

from pyspark.sql import functions as F

df = (spark.table("trips_enriched_tmp")
      .select(
          "total_amount","passenger_count","distance_km","duration_sec","speed_kmh",
          "color","pu_borough","do_borough","payment_type","tip_amount","pickup_ts"
      )
      .where(F.col("total_amount").isNotNull())
      .withColumn("month", F.date_format("pickup_ts","yyyy-MM"))
      .withColumn("day_of_week", F.date_format("pickup_ts","EEEE"))
      .withColumn("hour_of_day", F.hour("pickup_ts"))
)

# COMMAND ----------

from pyspark.sql import functions as F


TEST_MONTHS = ["2024-10", "2024-11", "2024-12"]
F_TEST = 0.03        # 3% for test months
TARGET_OVERALL = 0.04


total_rows = df.count()
test_rows  = df.filter(F.col("month").isin(TEST_MONTHS)).count()
p_test = test_rows / total_rows
print("Share of test months in full data:", f"{p_test:.2%}")

# ComputING sampling fractions
f_other = (TARGET_OVERALL - p_test*F_TEST) / (1 - p_test) if p_test < 1.0 else TARGET_OVERALL
print("Use fraction for train/val months:", f_other)

fractions = {
    r[0]: (F_TEST if r[0] in TEST_MONTHS else f_other) 
    for r in df.select("month").distinct().collect()
}
eda_sample = df.sampleBy("month", fractions=fractions, seed=42)

print("Total rows:", total_rows)
print("Sample rows:", eda_sample.count())
print("Final sample %:", 100*eda_sample.count()/total_rows)

# Split into train, val, test in fewer steps
train_sample, val_sample = (
    eda_sample
      .filter(~F.col("month").isin(TEST_MONTHS))  # exclude test months
      .randomSplit([0.8, 0.2], seed=42)           # split train/val
)
test_sample = eda_sample.filter(F.col("month").isin(TEST_MONTHS))

print("Train:", train_sample.count(),
      "Val:", val_sample.count(),
      "Test:", test_sample.count())


# COMMAND ----------

# MAGIC %md
# MAGIC #Baseline

# COMMAND ----------

baseline_tbl = spark.table("workspace.bde.q3_color_borough_stats")
baseline_tbl.printSchema()
print("Baseline columns:", baseline_tbl.columns)

# COMMAND ----------

from pyspark.sql import functions as F, Window
test_with_baseline = (
    spark.table("workspace.bde.trips_final").alias("t")
    .select(
        "color","pu_borough","do_borough",
        F.date_format("pickup_ts","yyyy-MM").alias("month_ym"),
        F.date_format("pickup_ts","EEEE").alias("dow_name"),
        F.hour("pickup_ts").alias("hour_of_day"),
        F.col("total_amount").alias("y_true")
    )
    .filter(F.col("month_ym").isin(TEST_MONTHS))
    .join(
        baseline_tbl.alias("b"),
        on=["color","pu_borough","do_borough","month_ym","dow_name","hour_of_day"],
        how="left"   
    )
    .withColumn(
        "y_hat",
        F.coalesce(F.col("b.avg_amount_per_trip"),
                   F.avg("b.avg_amount_per_trip").over(Window.partitionBy("color")))
    )
)
total = test_with_baseline.count()
matched = test_with_baseline.filter(F.col("b.avg_amount_per_trip").isNotNull()).count()
print(f"Test rows: {total:,} | matched baseline: {matched:,} ({matched/total:.2%})")

# RMSE
rmse = (test_with_baseline
        .select(F.sqrt(F.avg(F.pow(F.col("y_true") - F.col("y_hat"), 2))).alias("rmse"))
        .first()["rmse"])
print(f"Baseline RMSE (test): {rmse:,.4f}")


# COMMAND ----------

# MAGIC %md
# MAGIC #Modelling

# COMMAND ----------

train_sample.count()

# COMMAND ----------

LABEL = "total_amount"
NUM_COLS = ["passenger_count","distance_km","duration_sec","speed_kmh","hour_of_day"]
CAT_COLS = ["color","pu_borough","do_borough","payment_type","day_of_week","month"]

# COMMAND ----------

# DBTITLE 1,rau
train_pdf = (train_sample
             .select(NUM_COLS + CAT_COLS + [LABEL])
             .sample(fraction=0.05, seed=42)
             .toPandas())

# COMMAND ----------

train_pdf.shape

# COMMAND ----------

val_pdf = (val_sample
           .select(NUM_COLS + CAT_COLS + [LABEL])
           .sample(fraction=0.05, seed=42)
           .toPandas())

# COMMAND ----------

val_pdf.shape

# COMMAND ----------

test_pdf = (test_sample
            .select(NUM_COLS + CAT_COLS + [LABEL])
            .sample(fraction=1.0, seed=42)    
            .toPandas())

# COMMAND ----------

test_pdf.shape

# COMMAND ----------

import numpy as np
import pandas as pd
from threadpoolctl import threadpool_limits
from sklearn.compose import ColumnTransformer, TransformedTargetRegressor
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler, FunctionTransformer
from sklearn.impute import SimpleImputer
from sklearn.metrics import mean_squared_error, mean_absolute_error

threadpool_limits(limits=1)  

pre = ColumnTransformer([
    ("num", Pipeline([
        ("imp", SimpleImputer(strategy="median")),
        ("sc", StandardScaler())
    ]), NUM_COLS),
    ("cat", Pipeline([
        ("imp", SimpleImputer(strategy="most_frequent")),
        ("ohe", OneHotEncoder(handle_unknown="ignore", sparse_output=False)),
    ]), CAT_COLS),
])

def clean_target(df, LABEL="total_amount", eps=1e-3):
    # coerce to numeric
    y = pd.to_numeric(df[LABEL], errors="coerce")
    # replace NaN with 0, then clip to small positive
    y = y.fillna(0).clip(lower=eps)
    return y.values

Xtr = train_pdf[NUM_COLS+CAT_COLS]
ytr = clean_target(train_pdf, LABEL)

Xva = val_pdf[NUM_COLS+CAT_COLS]
yva = clean_target(val_pdf, LABEL)

Xte = test_pdf[NUM_COLS+CAT_COLS]
yte = clean_target(test_pdf, LABEL)

# log1p target wrapper
log_ttr = lambda reg: TransformedTargetRegressor(
    regressor=Pipeline([("pre", pre), ("est", reg)]),
    func=np.log1p, inverse_func=np.expm1
)

def eval_model(name, model, Xva, yva, Xte, yte):
    pred_va = model.predict(Xva)
    pred_te = model.predict(Xte)
    rmse = lambda y,p: np.sqrt(mean_squared_error(y,p))
    from sklearn.metrics import mean_absolute_error
    print(f"{name:12s} | Val RMSE: {rmse(yva,pred_va):.4f}  Val MAE: {mean_absolute_error(yva,pred_va):.4f}  "
          f"| Test RMSE: {rmse(yte,pred_te):.4f}  Test MAE: {mean_absolute_error(yte,pred_te):.4f}")


# COMMAND ----------

# DBTITLE 1,Save Linear and Ridge Models with Log Transformation
from sklearn.linear_model import LinearRegression, Ridge

lin = log_ttr(LinearRegression())
lin.fit(Xtr, ytr)

ridge = log_ttr(Ridge(alpha=1.0))  # try 0.1–10
ridge.fit(Xtr, ytr)
import joblib

# Paths in DBFS (persist across sessions)
lin_path   = "linear_model_0.5.pkl"
ridge_path ="ridge_model_0.5.pkl"

joblib.dump(lin, lin_path)
joblib.dump(ridge, ridge_path)

print("Saved models to DBFS.")

# COMMAND ----------

import joblib

lin_loaded   = joblib.load("linear_model_0.5.pkl") #traained in 0.5% data 
ridge_loaded = joblib.load("ridge_model_0.5.pkl")

print("Loaded models from DBFS.")

# COMMAND ----------

eval_model("Linear", lin_loaded, Xva, yva, Xte, yte)
eval_model("Ridge α=1", ridge_loaded, Xva, yva, Xte, yte)

# COMMAND ----------

from sklearn.linear_model import SGDRegressor
import joblib

sgd = log_ttr(SGDRegressor(
    loss="huber",             # robust
    penalty="elasticnet",
    alpha=1e-4,               # strength of regularization (tune 1e-5..1e-3)
    l1_ratio=0.15,            # mix of L1/L2
    learning_rate="invscaling",
    eta0=0.01,
    power_t=0.25,
    max_iter=2000,
    tol=1e-4,
    early_stopping=True,
    validation_fraction=0.1,
    random_state=42
))
sgd.fit(Xtr[:100000], ytr[:100000])
sgd_path   = "sgd_model_0.5.pkl" # trained on 0.05%

joblib.dump(sgd, sgd_path)
print("Saved models to DBFS.")



# COMMAND ----------

SDG_loaded   = joblib.load("sgd_model_0.5.pkl")
eval_model("SGD(Huber)", SDG_loaded , Xva, yva,Xte,yte)


# COMMAND ----------

from sklearn.neural_network import MLPRegressor

mlp = log_ttr(MLPRegressor(
    hidden_layer_sizes=(64,32),
    activation="relu",
    solver="adam",
    alpha=1e-4,              # L2
    learning_rate_init=1e-3,
    max_iter=300,
    early_stopping=True,
    n_iter_no_change=10,
    random_state=42
))
mlp.fit(Xtr, ytr)
mlp_path   = "mlp_model_0.5.pkl" # trained on 0.05%

joblib.dump(mlp, mlp_path)
print("Saved models to DBFS.")


# COMMAND ----------

mlp_loaded   = joblib.load("mlp_model_0.5.pkl")
eval_model("MLP(64,32)", mlp_loaded , Xva, yva,Xte,yte)