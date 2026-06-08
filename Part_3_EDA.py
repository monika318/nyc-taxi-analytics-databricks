# Databricks notebook source
# EDA
import seaborn as sns
import matplotlib.pyplot as plt
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from pyspark.ml.feature import StringIndexer, VectorAssembler, QuantileDiscretizer
from pyspark.ml.stat import ChiSquareTest

# COMMAND ----------

spark.table("workspace.bde.q3_color_borough_stats").printSchema()

# COMMAND ----------

spark.table("workspace.bde.trips_final").printSchema()


# COMMAND ----------

cols = spark.table("workspace.bde.trips_final").columns
print(cols)

# COMMAND ----------

schema_df = spark.table("workspace.bde.trips_final").dtypes
for name, dtype in schema_df:
    print(f"{name:25} {dtype}")

# COMMAND ----------

# DBTITLE 1,Load Trips Final Table Into DataFrame
trips_df = spark.table("workspace.bde.trips_final")
eda_df = trips_df.select("*")

# COMMAND ----------

eda_df.describe().show()


# COMMAND ----------

# DBTITLE 1,Identify and List All Numeric Columns in Dataframe
from pyspark.sql.types import NumericType

numeric_cols = [f.name for f in eda_df.schema.fields if isinstance(f.dataType, NumericType)]
print("Numeric columns:", numeric_cols)

# COMMAND ----------

# DBTITLE 1,Numeric Feature Correlations With Total Amount
target = "total_amount"

for col in numeric_cols:
    if col != target and col !="Fare_amount" and col!="Tolls_amount":
        corr = eda_df.stat.corr(col, target)
        print(f"Correlation between {col} and {target}: {corr}")

# COMMAND ----------

from pyspark.sql import Row

target = "total_amount"
correlations = []

for col in numeric_cols:
    if col != target and col !="fare_amount" and col!="tolls_amount":
        correlations.append(Row(column=col, correlation=eda_df.stat.corr(col, target)))

corr_df = spark.createDataFrame(correlations)

# COMMAND ----------

corr_pd = corr_df.toPandas()
top_corr = corr_pd.reindex(corr_pd.correlation.abs().sort_values(ascending=False).index)

# COMMAND ----------

plt.figure(figsize=(8,6))
sns.barplot(data=top_corr, x="correlation", y="column", palette="coolwarm")
plt.title("Top correlations with total_amount")
plt.show()


# COMMAND ----------

import matplotlib.pyplot as plt
eda_pdf["total_amount"].hist(bins=100, range=(0,200))  # cap at 200 for clarity
plt.title("Distribution of Total Amount")
plt.xlabel("Total Amount ($)")
plt.ylabel("Frequency")
plt.show()

# COMMAND ----------

eda_pdf.describe()

# COMMAND ----------

eda_pdf[["passenger_count","distance_km","duration_sec","speed_kmh","total_amount","tip_amount"]].corr()

# COMMAND ----------

cat_cols = ["color","pu_borough","do_borough","payment_type","day_of_week"]

for col in cat_cols:
    print(f"\n--- {col} ---")
    print(eda_pdf[col].value_counts(normalize=True).round(3).head(10))