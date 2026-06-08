# Databricks notebook source
catalog = "workspace"
schema  = "bde"
volume  = "assignment2"
GREEN_DST = f"/Volumes/{catalog}/{schema}/{volume}/green"
YELLOW_DST = f"/Volumes/{catalog}/{schema}/{volume}/yellow"

# COMMAND ----------

display(dbutils.fs.ls(GREEN_DST))

# COMMAND ----------

green_df  = spark.read.parquet(GREEN_DST)
display(green_df)

# COMMAND ----------

yellow_df = spark.read.parquet(YELLOW_DST)
display(yellow_df)

# COMMAND ----------

# from pyspark.sql import functions as F
# yellow.select(F.min("pickup_ts").alias("min"), F.max("pickup_ts").alias("max")).show(truncate=False)
# green.select(F.min("pickup_ts").alias("min"),  F.max("pickup_ts").alias("max")).show(truncate=False)


# COMMAND ----------

from pyspark.sql import functions as F

yellow = (yellow_df
    .withColumn("pickup_ts",  F.col("tpep_pickup_datetime").cast("timestamp"))
    .withColumn("dropoff_ts", F.col("tpep_dropoff_datetime").cast("timestamp"))
    .drop("tpep_pickup_datetime", "tpep_dropoff_datetime")   # drop old cols
)

green = (green_df
    .withColumn("pickup_ts",  F.col("lpep_pickup_datetime").cast("timestamp"))
    .withColumn("dropoff_ts", F.col("lpep_dropoff_datetime").cast("timestamp"))
    .drop("lpep_pickup_datetime", "lpep_dropoff_datetime")   # drop old cols
)

def add_derived(df):
    return (df
        .withColumn("duration_sec", F.unix_timestamp("dropoff_ts") - F.unix_timestamp("pickup_ts"))
        .withColumn("distance_km",  F.col("trip_distance").cast("double") * F.lit(1.60934))
        .withColumn(
            "speed_kmh",
            F.when(F.col("duration_sec") > 0,
                   F.col("distance_km") / (F.col("duration_sec")/3600.0))
        )
    )
def show_progress(name, df_before, df_after):
    b, a = df_before.count(), df_after.count()
    print(f"{name}: before={b:,} after={a:,} dropped={b-a:,} ({(b-a)/b:.2%})")


# COMMAND ----------

print("Yellow taxi columns:", len(yellow_df.columns))
print("Green taxi columns :", len(green_df.columns))
print("Yellow taxi columns:", len(yellow.columns))
print("Green taxi columns :", len(green.columns))
print("\nYellow schema:")
yellow_df.printSchema()
print("\nYellow schema:")
yellow.printSchema()

print("\nGreen schema:")
green.printSchema()

# COMMAND ----------

y_before = yellow
g_before = green

yellow = yellow.filter(F.col("dropoff_ts") >= F.col("pickup_ts"))
green  = green.filter(F.col("dropoff_ts") >= F.col("pickup_ts"))

show_progress("Rule 1 (dropoff >= pickup) - yellow", y_before, yellow)
show_progress("Rule 1 (dropoff >= pickup) - green",  g_before, green)

# COMMAND ----------

yellow = add_derived(yellow)
green  = add_derived(green)

# COMMAND ----------

# def negative_speed_pct(df, name):
#     total = df.count()
#     neg   = df.filter(F.col("speed_kmh") < 0).count()
#     pct   = (neg / total) * 100 if total > 0 else 0
#     print(f"{name}: {neg:,} / {total:,} = {pct:.2f}% negative speeds")
#     return pct

# # Apply for yellow and green
# negative_speed_pct(yellow, "Yellow")
# negative_speed_pct(green,  "Green")

# COMMAND ----------

y_before = yellow
g_before = green
yellow = yellow.filter(
    (F.col("speed_kmh") >= 0) | F.col("speed_kmh").isNull() | F.isnan("speed_kmh")
)
green  = green.filter(
    (F.col("speed_kmh") >= 0) | F.col("speed_kmh").isNull() | F.isnan("speed_kmh")
)

show_progress("Yellow (drop only negatives)", y_before, yellow)
show_progress("Green  (drop only negatives)", g_before,  green)

# COMMAND ----------

# Trips where the pickup/dropoff datetime is outside of the range

DATE_MIN = "2014-01-01"
DATE_MAX = "2024-12-31"

# Inspect
viol_y = yellow.filter( (F.col("pickup_ts").isNull()) | (F.col("dropoff_ts").isNull()) |
                        (F.col("pickup_ts") < F.to_timestamp(F.lit(DATE_MIN))) |
                        (F.col("pickup_ts") > F.to_timestamp(F.lit(DATE_MAX))) )
viol_g = green.filter(  (F.col("pickup_ts").isNull()) | (F.col("dropoff_ts").isNull()) |
                        (F.col("pickup_ts") < F.to_timestamp(F.lit(DATE_MIN))) |
                        (F.col("pickup_ts") > F.to_timestamp(F.lit(DATE_MAX))) )
display(viol_y.select("pickup_ts","dropoff_ts").limit(10))
display(viol_g.select("pickup_ts","dropoff_ts").limit(10))

# Filter
y_before = yellow
g_before = green

yellow = (yellow
    .filter(F.col("pickup_ts").isNotNull() & F.col("dropoff_ts").isNotNull())
    .filter((F.col("pickup_ts") >= F.to_timestamp(F.lit(DATE_MIN))) &
            (F.col("pickup_ts") <= F.to_timestamp(F.lit(DATE_MAX))))
)
green = (green
    .filter(F.col("pickup_ts").isNotNull() & F.col("dropoff_ts").isNotNull())
    .filter((F.col("pickup_ts") >= F.to_timestamp(F.lit(DATE_MIN))) &
            (F.col("pickup_ts") <= F.to_timestamp(F.lit(DATE_MAX))))
)

show_progress("Rule 2 (date range) - yellow", y_before, yellow)
show_progress("Rule 2 (date range) - green",  g_before, green)


# COMMAND ----------

# # Trips with negative speed
# # Inspect
# display(yellow.filter(F.col("speed_kmh") < 0).select("pickup_ts","dropoff_ts","distance_km","duration_sec","speed_kmh").limit(10))
# display(green.filter(F.col("speed_kmh") < 0).select("pickup_ts","dropoff_ts","distance_km","duration_sec","speed_kmh").limit(10))

# # Filter
# y_before = yellow
# g_before = green

# yellow = yellow.filter((F.col("speed_kmh").isNull()) | (F.col("speed_kmh") >= 0))
# green  = green.filter((F.col("speed_kmh").isNull()) | (F.col("speed_kmh") >= 0))

# show_progress("Rule 3 (no negative speed) - yellow", y_before, yellow)
# show_progress("Rule 3 (no negative speed) - green",  g_before, green)


# COMMAND ----------

# Trips with very high speed (look for NYC and outside of NYC speed limit )
MAX_SPEED = 150.0

# Inspect offenders
display(yellow.filter(F.col("speed_kmh") > MAX_SPEED).select("speed_kmh","distance_km","duration_sec").orderBy(F.col("speed_kmh").desc()).limit(10))
display(green.filter(F.col("speed_kmh") > MAX_SPEED).select("speed_kmh","distance_km","duration_sec").orderBy(F.col("speed_kmh").desc()).limit(10))

# Filter
y_before = yellow
g_before = green

yellow = yellow.filter((F.col("speed_kmh").isNull()) | (F.col("speed_kmh") <= MAX_SPEED))
green  = green.filter((F.col("speed_kmh").isNull()) | (F.col("speed_kmh") <= MAX_SPEED))

show_progress("Rule 4 (max speed) - yellow", y_before, yellow)
show_progress("Rule 4 (max speed) - green",  g_before, green)


# COMMAND ----------

# Trips that are travelling too short or too long (duration wise)

MIN_DURATION_SEC = 60
MAX_DURATION_SEC = 6*3600

# Inspect
display(yellow.filter( (F.col("duration_sec") < MIN_DURATION_SEC) | (F.col("duration_sec") > MAX_DURATION_SEC) )
        .select("duration_sec","pickup_ts","dropoff_ts").limit(10))
display(green.filter(  (F.col("duration_sec") < MIN_DURATION_SEC) | (F.col("duration_sec") > MAX_DURATION_SEC) )
        .select("duration_sec","pickup_ts","dropoff_ts").limit(10))

# Filter
y_before = yellow
g_before = green

yellow = yellow.filter( (F.col("duration_sec") >= MIN_DURATION_SEC) & (F.col("duration_sec") <= MAX_DURATION_SEC) )
green  = green.filter( (F.col("duration_sec") >= MIN_DURATION_SEC) & (F.col("duration_sec") <= MAX_DURATION_SEC) )

show_progress("Rule 5 (duration bounds) - yellow", y_before, yellow)
show_progress("Rule 5 (duration bounds) - green",  g_before, green)


# COMMAND ----------

# Trips that are travelling too short or too long (distance wise)
MIN_DIST_KM = 0.2
MAX_DIST_KM = 200.0

# Inspect
display(yellow.filter( (F.col("distance_km") < MIN_DIST_KM) | (F.col("distance_km") > MAX_DIST_KM) )
        .select("distance_km","duration_sec","speed_kmh").limit(10))
display(green.filter(  (F.col("distance_km") < MIN_DIST_KM) | (F.col("distance_km") > MAX_DIST_KM) )
        .select("distance_km","duration_sec","speed_kmh").limit(10))

# Filter
y_before = yellow
g_before = green

yellow = yellow.filter( (F.col("distance_km") >= MIN_DIST_KM) & (F.col("distance_km") <= MAX_DIST_KM) )
green  = green.filter( (F.col("distance_km") >= MIN_DIST_KM) & (F.col("distance_km") <= MAX_DIST_KM) )

show_progress("Rule 6 (distance bounds) - yellow", y_before, yellow)
show_progress("Rule 6 (distance bounds) - green",  g_before, green)


# COMMAND ----------

MIN_FARE = 3.3

# Keep trips with realistic fare OR if they are in special categories
yellow_before = yellow
green_before  = green

yellow = yellow.filter(
    (F.col("total_amount") >= MIN_FARE) |
    (F.col("payment_type").isin([3,4,6]))
)

green = green.filter(
    (F.col("total_amount") >= MIN_FARE) |
    (F.col("payment_type").isin([3,4,6]))
)

# Show progress
show_progress("Rule (min fare unless cancelled/voided) - yellow", yellow_before, yellow)
show_progress("Rule (min fare unless cancelled/voided) - green",  green_before, green)
# 

# COMMAND ----------

# Overall drop %
def overall_drop(original_df, cleaned_df, label):
    b, a = original_df.count(), cleaned_df.count() 
    print(f"{label} total drop: {(b-a)/b:.2%}  (before={b:,}, after={a:,})")

overall_drop(add_derived(yellow_df
    .withColumn("pickup_ts", F.col("tpep_pickup_datetime").cast("timestamp"))
    .withColumn("dropoff_ts", F.col("tpep_dropoff_datetime").cast("timestamp"))), yellow, "YELLOW")

overall_drop(add_derived(green_df
    .withColumn("pickup_ts", F.col("lpep_pickup_datetime").cast("timestamp"))
    .withColumn("dropoff_ts", F.col("lpep_dropoff_datetime").cast("timestamp"))), green, "GREEN")

# Persist for Part 2
yellow.write.mode("overwrite").saveAsTable("workspace.bde.yellow_clean")
green.write.mode("overwrite").saveAsTable("workspace.bde.green_clean")


# COMMAND ----------

yellow_clean = spark.table("workspace.bde.yellow_clean")
green_clean  = spark.table("workspace.bde.green_clean")
yellow_clean.count()

# COMMAND ----------

# Count the total number of rows for both green and yellow taxis.
green_count  = green_clean.count()
yellow_count = yellow_clean.count()

print(f"Green rows : {green_count:,}")
print(f"Yellow rows: {yellow_count:,}")
print(f"Total rows : {green_count + yellow_count:,}")


# COMMAND ----------

print("Yellow taxi columns:", len(yellow_df.columns))
print("Green taxi columns :", len(green_df.columns))
print("Yellow new taxi columns:", len(yellow_clean.columns))
print("Green new taxi columns :", len(green_clean.columns))
print("\nYellow schema:")
yellow_clean.printSchema()

print("\nGreen schema:")
green_clean.printSchema()

# COMMAND ----------


# Yellow: add ehail_fee and trip_type with 0
yellow_aligned = (yellow_clean
    .withColumn("ehail_fee", F.lit(0.0).cast("double"))
    .withColumn("trip_type", F.lit(0.0).cast("double"))
    .withColumn("color", F.lit("yellow"))
)

# Green: add airport_fee with 0
green_aligned = (green_clean
    .withColumn("airport_fee", F.lit(0.0).cast("double"))
    .withColumn("color", F.lit("green"))
)

# Union
trips_union = yellow_aligned.unionByName(green_aligned, allowMissingColumns=True)

# Check result
print("Yellow rows:", yellow_aligned.count())
print("Green rows :", green_aligned.count())
print("Union rows :", trips_union.count())
print("Columns    :", len(trips_union.columns))

trips_union.printSchema()
display(trips_union.limit(5))
display(trips_union.count())

# COMMAND ----------

display(trips_union.count())

# COMMAND ----------



# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql import types as T

# --- 1) Load the location CSV (adjust path) ---
ZONES_PATH = "/Volumes/workspace/bde/assignment2/taxi_zone_lookup.csv"  # <-- change if needed

zones = (spark.read
    .option("header", "true")
    .csv(ZONES_PATH)
    # normalize types & trim text
    .withColumn("LocationID", F.col("LocationID").cast(T.IntegerType()))
    .withColumn("Borough", F.trim(F.col("Borough")))
    .withColumn("Zone", F.trim(F.col("Zone")))
    .withColumn("service_zone", F.trim(F.col("service_zone")))
)

# --- 2) Prepare two aliases for pickup & dropoff joins ---
pu_zones = (zones
    .withColumnRenamed("LocationID", "pu_id")
    .withColumnRenamed("Borough", "pu_borough")
    .withColumnRenamed("Zone", "pu_zone")
    .withColumnRenamed("service_zone", "pu_service_zone")
)

do_zones = (zones
    .withColumnRenamed("LocationID", "do_id")
    .withColumnRenamed("Borough", "do_borough")
    .withColumnRenamed("Zone", "do_zone")
    .withColumnRenamed("service_zone", "do_service_zone")
)

# COMMAND ----------

# --- 3) Join twice (LEFT joins keep all trips) ---
# zones is tiny (~260 rows), so broadcast it for speed
trips_enriched = (trips_union
    .join(F.broadcast(pu_zones),  F.col("PULocationID").cast("int") == F.col("pu_id"), "left")
    .drop("pu_id")
    .join(F.broadcast(do_zones),  F.col("DOLocationID").cast("int") == F.col("do_id"), "left")
    .drop("do_id")
)

# COMMAND ----------

trips_enriched.count()

# COMMAND ----------

# --- 4) Quick QA: how many trips didn’t match a zone? ---
unmatched = (trips_enriched
    .select(
        F.sum(F.when(F.col("pu_borough").isNull(), 1).otherwise(0)).alias("missing_pu"),
        F.sum(F.when(F.col("do_borough").isNull(), 1).otherwise(0)).alias("missing_do")
    )
)
display(unmatched)

# COMMAND ----------

# Peek a few unmatched to investigate (should be near zero)
display(trips_enriched.filter(F.col("pu_borough").isNull()).select("PULocationID").distinct().limit(10))
display(trips_enriched.filter(F.col("do_borough").isNull()).select("DOLocationID").distinct().limit(10))

# COMMAND ----------

# --- 5) (Optional) Persist as a table for Part 2 SQL ---
(trips_enriched
 .write
 .format("delta")
 .mode("overwrite")
 .option("overwriteSchema", "true")   # allow schema change
 .saveAsTable("workspace.bde.trips_final"))

# Final count
print("Final rows:", trips_enriched.count())

# COMMAND ----------

display(trips_enriched.limit(5))

# COMMAND ----------

df = spark.table("workspace.bde.trips_final")

neg_count = df.filter(F.col("speed_kmh") < 0).count()
total     = df.count()

print(f"Negative speeds: {neg_count:,} / {total:,} = {(neg_count/total)*100:.4f}%")

# COMMAND ----------

# MAGIC %%sql
# MAGIC SHOW TABLES IN workspace.bde;
# MAGIC
# MAGIC SELECT COUNT(*) FROM workspace.bde.trips_final;
# MAGIC SELECT * FROM workspace.bde.trips_final LIMIT 10;
# MAGIC