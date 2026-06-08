# Databricks notebook source
# DBTITLE 1,1[a]
# MAGIC %sql
# MAGIC SELECT
# MAGIC   date_trunc('month', pickup_ts)     AS month_start,
# MAGIC   date_format(pickup_ts, 'yyyy-MM')  AS month_ym,
# MAGIC   date_format(pickup_ts, 'MMM yyyy') AS month_label,
# MAGIC   COUNT(*)                           AS total_trips
# MAGIC FROM workspace.bde.trips_final
# MAGIC WHERE pickup_ts IS NOT NULL
# MAGIC GROUP BY 1,2,3
# MAGIC ORDER BY month_start;

# COMMAND ----------

# DBTITLE 1,1[b]
# MAGIC %sql
# MAGIC WITH dow AS (
# MAGIC   SELECT
# MAGIC     date_trunc('month', pickup_ts)     AS month_start,
# MAGIC     date_format(pickup_ts, 'yyyy-MM')  AS month_ym,
# MAGIC     date_format(pickup_ts, 'MMM yyyy') AS month_label,
# MAGIC     date_format(pickup_ts, 'EEEE')     AS dow_name,
# MAGIC     COUNT(*)                           AS trips
# MAGIC   FROM workspace.bde.trips_final
# MAGIC   WHERE pickup_ts IS NOT NULL
# MAGIC   GROUP BY 1,2,3,4
# MAGIC )
# MAGIC SELECT
# MAGIC   month_label,
# MAGIC   month_ym,
# MAGIC   dow_name      AS top_day_of_week,
# MAGIC   trips         AS trips_on_top_dow
# MAGIC FROM (
# MAGIC   SELECT
# MAGIC     d.*,
# MAGIC     ROW_NUMBER() OVER (PARTITION BY month_start ORDER BY trips DESC, dow_name ASC) AS rn
# MAGIC   FROM dow d
# MAGIC )
# MAGIC WHERE rn = 1
# MAGIC ORDER BY month_ym;

# COMMAND ----------

# DBTITLE 1,1[c]
# MAGIC %sql
# MAGIC WITH hr AS (
# MAGIC   SELECT
# MAGIC     date_trunc('month', pickup_ts)     AS month_start,
# MAGIC     date_format(pickup_ts, 'yyyy-MM')  AS month_ym,
# MAGIC     date_format(pickup_ts, 'MMM yyyy') AS month_label,
# MAGIC     hour(pickup_ts)                    AS hour_of_day,
# MAGIC     COUNT(*)                           AS trips
# MAGIC   FROM workspace.bde.trips_final
# MAGIC   WHERE pickup_ts IS NOT NULL
# MAGIC   GROUP BY 1,2,3,4
# MAGIC )
# MAGIC SELECT
# MAGIC   month_label,
# MAGIC   month_ym,
# MAGIC   hour_of_day  AS top_hour_of_day,
# MAGIC   trips        AS trips_in_top_hour
# MAGIC FROM (
# MAGIC   SELECT
# MAGIC     h.*,
# MAGIC     ROW_NUMBER() OVER (PARTITION BY month_start ORDER BY trips DESC, hour_of_day ASC) AS rn
# MAGIC   FROM hr h
# MAGIC )
# MAGIC WHERE rn = 1
# MAGIC ORDER BY month_ym;

# COMMAND ----------

# DBTITLE 1,1[d]
# MAGIC %sql
# MAGIC SELECT
# MAGIC   date_trunc('month', pickup_ts)     AS month_start,
# MAGIC   date_format(pickup_ts, 'yyyy-MM')  AS month_ym,
# MAGIC   date_format(pickup_ts, 'MMM yyyy') AS month_label,
# MAGIC   ROUND(AVG(passenger_count), 2)     AS avg_passengers_per_trip
# MAGIC FROM workspace.bde.trips_final
# MAGIC WHERE pickup_ts IS NOT NULL
# MAGIC GROUP BY 1,2,3
# MAGIC ORDER BY month_start;
# MAGIC

# COMMAND ----------

# DBTITLE 1,1[e]
# MAGIC %sql
# MAGIC SELECT
# MAGIC   date_trunc('month', pickup_ts)     AS month_start,
# MAGIC   date_format(pickup_ts, 'yyyy-MM')  AS month_ym,
# MAGIC   date_format(pickup_ts, 'MMM yyyy') AS month_label,
# MAGIC   ROUND(AVG(total_amount), 2)        AS avg_amount_per_trip
# MAGIC FROM workspace.bde.trips_final
# MAGIC WHERE pickup_ts IS NOT NULL
# MAGIC GROUP BY 1,2,3
# MAGIC ORDER BY month_start;

# COMMAND ----------

# DBTITLE 1,1(f)
# MAGIC %sql
# MAGIC SELECT
# MAGIC   date_trunc('month', pickup_ts)      AS month_start,
# MAGIC   date_format(pickup_ts, 'yyyy-MM')   AS month_ym,
# MAGIC   date_format(pickup_ts, 'MMM yyyy')  AS month_label,
# MAGIC   ROUND(
# MAGIC     AVG(CASE WHEN passenger_count > 0 THEN total_amount / passenger_count END)
# MAGIC   , 2) AS avg_amount_per_passenger
# MAGIC FROM workspace.bde.trips_final
# MAGIC WHERE pickup_ts IS NOT NULL
# MAGIC GROUP BY 1,2,3
# MAGIC ORDER BY month_start;

# COMMAND ----------

# DBTITLE 1,2(a,b,c)
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE workspace.bde.q2_color_stats AS
# MAGIC WITH base AS (
# MAGIC   SELECT
# MAGIC     color,
# MAGIC     CAST(duration_sec / 60.0 AS DOUBLE) AS duration_min, 
# MAGIC     distance_km,
# MAGIC     speed_kmh
# MAGIC   FROM workspace.bde.trips_final
# MAGIC   WHERE duration_sec IS NOT NULL
# MAGIC     AND distance_km  IS NOT NULL
# MAGIC     AND speed_kmh    IS NOT NULL
# MAGIC )
# MAGIC SELECT
# MAGIC   color,
# MAGIC   -- Duration (minutes) with 2 decimals
# MAGIC   ROUND(AVG(duration_min), 2)                                AS avg_duration_min,
# MAGIC   ROUND(percentile_approx(duration_min, 0.5, 10000), 2)      AS median_duration_min,
# MAGIC   ROUND(MIN(duration_min), 2)                                AS min_duration_min,
# MAGIC   ROUND(MAX(duration_min), 2)                                AS max_duration_min,
# MAGIC
# MAGIC   -- Distance (km)
# MAGIC   ROUND(AVG(distance_km), 2)                                 AS avg_distance_km,
# MAGIC   ROUND(percentile_approx(distance_km, 0.5, 10000), 2)       AS median_distance_km,
# MAGIC   ROUND(MIN(distance_km), 2)                                 AS min_distance_km,
# MAGIC   ROUND(MAX(distance_km), 2)                                 AS max_distance_km,
# MAGIC
# MAGIC   -- Speed (km/h)
# MAGIC   ROUND(AVG(speed_kmh), 2)                                   AS avg_speed_kmh,
# MAGIC   ROUND(percentile_approx(speed_kmh, 0.5, 10000), 2)         AS median_speed_kmh,
# MAGIC   ROUND(MIN(speed_kmh), 2)                                   AS min_speed_kmh,
# MAGIC   ROUND(MAX(speed_kmh), 2)                                   AS max_speed_kmh
# MAGIC
# MAGIC FROM base
# MAGIC GROUP BY color
# MAGIC ORDER BY color;
# MAGIC
# MAGIC SELECT * FROM workspace.bde.q2_color_stats;

# COMMAND ----------

# DBTITLE 1,3(a,b,c)
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE workspace.bde.q3_color_borough_stats AS
# MAGIC SELECT
# MAGIC   color,
# MAGIC   pu_borough,
# MAGIC   do_borough,
# MAGIC   date_format(pickup_ts, 'yyyy-MM')  AS month_ym,
# MAGIC   date_format(pickup_ts, 'EEEE')     AS dow_name,
# MAGIC   hour(pickup_ts)                    AS hour_of_day,
# MAGIC   COUNT(*)                           AS total_trips,
# MAGIC   ROUND(AVG(distance_km), 2)         AS avg_distance_km,
# MAGIC   ROUND(AVG(total_amount), 2)        AS avg_amount_per_trip,
# MAGIC   ROUND(SUM(total_amount), 2)        AS total_amount_paid
# MAGIC FROM workspace.bde.trips_final   
# MAGIC WHERE pickup_ts IS NOT NULL
# MAGIC   AND pu_borough IS NOT NULL
# MAGIC   AND do_borough IS NOT NULL
# MAGIC GROUP BY
# MAGIC   color, pu_borough, do_borough,
# MAGIC   date_format(pickup_ts, 'yyyy-MM'),
# MAGIC   date_format(pickup_ts, 'EEEE'),
# MAGIC   hour(pickup_ts);
# MAGIC
# MAGIC SELECT * FROM workspace.bde.q3_color_borough_stats;
# MAGIC
# MAGIC -- Q4: 2024 revenue share of Top 10 borough pairs
# MAGIC WITH revenue_by_pair AS (
# MAGIC   SELECT
# MAGIC     pu_borough,
# MAGIC     do_borough,
# MAGIC     ROUND(SUM(total_amount), 2) AS total_revenue
# MAGIC   FROM workspace.bde.trips_final
# MAGIC   WHERE year(pickup_ts) = 2024
# MAGIC     AND pu_borough IS NOT NULL
# MAGIC     AND do_borough IS NOT NULL
# MAGIC   GROUP BY pu_borough, do_borough
# MAGIC ),
# MAGIC ranked AS (
# MAGIC   SELECT
# MAGIC     r.*,
# MAGIC     RANK() OVER (ORDER BY total_revenue DESC) AS rk
# MAGIC   FROM revenue_by_pair r
# MAGIC ),
# MAGIC total_rev AS (
# MAGIC   SELECT SUM(total_revenue) AS grand_total
# MAGIC   FROM revenue_by_pair
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC   pu_borough,
# MAGIC   do_borough,
# MAGIC   total_revenue,
# MAGIC   ROUND(100 * total_revenue / t.grand_total, 2) AS revenue_share_pct
# MAGIC FROM ranked r
# MAGIC CROSS JOIN total_rev t
# MAGIC WHERE rk <= 10
# MAGIC ORDER BY total_revenue DESC;

# COMMAND ----------

# DBTITLE 1,5-7
# MAGIC %sql
# MAGIC WITH base AS (
# MAGIC   SELECT
# MAGIC     *,
# MAGIC     CAST(duration_sec / 60.0 AS DOUBLE) AS duration_min
# MAGIC   FROM workspace.bde.trips_final
# MAGIC   WHERE duration_sec IS NOT NULL
# MAGIC     AND distance_km IS NOT NULL
# MAGIC     AND speed_kmh IS NOT NULL
# MAGIC     AND total_amount > 0                -- avoid div by zero for km/$
# MAGIC ),
# MAGIC tips AS (
# MAGIC   SELECT
# MAGIC     COUNT(*)                               AS total_trips,
# MAGIC     SUM(CASE WHEN tip_amount > 0 THEN 1 ELSE 0 END) AS trips_with_tips,
# MAGIC     SUM(CASE WHEN tip_amount >= 15 THEN 1 ELSE 0 END) AS trips_with_tips_15
# MAGIC   FROM base
# MAGIC ),
# MAGIC bins AS (
# MAGIC   SELECT
# MAGIC     CASE
# MAGIC       WHEN duration_min < 5 THEN 'Under 5 Mins'
# MAGIC       WHEN duration_min >= 5 AND duration_min < 10 THEN '5–10 Mins'
# MAGIC       WHEN duration_min >= 10 AND duration_min < 20 THEN '10–20 Mins'
# MAGIC       WHEN duration_min >= 20 AND duration_min < 30 THEN '20–30 Mins'
# MAGIC       WHEN duration_min >= 30 AND duration_min < 60 THEN '30–60 Mins'
# MAGIC       ELSE 'At least 60 Mins'
# MAGIC     END AS duration_bin,
# MAGIC     AVG(speed_kmh)                     AS avg_speed_kmh,
# MAGIC     AVG(distance_km / total_amount)    AS avg_km_per_dollar
# MAGIC   FROM base
# MAGIC   GROUP BY
# MAGIC     CASE
# MAGIC       WHEN duration_min < 5 THEN 'Under 5 Mins'
# MAGIC       WHEN duration_min >= 5 AND duration_min < 10 THEN '5–10 Mins'
# MAGIC       WHEN duration_min >= 10 AND duration_min < 20 THEN '10–20 Mins'
# MAGIC       WHEN duration_min >= 20 AND duration_min < 30 THEN '20–30 Mins'
# MAGIC       WHEN duration_min >= 30 AND duration_min < 60 THEN '30–60 Mins'
# MAGIC       ELSE 'At least 60 Mins'
# MAGIC     END
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC   -- overall tip stats (same for all rows)
# MAGIC   ROUND(100.0 * t.trips_with_tips / t.total_trips, 2)   AS pct_trips_with_tips,
# MAGIC   ROUND(100.0 * t.trips_with_tips_15 / NULLIF(t.trips_with_tips,0), 2) AS pct_tips_ge_15,
# MAGIC   -- bin-level metrics
# MAGIC   b.duration_bin,
# MAGIC   ROUND(b.avg_speed_kmh, 2)         AS avg_speed_kmh,
# MAGIC   ROUND(b.avg_km_per_dollar, 4)     AS avg_km_per_dollar
# MAGIC FROM bins b
# MAGIC CROSS JOIN tips t
# MAGIC ORDER BY 
# MAGIC   CASE b.duration_bin
# MAGIC     WHEN 'Under 5 Mins' THEN 1
# MAGIC     WHEN '5–10 Mins' THEN 2
# MAGIC     WHEN '10–20 Mins' THEN 3
# MAGIC     WHEN '20–30 Mins' THEN 4
# MAGIC     WHEN '30–60 Mins' THEN 5
# MAGIC     ELSE 6
# MAGIC   END;

# COMMAND ----------

# DBTITLE 1,8
# MAGIC %sql
# MAGIC WITH base AS (
# MAGIC   SELECT
# MAGIC     *,
# MAGIC     CAST(duration_sec / 60.0 AS DOUBLE) AS duration_min
# MAGIC   FROM workspace.bde.trips_final
# MAGIC   WHERE duration_sec IS NOT NULL
# MAGIC     AND total_amount IS NOT NULL
# MAGIC ),
# MAGIC bins AS (
# MAGIC   SELECT
# MAGIC     CASE
# MAGIC       WHEN duration_min < 5 THEN 'Under 5 Mins'
# MAGIC       WHEN duration_min >= 5 AND duration_min < 10 THEN '5–10 Mins'
# MAGIC       WHEN duration_min >= 10 AND duration_min < 20 THEN '10–20 Mins'
# MAGIC       WHEN duration_min >= 20 AND duration_min < 30 THEN '20–30 Mins'
# MAGIC       WHEN duration_min >= 30 AND duration_min < 60 THEN '30–60 Mins'
# MAGIC       ELSE 'At least 60 Mins'
# MAGIC     END AS duration_bin,
# MAGIC     ROUND(AVG(total_amount), 2) AS avg_revenue_per_trip,
# MAGIC     ROUND(AVG(speed_kmh), 2)    AS avg_speed_kmh,
# MAGIC     ROUND(AVG(try_divide(distance_km, total_amount)), 4) AS avg_km_per_dollar
# MAGIC   FROM base
# MAGIC   GROUP BY
# MAGIC     CASE
# MAGIC       WHEN duration_min < 5 THEN 'Under 5 Mins'
# MAGIC       WHEN duration_min >= 5 AND duration_min < 10 THEN '5–10 Mins'
# MAGIC       WHEN duration_min >= 10 AND duration_min < 20 THEN '10–20 Mins'
# MAGIC       WHEN duration_min >= 20 AND duration_min < 30 THEN '20–30 Mins'
# MAGIC       WHEN duration_min >= 30 AND duration_min < 60 THEN '30–60 Mins'
# MAGIC       ELSE 'At least 60 Mins'
# MAGIC     END
# MAGIC )
# MAGIC SELECT *
# MAGIC FROM bins
# MAGIC ORDER BY avg_revenue_per_trip DESC;
# MAGIC -- at least 60 mins 
# MAGIC