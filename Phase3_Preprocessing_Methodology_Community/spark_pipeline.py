from pathlib import Path
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count
from pyspark.sql.window import Window
from pyspark.sql.functions import rank, lag, lead


# ============================================================
# NYC Taxi Fare Prediction Project
# Phase 3: Spark Pipeline Script
# SparkSQL + Window Functions Analysis
# ============================================================
# This script loads the cleaned 2024 Yellow Taxi dataset,
# demonstrates SparkSQL queries using createOrReplaceTempView,
# and applies Window Functions (rank, lag, lead) for analysis.
# ============================================================

# Define project paths
project_path = Path(__file__).resolve().parents[1]

cleaned_data_path = project_path / "data" / "processed" / "cleaned_yellow_2024"

# Optional Windows Hadoop setup for Spark writing
hadoop_home = Path(r"C:\hadoop")
if hadoop_home.exists():
    os.environ["HADOOP_HOME"] = str(hadoop_home)
    os.environ["PATH"] = str(hadoop_home / "bin") + ";" + os.environ.get("PATH", "")


# ============================================================
# 1. Start Spark Session
# ============================================================

spark = SparkSession.builder \
    .appName("NYC_Taxi_SparkSQL_WindowFunctions") \
    .config("spark.driver.memory", "4g") \
    .config("spark.sql.shuffle.partitions", "8") \
    .getOrCreate()

print("Spark session started.")
print("Spark version:", spark.version)


# ============================================================
# 2. Load Cleaned Dataset
# ============================================================

if not cleaned_data_path.exists():
    raise FileNotFoundError(
        f"Cleaned dataset not found at: {cleaned_data_path}\n"
        "Please run preprocessing_pipeline.py first."
    )

clean_df = spark.read.parquet(str(cleaned_data_path).replace("\\", "/"))

print("Cleaned dataset loaded.")
print("Rows:", clean_df.count())
print("Columns:", len(clean_df.columns))


# ============================================================
# 3. SparkSQL Analysis
# ============================================================
# Register the cleaned DataFrame as a temporary SQL view
# so we can query it using standard SQL syntax.

clean_df.createOrReplaceTempView("taxi_trips")

print("\nTemporary SQL view 'taxi_trips' created.")


# SQL Query 1: Trip count and average fare by pickup hour
print("\n[SQL Query 1] Trip count and average fare by pickup hour:")
sql_trips_by_hour = spark.sql("""
    SELECT
        pickup_hour,
        COUNT(*)                   AS trip_count,
        ROUND(AVG(fare_amount), 2) AS avg_fare
    FROM taxi_trips
    GROUP BY pickup_hour
    ORDER BY pickup_hour
""")
sql_trips_by_hour.show(24)


# SQL Query 2: Top 10 busiest pickup locations
print("\n[SQL Query 2] Top 10 busiest pickup locations:")
sql_top_locations = spark.sql("""
    SELECT
        PULocationID,
        COUNT(*)                   AS trip_count,
        ROUND(AVG(fare_amount), 2) AS avg_fare
    FROM taxi_trips
    GROUP BY PULocationID
    ORDER BY trip_count DESC
    LIMIT 10
""")
sql_top_locations.show()


# SQL Query 3: Fare statistics by payment type
print("\n[SQL Query 3] Fare statistics by payment type:")
sql_fare_by_payment = spark.sql("""
    SELECT
        payment_type,
        COUNT(*)                   AS trip_count,
        ROUND(AVG(fare_amount), 2) AS avg_fare,
        ROUND(MIN(fare_amount), 2) AS min_fare,
        ROUND(MAX(fare_amount), 2) AS max_fare
    FROM taxi_trips
    GROUP BY payment_type
    ORDER BY payment_type
""")
sql_fare_by_payment.show()


# SQL Query 4: Monthly trip statistics
print("\n[SQL Query 4] Monthly trip statistics:")
sql_monthly_stats = spark.sql("""
    SELECT
        pickup_month,
        COUNT(*)                          AS trip_count,
        ROUND(AVG(fare_amount),         2) AS avg_fare,
        ROUND(AVG(trip_distance),       2) AS avg_distance,
        ROUND(AVG(trip_duration_minutes), 2) AS avg_duration_min
    FROM taxi_trips
    GROUP BY pickup_month
    ORDER BY pickup_month
""")
sql_monthly_stats.show()


# ============================================================
# 4. Window Functions Analysis
# ============================================================
# Aggregate average fare and trip count per month per hour,
# then apply rank(), lag(), and lead() window functions.

print("\n[Window Functions] Aggregating hourly monthly stats...")
hourly_monthly_stats = clean_df.groupBy("pickup_month", "pickup_hour") \
    .agg(
        count("*").alias("trip_count"),
        avg("fare_amount").alias("avg_fare")
    ) \
    .orderBy("pickup_month", "pickup_hour")


# Define windows
window_by_count = Window.partitionBy("pickup_month").orderBy(col("trip_count").desc())
window_by_hour  = Window.partitionBy("pickup_month").orderBy(col("pickup_hour").asc())

# Apply rank(), lag(), lead()
hourly_window_df = hourly_monthly_stats \
    .withColumn("rank_by_trips",      rank().over(window_by_count)) \
    .withColumn("prev_hour_avg_fare", lag("avg_fare",  1).over(window_by_hour)) \
    .withColumn("next_hour_avg_fare", lead("avg_fare", 1).over(window_by_hour)) \
    .withColumn(
        "fare_change_from_prev",
        col("avg_fare") - col("prev_hour_avg_fare")
    )

print("\n[Window Functions] Results for January (month=1):")
hourly_window_df.filter(col("pickup_month") == 1).orderBy("pickup_hour").show(24)


# Busiest hour per month
print("\n[Window Functions] Busiest hour per month (rank = 1 by trip count):")
busiest_hour_per_month = hourly_window_df \
    .filter(col("rank_by_trips") == 1) \
    .select("pickup_month", "pickup_hour", "trip_count", "avg_fare") \
    .orderBy("pickup_month")

busiest_hour_per_month.show()


# ============================================================
# 5. Stop Spark
# ============================================================

spark.stop()
print("\nSpark session stopped.")
