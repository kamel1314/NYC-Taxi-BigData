from pathlib import Path
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    lit,
    unix_timestamp,
    hour,
    dayofweek,
    month,
    when
)


# ============================================================
# NYC Taxi Fare Prediction Project
# Phase 3: Preprocessing Pipeline
# ============================================================

# Define project paths
project_path = Path(__file__).resolve().parents[1]

raw_year_path = project_path / "data" / "raw" / "yellow_2024"
processed_path = project_path / "data" / "processed"
cleaned_output_path = processed_path / "cleaned_yellow_2024"

# Optional Windows Hadoop setup for Spark writing
# This is needed on some Windows machines when saving Spark output.
hadoop_home = Path(r"D:\hadoop")

if hadoop_home.exists():
    os.environ["HADOOP_HOME"] = str(hadoop_home)
    os.environ["hadoop.home.dir"] = str(hadoop_home)
    os.environ["PATH"] = str(hadoop_home / "bin") + ";" + os.environ["PATH"]


# Start Spark session
spark = SparkSession.builder \
    .appName("NYC_Taxi_Preprocessing_Pipeline") \
    .config("spark.driver.extraJavaOptions", "-Dhadoop.home.dir=D:/hadoop") \
    .config("spark.executor.extraJavaOptions", "-Dhadoop.home.dir=D:/hadoop") \
    .getOrCreate()

print("Spark session started.")


# ============================================================
# 1. Load Raw 2024 Yellow Taxi Data
# ============================================================

parquet_files = sorted(raw_year_path.glob("*.parquet"))

if not parquet_files:
    raise FileNotFoundError(f"No Parquet files found in: {raw_year_path}")

all_file_paths = [str(file).replace("\\", "/") for file in parquet_files]

taxi_df = spark.read.parquet(*all_file_paths)

raw_row_count = taxi_df.count()
raw_column_count = len(taxi_df.columns)

print("Raw dataset loaded.")
print("Raw rows:", raw_row_count)
print("Raw columns:", raw_column_count)


# ============================================================
# 2. Create Working DataFrame
# ============================================================

clean_df = taxi_df


# ============================================================
# 3. Create Trip Duration Feature
# ============================================================

clean_df = clean_df.withColumn(
    "trip_duration_minutes",
    (unix_timestamp(col("tpep_dropoff_datetime")) - unix_timestamp(col("tpep_pickup_datetime"))) / 60
)

print("trip_duration_minutes created.")


# ============================================================
# 4. Filter Invalid Timestamps and Durations
# ============================================================

rows_before_time_filter = clean_df.count()

valid_pickup_start = lit("2024-01-01 00:00:00").cast("timestamp")
valid_pickup_end = lit("2025-01-01 00:00:00").cast("timestamp")

valid_dropoff_start = lit("2024-01-01 00:00:00").cast("timestamp")
valid_dropoff_end = lit("2025-01-02 00:00:00").cast("timestamp")

clean_df = clean_df.filter(
    (col("tpep_pickup_datetime") >= valid_pickup_start) &
    (col("tpep_pickup_datetime") < valid_pickup_end) &
    (col("tpep_dropoff_datetime") >= valid_dropoff_start) &
    (col("tpep_dropoff_datetime") < valid_dropoff_end) &
    (col("trip_duration_minutes") > 0) &
    (col("trip_duration_minutes") <= 180)
)

rows_after_time_filter = clean_df.count()

print("Timestamp/duration filtering complete.")
print("Rows before:", rows_before_time_filter)
print("Rows after:", rows_after_time_filter)
print("Rows removed:", rows_before_time_filter - rows_after_time_filter)


# ============================================================
# 5. Filter Invalid Numeric Values
# ============================================================

rows_before_numeric_filter = clean_df.count()

clean_df = clean_df.filter(
    (col("passenger_count") > 0) &
    (col("trip_distance") > 0) &
    (col("fare_amount") > 0) &
    (col("total_amount") > 0)
)

rows_after_numeric_filter = clean_df.count()

print("Numeric filtering complete.")
print("Rows before:", rows_before_numeric_filter)
print("Rows after:", rows_after_numeric_filter)
print("Rows removed:", rows_before_numeric_filter - rows_after_numeric_filter)


# ============================================================
# 6. Filter Extreme Outliers
# ============================================================

rows_before_outlier_filter = clean_df.count()

clean_df = clean_df.filter(
    (col("trip_distance") <= 100) &
    (col("fare_amount") <= 500) &
    (col("total_amount") <= 600) &
    (col("trip_duration_minutes") <= 180)
)

rows_after_outlier_filter = clean_df.count()

print("Outlier filtering complete.")
print("Rows before:", rows_before_outlier_filter)
print("Rows after:", rows_after_outlier_filter)
print("Rows removed:", rows_before_outlier_filter - rows_after_outlier_filter)


# ============================================================
# 7. Feature Engineering
# ============================================================

clean_df = clean_df.withColumn("pickup_hour", hour(col("tpep_pickup_datetime")))
clean_df = clean_df.withColumn("pickup_day", dayofweek(col("tpep_pickup_datetime")))
clean_df = clean_df.withColumn("pickup_month", month(col("tpep_pickup_datetime")))

clean_df = clean_df.withColumn(
    "is_weekend",
    when(col("pickup_day").isin([1, 7]), 1).otherwise(0)
)

clean_df = clean_df.withColumn(
    "fare_per_mile",
    col("fare_amount") / col("trip_distance")
)

print("Feature engineering complete.")


# ============================================================
# 8. Save Cleaned Dataset
# ============================================================

cleaned_row_count = clean_df.count()
cleaned_column_count = len(clean_df.columns)

clean_df.write.mode("overwrite").parquet(str(cleaned_output_path).replace("\\", "/"))

print("Cleaned dataset saved.")
print("Cleaned output path:", cleaned_output_path)
print("Cleaned rows:", cleaned_row_count)
print("Cleaned columns:", cleaned_column_count)
print("Total rows removed:", raw_row_count - cleaned_row_count)


# Stop Spark
spark.stop()
print("Spark session stopped.")