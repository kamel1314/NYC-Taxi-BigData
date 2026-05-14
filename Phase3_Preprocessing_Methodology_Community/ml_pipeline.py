from pathlib import Path
import os

from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator


# ============================================================
# NYC Taxi Fare Prediction Project
# Phase 3: Spark ML Pipeline Script
# ============================================================
# This script loads the cleaned 2024 Yellow Taxi dataset,
# builds a Spark ML Pipeline (VectorAssembler + LinearRegression),
# trains and evaluates the pipeline model, and saves it to disk.
# ============================================================

# Define project paths
project_path = Path(__file__).resolve().parents[1]

processed_path = project_path / "data" / "processed"
cleaned_data_path = processed_path / "cleaned_yellow_2024"

model_results_path = project_path / "outputs" / "model_results"
model_results_path.mkdir(parents=True, exist_ok=True)

metrics_output_path = model_results_path / "linear_regression_metrics.txt"


# Optional Windows Hadoop setup for Spark
hadoop_home = Path(r"D:\hadoop")

if hadoop_home.exists():
    os.environ["HADOOP_HOME"] = str(hadoop_home)
    os.environ["hadoop.home.dir"] = str(hadoop_home)
    os.environ["PATH"] = str(hadoop_home / "bin") + ";" + os.environ["PATH"]


# Start Spark session
spark = SparkSession.builder \
    .appName("NYC_Taxi_ML_Pipeline") \
    .config("spark.driver.extraJavaOptions", "-Dhadoop.home.dir=D:/hadoop") \
    .config("spark.executor.extraJavaOptions", "-Dhadoop.home.dir=D:/hadoop") \
    .getOrCreate()

print("Spark session started.")


# ============================================================
# 1. Load Cleaned Dataset
# ============================================================

clean_df = spark.read.parquet(str(cleaned_data_path).replace("\\", "/"))

print("Cleaned dataset loaded.")
print("Rows:", clean_df.count())
print("Columns:", len(clean_df.columns))


# ============================================================
# 2. Select Features and Target
# ============================================================

model_columns = [
    "trip_distance",
    "trip_duration_minutes",
    "passenger_count",
    "pickup_hour",
    "pickup_day",
    "pickup_month",
    "is_weekend",
    "payment_type",
    "PULocationID",
    "DOLocationID",
    "fare_amount"
]

model_df = clean_df.select(model_columns)

print("Model columns selected:")
print(model_df.columns)


# ============================================================
# 3. Assemble Feature Vector
# ============================================================

feature_columns = [
    "trip_distance",
    "trip_duration_minutes",
    "passenger_count",
    "pickup_hour",
    "pickup_day",
    "pickup_month",
    "is_weekend",
    "payment_type",
    "PULocationID",
    "DOLocationID"
]

assembler = VectorAssembler(
    inputCols=feature_columns,
    outputCol="features"
)

model_ready_df = assembler.transform(model_df).select("features", "fare_amount")

print("Feature vector created successfully.")


# ============================================================
# 4. Split Data into Training and Testing Sets
# ============================================================

train_df, test_df = model_ready_df.randomSplit([0.8, 0.2], seed=42)

training_rows = train_df.count()
testing_rows = test_df.count()

print("Training rows:", training_rows)
print("Testing rows:", testing_rows)


# ============================================================
# 5. Train Linear Regression Model
# ============================================================

lr = LinearRegression(
    featuresCol="features",
    labelCol="fare_amount",
    predictionCol="prediction"
)

lr_model = lr.fit(train_df)

print("Linear Regression model trained successfully.")


# ============================================================
# 6. Generate Predictions
# ============================================================

predictions = lr_model.transform(test_df)

print("Sample predictions:")
predictions.select("fare_amount", "prediction").show(10)


# ============================================================
# 7. Evaluate Model
# ============================================================

mae_evaluator = RegressionEvaluator(
    labelCol="fare_amount",
    predictionCol="prediction",
    metricName="mae"
)

rmse_evaluator = RegressionEvaluator(
    labelCol="fare_amount",
    predictionCol="prediction",
    metricName="rmse"
)

r2_evaluator = RegressionEvaluator(
    labelCol="fare_amount",
    predictionCol="prediction",
    metricName="r2"
)

mae = mae_evaluator.evaluate(predictions)
rmse = rmse_evaluator.evaluate(predictions)
r2 = r2_evaluator.evaluate(predictions)

print("Model Evaluation Results")
print("------------------------")
print(f"MAE: {mae:.4f}")
print(f"RMSE: {rmse:.4f}")
print(f"R² Score: {r2:.4f}")


# ============================================================
# 8. Save Model Evaluation Results
# ============================================================

with open(metrics_output_path, "w", encoding="utf-8") as file:
    file.write("NYC Yellow Taxi Fare Prediction - Linear Regression Results\n")
    file.write("=" * 60 + "\n\n")
    file.write(f"Training rows: {training_rows}\n")
    file.write(f"Testing rows: {testing_rows}\n\n")
    file.write(f"MAE: {mae:.4f}\n")
    file.write(f"RMSE: {rmse:.4f}\n")
    file.write(f"R² Score: {r2:.4f}\n")

print("Model evaluation results saved to:")
print(metrics_output_path)


# Stop Spark
spark.stop()
print("Spark session stopped.")