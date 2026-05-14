# System Architecture — NYC Taxi Fare Prediction

This document describes the complete system architecture used in the project, including the original local workflow and the full Big Data stack that was added to meet course requirements.

---

## 1. Architecture Overview

The project went through two stages of architecture:

### Original Workflow (Phase 1–3 initial implementation)

```
NYC TLC Website  (manual browser download)
        ↓
Local Windows Folder  (data/raw/yellow_2024/)
        ↓
Apache Spark — DataFrame Loading
        ↓
Schema Validation + Data Quality Inspection
        ↓
Spark DataFrame API — Preprocessing & Feature Engineering
        ↓
SparkSQL + Window Functions — Analysis
        ↓
Matplotlib Charts  (from aggregated Spark output)
        ↓
Spark MLlib — VectorAssembler + LinearRegression
        ↓
Spark ML Pipeline — fit + save
        ↓
Local outputs/  (figures, metrics, model)
```

**What was wrong with this:** data was downloaded manually (no ingestion script), stored only in a local folder (no HDFS), and visualized with Matplotlib (not on the rubric). Three of the six required Big Data categories were uncovered.

---

### Full Workflow (Final Architecture)

```
NYC TLC CloudFront API
        ↓  data_ingestion.py  (API Integration)
Local Folder  data/raw/yellow_2024/  (intermediate staging)
        ↓  hdfs_setup.py
HDFS  hdfs://namenode:9000/taxi/raw/  (Big Data Storage)
        ↓
Apache Spark 4.1.1 — DataFrame Loading from HDFS
        ↓
Schema Validation + Data Quality Inspection
        ↓
Spark DataFrame API — Preprocessing (4 steps, 5.5M rows removed)
        ↓
Feature Engineering  (6 new columns)
        ↓
SparkSQL  (4 queries via createOrReplaceTempView)
        ↓
Window Functions  (rank, lag, lead — partitioned by month)
        ↓
Spark MLlib — VectorAssembler + LinearRegression + ML Pipeline
        ↓
Model Evaluation  (MAE=2.16, RMSE=5.93, R²=0.89)
        ↓
Apache Zeppelin 0.10.1  (interactive visualization dashboard)
        ↓
outputs/  (figures, metrics, saved Pipeline model)
```

This matches the course reference stack:
```
API → Local Staging → HDFS → Spark → MLlib → Zeppelin
```

---

## 2. Technology Stack

| Category | Component | Tool / Version |
|---|---|---|
| Data ingestion | API Integration script | `data_ingestion.py` → NYC TLC CloudFront |
| Data format | Columnar file format | Apache Parquet |
| Staging storage | Local intermediate folder | `data/raw/yellow_2024/` (Windows) |
| Distributed storage | HDFS via Docker | Hadoop 3.2.1 — `hdfs://namenode:9000/taxi/raw/` |
| Cluster management | Docker containers | `docker-hadoop-spark-jupyter` |
| Big Data framework | Distributed processing | Apache Spark / PySpark 4.1.1 |
| Processing API | DataFrame transformations | Spark DataFrame API |
| SQL queries | SQL analytics on DataFrames | SparkSQL (`createOrReplaceTempView` + `spark.sql`) |
| Window analysis | Partitioned ranking and lag | `pyspark.sql.window.Window` |
| ML assembler | Feature vector creation | `pyspark.ml.feature.VectorAssembler` |
| ML model | Regression | `pyspark.ml.regression.LinearRegression` |
| ML pipeline | End-to-end training object | `pyspark.ml.Pipeline` |
| Evaluation | Regression metrics | `pyspark.ml.evaluation.RegressionEvaluator` |
| Visualization | Interactive dashboard | Apache Zeppelin 0.10.1 |
| Secondary viz | Static charts | Matplotlib (aggregated Spark output) |
| Runtime | Python environment | Python 3.11, Java 17 (OpenJDK 17.0.19) |
| Development | Notebooks and scripts | Jupyter Notebook / VS Code |
| OS | Local machine | Windows |
| Hadoop helper | Spark write support on Windows | winutils.exe + hadoop.dll (`C:\hadoop\bin`) |

---

## 3. Data Ingestion Layer

**Script:** `Phase3_Preprocessing_Methodology_Community/data_ingestion.py`  
**Category:** API Integration

The NYC TLC publishes monthly Parquet files through a CloudFront CDN endpoint. `data_ingestion.py` downloads all 12 files programmatically using HTTP requests:

```python
BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"

for month in range(1, 13):
    filename = f"yellow_tripdata_2024-{month:02d}.parquet"
    url = f"{BASE_URL}/{filename}"
    response = requests.get(url, stream=True, timeout=180)
    # saves to data/raw/yellow_2024/
```

Files already downloaded are skipped. This replaces the original approach of manually downloading each file from a browser.

---

## 4. Storage Layer

### Local Staging (intermediate)

Raw files are kept unchanged in a local folder before being loaded into HDFS:

```
data/raw/
├── yellow_tripdata_2024-01.parquet      (Phase 1 inspection copy)
└── yellow_2024/
    ├── yellow_tripdata_2024-01.parquet
    ├── yellow_tripdata_2024-02.parquet
    ├── ...
    └── yellow_tripdata_2024-12.parquet
```

**Why local staging exists:** HDFS runs inside Docker containers. The local folder is the permanent source used to populate HDFS. If Docker volumes are lost, the local files allow re-uploading without re-downloading.

### HDFS (Big Data Storage)

**Script:** `Phase3_Preprocessing_Methodology_Community/hdfs_setup.py`  
**Cluster:** `docker-hadoop-spark-jupyter` (Hadoop 3.2.1)

```
hdfs://namenode:9000/
├── taxi/raw/           12 raw monthly Parquet files
└── taxi/processed/     cleaned Parquet output (35,602,215 rows)
```

HDFS setup commands:
```bash
docker exec namenode hdfs dfs -mkdir -p /taxi/raw
docker exec namenode hdfs dfs -put yellow_tripdata_2024-*.parquet /taxi/raw/
```

Spark reads from HDFS instead of the local folder:
```python
df = spark.read.parquet("hdfs://namenode:9000/taxi/raw")
```

**HDFS Web UI:** http://localhost:9870  
**YARN Resource Manager:** http://localhost:8088

---

## 5. Spark Processing Layer

**Spark version:** 4.1.1 | **Java version:** OpenJDK 17.0.19

Spark reads all 12 Parquet files from HDFS as a single unified DataFrame and handles all processing:

- Loading 41,169,720 rows as one DataFrame
- Schema validation and data quality inspection
- All preprocessing via the Spark DataFrame API
- Feature engineering (6 new columns)
- SparkSQL queries via temporary views
- Window Function analysis (rank, lag, lead)
- EDA aggregations (groupBy, count, avg)
- Feature vector assembly with VectorAssembler
- Linear Regression training on 28.4M rows
- ML Pipeline wrapping assembler + model
- Saving cleaned output and fitted Pipeline model

**Pandas** was used only to convert small aggregated results (<24 rows) for Matplotlib. All heavy processing used Spark.

---

## 6. Preprocessing Layer — Actual Results

| Step | Rule | Rows Before | Rows After | Removed |
|---|---|---:|---:|---:|
| Trip duration feature | Created `trip_duration_minutes` | 41,169,720 | 41,169,720 | 0 |
| Timestamp filter | Pickup/drop-off within 2024, duration 0–180 min | 41,169,720 | 41,130,827 | 38,893 |
| Numeric filter | `passenger_count`, `trip_distance`, `fare_amount`, `total_amount` all > 0 | 41,130,827 | 35,602,932 | 5,527,895 |
| Outlier filter | Distance ≤100mi, fare ≤$500, total ≤$600 | 35,602,932 | 35,602,215 | 717 |

**After preprocessing:** 0 missing values in all 25 columns.

---

## 7. Feature Engineering Layer

| Feature | Formula / Method |
|---|---|
| `trip_duration_minutes` | `(unix_timestamp(dropoff) - unix_timestamp(pickup)) / 60` |
| `pickup_hour` | `hour(tpep_pickup_datetime)` |
| `pickup_day` | `dayofweek(tpep_pickup_datetime)` (1=Sun, 7=Sat) |
| `pickup_month` | `month(tpep_pickup_datetime)` |
| `is_weekend` | `when(pickup_day.isin([1, 7]), 1).otherwise(0)` |
| `fare_per_mile` | `fare_amount / trip_distance` — EDA only, not model input |

---

## 8. SparkSQL and Window Functions Layer

### SparkSQL

```python
clean_df.createOrReplaceTempView("taxi_trips")

spark.sql("SELECT pickup_hour, COUNT(*), ROUND(AVG(fare_amount),2) FROM taxi_trips GROUP BY pickup_hour ORDER BY pickup_hour")
spark.sql("SELECT PULocationID, COUNT(*) FROM taxi_trips GROUP BY PULocationID ORDER BY COUNT(*) DESC LIMIT 10")
spark.sql("SELECT payment_type, COUNT(*), ROUND(AVG(fare_amount),2), MIN(fare_amount), MAX(fare_amount) FROM taxi_trips GROUP BY payment_type")
spark.sql("SELECT pickup_month, COUNT(*), ROUND(AVG(fare_amount),2), ROUND(AVG(trip_distance),2) FROM taxi_trips GROUP BY pickup_month ORDER BY pickup_month")
```

### Window Functions

```python
from pyspark.sql.window import Window
from pyspark.sql.functions import rank, lag, lead

window_by_count = Window.partitionBy("pickup_month").orderBy(col("trip_count").desc())
window_by_hour  = Window.partitionBy("pickup_month").orderBy(col("pickup_hour").asc())

hourly_df \
    .withColumn("rank_by_trips",      rank().over(window_by_count)) \
    .withColumn("prev_hour_avg_fare", lag("avg_fare", 1).over(window_by_hour)) \
    .withColumn("next_hour_avg_fare", lead("avg_fare", 1).over(window_by_hour))
```

---

## 9. Machine Learning Layer

**Framework:** Spark MLlib

**Input features:**
```
trip_distance, trip_duration_minutes, passenger_count,
pickup_hour, pickup_day, pickup_month, is_weekend,
payment_type, PULocationID, DOLocationID
```

**Pipeline:**
```python
assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
lr = LinearRegression(featuresCol="features", labelCol="fare_amount")
pipeline = Pipeline(stages=[assembler, lr])
pipeline_model = pipeline.fit(train_df)
```

**Train/test split:** 80/20, seed=42

| Set | Rows |
|---|---:|
| Training | 28,480,061 |
| Testing | 7,122,154 |

**Model evaluation:**

| Metric | Value |
|---|---|
| MAE | 2.1607 |
| RMSE | 5.9255 |
| R² Score | 0.8934 |

---

## 10. Visualization Layer

### Original (Matplotlib)
Static PNG charts generated from aggregated Spark output. 6 charts saved to `outputs/figures/`. Matplotlib is not a Big Data visualization tool and does not appear on the course rubric.

### Added (Apache Zeppelin 0.10.1)
Interactive dashboard running inside the Docker cluster alongside HDFS and Spark.

**URL:** http://localhost:8090  
**Notebook:** NYC Taxi EDA — Zeppelin Dashboard

| Paragraph | Chart type | What it shows |
|---|---|---|
| Trips by pickup hour | Bar chart | Demand peaks at 6 PM, drops at 4 AM |
| Monthly trip volume | Bar chart | September has highest fares ($20.72 avg) |
| Payment type breakdown | Pie chart | Credit card dominates (~68%) |
| Top 10 pickup locations | Bar chart | Location IDs 132, 161, 237 are busiest |
| Weekend vs weekday | Bar chart | Weekdays have more trips; weekends higher fares |

Zeppelin reads directly from HDFS at `hdfs://namenode:9000/taxi/raw/` and uses PySpark (`%pyspark` interpreter) for all aggregations.

---

## 11. Output Layer

| Output | Location |
|---|---|
| Cleaned dataset (local) | `data/processed/cleaned_yellow_2024/` |
| Cleaned dataset (HDFS) | `hdfs://namenode:9000/taxi/processed/` |
| Raw quality report | `outputs/raw_data_quality_summary.txt` |
| 6 Matplotlib EDA charts | `outputs/figures/*.png` |
| Zeppelin dashboard | http://localhost:8090 |
| Model metrics | `outputs/model_results/linear_regression_metrics.txt` |
| Fitted Pipeline model | `outputs/model_results/lr_pipeline_model/` |

---

## 12. Rubric Coverage

| Category | Tool Used |
|---|---|
| Core Big Data Platform | Apache Spark / PySpark 4.1.1 |
| Storage / Data Management | HDFS (Hadoop 3.2.1 via Docker) |
| Data Ingestion | API Integration (`data_ingestion.py`) |
| Processing Framework | Spark DataFrame API |
| Machine Learning | Spark MLlib + ML Pipeline |
| Visualization / Reporting | Apache Zeppelin 0.10.1 |
