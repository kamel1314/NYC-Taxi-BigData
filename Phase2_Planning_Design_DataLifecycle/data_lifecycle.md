# Data Lifecycle — NYC Taxi Fare Prediction

This document describes how data actually moved through the project from start to finish. All numbers below are real results from executed code, not estimates.

---

## Note on Workflow Evolution

The project initially used a local-only workflow. After reviewing the course rubric, three gaps were identified and corrected:

| Gap | Original (wrong) | Final (correct) |
|---|---|---|
| Data ingestion | Manual browser download | `data_ingestion.py` — API Integration |
| Storage | Local Windows folder only | HDFS via Docker (Hadoop 3.2.1) |
| Visualization | Matplotlib static charts | Apache Zeppelin 0.10.1 dashboard |

The stages below describe the **final complete workflow**.

---

## Stage 1 — Data Acquisition (API Integration)

**Script:** `Phase3_Preprocessing_Methodology_Community/data_ingestion.py`

The original approach downloaded files manually from a browser — not appropriate for a Big Data pipeline. This was replaced with a programmatic API integration script that downloads all 12 files from the NYC TLC CloudFront endpoint:

```python
BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
for month in range(1, 13):
    filename = f"yellow_tripdata_2024-{month:02d}.parquet"
    response = requests.get(f"{BASE_URL}/{filename}", stream=True)
    # saves to data/raw/yellow_2024/
```

**Result:**
- 12 monthly Parquet files downloaded programmatically
- Total raw size: **660.90 MB**
- Satisfies the 500 MB+ course requirement

---

## Stage 2 — Local Staging + HDFS Storage

### Local Staging (intermediate)

After download, raw files are saved locally unchanged:
```
data/raw/yellow_2024/
├── yellow_tripdata_2024-01.parquet   (47.6 MB)
├── yellow_tripdata_2024-02.parquet   (48.0 MB)
├── ...
└── yellow_tripdata_2024-12.parquet   (58.7 MB)
```

The local folder serves as a permanent staging area. Raw files are never modified.

### HDFS Storage (Big Data Storage)

**Script:** `Phase3_Preprocessing_Methodology_Community/hdfs_setup.py`  
**Cluster:** `docker-hadoop-spark-jupyter` — Hadoop 3.2.1

`hdfs_setup.py` copies all 12 files from local staging into HDFS:

```bash
docker exec namenode hdfs dfs -mkdir -p /taxi/raw
docker exec namenode hdfs dfs -put yellow_tripdata_2024-*.parquet /taxi/raw/
```

**HDFS paths:**
```
hdfs://namenode:9000/taxi/raw/        ← 12 raw Parquet files
hdfs://namenode:9000/taxi/processed/  ← cleaned output (35,602,215 rows)
```

**Why keep local files:** HDFS runs inside Docker. If Docker volumes are lost, the local files allow re-uploading without re-downloading 660 MB. Local files are the source of truth; HDFS is the working storage for Spark.

---

## Stage 3 — Data Loading with Spark

Spark loaded all 12 monthly files together as a single DataFrame.

**Raw dataset after loading:**

| Property | Value |
|---|---|
| Rows | 41,169,720 |
| Columns | 19 |
| Format | Parquet |
| Spark version | 4.1.1 |

**Spark schema (all columns):**

```
VendorID              integer
tpep_pickup_datetime  timestamp_ntz
tpep_dropoff_datetime timestamp_ntz
passenger_count       long
trip_distance         double
RatecodeID            long
store_and_fwd_flag    string
PULocationID          integer
DOLocationID          integer
payment_type          long
fare_amount           double
extra                 double
mta_tax               double
tip_amount            double
tolls_amount          double
improvement_surcharge double
total_amount          double
congestion_surcharge  double
Airport_fee           double
```

---

## Stage 4 — Schema Validation and Data Quality Inspection

Executed in: `notebooks/Phase3_Spark_Processing.ipynb`

### Missing Values Found

| Column | Missing Count | % of Total |
|---|---:|---:|
| passenger_count | 4,091,232 | 9.94% |
| RatecodeID | 4,091,232 | 9.94% |
| store_and_fwd_flag | 4,091,232 | 9.94% |
| congestion_surcharge | 4,091,232 | 9.94% |
| Airport_fee | 4,091,232 | 9.94% |
| All other columns | 0 | 0% |

### Timestamp Problems

```
Earliest pickup:  2002-12-31 16:46:07
Latest pickup:    2026-06-26 23:53:12
```

Rows with abnormal timestamps (outside 2024): **56**

### Invalid Numeric Values

| Issue | Count |
|---|---:|
| Zero or negative passenger_count | 401,354 |
| Zero or negative trip_distance | 776,305 |
| Negative fare_amount | 731,024 |
| Zero fare_amount | 17,260 |
| Negative total_amount | 609,344 |

### Trip Duration Issues

| Issue | Count |
|---|---:|
| Zero or negative duration | 13,510 |
| Duration over 3 hours | 25,349 |
| Duration over 6 hours | 22,023 |

**Saved quality report:** `outputs/raw_data_quality_summary.txt`

---

## Stage 5 — Data Preprocessing

Executed in: `notebooks/Phase3_Preprocessing_EDA_Modeling.ipynb`

All preprocessing used the **Spark DataFrame API** — no Pandas was used at this stage.

### Step 1 — Trip Duration Feature Created

```python
trip_duration_minutes = (dropoff_unix - pickup_unix) / 60
```

### Step 2 — Timestamp and Duration Filter

Rules applied:
```
pickup >= 2024-01-01  and  pickup < 2025-01-01
drop-off >= 2024-01-01  and  drop-off < 2025-01-02
trip_duration_minutes > 0  and  <= 180
```

| | Rows |
|---|---:|
| Before | 41,169,720 |
| After | 41,130,827 |
| Removed | 38,893 |

### Step 3 — Numeric Validity Filter

Rules applied:
```
passenger_count > 0
trip_distance > 0
fare_amount > 0
total_amount > 0
```

| | Rows |
|---|---:|
| Before | 41,130,827 |
| After | 35,602,932 |
| Removed | 5,527,895 |

After this step: **0 missing values** in all columns.

### Step 4 — Extreme Outlier Filter

Thresholds selected after inspecting approximate quantiles:
```
trip_distance <= 100 miles
fare_amount <= 500
total_amount <= 600
trip_duration_minutes <= 180
```

| | Rows |
|---|---:|
| Before | 35,602,932 |
| After | 35,602,215 |
| Removed | 717 |

---

## Stage 6 — Feature Engineering

Six new columns were created using the Spark DataFrame API:

| Feature | How It Was Created |
|---|---|
| `trip_duration_minutes` | `(unix_timestamp(dropoff) - unix_timestamp(pickup)) / 60` |
| `pickup_hour` | `hour(tpep_pickup_datetime)` |
| `pickup_day` | `dayofweek(tpep_pickup_datetime)` — 1=Sun, 7=Sat |
| `pickup_month` | `month(tpep_pickup_datetime)` |
| `is_weekend` | 1 if pickup_day in [1, 7], else 0 |
| `fare_per_mile` | `fare_amount / trip_distance` — EDA only, not used in model |

---

## Stage 7 — Cleaned Data Storage

Cleaned dataset saved to:
```
data/processed/cleaned_yellow_2024/
```

| Property | Value |
|---|---|
| Format | Parquet (Spark output, 12 part files) |
| Rows | 35,602,215 |
| Columns | 25 (19 original + 6 engineered) |
| Rows removed total | 5,567,505 |

---

## Stage 8 — SparkSQL Analysis

A temporary SQL view was registered:
```python
clean_df.createOrReplaceTempView("taxi_trips")
```

Four SQL queries were executed:

1. Trip count and average fare by pickup hour
2. Top 10 busiest pickup locations by trip count
3. Fare statistics (avg, min, max) by payment type
4. Monthly trip count, average fare, and average distance

---

## Stage 9 — Window Functions Analysis

Window functions were applied using `pyspark.sql.window.Window`:

- `rank()` — ranked each pickup hour by trip count within each month
- `lag()` — compared each hour's average fare to the previous hour
- `lead()` — compared each hour's average fare to the next hour

Window specification:
```python
Window.partitionBy("pickup_month").orderBy("pickup_hour")
```

---

## Stage 10 — Exploratory Data Analysis

Aggregations were done using Spark. Small samples or aggregated results were used for visualization with Matplotlib.

### Trips by Pickup Hour

| Hour | Trips |
|---|---:|
| 0 | 953,970 |
| 1 | 622,853 |
| 2 | 405,575 |
| 3 | 259,835 |
| 4 | 166,718 |
| 5 | 195,984 |
| 6 | 458,308 |
| 7 | 926,663 |
| 8 | 1,301,064 |
| 9 | 1,507,627 |
| 10 | 1,663,010 |
| 11 | 1,805,941 |
| 12 | 1,959,633 |
| 13 | 2,031,948 |
| 14 | 2,181,772 |
| 15 | 2,248,894 |
| 16 | 2,282,320 |
| 17 | 2,454,967 |
| 18 | 2,555,714 |
| 19 | 2,253,277 |
| 20 | 2,043,413 |
| 21 | 2,066,837 |
| 22 | 1,857,893 |
| 23 | 1,397,999 |

Lowest demand: 4 AM. Peak demand: 6 PM.

### Average Fare by Pickup Hour

| Hour | Avg Fare ($) |
|---|---:|
| 0 | 20.30 |
| 1 | 18.07 |
| 2 | 16.57 |
| 3 | 17.58 |
| 4 | 23.82 |
| 5 | 28.51 |
| 6 | 23.13 |
| 7 | 19.53 |
| 8 | 18.57 |
| 9 | 18.64 |
| 10 | 19.03 |
| 11 | 19.29 |
| 12 | 19.72 |
| 13 | 20.45 |
| 14 | 21.24 |
| 15 | 21.22 |
| 16 | 21.29 |
| 17 | 19.73 |
| 18 | 18.44 |
| 19 | 18.59 |
| 20 | 18.96 |
| 21 | 19.04 |
| 22 | 19.65 |
| 23 | 20.95 |

Highest average fare: 5 AM (~$28.51 — likely early airport trips). Lowest: 2 AM (~$16.57).

### Average Fare by Month

| Month | Avg Fare ($) |
|---|---:|
| January | 18.43 |
| February | 18.37 |
| March | 19.14 |
| April | 19.45 |
| May | 19.98 |
| June | 19.81 |
| July | 20.20 |
| August | 20.41 |
| September | 20.72 |
| October | 20.41 |
| November | 19.65 |
| December | 20.19 |

Highest: September ($20.72). Lowest: February ($18.37).

### Saved Chart Files

```
outputs/figures/trips_by_pickup_hour.png
outputs/figures/fare_amount_distribution.png
outputs/figures/trip_distance_distribution.png
outputs/figures/trip_distance_vs_fare_amount.png
outputs/figures/average_fare_by_pickup_hour.png
outputs/figures/average_fare_by_pickup_month.png
```

---

## Stage 11 — Machine Learning

**Framework:** Spark MLlib

**Target variable:** `fare_amount`

**Input features (10 columns):**
```
trip_distance, trip_duration_minutes, passenger_count,
pickup_hour, pickup_day, pickup_month, is_weekend,
payment_type, PULocationID, DOLocationID
```

**Pipeline:**
```
VectorAssembler  →  LinearRegression
```

Built using `pyspark.ml.Pipeline(stages=[assembler, lr])`.

**Train/test split (80/20, seed=42):**

| Set | Rows |
|---|---:|
| Training | 28,480,061 |
| Testing | 7,122,154 |

**Model evaluation results:**

| Metric | Value | Meaning |
|---|---|---|
| MAE | 2.1607 | Average prediction error of ~$2.16 |
| RMSE | 5.9255 | Root mean squared error — penalises larger misses |
| R² | 0.8934 | Model explains ~89.3% of fare variation |

**Saved outputs:**
```
outputs/model_results/linear_regression_metrics.txt
outputs/model_results/lr_pipeline_model/   (fitted Spark Pipeline model)
```

---

## Stage 12 — Zeppelin Visualization

**Tool:** Apache Zeppelin 0.10.1  
**URL:** http://localhost:8090  
**Notebook:** NYC Taxi EDA — Zeppelin Dashboard

Zeppelin was added to replace Matplotlib as the primary Big Data visualization tool. Unlike Matplotlib (which produces static PNGs), Zeppelin provides an interactive dashboard where chart types can be switched without rewriting code.

Zeppelin runs inside the same Docker cluster as HDFS and Spark. It reads directly from HDFS using the `%pyspark` interpreter:

```python
df = spark.read.parquet("hdfs://namenode:9000/taxi/raw")
```

**Dashboard paragraphs:**

| Paragraph | Chart Type | Key Finding |
|---|---|---|
| Trips by pickup hour | Bar chart | Peak demand at 6 PM (2,555,714 trips) |
| Monthly trip volume | Bar chart | September has highest avg fare ($20.72) |
| Payment type breakdown | Pie chart | Credit card dominates (~68% of trips) |
| Top 10 pickup locations | Bar chart | Locations 132, 161, 237 are busiest |
| Weekend vs weekday | Bar chart | Weekdays have more trips; weekends higher fares |

---

## Stage 13 — Documentation and Reporting

Scripts produced:
```
Phase3_Preprocessing_Methodology_Community/data_ingestion.py     API Integration
Phase3_Preprocessing_Methodology_Community/hdfs_setup.py         HDFS upload
Phase3_Preprocessing_Methodology_Community/preprocessing_pipeline.py
Phase3_Preprocessing_Methodology_Community/spark_pipeline.py
Phase3_Preprocessing_Methodology_Community/ml_pipeline.py
Phase3_Preprocessing_Methodology_Community/hdfs_commands.md      All cluster commands
Phase3_Preprocessing_Methodology_Community/community_contribution.md
```

Notebooks produced:
```
notebooks/Phase1_Dataset_Introduction.ipynb
notebooks/Phase3_Spark_Processing.ipynb
notebooks/Phase3_Preprocessing_EDA_Modeling.ipynb   (104 cells)
docker-hadoop-spark-jupyter/.../zeppelin-notebooks/NYC_Taxi_EDA/note.json
```

---

## Summary

| Stage | Input | Output |
|---|---|---|
| API Ingestion | NYC TLC CloudFront endpoint | 12 Parquet files, 660.90 MB |
| Local Staging | Downloaded files | `data/raw/yellow_2024/` (unchanged) |
| HDFS Storage | Local Parquet files | `hdfs://namenode:9000/taxi/raw/` |
| Spark Loading | HDFS path | 41,169,720 rows, 19 cols |
| Preprocessing | 41,169,720 rows | 35,602,215 rows, 25 cols |
| EDA | Cleaned DataFrame | SparkSQL results, Window analysis |
| ML Pipeline | Cleaned data | MAE=2.16, RMSE=5.93, R²=0.89 |
| Zeppelin | HDFS data | 5 interactive charts |
