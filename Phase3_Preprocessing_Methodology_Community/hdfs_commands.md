# HDFS & Cluster Commands — NYC Taxi Fare Prediction

This document covers all commands needed to start the Hadoop/Spark/Zeppelin
cluster and manage data in HDFS.

---

## 1. Start the Cluster

Navigate to the docker-hadoop-spark-jupyter folder and start all services:

```bash
cd "D:\UNI\Third_second\CSCI461 Introduction to Big Data\docker-hadoop-spark-jupyter\docker-hadoop-spark-jupyter"
docker-compose up -d
```

Wait about 30–60 seconds for all services to fully start.

**Services started:**

| Service | URL |
|---|---|
| HDFS Namenode UI | http://localhost:9870 |
| YARN Resource Manager | http://localhost:8088 |
| Spark Master UI | http://localhost:8080 |
| Jupyter Notebook | http://localhost:8888 |
| Apache Zeppelin | http://localhost:8090 |

---

## 2. Load Raw Data into HDFS

After the cluster is running, copy the 12 Parquet files into HDFS:

```bash
cd "D:\UNI\Third_second\CSCI461 Introduction to Big Data\NYC_Taxi_BigData_Projec"
python Phase3_Preprocessing_Methodology_Community/hdfs_setup.py
```

This script:
- Creates `/taxi/raw/` and `/taxi/processed/` directories in HDFS
- Copies all 12 monthly Parquet files into `hdfs://namenode:9000/taxi/raw/`
- Verifies the upload and prints the HDFS paths

---

## 3. Useful HDFS Commands

Run these inside a terminal to inspect HDFS directly:

```bash
# List files in HDFS
docker exec namenode hdfs dfs -ls /taxi/raw/

# Check total size of uploaded data
docker exec namenode hdfs dfs -du -h /taxi/raw/

# Check HDFS health
docker exec namenode hdfs dfsadmin -report

# Create a directory manually
docker exec namenode hdfs dfs -mkdir -p /taxi/processed/

# Delete a file or folder from HDFS
docker exec namenode hdfs dfs -rm -r /taxi/raw/some_file.parquet
```

---

## 4. Read from HDFS in Spark (PySpark)

Use these paths in any Spark notebook or script instead of local paths:

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("NYC Taxi — HDFS") \
    .getOrCreate()

# Read raw data from HDFS
HDFS_RAW = "hdfs://namenode:9000/taxi/raw"
df = spark.read.parquet(HDFS_RAW)
print(f"Rows: {df.count():,}")

# Save cleaned output to HDFS
HDFS_PROCESSED = "hdfs://namenode:9000/taxi/processed"
clean_df.write.mode("overwrite").parquet(HDFS_PROCESSED)
```

---

## 5. Apache Zeppelin — Visualization

1. Open http://localhost:8090 in your browser
2. In the Notebook menu, find **NYC Taxi EDA — Zeppelin Dashboard**
3. Click **Run All Paragraphs**
4. For each result table, use the chart buttons (bar, pie, line) to switch views

The notebook covers:
- Trips and average fare by pickup hour (bar chart)
- Monthly trip volume and average fare (bar chart)
- Payment type breakdown (pie chart)
- Top 10 busiest pickup locations (bar chart)
- Weekend vs weekday comparison (bar chart)

---

## 6. Stop the Cluster

When you're done, stop all containers:

```bash
cd "D:\UNI\Third_second\CSCI461 Introduction to Big Data\docker-hadoop-spark-jupyter\docker-hadoop-spark-jupyter"
docker-compose down
```

> **Important:** Do NOT run `docker-compose down -v` — the `-v` flag deletes
> the HDFS volumes and you will lose all data uploaded to HDFS.
> Always use `docker-compose down` (without `-v`) to preserve the data.

---

## 7. Re-upload Data After Restart

If you stopped the cluster with `docker-compose down` and the HDFS data was
preserved, you do not need to re-upload. If the data was lost (e.g. after
`docker-compose down -v`), re-run:

```bash
python Phase3_Preprocessing_Methodology_Community/hdfs_setup.py
```

> **Why keep local files:** The raw Parquet files in `data/raw/yellow_2024/`
> are the permanent source. HDFS inside Docker is temporary — if volumes are
> lost, the local files are used to re-populate HDFS. Never delete the local
> raw files.

---

## 8. Download Data via API (data_ingestion.py)

To download the 12 monthly files programmatically from the NYC TLC endpoint:

```bash
cd "D:\UNI\Third_second\CSCI461 Introduction to Big Data\NYC_Taxi_BigData_Projec"
python Phase3_Preprocessing_Methodology_Community/data_ingestion.py
```

Files already downloaded are skipped automatically. This satisfies the
**API Integration** data ingestion requirement.

---

## Summary of HDFS Paths

| Data | HDFS Path |
|---|---|
| Raw monthly Parquet files | `hdfs://namenode:9000/taxi/raw/` |
| Cleaned preprocessed output | `hdfs://namenode:9000/taxi/processed/` |
