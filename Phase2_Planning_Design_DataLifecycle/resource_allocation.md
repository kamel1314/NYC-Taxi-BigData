# Resource Allocation — NYC Taxi Fare Prediction

This document describes the resources that were actually used in the project. All versions, sizes, and outcomes reflect the final executed state of the project.

---

## 1. Hardware Resources

The entire project was developed and executed on a local Windows machine. No cloud resources were used.

| Resource | What It Was Used For |
|---|---|
| Local Windows PC | All development, notebook execution, Spark processing, and storage |
| Local SSD/HDD | Stored 660.90 MB of raw Parquet files, cleaned output, model artifacts, charts, and notebooks |
| RAM | Spark loaded and processed 41,169,720 rows in-memory; Spark's lazy evaluation kept memory usage manageable |
| CPU | Ran all Spark jobs locally — preprocessing, groupBy aggregations, quantile calculations, model training on 28.4M rows |

**What actually fit locally:**
- Raw dataset: 660.90 MB across 12 Parquet files
- Cleaned output: 35,602,215 rows saved as Parquet part files
- Spark handled everything without running out of memory

---

## 2. Software Resources

| Software / Tool | Version | What It Was Used For |
|---|---|---|
| Python | 3.11 | All scripting, notebooks, and Spark sessions |
| Jupyter Notebook / VS Code | Latest | Notebook development and execution |
| Apache Spark / PySpark | 4.1.1 | Main Big Data framework — all processing, SQL, ML |
| Java (OpenJDK) | 17.0.19 | Required runtime for Spark (downgraded from Java 25 due to incompatibility) |
| Spark DataFrame API | (part of PySpark 4.1.1) | Loading, filtering, transformation, feature engineering, aggregation |
| SparkSQL | (part of PySpark 4.1.1) | 4 SQL queries via `createOrReplaceTempView` + `spark.sql` |
| Spark Window Functions | (part of PySpark 4.1.1) | `rank()`, `lag()`, `lead()` — partitioned by month, ordered by hour |
| Spark MLlib | (part of PySpark 4.1.1) | `VectorAssembler`, `LinearRegression`, `RegressionEvaluator` |
| Spark ML Pipeline | (part of PySpark 4.1.1) | `Pipeline(stages=[assembler, lr])` — end-to-end training object |
| Docker Desktop | Latest | Container runtime for HDFS + Spark + Zeppelin cluster |
| docker-hadoop-spark-jupyter | Hadoop 3.2.1 / Spark 3.0.0 | Docker cluster providing HDFS, YARN, Spark, Jupyter, Zeppelin |
| HDFS (via Docker) | Hadoop 3.2.1 | Distributed storage — raw and cleaned Parquet files at `hdfs://namenode:9000/taxi/` |
| Apache Zeppelin | 0.10.1 | Interactive Big Data visualization dashboard (rubric: Visualization/Reporting) |
| winutils.exe + hadoop.dll | Hadoop 3.3.1 | Required for Spark to write files on Windows; placed in `C:\hadoop\bin` |
| Pandas | Latest | Used ONLY to convert small aggregated results (<24 rows) to DataFrames for Matplotlib |
| Matplotlib | Latest | Secondary visualizations — 6 static charts from Spark aggregated output |
| PyArrow | Latest | Reading Parquet files |
| requests | Latest | HTTP downloads in `data_ingestion.py` — API Integration |
| Microsoft Excel | (via openpyxl) | Gantt chart |
| Microsoft Word | Latest | Research paper (Phase 1 and Phase 3 papers) |

**Key setup note:** Spark 4.1.1 was incompatible with Java 25 (the default installed version). OpenJDK 17.0.19 was installed and used instead. Spark ran correctly with Java 17.

---

## 3. Data Resources

| Resource | Description | Actual Size / Count |
|---|---|---|
| January 2024 Yellow Taxi (Phase 1) | Initial inspection sample only — no preprocessing | 2,964,624 rows, 19 columns |
| Full 2024 Yellow Taxi (12 monthly files) | Main implementation dataset | 41,169,720 rows, 660.90 MB |
| Cleaned dataset output | Preprocessed Parquet files saved by Spark | 35,602,215 rows, 25 columns |

**Dataset size requirement:** The course required 500 MB or larger. The full 2024 dataset (660.90 MB) satisfies this requirement.

**Actual raw data structure used:**

```
data/raw/
├── yellow_tripdata_2024-01.parquet      (Phase 1 inspection copy)
└── yellow_2024/
    ├── yellow_tripdata_2024-01.parquet
    ├── yellow_tripdata_2024-02.parquet
    ├── ...
    └── yellow_tripdata_2024-12.parquet
```

The 12 monthly files were kept separate and loaded together with `spark.read.parquet(*all_file_paths)`. This mirrors an HDFS-style layout and avoids manual merging.

**Actual processed data structure:**

```
data/processed/
└── cleaned_yellow_2024/     (12 Parquet part files + _SUCCESS marker)
```

**Actual output structure:**

```
outputs/
├── raw_data_quality_summary.txt
├── figures/
│   ├── trips_by_pickup_hour.png
│   ├── fare_amount_distribution.png
│   ├── trip_distance_distribution.png
│   ├── trip_distance_vs_fare_amount.png
│   ├── average_fare_by_pickup_hour.png
│   └── average_fare_by_pickup_month.png
└── model_results/
    ├── linear_regression_metrics.txt
    └── lr_pipeline_model/              (fitted Spark Pipeline model)
```

---

## 4. Personnel

This was a solo project completed for CSCI461: Introduction to Big Data (Spring 2026).

| Role | Responsibilities Completed |
|---|---|
| Student (solo developer) | Dataset collection, Spark setup, preprocessing, feature engineering, SparkSQL, Window Functions, EDA, ML modeling, pipeline scripting, and all documentation |
| Course Instructor / Evaluator | Provided rubric, labs, and project requirements |
| Course Labs | Used as reference for Spark patterns, MLlib usage, and pipeline design |

---

## 5. Budget

All resources used were free or already available.

| Item | Cost |
|---|---:|
| NYC TLC dataset | Free (public) |
| Python 3.11 | Free |
| Apache Spark / PySpark 4.1.1 | Free |
| OpenJDK 17.0.19 | Free |
| winutils.exe / hadoop.dll | Free |
| Jupyter Notebook / VS Code | Free |
| Matplotlib, Pandas, PyArrow | Free |
| Local computer and storage | Already available |
| Microsoft Word / Excel | Already available |

**Total additional cost: $0**

No cloud services (AWS EMR, Databricks, Google Dataproc) were used. The entire project ran on local hardware.

---

## 6. Resource Constraints Encountered

| Constraint | What Happened | How It Was Handled |
|---|---|---|
| Java version incompatibility | Spark 4.1.1 failed with Java 25 | Installed OpenJDK 17.0.19; Spark ran correctly |
| Spark write failure on Windows | Hadoop home directory error when saving Parquet output | Downloaded winutils.exe + hadoop.dll for Hadoop 3.3.1; set `HADOOP_HOME=C:\hadoop` |
| HADOOP_HOME timing issue | Setting `os.environ["HADOOP_HOME"]` after SparkSession had no effect | Moved HADOOP_HOME setup to the first notebook cell, before any imports |
| Processing time on 41M rows | Some operations (counts, quantile calc, model training) took several minutes | Set `spark.sql.shuffle.partitions = 8` (reduced from default 200) to speed up local aggregations |
| Visualization memory | Could not plot 35M raw rows directly | Used Spark aggregations and small samples (<24 rows) before converting to Pandas for Matplotlib |

---

## 7. Final Resource Summary

| Resource | Planned | Actual |
|---|---|---|
| Dataset size | 500 MB+ | 660.90 MB ✅ |
| Processing framework | Spark/PySpark | PySpark 4.1.1 ✅ |
| Java runtime | Java 11 or 17 | OpenJDK 17.0.19 ✅ |
| Cloud resources | Not planned | Not needed ✅ |
| ML framework | Spark MLlib | Spark MLlib + ML Pipeline ✅ |
| Model performance | R² > 0.80 target | R² = 0.8934 ✅ |
| Total cost | $0 | $0 ✅ |