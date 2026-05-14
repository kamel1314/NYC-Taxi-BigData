# Project Planning — NYC Taxi Fare Prediction

This document shows the project roadmap with actual completion status for each milestone.

---

## Project Overview

| Property | Value |
|---|---|
| Project | NYC Taxi Fare Prediction Using Big Data Analytics |
| Course | CSCI461: Introduction to Big Data (Spring 2026) |
| Dataset | NYC TLC Yellow Taxi Trip Records 2024 |
| Main Tool | Apache Spark / PySpark 4.1.1 |
| Target Variable | `fare_amount` |

---

## Completed Milestones

### Phase 1 — Dataset Introduction ✅

| Milestone | Task | Output | Status |
|---|---|---|---|
| M1 | Select project topic and dataset | NYC Taxi Fare Prediction selected | ✅ Done |
| M2 | Download January 2024 Yellow Taxi file | `data/raw/yellow_tripdata_2024-01.parquet` | ✅ Done |
| M3 | Inspect January 2024 sample using Spark and Pandas | `notebooks/Phase1_Dataset_Introduction.ipynb` | ✅ Done |
| M4 | Document project idea, objective, and dataset | `Phase1_Idea_Objective_Dataset/` files | ✅ Done |

**Phase 1 Key Numbers:**
- January 2024 sample: 2,964,624 rows, 19 columns
- Missing values found in 5 columns (4.73% of rows)
- Abnormal timestamps, negative fares, zero distances identified

---

### Phase 2 — Planning and Design ✅

| Milestone | Task | Output | Status |
|---|---|---|---|
| M5 | Write project planning and milestone documents | `project_planning.md` | ✅ Done |
| M6 | Design Big Data system architecture | `system_architecture.md` | ✅ Done |
| M7 | Define data lifecycle stages | `data_lifecycle.md` | ✅ Done |
| M8 | Write resource allocation and risk assessment | `resource_allocation.md`, `risk_assessment.md` | ✅ Done |
| M9 | Create Gantt chart timeline | `gantt_chart.xlsx` | ✅ Done |
| M10 | Write paper Part 1 (Introduction and Methodology) | `paper_methodology_part1.docx` | ✅ Done |

---

### Phase 3 — Preprocessing, Methodology, and Community ✅

| Milestone | Task | Output | Status |
|---|---|---|---|
| M11 | Download full 2024 Yellow Taxi dataset (12 months) | `data/raw/yellow_2024/` — 660.90 MB | ✅ Done |
| M12 | Load and inspect full dataset with Spark | `notebooks/Phase3_Spark_Processing.ipynb` | ✅ Done |
| M13 | Apply preprocessing (timestamps, numerics, outliers) | `data/processed/cleaned_yellow_2024/` | ✅ Done |
| M14 | Feature engineering (6 new columns) | Added to cleaned dataset | ✅ Done |
| M15 | SparkSQL analysis (4 queries using temp views) | Notebook Section 31 | ✅ Done |
| M16 | Window Functions analysis (rank, lag, lead) | Notebook Section 32 | ✅ Done |
| M17 | Exploratory data analysis and visualizations | `outputs/figures/` — 6 charts | ✅ Done |
| M18 | Build Spark ML Pipeline (VectorAssembler + LinearRegression) | Notebook Section 40 | ✅ Done |
| M19 | Evaluate model with MAE, RMSE, R² | `outputs/model_results/linear_regression_metrics.txt` | ✅ Done |
| M20 | Save fitted Pipeline model to disk | `outputs/model_results/lr_pipeline_model/` | ✅ Done |
| M21 | Write standalone pipeline scripts | `spark_pipeline.py`, `ml_pipeline.py`, `preprocessing_pipeline.py` | ✅ Done |
| M22 | Write community contribution document | `community_contribution.md` | ✅ Done |
| M23 | Write paper Part 2 (Results and Discussion) | `paper_results_discussion.docx` | ✅ Done |

### Rubric Gap Fixes ✅

After auditing against the course rubric, three Big Data categories were uncovered in the original workflow. The original approach downloaded files manually, stored them only in a local Windows folder, and used Matplotlib for visualization — none of which satisfy the Storage, Ingestion, or Visualization requirements. These were corrected:

| Milestone | Gap Fixed | Output | Status |
|---|---|---|---|
| M24 | Data Ingestion — replaced manual browser download with API Integration script | `data_ingestion.py` | ✅ Done |
| M25 | Storage — deployed HDFS via Docker, uploaded all 12 Parquet files to `hdfs://namenode:9000/taxi/raw/` | `hdfs_setup.py`, `hdfs_commands.md` | ✅ Done |
| M26 | Visualization — added Apache Zeppelin 0.10.1 dashboard connected to HDFS and Spark | `zeppelin-notebooks/NYC_Taxi_EDA/note.json` | ✅ Done |

---

## Final Dataset Result

| Property | Value |
|---|---|
| Raw files | 12 monthly Parquet files |
| Raw size | 660.90 MB |
| Raw rows | 41,169,720 |
| Cleaned rows | 35,602,215 |
| Rows removed | 5,567,505 |
| Cleaned columns | 25 |

---

## Final Model Results

| Metric | Value |
|---|---|
| Model | Spark MLlib Linear Regression |
| Training rows | 28,480,061 |
| Testing rows | 7,122,154 |
| MAE | 2.1607 |
| RMSE | 5.9255 |
| R² Score | 0.8934 |

---

## Technology Stack Used

| Component | Tool |
|---|---|
| Data format | Parquet |
| Big Data framework | Apache Spark / PySpark 4.1.1 |
| Processing API | Spark DataFrame API |
| SQL queries | SparkSQL (`createOrReplaceTempView` + `spark.sql`) |
| Window analysis | `pyspark.sql.window.Window` |
| Machine learning | Spark MLlib — VectorAssembler, LinearRegression |
| ML Pipeline | `pyspark.ml.Pipeline` |
| Visualization | Matplotlib (on aggregated/sampled Spark output) |
| Environment | Python 3.11, Java 17 (OpenJDK 17.0.19), Windows |
| Notebooks | Jupyter Notebook / VS Code |

---

## Project Files Summary

```
NYC_Taxi_BigData_Projec/
├── data/
│   ├── raw/
│   │   ├── yellow_tripdata_2024-01.parquet         (Phase 1 sample)
│   │   └── yellow_2024/                            (12 monthly files, 660.90 MB)
│   └── processed/
│       └── cleaned_yellow_2024/                    (35,602,215 rows, 25 cols)
│
├── notebooks/
│   ├── Phase1_Dataset_Introduction.ipynb
│   ├── Phase3_Spark_Processing.ipynb
│   └── Phase3_Preprocessing_EDA_Modeling.ipynb    (104 cells, all executed)
│
├── Phase1_Idea_Objective_Dataset/
│   ├── project_idea.md
│   ├── dataset_description.md
│   └── paper_introduction.docx
│
├── Phase2_Planning_Design_DataLifecycle/
│   ├── project_planning.md
│   ├── resource_allocation.md
│   ├── risk_assessment.md
│   ├── system_architecture.md
│   ├── data_lifecycle.md
│   ├── gantt_chart.xlsx
│   └── paper_methodology_part1.docx
│
├── Phase3_Preprocessing_Methodology_Community/
│   ├── preprocessing_pipeline.py
│   ├── spark_pipeline.py
│   ├── ml_pipeline.py
│   ├── community_contribution.md
│   └── paper_results_discussion.docx
│
└── outputs/
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
        └── lr_pipeline_model/
```
