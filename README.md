# NYC Taxi Fare Prediction Using Big Data Analytics

**Course:** CSCI461 — Introduction to Big Data (Spring 2026)  
**Dataset:** NYC TLC Yellow Taxi Trip Records 2024  
**Framework:** Apache Spark / PySpark 4.1.1

---

## Project Overview

This project builds a full Big Data pipeline to predict NYC taxi fares using 41.1 million real trip records from 2024. It covers data ingestion, HDFS storage, Spark preprocessing, exploratory analysis, machine learning, and visualization.

**Final model result:** Linear Regression — R² = 0.8934, MAE = $2.16

---

## Project Structure

```
NYC_Taxi_BigData_Projec/
├── data/
│   ├── raw/yellow_2024/               12 monthly Parquet files (660.90 MB)
│   └── processed/cleaned_yellow_2024/ Cleaned output (35,602,215 rows)
│
├── notebooks/
│   ├── Phase1_Dataset_Introduction.ipynb
│   ├── Phase3_Spark_Processing.ipynb
│   └── Phase3_Preprocessing_EDA_Modeling.ipynb   (main — 104 cells)
│
├── Phase1_Idea_Objective_Dataset/
│   ├── project_idea.md
│   ├── dataset_description.md
│   └── paper_introduction.docx
│
├── Phase2_Planning_Design_DataLifecycle/
│   ├── project_planning.md
│   ├── system_architecture.md
│   ├── data_lifecycle.md
│   ├── resource_allocation.md
│   ├── risk_assessment.md
│   └── gantt_chart.xlsx
│
├── Phase3_Preprocessing_Methodology_Community/
│   ├── data_ingestion.py             API Integration — downloads NYC TLC data
│   ├── hdfs_setup.py                 Copies raw data into HDFS
│   ├── preprocessing_pipeline.py     Spark preprocessing steps
│   ├── spark_pipeline.py             SparkSQL + Window Functions
│   ├── ml_pipeline.py                Spark MLlib training + evaluation
│   └── community_contribution.md
│
└── outputs/
    ├── raw_data_quality_summary.txt
    ├── figures/                       6 EDA charts (PNG)
    └── model_results/
        ├── linear_regression_metrics.txt
        └── lr_pipeline_model/         Saved Spark Pipeline model
```

---

## Big Data Stack

| Category | Tool |
|---|---|
| Core platform | Apache Spark / PySpark 4.1.1 |
| Storage | HDFS via Docker (Hadoop 3.2.1) |
| Data ingestion | API Integration — NYC TLC CloudFront endpoint |
| Processing | Spark DataFrame API, SparkSQL, Window Functions |
| Machine learning | Spark MLlib — VectorAssembler, LinearRegression, ML Pipeline |
| Visualization | Apache Zeppelin 0.10.1 |

---

## How to Run

### 1. Download the data (if not already present)
```bash
cd NYC_Taxi_BigData_Projec
python Phase3_Preprocessing_Methodology_Community/data_ingestion.py
```

### 2. Start the Hadoop + Spark + Zeppelin cluster
```bash
cd ..\docker-hadoop-spark-jupyter\docker-hadoop-spark-jupyter
docker-compose up -d
```

### 3. Load data into HDFS
```bash
cd NYC_Taxi_BigData_Projec
python Phase3_Preprocessing_Methodology_Community/hdfs_setup.py
```

### 4. Open the interfaces
- Jupyter:  http://localhost:8888
- Zeppelin: http://localhost:8090
- HDFS UI:  http://localhost:9870
- YARN UI:  http://localhost:8088

### 5. Run the standalone scripts
```bash
python Phase3_Preprocessing_Methodology_Community/preprocessing_pipeline.py
python Phase3_Preprocessing_Methodology_Community/spark_pipeline.py
python Phase3_Preprocessing_Methodology_Community/ml_pipeline.py
```

---

## Key Results

| Metric | Value |
|---|---|
| Raw dataset | 41,169,720 rows — 660.90 MB |
| After preprocessing | 35,602,215 rows — 25 columns |
| Rows removed | 5,567,505 |
| Model | Spark MLlib Linear Regression |
| Training rows | 28,480,061 |
| MAE | 2.1607 |
| RMSE | 5.9255 |
| R² Score | 0.8934 |

---

## Requirements

- Python 3.11
- Java 17 (OpenJDK 17.0.19)
- Apache Spark / PySpark 4.1.1
- Docker Desktop (for HDFS + Zeppelin)
- See `requirements.txt` for Python packages
