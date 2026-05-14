# NYC Taxi Fare Prediction Using Big Data Analytics

## 1. Project Idea

This project focuses on analyzing NYC Yellow Taxi trip records and predicting taxi fare amounts using Big Data analytics techniques.

The dataset is collected from the official NYC Taxi & Limousine Commission (TLC) Trip Record Data. The TLC dataset is a real-world raw transportation dataset published as separate monthly Parquet files.

In Phase 1, the January 2024 Yellow Taxi file is used as an initial inspected monthly sample to understand the dataset structure, columns, data types, and early data quality issues. The January 2024 sample contains `2,964,624` rows and `19` columns.

However, the final project will not depend only on this one-month sample. To satisfy the Big Data project requirement of using a dataset of at least `500 MB`, the final implementation will use multiple 2024 Yellow Taxi monthly Parquet files, or the full 2024 year if feasible. These monthly files will be processed using Spark-based Big Data processing.

Each row in the dataset represents one completed taxi trip. The dataset includes trip information such as pickup time, drop-off time, passenger count, trip distance, pickup location ID, drop-off location ID, payment type, fare amount, tip amount, tolls, surcharges, and total amount.

The main idea is to use this large transportation dataset to understand fare patterns and build a machine learning model that predicts the taxi fare amount based on trip-related features.

## 2. Problem Statement

Taxi fare amount depends on several factors, including trip distance, trip duration, pickup time, passenger count, payment type, and pickup/drop-off locations.

New York City produces a large number of taxi trip records every month. Because the dataset contains millions of records and is published as large monthly files, manual analysis is inefficient and not suitable for large-scale processing.

The problem this project addresses is how to process large raw taxi trip data, identify data quality issues, extract useful trip features, and use those features to predict taxi fare amount in a scalable way.

## 3. Big Data Significance

This project is relevant to Big Data because the NYC TLC trip records are large, real-world, raw transportation datasets.

The January 2024 Yellow Taxi file alone contains nearly 3 million trip records. The final project will use multiple monthly files from 2024 to meet the minimum dataset size requirement of `500 MB` or larger.

The dataset is published in Parquet format, which is commonly used in Big Data environments because it supports efficient storage and column-based processing.

The project will rely mainly on Big Data processing tools, especially Spark and the Spark DataFrame API, rather than only using small-scale Python scripts. Spark will be used for scalable loading, preprocessing, feature engineering, and analysis of the selected monthly files.

## 4. Real-World Applicability

Taxi fare prediction has practical value for passengers, taxi companies, transportation researchers, and city planning authorities.

Passengers can better understand expected trip costs. Taxi companies can analyze fare behavior and trip patterns. Transportation authorities can use the analysis to study urban mobility, demand patterns, and pricing behavior.

The project can also help identify abnormal trip records, unrealistic fare values, and data quality issues in transportation datasets.

## 5. Project Objective

The objective of this project is to build a Big Data analytics pipeline that processes raw NYC Yellow Taxi trip records, analyzes trip and fare patterns, and predicts taxi fare amount using machine learning.

The target variable for prediction is:

- `fare_amount`

The objective is specific because the project focuses on taxi fare prediction. It is measurable because the model will be evaluated using regression metrics such as Mean Absolute Error, Root Mean Squared Error, and R² Score. It is achievable because the dataset contains relevant trip features such as distance, time, passenger count, payment type, and location IDs. It is relevant because fare prediction is connected to real-world transportation analytics. It is time-bound because the work will be completed across the required project phases.

## 6. Success Criteria

The project will be considered successful if it achieves the following:

- Uses an official real-world raw dataset from NYC TLC.
- Uses enough monthly Yellow Taxi Parquet files to satisfy the `500 MB` minimum dataset size requirement.
- Uses Spark-based Big Data processing as the main processing environment.
- Inspects and describes the January 2024 sample correctly in Phase 1.
- Identifies missing values, abnormal records, invalid values, and outliers.
- Applies preprocessing rules during Phase 3 to clean the selected monthly files.
- Creates useful features such as trip duration, pickup hour, pickup day, and possibly weekend indicator.
- Produces meaningful visualizations about taxi trip and fare patterns.
- Builds a working machine learning model to predict `fare_amount`.
- Evaluates the model using MAE, RMSE, and R² Score.
- Explains the project methodology, results, and limitations clearly in the final report.

## 7. Project Scope

The project focuses on NYC Yellow Taxi trip records from 2024.

Phase 1 uses the January 2024 Yellow Taxi file as an initial inspected sample to understand the dataset structure and quality. The January sample is not treated as the full final dataset.

The final implementation will use multiple monthly Yellow Taxi files from 2024, or the full 2024 year if feasible, so that the project meets the required dataset size and Big Data processing expectations.

The main prediction target is:

- `fare_amount`

Possible input features include:

- `trip_distance`
- `passenger_count`
- `tpep_pickup_datetime`
- `tpep_dropoff_datetime`
- `PULocationID`
- `DOLocationID`
- `payment_type`

Additional features will be created later during preprocessing and feature engineering, such as:

- `trip_duration_minutes`
- `pickup_hour`
- `pickup_day`
- `pickup_month`
- `is_weekend`

The project includes dataset inspection, project planning, system design, data lifecycle explanation, preprocessing, exploratory data analysis, feature engineering, Spark-based processing, machine learning model building, model evaluation, technical documentation, and final reporting.

## 8. Initial Data Quality Issues Identified

The Phase 1 notebook identified several data quality issues in the January 2024 sample.

These include:

- Missing values in `passenger_count`, `RatecodeID`, `store_and_fwd_flag`, `congestion_surcharge`, and `Airport_fee`.
- Each of these columns has `140,162` missing values, which is approximately `4.73%` of the January 2024 sample.
- `passenger_count` has a minimum value of `0`.
- `trip_distance` has a minimum value of `0` and a maximum value of `312,722.3` miles.
- `fare_amount` contains negative values, with a minimum of `-899`.
- `tip_amount` contains negative values, with a minimum of `-80`.
- `total_amount` contains negative values, with a minimum of `-900`.
- Some timestamp values are abnormal, such as pickup and drop-off dates from `2002`, even though the file represents January 2024.

These issues are only identified in Phase 1. They will be handled later during Phase 3 preprocessing.

When the project expands to more monthly files, the same validation checks will be applied to all selected months because similar data quality issues may appear in other files.

## 9. Planned Big Data Approach

The final project will use Spark-based processing as the main Big Data approach.

The planned workflow is:

1. Store raw monthly Parquet files in the `data/raw` folder.
2. Load selected monthly files using Spark DataFrame API.
3. Apply consistent preprocessing rules across all selected files.
4. Create new features such as trip duration and pickup hour.
5. Perform exploratory data analysis and aggregations.
6. Train a fare prediction model.
7. Evaluate the model using regression metrics.
8. Save cleaned data, figures, model results, and documentation.

Pandas may be used for initial inspection and small sample viewing, but Spark will be used as the main processing framework for the final implementation.