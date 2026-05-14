# Community Contribution

## 1. Purpose of the Community Contribution

This project contributes to the transportation and data analytics community by using real-world NYC Yellow Taxi trip records to study taxi fare patterns and build a fare prediction model.

The project uses official public data from the NYC Taxi & Limousine Commission and applies Big Data processing methods to clean, analyze, and model the data.

## 2. Real-World Value

Taxi trip data is useful for understanding urban transportation behavior.

By analyzing Yellow Taxi trip records, the project can help identify patterns related to:

- Taxi demand by pickup hour
- Fare amount distribution
- Trip distance distribution
- Relationship between trip distance and fare amount
- Average fare changes across time
- Data quality problems in public transportation datasets

These insights can be useful for students, researchers, transportation analysts, and city planning discussions.

## 3. Data Quality Contribution

One important contribution of this project is documenting data quality issues in a real-world raw dataset.

The project identified several problems in the 2024 Yellow Taxi dataset, including:

- Missing values
- Abnormal timestamps outside 2024
- Zero or negative passenger counts
- Zero or negative trip distances
- Negative fare amounts
- Negative total amounts
- Invalid trip durations
- Extreme outliers

These issues show why preprocessing is necessary before using raw transportation data for analysis or machine learning.

## 4. Big Data Processing Contribution

The project demonstrates how Spark can be used to process a large real-world dataset.

The full 2024 Yellow Taxi dataset used in Phase 3 contains:

- `41,169,720` rows
- `19` original columns
- `660.90 MB` of raw Parquet files

This satisfies the course requirement of using a dataset larger than `500 MB`.

Instead of manually merging monthly files, the project keeps the monthly Parquet files separate and loads them together using Spark.

This approach reflects a scalable Big Data workflow.

## 5. Machine Learning Contribution

The project builds a fare prediction model using Spark MLlib.

The model predicts:

- `fare_amount`

using trip-related features such as:

- `trip_distance`
- `trip_duration_minutes`
- `passenger_count`
- `pickup_hour`
- `pickup_day`
- `pickup_month`
- `is_weekend`
- `payment_type`
- `PULocationID`
- `DOLocationID`

The Linear Regression model achieved:

| Metric | Value |
|---|---:|
| MAE | `2.1607` |
| RMSE | `5.9255` |
| R² Score | `0.8934` |

These results provide a baseline model for taxi fare prediction using Spark MLlib.

## 6. Educational Contribution

This project is useful as an educational example for Big Data students because it shows a complete workflow:

1. Selecting a real-world raw dataset
2. Inspecting the dataset
3. Identifying data quality issues
4. Using Spark for large-scale processing
5. Applying preprocessing rules
6. Creating useful features
7. Performing exploratory data analysis
8. Training a machine learning model using Spark MLlib
9. Evaluating the model
10. Saving outputs for reporting

The project also demonstrates the difference between small-scale inspection using Pandas and larger-scale processing using Spark.

## 7. Reproducibility Contribution

The project is organized into clear folders for raw data, processed data, notebooks, scripts, outputs, and reports.

Raw data is kept unchanged in:

`data/raw/`

Cleaned data is saved separately in:

`data/processed/`

Figures are saved in:

`outputs/figures/`

Model results are saved in:

`outputs/model_results/`

This structure helps make the project easier to understand, reproduce, and discuss.

## 8. Limitations

The project has some limitations.

The model is a baseline Linear Regression model. More advanced models may improve prediction performance, but the project focuses on using course-relevant Big Data tools and keeping the workflow understandable.

The project also uses taxi zone IDs directly instead of joining the Taxi Zone Lookup table. In future work, the location IDs could be connected to readable borough and zone names for richer geographic analysis.

## 9. Future Improvements

Possible future improvements include:

- Using the Taxi Zone Lookup table to analyze pickup and drop-off boroughs
- Comparing Linear Regression with another Spark MLlib model
- Adding more location-based features
- Creating more advanced visualizations
- Testing whether airport trips behave differently from normal city trips
- Deploying the project in a real HDFS or cloud Spark environment

## 10. Summary

This project contributes by showing how a large public transportation dataset can be processed, cleaned, analyzed, and modeled using Big Data tools.

It provides practical value by studying taxi fare behavior and educational value by demonstrating a complete Spark-based Big Data workflow.