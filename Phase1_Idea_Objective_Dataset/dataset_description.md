# Dataset Description

## 1. Dataset Source

The dataset used in this project is the official NYC Taxi & Limousine Commission (TLC) Trip Record Data.

Source URL:

```
https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
```

The TLC publishes taxi trip records as monthly Parquet files. These are real-world raw records collected from taxi service operations in New York City. The data is not pre-cleaned or ready-made — it requires preprocessing before use.

---

## 2. Dataset Used in This Project

### Phase 1 — Initial Inspection Sample

| Property | Value |
|---|---|
| File | `yellow_tripdata_2024-01.parquet` |
| Location | `data/raw/yellow_tripdata_2024-01.parquet` |
| Purpose | Dataset structure inspection only — no preprocessing, no modeling |
| Rows | 2,964,624 |
| Columns | 19 |

### Phase 3 — Full Implementation Dataset

| Property | Value |
|---|---|
| Files | 12 monthly Parquet files: Jan–Dec 2024 |
| Location | `data/raw/yellow_2024/` |
| Total raw size | 660.90 MB |
| Total raw rows | 41,169,720 |
| Columns | 19 |
| Format | Parquet |

This satisfies the course requirement of using a dataset of at least 500 MB.

---

## 3. Dataset Columns

All 19 columns are present in both the Phase 1 sample and the full 2024 dataset:

| Column | Type (Spark) | Meaning |
|---|---|---|
| `VendorID` | integer | Technology provider that submitted the record |
| `tpep_pickup_datetime` | timestamp_ntz | Date and time when the taxi meter started |
| `tpep_dropoff_datetime` | timestamp_ntz | Date and time when the taxi meter stopped |
| `passenger_count` | long | Number of passengers |
| `trip_distance` | double | Trip distance in miles |
| `RatecodeID` | long | Rate type code for the trip |
| `store_and_fwd_flag` | string | Whether trip was stored before forwarding |
| `PULocationID` | integer | Pickup taxi zone ID |
| `DOLocationID` | integer | Drop-off taxi zone ID |
| `payment_type` | long | Payment method code |
| `fare_amount` | double | Taxi fare calculated for the trip |
| `extra` | double | Extra charges |
| `mta_tax` | double | MTA tax |
| `tip_amount` | double | Tip paid by passenger |
| `tolls_amount` | double | Toll charges |
| `improvement_surcharge` | double | Improvement surcharge |
| `total_amount` | double | Total amount paid |
| `congestion_surcharge` | double | Congestion surcharge (NYC zones) |
| `Airport_fee` | double | Airport-related fee if applicable |

---

## 4. Phase 1 — January 2024 Sample Inspection Results

### Missing Values

The following columns each had exactly `140,162` missing values:

```
passenger_count
RatecodeID
store_and_fwd_flag
congestion_surcharge
Airport_fee
```

Missing percentage: `140,162 / 2,964,624 ≈ 4.73%`

All other columns had 0 missing values.

### Statistical Issues Found

| Column | Problem | Example |
|---|---|---|
| `passenger_count` | Min value = 0 | Zero passengers recorded |
| `trip_distance` | Min = 0, Max = 312,722.3 miles | Extreme outliers |
| `fare_amount` | Min = -899, Max = 5,000 | Negative fares |
| `tip_amount` | Min = -80, Max = 428 | Negative tips |
| `total_amount` | Min = -900, Max = 5,000 | Negative totals |

### Timestamp Range (Phase 1 Sample)

```
Earliest pickup:    2002-12-31 22:59:39  (abnormal — should be Jan 2024)
Latest pickup:      2024-02-01 00:01:15
Earliest drop-off:  2002-12-31 23:05:41  (abnormal)
Latest drop-off:    2024-02-02 13:56:52
```

Records from 2002 are clearly invalid for a January 2024 dataset and were flagged for filtering.

---

## 5. Full 2024 Dataset — Data Quality Issues Found

### Missing Values

| Column | Missing Count |
|---|---:|
| passenger_count | 4,091,232 |
| RatecodeID | 4,091,232 |
| store_and_fwd_flag | 4,091,232 |
| congestion_surcharge | 4,091,232 |
| Airport_fee | 4,091,232 |
| All other columns | 0 |

Missing percentage: `4,091,232 / 41,169,720 ≈ 9.94%`

### Timestamp Problems

```
Earliest pickup:  2002-12-31 16:46:07
Latest pickup:    2026-06-26 23:53:12
```

Abnormal records (pickup/drop-off outside 2024): **56 rows** (0.000136% of dataset)

### Invalid Numeric Values

| Issue | Count |
|---|---:|
| Zero or negative passenger_count | 401,354 |
| Zero or negative trip_distance | 776,305 |
| Negative fare_amount | 731,024 |
| Zero fare_amount | 17,260 |
| Negative tip_amount | 1,331 |
| Negative total_amount | 609,344 |
| Zero total_amount | 5,062 |

### Raw Statistical Extremes

| Column | Min | Max |
|---|---|---|
| passenger_count | 0 | 9 |
| trip_distance | 0.0 | 398,608.62 |
| fare_amount | -2,261.2 | 335,544.44 |
| tip_amount | -300.0 | 999.99 |
| tolls_amount | -140.63 | 1,702.88 |
| total_amount | -2,265.45 | 335,550.94 |

### Trip Duration Issues

| Issue | Count |
|---|---:|
| Zero or negative duration | 13,510 |
| Duration over 3 hours | 25,349 |
| Duration over 6 hours | 22,023 |

---

## 6. Preprocessing Applied in Phase 3

All preprocessing was applied using the Spark DataFrame API on the full 2024 dataset.

### Step 1 — Timestamp and Duration Filter

```
Pickup:    2024-01-01 to 2024-12-31
Drop-off:  2024-01-01 to 2025-01-01 (allows midnight crossover)
Duration:  > 0 and <= 180 minutes
```

| Stage | Rows |
|---|---:|
| Before filter | 41,169,720 |
| After filter | 41,130,827 |
| Removed | 38,893 |

### Step 2 — Numeric Validity Filter

```
passenger_count > 0
trip_distance > 0
fare_amount > 0
total_amount > 0
```

| Stage | Rows |
|---|---:|
| Before filter | 41,130,827 |
| After filter | 35,602,932 |
| Removed | 5,527,895 |

After this step: **0 missing values** in all columns.

### Step 3 — Extreme Outlier Filter

```
trip_distance <= 100 miles
fare_amount <= 500
total_amount <= 600
trip_duration_minutes <= 180
```

| Stage | Rows |
|---|---:|
| Before filter | 35,602,932 |
| After filter | 35,602,215 |
| Removed | 717 |

### Step 4 — Feature Engineering

Six new columns were created:

| Feature | Description |
|---|---|
| `trip_duration_minutes` | Seconds between pickup and drop-off, divided by 60 |
| `pickup_hour` | Hour extracted from pickup datetime (0–23) |
| `pickup_day` | Day of week (1=Sunday, 7=Saturday) |
| `pickup_month` | Month (1–12) |
| `is_weekend` | 1 if pickup_day is Saturday or Sunday, else 0 |
| `fare_per_mile` | fare_amount / trip_distance — used for EDA only, not model input |

---

## 7. Final Cleaned Dataset

| Property | Value |
|---|---|
| Rows | 35,602,215 |
| Columns | 25 (19 original + 6 engineered) |
| Rows removed | 5,567,505 |
| Saved to | `data/processed/cleaned_yellow_2024/` |
| Format | Parquet (Spark part files + `_SUCCESS`) |

---

## 8. Target Variable and Model Features

**Target:** `fare_amount`

**Input features used for modeling:**

```
trip_distance
trip_duration_minutes
passenger_count
pickup_hour
pickup_day
pickup_month
is_weekend
payment_type
PULocationID
DOLocationID
```

`fare_per_mile` was excluded from modeling because it is derived from `fare_amount`, which would cause data leakage.

---

## 9. Legal Use and Privacy

The NYC TLC dataset is publicly available and legally usable for academic projects. It does not contain direct passenger names or personal identity fields. The project used the data only for transportation analytics and fare prediction research.
