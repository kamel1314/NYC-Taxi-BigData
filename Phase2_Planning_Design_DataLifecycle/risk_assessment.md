# Risk Assessment — NYC Taxi Fare Prediction

This document lists the risks that were identified during project planning, and for each one explains whether it actually occurred and how it was resolved.

---

## Risk Summary Table

| # | Risk | Occurred? | Resolution |
|---|---|---|---|
| 1 | Dataset too small (< 500 MB) | ❌ No | Full 2024 year = 660.90 MB — requirement met |
| 2 | Large file processing limitations | ❌ No | Spark handled 41.1M rows without issue |
| 3 | Missing values | ✅ Yes | Handled by filtering out rows with invalid numerics |
| 4 | Abnormal timestamps | ✅ Yes | 56 rows had dates from 2002–2026; filtered with date range rules |
| 5 | Invalid trip distances | ✅ Yes | Zero and extreme distances removed (5.5M+ rows filtered) |
| 6 | Negative fare values | ✅ Yes | 731,024 negative fare rows removed |
| 7 | Negative payment values | ✅ Yes | Filtered via `total_amount > 0` |
| 8 | Inconsistent monthly schemas | ❌ No | All 12 files had identical 19-column schema |
| 9 | Storage limitations | ❌ No | 660.90 MB was manageable locally |
| 10 | Spark setup issues | ✅ Yes | Multiple issues — all resolved (see below) |
| 11 | Long processing time | ✅ Partial | Some operations on 41M rows took time; acceptable |
| 12 | Model performance limitations | ❌ No | R²=0.89 is a strong result for a baseline model |
| 13 | Overclaiming results | ❌ No | Only verified, executed results are reported |
| 14 | Visualization memory issues | ❌ No | Used Spark aggregations + small samples for Matplotlib |
| 15 | Data privacy concerns | ❌ No | Public TLC data, no personal identifiers |
| 16 | Raw data modification | ❌ No | Raw files kept unchanged; cleaned output saved separately |

---

## Detailed Risk Outcomes

### Risk 3 — Missing Values ✅ Occurred and Resolved

**What happened:** 5 columns had missing values in the full 2024 dataset.

| Column | Missing Count | % of Rows |
|---|---:|---:|
| passenger_count | 4,091,232 | 9.94% |
| RatecodeID | 4,091,232 | 9.94% |
| store_and_fwd_flag | 4,091,232 | 9.94% |
| congestion_surcharge | 4,091,232 | 9.94% |
| Airport_fee | 4,091,232 | 9.94% |

**Resolution:** Rows with `passenger_count <= 0` were removed as part of the numeric validity filter. After filtering, all remaining columns had 0 missing values. The other missing columns (`RatecodeID`, `store_and_fwd_flag`, `congestion_surcharge`, `Airport_fee`) were not used as model features, so their missing values did not affect the final cleaned dataset.

---

### Risk 4 — Abnormal Timestamps ✅ Occurred and Resolved

**What happened:** Raw dataset contained timestamps from 2002 and as far as 2026.

```
Earliest pickup:  2002-12-31 16:46:07
Latest pickup:    2026-06-26 23:53:12
```

Total abnormal rows: 56 (0.000136% of dataset)

**Resolution:** Applied a strict date filter:
```
pickup >= 2024-01-01 and pickup < 2025-01-01
drop-off >= 2024-01-01 and drop-off < 2025-01-02
```

38,893 rows were removed in the timestamp filtering step (including duration-invalid rows).

---

### Risk 5 — Invalid Trip Distances ✅ Occurred and Resolved

**What happened:** Maximum raw trip distance was 398,608.62 miles — physically impossible.

```
Zero or negative trip_distance: 776,305 rows
trip_distance > 100 miles: 491 rows
```

**Resolution:** Applied two rules:
1. `trip_distance > 0` (numeric validity filter — removed most zero/negative cases)
2. `trip_distance <= 100` (outlier filter — removed extreme outliers)

---

### Risk 6 — Negative Fare Values ✅ Occurred and Resolved

**What happened:**
- Minimum raw `fare_amount`: -2,261.2
- Negative fare rows: 731,024

**Resolution:** Applied `fare_amount > 0` filter. Additional outlier cap of `fare_amount <= 500` removed 317 extreme high-end fares.

---

### Risk 10 — Spark Setup Issues ✅ Occurred and Resolved

Three separate issues were encountered and resolved:

**Issue 1 — Java compatibility:**
Spark 4.1.1 had compatibility problems with Java 25 (the default installed version).

*Resolution:* Installed OpenJDK 17.0.19. Spark ran successfully with Java 17.

**Issue 2 — Spark write failure on Windows (winutils.exe):**
When Spark tried to save the cleaned Parquet output, it failed with a Hadoop home directory error. Spark on Windows requires `winutils.exe` and `hadoop.dll` to write files.

*Resolution:* Downloaded `winutils.exe` and `hadoop.dll` for Hadoop 3.3.1, placed them in `C:\hadoop\bin`, set `HADOOP_HOME=C:\hadoop`.

**Issue 3 — HADOOP_HOME must be set before SparkSession:**
Setting `os.environ["HADOOP_HOME"]` after the SparkSession was already created had no effect — Spark had already initialised.

*Resolution:* Moved the HADOOP_HOME setup to the first cell of the notebook, before any imports or SparkSession creation.

---

### Risk 11 — Long Processing Time ✅ Partially Occurred

Some Spark operations on 41.1 million rows took noticeable time on a local machine (several minutes for full dataset counts, quantile calculations, and model training on 28.4M rows). This was expected and acceptable.

**Mitigation used:** `spark.sql.shuffle.partitions = 8` (reduced from default 200) to speed up aggregations on a single local machine.

---

## Final Data Quality Numbers

| Issue | Raw Count | After Preprocessing |
|---|---:|---:|
| Total rows | 41,169,720 | 35,602,215 |
| Missing values | 4,091,232 per column | 0 |
| Abnormal timestamps | 56 | 0 |
| Negative/zero fares | 748,284 | 0 |
| Zero trip distances | 776,305 | 0 |
| Extreme outliers | ~18,000 | 0 |

**Total rows removed during preprocessing: 5,567,505**
