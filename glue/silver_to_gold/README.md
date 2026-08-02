# Silver-to-Gold Automation

This package converts the standardized Silver Parquet flight dataset into a business-ready Gold layer for analytics, dashboards, and machine learning.

The pipeline creates:

- `FACT_FLIGHTS`
- `DIM_AIRLINE`
- `DIM_AIRPORT`
- `DIM_DATE`
- `DIM_ROUTE`
- `ML_DATASET`
- `VIZ_DELAY_ANALYTICS`
- `VIZ_RELIABILITY_ANALYTICS`

The pipeline runs on Amazon EMR using Apache Spark on YARN.

---

## Repository Layout

```text
flight-delay-analysis/
└── glue/
    └── silver_to_gold/
        ├── main.py
        ├── config.py
        ├── constants.py
        ├── spark_setup.py
        ├── transformations.py
        ├── dimensions.py
        ├── ml_dataset.py
        ├── visualization.py
        ├── validation.py
        ├── writers.py
        ├── silver_to_gold_lib.zip
        └── README.md
```

### Module responsibilities

| Module | Responsibility |
|---|---|
| `main.py` | Pipeline entry point and execution orchestration |
| `config.py` | Input, output, partition, and runtime configuration |
| `constants.py` | Reusable constants and mappings |
| `spark_setup.py` | Spark session configuration |
| `transformations.py` | Silver-to-Gold feature engineering |
| `dimensions.py` | Fact and dimension table construction |
| `ml_dataset.py` | Training-history reliability features and ML dataset creation |
| `visualization.py` | Dashboard-oriented aggregate table creation |
| `validation.py` | Fail-fast output validation |
| `writers.py` | Gold Parquet output writes |

---

## Current S3 Paths

### Silver input

```text
s3://airline-dataset-2020-2025/Silver/Flight_Data_2020_2025/
```

### Gold output

```text
s3://airline-dataset-2020-2025/GoldA/
```

### Expected Gold structure

```text
Gold/
├── FACT_FLIGHTS/
├── DIM_AIRLINE/
├── DIM_AIRPORT/
├── DIM_DATE/
├── DIM_ROUTE/
├── ML_DATASET/
├── VIZ_DELAY_ANALYTICS/
└── VIZ_RELIABILITY_ANALYTICS/
```

The following tables are partitioned by `Year` and `Month`:

```text
FACT_FLIGHTS
ML_DATASET
VIZ_DELAY_ANALYTICS
VIZ_RELIABILITY_ANALYTICS
```

---

## Current EMR Configuration

```text
Amazon EMR version: 5.20.1
Spark version: 2.4.0
Python runtime: /usr/bin/python3

Primary nodes: 1
Core nodes: 4
Task nodes: 0
```

The production job uses:

```text
Executors: 4
Cores per executor: 2
Executor memory: 4 GB
Executor memory overhead: 2 GB
Total executor cores: 8
Shuffle partitions: 96
Dynamic allocation: disabled
```

---

# Pipeline Execution Flow

```text
Silver Parquet
      ↓
Repartition Silver data
      ↓
Build and persist Gold base using DISK_ONLY
      ↓
Build dimensions
      ↓
Build FACT_FLIGHTS
      ↓
Build ML_DATASET
      ↓
Build visualization aggregate tables
      ↓
Validate FACT_FLIGHTS
      ↓
Write all Gold outputs to S3
```

The visualization tables reuse the Fact and dimension DataFrames already created in the same Spark application. They do not read the newly written Gold data back from S3.

---

# Execution Steps

## 1. Connect to the EMR Primary Node

Use MobaXterm or SSH with the project PEM key:

```bash
ssh -i <pem-key-path> hadoop@<emr-primary-public-dns>
```

Move to the repository:

```bash
cd /home/hadoop/flight-delay-analysis
```

---

## 2. Pull the Latest Git Code

The active branch is `develop`.

```bash
git checkout develop
git pull origin develop
```

Move to the Silver-to-Gold folder:

```bash
cd /home/hadoop/flight-delay-analysis/glue/silver_to_gold
```

Verify the files:

```bash
pwd
ls -lh
```

Expected files:

```text
main.py
config.py
constants.py
spark_setup.py
transformations.py
dimensions.py
ml_dataset.py
visualization.py
validation.py
writers.py
silver_to_gold_lib.zip
README.md
```

---

## 3. Rebuild `silver_to_gold_lib.zip`

Rebuild the ZIP whenever any helper module changes.

`main.py` is submitted separately and must not be added to the helper ZIP.

```bash
rm -f silver_to_gold_lib.zip
```

```bash
zip -j silver_to_gold_lib.zip \
  config.py \
  constants.py \
  spark_setup.py \
  transformations.py \
  dimensions.py \
  ml_dataset.py \
  visualization.py \
  validation.py \
  writers.py
```

Verify the ZIP:

```bash
unzip -l silver_to_gold_lib.zip
```

Or:

```bash
zipinfo -1 silver_to_gold_lib.zip | sort
```

The modules must appear directly at the ZIP root:

```text
config.py
constants.py
dimensions.py
ml_dataset.py
spark_setup.py
transformations.py
validation.py
visualization.py
writers.py
```

Incorrect structure:

```text
silver_to_gold/config.py
silver_to_gold/visualization.py
```

Correct structure:

```text
config.py
visualization.py
```

---

## 4. Validate Python Files

Confirm Python 3:

```bash
/usr/bin/python3 --version
```

Validate all scripts:

```bash
/usr/bin/python3 -m py_compile \
  main.py \
  config.py \
  constants.py \
  spark_setup.py \
  transformations.py \
  dimensions.py \
  ml_dataset.py \
  visualization.py \
  validation.py \
  writers.py
```

No output means syntax validation passed.

---

## 5. Run an Import Validation Test

This detects missing or stale modules before submitting the full YARN application.

```bash
cat > /tmp/silver_to_gold_import_test.py <<'PY'
from config import PipelineConfig
from dimensions import (
    build_dim_airline,
    build_dim_airport,
    build_dim_date,
    build_dim_route,
    build_fact_flights,
)
from ml_dataset import build_ml_dataset
from spark_setup import configure_spark
from transformations import build_gold_base
from validation import validate_fact
from visualization import build_visualization_tables
from writers import write_gold_outputs

print("ALL SILVER-TO-GOLD IMPORTS PASSED")
PY
```

Run the test:

```bash
spark-submit \
  --master local[1] \
  --conf spark.pyspark.python=/usr/bin/python3 \
  --conf spark.pyspark.driver.python=/usr/bin/python3 \
  --py-files ./silver_to_gold_lib.zip \
  /tmp/silver_to_gold_import_test.py
```

Expected output:

```text
ALL SILVER-TO-GOLD IMPORTS PASSED
```

Do not submit the production application until this test passes.

---

## 6. Verify S3 Access

Check the Silver input:

```bash
aws s3 ls \
  s3://airline-dataset-2020-2025/Silver/Flight_Data_2020_2025/
```

Check the Gold output location:

```bash
aws s3 ls s3://airline-dataset-2020-2025/Gold/
```

Optional write-permission test:

```bash
echo "permission-test" > /tmp/gold-permission-test.txt
```

```bash
aws s3 cp \
  /tmp/gold-permission-test.txt \
  s3://airline-dataset-2020-2025/Gold/_permission_test/test.txt
```

```bash
aws s3 rm \
  s3://airline-dataset-2020-2025/Gold/_permission_test/test.txt
```

---

# Production Spark Submission

Run this command from:

```text
/home/hadoop/flight-delay-analysis/glue/silver_to_gold
```

```bash
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --driver-memory 2g \
  --conf spark.pyspark.python=/usr/bin/python3 \
  --conf spark.pyspark.driver.python=/usr/bin/python3 \
  --conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=/usr/bin/python3 \
  --conf spark.executorEnv.PYSPARK_PYTHON=/usr/bin/python3 \
  --conf spark.dynamicAllocation.enabled=false \
  --num-executors 4 \
  --executor-cores 2 \
  --executor-memory 4g \
  --conf spark.executor.memoryOverhead=2048 \
  --py-files ./silver_to_gold_lib.zip \
  ./main.py \
  --job-name silver-to-gold-production \
  --input-path s3://airline-dataset-2020-2025/Silver/Flight_Data_2020_2025/ \
  --gold-base-path s3://airline-dataset-2020-2025/GoldA/ \
  --output-mode overwrite \
  --train-end-date 2023-12-31 \
  --validation-year 2024 \
  --test-year 2025 \
  --shuffle-partitions 96
```

# Storage and Caching Strategy

The current implementation uses the following strategy:

```text
Silver DataFrame
└── Repartitioned to 96 partitions

Gold Base
└── Persisted once using DISK_ONLY

DIM_AIRLINE
DIM_AIRPORT
DIM_ROUTE
└── Persisted temporarily using MEMORY_AND_DISK
```

The cached dimensions are reused by:

- `FACT_FLIGHTS`
- `VIZ_DELAY_ANALYTICS`
- `VIZ_RELIABILITY_ANALYTICS`

All cached DataFrames are unpersisted before the Spark application stops.

The pipeline does not use `localCheckpoint()`.

---

# Gold Output Definitions

## `FACT_FLIGHTS`

Grain:

```text
One row per flight
```

Purpose:

- detailed operational analysis;
- airline, airport, route, and date reporting;
- drill-through from BI dashboards;
- source for aggregate visualization tables.

Partitioning:

```text
Year
Month
```

---

## Dimension Tables

### `DIM_AIRLINE`

Contains airline identifiers, descriptive attributes, flight statistics, and historical reliability metrics.

### `DIM_AIRPORT`

Contains airport identifiers, city, state, region, operational statistics, and departure/arrival reliability metrics.

### `DIM_DATE`

Contains reusable calendar attributes for time-based analysis.

### `DIM_ROUTE`

Contains route identifiers, origin-destination information, route statistics, and historical reliability metrics.

---

## `ML_DATASET`

Purpose:

- pre-departure arrival-delay prediction;
- default target: `ArrDel15`;
- chronological train, validation, and test split;
- training-history-only reliability features.

Split configuration:

```text
Train: flights up to 2023-12-31
Validation: 2024
Test: 2025
```

The ML dataset excludes cancelled and diverted flights because the `ArrDel15` target is intended for eligible completed-flight delay prediction.

Partitioning:

```text
Year
Month
```

---

## `VIZ_DELAY_ANALYTICS`

Purpose:

- Power BI and Tableau delay dashboards;
- reduce repeated scans of the flight-level Fact table;
- provide dashboard-ready delay KPIs.

Grain:

```text
Year
+ Month
+ Marketing Airline
+ Primary Delay Cause
+ Delay Category
+ Season
```

Main measures include:

- total flights;
- on-time flights;
- delayed flights;
- cancelled flights;
- diverted flights;
- average arrival and departure delay;
- average delay by cause;
- total delay minutes by cause;
- arrival-delay duration buckets;
- delay rate;
- on-time percentage;
- cancellation rate;
- diversion rate;
- delay contribution percentage.

Partitioning:

```text
Year
Month
```

---

## `VIZ_RELIABILITY_ANALYTICS`

Purpose:

- Power BI and Tableau airline, airport, and route reliability dashboards;
- combine monthly operational KPIs with descriptive reliability attributes.

Grain:

```text
Year
+ Month
+ Season
+ Marketing Airline
+ Route
+ Origin Airport
+ Destination Airport
```

Main fields include:

- airline reliability attributes;
- route reliability attributes;
- origin-airport reliability attributes;
- destination-airport reliability attributes;
- total and completed flights;
- cancelled and diverted flights;
- on-time and delayed flights;
- average arrival and departure delay;
- delay-cause measures;
- on-time percentage;
- delay rate;
- cancellation rate;
- diversion rate;
- completion rate.

The historical reliability attributes come from the dimension tables. The period-specific operational rates are calculated from each visualization-table group.

Partitioning:

```text
Year
Month
```

---

# Reliability and ML Behavior

Reliability scores are calculated from historical actual flight performance before ML model training. They are input features, not outputs of the delay prediction model.

The weighted reliability formula used for airline, origin-airport, and route analysis is based on:

```text
70% on-time performance
20% non-cancellation performance
10% non-diversion performance
```

The ML dataset uses reliability features calculated from training history only, preventing 2024 and 2025 actual outcomes from leaking into validation and test features.

The column below records this behavior:

```text
ReliabilityFeatureScope = TRAINING_HISTORY_ONLY
```

---

# Monitor the Application

## List running applications

```bash
yarn application -list -appStates RUNNING
```

## Check application status

```bash
yarn application -status application_XXXXXXXXXXXX_XXXX
```

A successful application should show:

```text
State       : FINISHED
Final-State : SUCCEEDED
```

## Check cluster nodes

```bash
yarn node -list
```

All four core nodes should show:

```text
RUNNING
```

## Open the Spark UI

Use the tracking URL returned by:

```bash
yarn application -status application_XXXXXXXXXXXX_XXXX
```

Monitor:

```text
Jobs
Stages
Executors
Storage
SQL
```

Healthy execution indicators:

- active executors;
- zero dead executors;
- no continuously increasing failed tasks;
- stage task counts gradually increasing;
- output write stages completing successfully.

---

# View Logs

For a completed or failed application:

```bash
yarn logs \
  -applicationId application_XXXXXXXXXXXX_XXXX \
  > /tmp/silver-to-gold.log
```

Inspect the final lines:

```bash
tail -n 200 /tmp/silver-to-gold.log
```

Search for errors:

```bash
grep -n -A 20 -B 10 \
  -E "ERROR|Exception|Traceback|SyntaxError|ImportError|ModuleNotFoundError|OutOfMemory|ExecutorLost|FetchFailed|Killed" \
  /tmp/silver-to-gold.log
```

When an application fails with YARN exit code `13`, inspect the ApplicationMaster logs for Python syntax, import, permission, or stale-ZIP errors.

---

# Stop a Stuck or Incorrect Application

List running applications:

```bash
yarn application -list -appStates RUNNING
```

Kill the required application:

```bash
yarn application -kill application_XXXXXXXXXXXX_XXXX
```

Do not kill a job only because a large stage takes several minutes. Confirm that there is no task progress and no new driver output first.

---

# Verify Gold Output

After successful completion:

```bash
aws s3 ls s3://airline-dataset-2020-2025/Gold/
```

Expected directories:

```text
DIM_AIRLINE/
DIM_AIRPORT/
DIM_DATE/
DIM_ROUTE/
FACT_FLIGHTS/
ML_DATASET/
VIZ_DELAY_ANALYTICS/
VIZ_RELIABILITY_ANALYTICS/
```

## Verify Fact partitions

```bash
aws s3 ls \
  s3://airline-dataset-2020-2025/Gold/FACT_FLIGHTS/
```

Expected:

```text
Year=2020/
Year=2021/
Year=2022/
Year=2023/
Year=2024/
Year=2025/
```

Check one year:

```bash
aws s3 ls \
  s3://airline-dataset-2020-2025/Gold/FACT_FLIGHTS/Year=2025/
```

Expected month partitions:

```text
Month=1/
Month=2/
...
Month=12/
```

## Verify ML partitions

```bash
aws s3 ls \
  s3://airline-dataset-2020-2025/Gold/ML_DATASET/
```

## Verify delay-visualization partitions

```bash
aws s3 ls \
  s3://airline-dataset-2020-2025/Gold/VIZ_DELAY_ANALYTICS/
```

## Verify reliability-visualization partitions

```bash
aws s3 ls \
  s3://airline-dataset-2020-2025/Gold/VIZ_RELIABILITY_ANALYTICS/
```

The four partitioned outputs should contain `Year=` and `Month=` folders.

## Check total Gold output size

```bash
aws s3 ls \
  s3://airline-dataset-2020-2025/Gold/ \
  --recursive \
  --summarize \
  --human-readable
```

---

# BI Consumption

Recommended connection path:

```text
Gold Parquet in S3
        ↓
AWS Glue Data Catalog
        ↓
Amazon Athena
        ↓
Power BI or Tableau
```

Register all eight Gold tables in the Glue Data Catalog.

For dashboard pages, prefer:

```text
VIZ_DELAY_ANALYTICS
VIZ_RELIABILITY_ANALYTICS
```

Use `FACT_FLIGHTS` for detailed drill-through and ad hoc analysis.

Do not use `ML_DATASET` as the primary business dashboard source because it has ML-specific filtering, train/validation/test splits, and model features.

---

# Pipeline Behavior

- `CancellationCode` is excluded.
- `Tail_Number` is excluded.
- Fact and dimension tables use star-schema modelling.
- Reliability metrics remain in dimension tables.
- The ML dataset uses training-history-only reliability features.
- Cancelled and diverted flights are excluded from the `ArrDel15` ML dataset.
- Visualization tables are generated inside the same Spark application.
- Visualization tables reuse the existing Fact and dimension DataFrames.
- The Gold base is persisted once using `DISK_ONLY`.
- Reused dimensions are temporarily persisted using `MEMORY_AND_DISK`.
- The pipeline does not use `localCheckpoint()`.

---

# Team Workflow

## Ingestion Team

1. Download source flight files.
2. Upload raw files to the Bronze S3 layer.
3. Confirm Bronze completion.

## Bronze-to-Silver Team

1. Run the Bronze-to-Silver pipeline.
2. Standardize the required Silver columns.
3. Write Silver Parquet data.
4. Confirm Silver completion.

## Silver-to-Gold Team

1. Pull the latest `develop` branch.
2. Rebuild `silver_to_gold_lib.zip`.
3. Confirm `visualization.py` is present in the ZIP.
4. Validate Python syntax.
5. Run the import validation test.
6. Verify Silver and Gold S3 access.
7. Submit the Spark application in YARN cluster mode.
8. Monitor the Spark application.
9. Verify all eight Gold outputs.
10. Run Gold validation.
11. Register all Gold tables in AWS Glue Data Catalog.
12. Query the tables using Athena.
13. Connect Power BI or Tableau to the visualization tables.

---
