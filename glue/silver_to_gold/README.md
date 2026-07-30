# Silver-to-Gold Automation

This package converts the standardized Silver Parquet flight dataset into:

- FACT_FLIGHTS
- DIM_AIRLINE
- DIM_AIRPORT
- DIM_DATE
- DIM_ROUTE
- ML_DATASET

The pipeline runs on Amazon EMR using Spark on YARN.

---

## Repository Layout

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
        ├── validation.py
        ├── writers.py
        ├── silver_to_gold_lib.zip
        └── README.md

---

## Current Paths

### Silver input

s3://airline-dataset-2020-2025/Silver/Flight_Data_2020_2025/

### Gold output

s3://airline-dataset-2020-2025/Gold/

Expected Gold output structure:

Gold/
├── FACT_FLIGHTS/
├── DIM_AIRLINE/
├── DIM_AIRPORT/
├── DIM_DATE/
├── DIM_ROUTE/
└── ML_DATASET/

FACT_FLIGHTS and ML_DATASET are partitioned by Year and Month.

---

## Current EMR Configuration

Amazon EMR version: 5.20.1
Spark version: 2.4.0
Python runtime: /usr/bin/python3

Primary nodes: 1
Core nodes: 4
Task nodes: 0

The production job uses four fixed Spark executors and disables dynamic allocation.

---

# Execution Steps

## 1. Connect to the EMR Primary Node

Use MobaXterm or SSH:

```bash
ssh -i .pem key used
```

Move to the repository:

```bash
cd /home/hadoop/flight-delay-analysis
```

---

## 2. Pull the Latest Git Code

The active branch is develop.

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

main.py
config.py
constants.py
spark_setup.py
transformations.py
dimensions.py
ml_dataset.py
validation.py
writers.py
silver_to_gold_lib.zip
README.md

---

## 3. Rebuild silver_to_gold_lib.zip

Rebuild the ZIP whenever any helper module changes.

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
  validation.py \
  writers.py
```

Verify:

```bash
unzip -l silver_to_gold_lib.zip
```

The modules must appear directly at the ZIP root:

config.py
constants.py
spark_setup.py
transformations.py
dimensions.py
ml_dataset.py
validation.py
writers.py

---

## 4. Validate Python Files

Confirm Python 3:

```bash
/usr/bin/python3 --version
```

Validate all scripts:

```bash
/ usr/bin/python3 -m py_compile \
  main.py \
  config.py \
  constants.py \
  spark_setup.py \
  transformations.py \
  dimensions.py \
  ml_dataset.py \
  validation.py \
  writers.py
```

No output means syntax validation passed.

---

## 5. Verify S3 Access

Check the Silver input:

```bash
aws s3 ls s3://airline-dataset-2020-2025/Silver/Flight_Data_2020_2025/
```

Check the Gold output location:

```bash
aws s3 ls s3://airline-dataset-2020-2025/Gold/
```

Optional write-permission test:

```bash
echo "permission-test" > /tmp/gold-permission-test.txt

aws s3 cp \
  /tmp/gold-permission-test.txt \
  s3://airline-dataset-2020-2025/Gold/_permission_test/test.txt

aws s3 rm \
  s3://airline-dataset-2020-2025/Gold/_permission_test/test.txt
```

---

# Production Spark Submission

Run this command from:

/home/hadoop/flight-delay-analysis/glue/silver_to_gold

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
  --gold-base-path s3://airline-dataset-2020-2025/Gold/ \
  --output-mode overwrite \
  --train-end-date 2023-12-31 \
  --validation-year 2024 \
  --test-year 2025 \
  --shuffle-partitions 96
```

Important:

- Run only after rebuilding silver_to_gold_lib.zip.
- Every \ must be the final character on its line.
- Do not place spaces after \.
- Do not submit another Silver-to-Gold job while one is already running.
- YARN cluster mode allows the job to continue if MobaXterm disconnects.

---

# Spark Resource Configuration

Executors: 4
Cores per executor: 2
Executor memory: 4 GB
Executor memory overhead: 2 GB
Total executor cores: 8
Shuffle partitions: 96
Dynamic allocation: disabled

The Gold base is:

- repartitioned into 96 partitions
- persisted once using DISK_ONLY
- processed without localCheckpoint()

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

Successful completion should show:

State       : FINISHED
Final-State : SUCCEEDED

## Check cluster nodes

```bash
yarn node -list
```

All four core nodes should show RUNNING.

## Open Spark UI

Use the tracking URL returned by:

```bash
yarn application -status application_XXXXXXXXXXXX_XXXX
```

Monitor:

Jobs
Stages
Executors
Storage
SQL

Healthy execution indicators:

- active executors
- zero dead executors
- zero failed tasks
- stage task counts gradually increasing

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
  -E "ERROR|Exception|Traceback|OutOfMemory|ExecutorLost|FetchFailed|Killed" \
  /tmp/silver-to-gold.log
```

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

Do not kill a job only because one large stage takes several minutes. Confirm that there is no task progress and no new driver output first.

---

# Verify Gold Output

After successful completion:

```bash
aws s3 ls s3://airline-dataset-2020-2025/Gold/
```

Expected directories:

DIM_AIRLINE/
DIM_AIRPORT/
DIM_DATE/
DIM_ROUTE/
FACT_FLIGHTS/
ML_DATASET/

## Verify Fact partitions

```bash
aws s3 ls \
  s3://airline-dataset-2020-2025/Gold/FACT_FLIGHTS/
```

Expected:

Year=2020/
Year=2021/
Year=2022/
Year=2023/
Year=2024/
Year=2025/

Check one year:

```bash
aws s3 ls \
  s3://airline-dataset-2020-2025/Gold/FACT_FLIGHTS/Year=2025/
```

Expected month partitions:

Month=1/
Month=2/
...
Month=12/

## Verify ML partitions

```bash
aws s3 ls \
  s3://airline-dataset-2020-2025/Gold/ML_DATASET/
```

The ML dataset should also contain Year= and Month= partitions.

## Check total Gold output size

```bash
aws s3 ls \
  s3://airline-dataset-2020-2025/Gold/ \
  --recursive \
  --summarize \
  --human-readable
```

---

# Pipeline Behavior

- CancellationCode is excluded.
- Tail_Number is excluded.
- Reliability metrics remain in dimension tables.
- The ML dataset uses training-history-only reliability features.
- Cancelled and diverted flights are excluded from the ArrDel15 ML dataset.
- The ML split is:

Train: flights up to 2023-12-31
Validation: 2024
Test: 2025

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

1. Pull the latest develop branch.
2. Rebuild silver_to_gold_lib.zip.
3. Validate Python syntax.
4. Verify Silver and Gold S3 access.
5. Submit the Spark job in YARN cluster mode.
6. Monitor the Spark application.
7. Verify all six Gold outputs.
8. Run Gold validation.
9. Register Gold tables in AWS Glue Data Catalog.
10. Query the tables using Athena or connect them to Power BI.

---

# Important Notes

- Do not run the production job through Jupyter.
- Do not use localCheckpoint() in this pipeline.
- Do not use coalesce(1) for large outputs.
- Do not manually broadcast the route dimension.
- Do not persist the training-history DataFrame separately.
- Do not reuse an old ZIP after changing helper modules.
- Do not run multiple Silver-to-Gold applications simultaneously.
- Use YARN cluster mode for production execution.
