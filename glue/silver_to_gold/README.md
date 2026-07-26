# Silver-to-Gold Automation

This package converts the standardized Silver Parquet flight dataset into:

- `FACT_FLIGHTS`
- `DIM_AIRLINE`
- `DIM_AIRPORT`
- `DIM_DATE`
- `DIM_ROUTE`
- `ML_DATASET`

## Repository layout

```text
silver_to_gold/
├── main.py
├── config.py
├── constants.py
├── spark_setup.py
├── transformations.py
├── dimensions.py
├── ml_dataset.py
├── validation.py
├── writers.py
└── README.md
```

## EMR execution

Upload the folder to the primary node or S3, then run:

```bash
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --py-files s3://YOUR-BUCKET/jobs/silver_to_gold_lib.zip \
  s3://YOUR-BUCKET/jobs/main.py \
  --job-name silver-to-gold \
  --input-path s3://airline-dataset-2020-2025/Silver/Flight_Data_2020_2025/ \
  --gold-base-path s3://airline-dataset-2020-2025/Gold/ \
  --output-mode overwrite \
  --train-end-date 2023-12-31 \
  --validation-year 2024 \
  --test-year 2025 \
  --shuffle-partitions 64
```

Use `main.py` as the main script and package the remaining Python modules into `silver_to_gold_lib.zip`.

## AWS Glue execution

Upload `main.py` as the Glue script. Upload the library zip and configure it under:

```text
Job details → Advanced properties → Python library path
```

Example job parameters:

```text
--INPUT_PATH
s3://airline-dataset-2020-2025/Silver/Flight_Data_2020_2025/

--GOLD_BASE_PATH
s3://airline-dataset-2020-2025/Gold/

--OUTPUT_MODE
overwrite

--TRAIN_END_DATE
2023-12-31

--VALIDATION_YEAR
2024

--TEST_YEAR
2025

--SHUFFLE_PARTITIONS
64
```

Glue automatically provides `--JOB_NAME`.

## Recommended cluster

For the current dataset:

```text
1 Primary: m5.xlarge
2 Core:    m5.xlarge
```

The pipeline persists only the reusable Gold base with `DISK_ONLY`.
The training subset is not separately cached.

## Important behavior

- Fact and ML outputs are partitioned by `Year` and `Month`.
- Reliability values remain in dimensions.
- The ML dataset receives training-history-only reliability features.
- Cancelled and diverted flights are excluded from the `ArrDel15` model dataset.
- `CancellationCode` and `Tail_Number` are intentionally excluded.

