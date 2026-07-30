"""Glue/EMR entry point for the Silver-to-Gold pipeline."""

import argparse
import sys
import traceback

from pyspark.sql import SparkSession
from pyspark import StorageLevel

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
from writers import write_gold_outputs


def _glue_arguments():
    # EMR spark-submit does not provide --JOB_NAME.
    # Only use Glue argument parsing when the Glue-specific argument exists.
    if "--JOB_NAME" not in sys.argv:
        return None

    try:
        from awsglue.utils import getResolvedOptions
    except ImportError:
        return None

    required = ["JOB_NAME", "INPUT_PATH", "GOLD_BASE_PATH"]
    optional = [
        "OUTPUT_MODE",
        "TRAIN_END_DATE",
        "VALIDATION_YEAR",
        "TEST_YEAR",
        "SHUFFLE_PARTITIONS",
    ]

    present = [
        name
        for name in optional
        if "--{}".format(name) in sys.argv
    ]

    args = getResolvedOptions(sys.argv, required + present)
    return {
        "job_name": args["JOB_NAME"],
        "input_path": args["INPUT_PATH"],
        "gold_base_path": args["GOLD_BASE_PATH"],
        "output_mode": args.get("OUTPUT_MODE", "overwrite"),
        "train_end_date": args.get(
            "TRAIN_END_DATE",
            "2023-12-31",
        ),
        "validation_year": int(args.get("VALIDATION_YEAR", 2024)),
        "test_year": int(args.get("TEST_YEAR", 2025)),
        "shuffle_partitions": int(
            args.get("SHUFFLE_PARTITIONS", 64)
        ),
        "is_glue": True,
    }


def _emr_arguments():
    parser = argparse.ArgumentParser(
        description="Silver-to-Gold airline transformation job"
    )
    parser.add_argument("--job-name", default="silver-to-gold")
    parser.add_argument("--input-path", required=True)
    parser.add_argument("--gold-base-path", required=True)
    parser.add_argument("--output-mode", default="overwrite")
    parser.add_argument("--train-end-date", default="2023-12-31")
    parser.add_argument("--validation-year", type=int, default=2024)
    parser.add_argument("--test-year", type=int, default=2025)
    parser.add_argument(
        "--shuffle-partitions",
        type=int,
        default=64,
    )
    parsed = parser.parse_args()
    values = vars(parsed)
    values["is_glue"] = False
    return values


def _parse_arguments():
    glue_args = _glue_arguments()
    if glue_args is not None:
        return glue_args
    return _emr_arguments()


def run_pipeline(spark, config):
    silver_df = spark.read.parquet(config.input_path)

    gold_base_df = (
        build_gold_base(silver_df)
        .repartition(config.shuffle_partitions)
        .persist(StorageLevel.DISK_ONLY)
    )

    print(
        "Gold Base repartitioned to {} partitions.".format(
            gold_base_df.rdd.getNumPartitions()
        )
    )

    print("Gold Base persisted. Triggering materialization...")

    gold_base_count = gold_base_df.count()
    print("Gold Base Row Count:", gold_base_count)

    print(
        "Gold Base materialized successfully. Rows = {}".format(
            gold_base_count
        )
    )

    try:
        print("Building DIM_DATE...")
        dim_date_df = build_dim_date(gold_base_df)

        print("Building DIM_AIRLINE...")
        dim_airline_df = build_dim_airline(gold_base_df)

        print("Building DIM_AIRPORT...")
        dim_airport_df = build_dim_airport(gold_base_df)

        print("Building DIM_ROUTE...")
        dim_route_df = build_dim_route(gold_base_df)

        print("Building FACT_FLIGHTS...")
        fact_flights_df = build_fact_flights(gold_base_df)

        print("Building ML_DATASET...")
        ml_dataset_df = build_ml_dataset(
            gold_base_df=gold_base_df,
            train_end_date=config.train_end_date,
            validation_year=config.validation_year,
            test_year=config.test_year,
        )

        print("Validating FACT_FLIGHTS...")
        validate_fact(
            fact_df=fact_flights_df,
            expected_row_count=gold_base_count,
        )

        print("Writing Gold tables...")
        write_gold_outputs(
            fact_flights_df=fact_flights_df,
            dim_airline_df=dim_airline_df,
            dim_airport_df=dim_airport_df,
            dim_date_df=dim_date_df,
            dim_route_df=dim_route_df,
            ml_dataset_df=ml_dataset_df,
            config=config,
        )

        print(
            "Silver-to-Gold pipeline completed successfully. Source rows: {}".format(
                gold_base_count
            )
        )

    finally:
        gold_base_df.unpersist(blocking=True)

def main():
    arguments = _parse_arguments()

    spark = (
        SparkSession.builder
        .appName(arguments["job_name"])
        .getOrCreate()
    )


    print("Python executable:", sys.executable)
    print("Spark version:", spark.version)

    config = PipelineConfig(
        input_path=arguments["input_path"],
        gold_base_path=arguments["gold_base_path"],
        output_mode=arguments["output_mode"],
        train_end_date=arguments["train_end_date"],
        validation_year=arguments["validation_year"],
        test_year=arguments["test_year"],
        shuffle_partitions=arguments["shuffle_partitions"],
    )

    configure_spark(spark, config)

    glue_job = None
    if arguments["is_glue"]:
        from awsglue.context import GlueContext
        from awsglue.job import Job

        glue_context = GlueContext(spark.sparkContext)
        glue_job = Job(glue_context)
        glue_job.init(arguments["job_name"], arguments)

    try:
        run_pipeline(spark, config)
        if glue_job is not None:
            glue_job.commit()
    except Exception:
        traceback.print_exc()
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
