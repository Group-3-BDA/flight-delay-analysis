"""
bronze_to_silver.py
-------------------
Bronze to Silver ETL Pipeline
"""

# ==========================================================
# IMPORTS
# ==========================================================

import logging
from pyspark.sql import SparkSession

import config

from utils import (
    select_required_columns,
    standardize_column_names,
    convert_datatypes,
    validate_dataframe,
    write_silver
)

# ==========================================================
# LOGGER CONFIGURATION
# ==========================================================

logging.basicConfig(
    level=getattr(logging, config.LOG_LEVEL),
    format=config.LOG_FORMAT
)

logger = logging.getLogger(__name__)

# ==========================================================
# CREATE SPARK SESSION
# ==========================================================

def create_spark_session():

    logger.info("Creating Spark Session...")

    spark = (
        SparkSession.builder
        .appName(config.APP_NAME)
        .config(
            "spark.sql.shuffle.partitions",
            config.SHUFFLE_PARTITIONS
        )
        .config(
            "spark.sql.adaptive.enabled",
            str(config.ADAPTIVE_EXECUTION).lower()
        )
        .getOrCreate()
    )

    logger.info("Spark Session Created Successfully")

    return spark


# ==========================================================
# READ BRONZE DATA
# ==========================================================

def read_bronze_data(spark):

    logger.info("Reading Bronze Dataset...")

    bronze_df = (
        spark.read
        .format(config.READ_FORMAT)
        .options(**config.CSV_OPTIONS)
        .load(config.BRONZE_PATHS)
    )

    logger.info("Bronze Dataset Loaded Successfully")

    return bronze_df


# ==========================================================
# MAIN FUNCTION
# ==========================================================

def main():

    spark = None

    try:

        logger.info("=" * 60)
        logger.info("Starting Bronze to Silver ETL Pipeline")
        logger.info("=" * 60)

        # Create Spark Session
        spark = create_spark_session()

        # Read Bronze Layer
        bronze_df = read_bronze_data(spark)

        # Step 1 - Standardize column names
        bronze_df = standardize_column_names(bronze_df)

        # Step 2 - Select required columns
        silver_df = select_required_columns(
            bronze_df,
            config.REQUIRED_COLUMNS
        )

        

        # Step 3 - Convert datatypes
        silver_df = convert_datatypes(
            silver_df,
            config.DATATYPE_MAPPING
        )

        # Step 4 - Validate dataframe
        if validate_dataframe(silver_df):

            logger.info("Validation Successful")

            # Step 5 - Write Silver layer
            write_silver(
                silver_df,
                config.SILVER_PATH
            )

        else:

            logger.error("Validation Failed")

        logger.info("Pipeline Completed Successfully")

    except Exception as e:

        logger.exception(f"Pipeline Failed: {e}")
        raise

    finally:

        if spark:
            spark.stop()
            logger.info("Spark Session Stopped")


if __name__ == "__main__":
    main()
