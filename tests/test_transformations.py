import pytest
from pyspark.sql import SparkSession

from glue.silver_to_gold.transformations import (
    validate_source_columns,
)

spark = (
    SparkSession.builder
    .master("local[1]")
    .appName("transformation-tests")
    .getOrCreate()
)


def test_validate_source_columns_missing():

    df = spark.createDataFrame(
        [(1,)],
        ["FlightID"]
    )

    with pytest.raises(ValueError):
        validate_source_columns(df)
