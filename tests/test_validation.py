from pyspark.sql import SparkSession
import pytest

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
)






from glue.silver_to_gold.validation import validate_fact

spark = (
    SparkSession.builder
    .master("local[1]")
    .appName("validation-test")
    .getOrCreate()
)


def test_validate_fact_success():

    data = [
        (
            "F1",
            20250101,
            "A1",
            "O1",
            "OR1",
            "DE1",
            "R1",
        )
    ]

    columns = [
        "FlightKey",
        "DateKey",
        "MarketingAirlineKey",
        "OperatingAirlineKey",
        "OriginAirportKey",
        "DestAirportKey",
        "RouteKey",
    ]

    df = spark.createDataFrame(data, columns)

    validate_fact(df, 1)


def test_validate_fact_row_count_failure():

    data = [
        (
            "F1",
            20250101,
            "A1",
            "O1",
            "OR1",
            "DE1",
            "R1",
        )
    ]

    columns = [
        "FlightKey",
        "DateKey",
        "MarketingAirlineKey",
        "OperatingAirlineKey",
        "OriginAirportKey",
        "DestAirportKey",
        "RouteKey",
    ]

    df = spark.createDataFrame(data, columns)

    with pytest.raises(ValueError):
        validate_fact(df, 2)


def test_validate_fact_null_key_failure():

    data = [
        (
            None,
            20250101,
            "A1",
            "O1",
            "OR1",
            "DE1",
            "R1",
        )
    ]

    columns = [
        "FlightKey",
        "DateKey",
        "MarketingAirlineKey",
        "OperatingAirlineKey",
        "OriginAirportKey",
        "DestAirportKey",
        "RouteKey",
    ]    
    schema = StructType([
        StructField("FlightKey", StringType(), True),
        StructField("DateKey", IntegerType(), True),
        StructField("MarketingAirlineKey", StringType(), True),
        StructField("OperatingAirlineKey", StringType(), True),
        StructField("OriginAirportKey", StringType(), True),
        StructField("DestAirportKey", StringType(), True),
        StructField("RouteKey", StringType(), True),
    ])

    df = spark.createDataFrame(data, schema)


    with pytest.raises(ValueError):
        validate_fact(df, 1)
