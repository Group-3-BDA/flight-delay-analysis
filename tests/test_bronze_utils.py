from pyspark.sql import SparkSession
import pytest

from glue.bronze_to_silver.utils import (
    select_required_columns,
    standardize_column_names,
    convert_datatypes,
    validate_dataframe,
)

spark = (
    SparkSession.builder
    .master("local[1]")
    .appName("unit-test")
    .getOrCreate()
)


def test_select_required_columns():
    data = [(1, "AA", "JFK"), (2, "DL", "ATL")]

    df = spark.createDataFrame(
        data,
        ["FlightID", "Airline", "Origin"],
    )

    result = select_required_columns(
        df,
        ["FlightID", "Airline"],
    )

    assert result.columns == [
        "FlightID",
        "Airline",
    ]


def test_standardize_column_names():
    df = spark.createDataFrame(
        [(1, 2)],
        ["Flight Number", "Departure-Time"],
    )

    result = standardize_column_names(df)

    assert result.columns == [
        "Flight_Number",
        "Departure-Time",
    ]


def test_convert_datatypes():

    df = spark.createDataFrame(
        [("2025-01-01",)],
        ["FlightDate"],
    )

    datatype_mapping = {
        "FlightDate": "date",
    }

    result = convert_datatypes(
        df,
        datatype_mapping,
    )

    assert dict(result.dtypes)["FlightDate"] == "date"

def test_validate_dataframe_success():
    df = spark.createDataFrame(
        [(1,)],
        ["id"],
    )

    validate_dataframe(df)


