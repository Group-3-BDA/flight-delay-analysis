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
        "Departure_Time",
    ]


def test_convert_datatypes():
    df = spark.createDataFrame(
        [("2025-01-01",)],
        ["FlightDate"],
    )

    result = convert_datatypes(df)

    assert dict(result.dtypes)["FlightDate"] == "date"


def test_validate_dataframe_success():
    df = spark.createDataFrame(
        [(1,)],
        ["id"],
    )

    validate_dataframe(df)


def test_validate_dataframe_failure():
    df = spark.createDataFrame(
        [],
        "id INT",
    )

    with pytest.raises(ValueError):
        validate_dataframe(df)
