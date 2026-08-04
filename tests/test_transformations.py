from pyspark.sql import SparkSession
import pytest

from glue.silver_to_gold.transformations import (
    validate_source_columns,
    add_hhmm_features,
    add_calendar_features,
    add_period_features,
    add_delay_features,
    add_status_features,
)

spark = (
    SparkSession.builder.master("local[1]")
    .appName("transformations-test")
    .getOrCreate()
)


def test_validate_source_columns_missing():
    df = spark.createDataFrame(
        [(1,)],
        ["Year"],
    )

    with pytest.raises(ValueError):
        validate_source_columns(df)


def test_add_hhmm_features():
    df = spark.createDataFrame(
        [(930,)],
        ["CRSDepTime"],
    )

    result = add_hhmm_features(
        df,
        "CRSDepTime",
        "Departure",
    )

    row = result.collect()[0]

    assert row["DepartureHour"] == 9
    assert row["DepartureMinute"] == 30
    assert row["DepartureTimeHHMM"] == "09:30"


def test_add_calendar_features():
    df = spark.createDataFrame(
        [(2025, 7, 6)],
        ["Year", "Month", "DayOfWeek"],
    )

    result = add_calendar_features(df)

    row = result.collect()[0]

    assert row["WeekendIndicator"] == 1
    assert row["SeasonIndicator"] == "Summer"


def test_add_period_features():
    df = spark.createDataFrame(
        [(8, 20)],
        ["DepartureHour", "ArrivalHour"],
    )

    result = add_period_features(df)

    row = result.collect()[0]

    assert row["DeparturePeriod"] == "Morning"
    assert row["ArrivalPeriod"] == "Evening"
    assert row["PeakHourIndicator"] == 1


def test_add_delay_features():
    df = spark.createDataFrame(
        [(10, 45, 0, 0)],
        [
            "DepDelay",
            "ArrDelay",
            "Cancelled",
            "Diverted",
        ],
    )

    result = add_delay_features(df)

    row = result.collect()[0]

    assert row["HasDepDelay"] == 1
    assert row["HasArrDelay"] == 1
    assert row["DelayCategory"] == "Minor Delay"


def test_add_status_features_completed():
    df = spark.createDataFrame(
        [(0, 0)],
        [
            "Cancelled",
            "Diverted",
        ],
    )

    result = add_status_features(df)

    row = result.collect()[0]

    assert row["FlightStatus"] == "Completed"
    assert row["CompletedFlightFlag"] == 1


def test_add_status_features_cancelled():
    df = spark.createDataFrame(
        [(1, 0)],
        [
            "Cancelled",
            "Diverted",
        ],
    )

    result = add_status_features(df)

    row = result.collect()[0]

    assert row["FlightStatus"] == "Cancelled"
