from pyspark.sql import SparkSession

from glue.bronze_to_silver.utils import select_required_columns


spark = (
    SparkSession.builder
    .master("local[1]")
    .appName("unit-test")
    .getOrCreate()
)


def test_select_required_columns():

    data = [
        (1, "AA", "JFK"),
        (2, "DL", "ATL")
    ]

    df = spark.createDataFrame(
        data,
        ["FlightID", "Airline", "Origin"]
    )

    result = select_required_columns(
        df,
        ["FlightID", "Airline"]
    )

    assert result.columns == [
        "FlightID",
        "Airline"
    ]
