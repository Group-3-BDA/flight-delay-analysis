from pyspark.sql import SparkSession

from glue.silver_to_gold.validation import validate_fact

spark = (
    SparkSession.builder
    .master("local[1]")
    .appName("validation-tests")
    .getOrCreate()
)


def test_validate_fact_success():

    rows = [
        (
            1,
            1,
            1,
            1,
            1,
            1,
            1,
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

    df = spark.createDataFrame(rows, columns)

    validate_fact(df, 1)
