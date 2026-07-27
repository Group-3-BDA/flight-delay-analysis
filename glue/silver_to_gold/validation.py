"""Minimal fail-fast checks used inside the production job."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def validate_fact(
    fact_df: DataFrame,
    expected_row_count: int,
) -> None:
    actual_count = fact_df.count()
    if actual_count != expected_row_count:
        raise ValueError(
            "FACT_FLIGHTS row count mismatch: expected {}, got {}".format(
                expected_row_count,
                actual_count,
            )
        )

    key_columns = [
        "FlightKey",
        "DateKey",
        "MarketingAirlineKey",
        "OperatingAirlineKey",
        "OriginAirportKey",
        "DestAirportKey",
        "RouteKey",
    ]

    null_counts = fact_df.select(
        *[
            F.sum(
                F.when(F.col(column_name).isNull(), 1).otherwise(0)
            ).alias(column_name)
            for column_name in key_columns
        ]
    ).collect()[0].asDict()

    invalid = {
        key: value
        for key, value in null_counts.items()
        if value not in (None, 0)
    }
    if invalid:
        raise ValueError(
            "Required Gold keys contain null values: {}".format(invalid)
        )
