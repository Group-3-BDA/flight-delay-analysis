"""Leakage-controlled pre-departure ML dataset generation."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def _reliability_score(
    on_time_rate,
    cancellation_rate,
    diversion_rate,
):
    return F.round(
        100
        * (
            0.70 * F.coalesce(on_time_rate, F.lit(0.0))
            + 0.20 * (1 - F.coalesce(cancellation_rate, F.lit(0.0)))
            + 0.10 * (1 - F.coalesce(diversion_rate, F.lit(0.0)))
        ),
        2,
    )


def build_training_scores(
    gold_base_df: DataFrame,
    train_end_date: str,
):
    history = gold_base_df.filter(
        F.col("FlightDate") <= F.to_date(F.lit(train_end_date))
    )

    airline_scores = (
        history.groupBy(F.col("MarketingAirlineKey").alias("AirlineKey"))
        .agg(
            F.avg(
                F.when(
                    (F.col("Cancelled") == 0)
                    & (F.col("Diverted") == 0)
                    & F.col("ArrDelay").isNotNull(),
                    F.when(
                        F.col("ArrDelay") <= 15,
                        1.0,
                    ).otherwise(0.0),
                )
            ).alias("OnTimeRate"),
            F.avg(
                F.when(
                    F.col("Cancelled") == 1,
                    1.0,
                ).otherwise(0.0)
            ).alias("CancellationRate"),
            F.avg(
                F.when(
                    F.col("Diverted") == 1,
                    1.0,
                ).otherwise(0.0)
            ).alias("DiversionRate"),
        )
        .withColumn(
            "AirlineReliabilityScore",
            _reliability_score(
                F.col("OnTimeRate"),
                F.col("CancellationRate"),
                F.col("DiversionRate"),
            ),
        )
        .select("AirlineKey", "AirlineReliabilityScore")
    )

    origin_scores = (
        history.groupBy(F.col("OriginAirportKey").alias("AirportKey"))
        .agg(
            F.avg(
                F.when(
                    (F.col("Cancelled") == 0) & F.col("DepDelay").isNotNull(),
                    F.when(
                        F.col("DepDelay") <= 15,
                        1.0,
                    ).otherwise(0.0),
                )
            ).alias("OnTimeRate"),
            F.avg(
                F.when(
                    F.col("Cancelled") == 1,
                    1.0,
                ).otherwise(0.0)
            ).alias("CancellationRate"),
            F.avg(
                F.when(
                    F.col("Diverted") == 1,
                    1.0,
                ).otherwise(0.0)
            ).alias("DiversionRate"),
        )
        .withColumn(
            "OriginAirportReliabilityScore",
            _reliability_score(
                F.col("OnTimeRate"),
                F.col("CancellationRate"),
                F.col("DiversionRate"),
            ),
        )
        .select("AirportKey", "OriginAirportReliabilityScore")
    )

    destination_scores = (
        history.groupBy(F.col("DestAirportKey").alias("AirportKey"))
        .agg(
            F.avg(
                F.when(
                    (F.col("Cancelled") == 0)
                    & (F.col("Diverted") == 0)
                    & F.col("ArrDelay").isNotNull(),
                    F.when(
                        F.col("ArrDelay") <= 15,
                        1.0,
                    ).otherwise(0.0),
                )
            ).alias("ArrivalOnTimeRate")
        )
        .withColumn(
            "DestAirportReliabilityScore",
            F.round(
                100
                * F.coalesce(
                    F.col("ArrivalOnTimeRate"),
                    F.lit(0.0),
                ),
                2,
            ),
        )
        .select("AirportKey", "DestAirportReliabilityScore")
    )

    route_scores = (
        history.groupBy("RouteKey")
        .agg(
            F.avg(
                F.when(
                    (F.col("Cancelled") == 0)
                    & (F.col("Diverted") == 0)
                    & F.col("ArrDelay").isNotNull(),
                    F.when(
                        F.col("ArrDelay") <= 15,
                        1.0,
                    ).otherwise(0.0),
                )
            ).alias("OnTimeRate"),
            F.avg(
                F.when(
                    F.col("Cancelled") == 1,
                    1.0,
                ).otherwise(0.0)
            ).alias("CancellationRate"),
            F.avg(
                F.when(
                    F.col("Diverted") == 1,
                    1.0,
                ).otherwise(0.0)
            ).alias("DiversionRate"),
        )
        .withColumn(
            "RouteReliabilityScore",
            _reliability_score(
                F.col("OnTimeRate"),
                F.col("CancellationRate"),
                F.col("DiversionRate"),
            ),
        )
        .select("RouteKey", "RouteReliabilityScore")
    )

    return (
        airline_scores,
        origin_scores,
        destination_scores,
        route_scores,
    )


def build_ml_dataset(
    gold_base_df: DataFrame,
    train_end_date: str,
    validation_year: int,
    test_year: int,
) -> DataFrame:
    (
        airline_scores,
        origin_scores,
        destination_scores,
        route_scores,
    ) = build_training_scores(gold_base_df, train_end_date)

    ml_base = gold_base_df.filter(
        (F.col("Cancelled") == 0)
        & (F.col("Diverted") == 0)
        & F.col("ArrDel15").isNotNull()
    ).select(
        "FlightKey",
        "FlightDate",
        "Year",
        "Quarter",
        "Month",
        "DayofMonth",
        "DayOfWeek",
        "DepartureHour",
        "ArrivalHour",
        "DeparturePeriod",
        "ArrivalPeriod",
        "PeakHourIndicator",
        "WeekendIndicator",
        "SeasonIndicator",
        "MarketingAirlineKey",
        "OperatingAirlineKey",
        "OriginAirportKey",
        "DestAirportKey",
        "RouteKey",
        "Distance",
        "ScheduledElapsedTimeMinutes",
        "DistanceCategory",
        "CodeshareFlag",
        "IntraStateRouteFlag",
        "ArrDel15",
    )

    return (
        ml_base.join(
            airline_scores.withColumnRenamed(
                "AirlineKey",
                "MarketingAirlineKey",
            ),
            on="MarketingAirlineKey",
            how="left",
        )
        .join(
            origin_scores.withColumnRenamed(
                "AirportKey",
                "OriginAirportKey",
            ),
            on="OriginAirportKey",
            how="left",
        )
        .join(
            destination_scores.withColumnRenamed(
                "AirportKey",
                "DestAirportKey",
            ),
            on="DestAirportKey",
            how="left",
        )
        .join(route_scores, on="RouteKey", how="left")
        .withColumn(
            "DatasetSplit",
            F.when(
                F.col("FlightDate") <= F.to_date(F.lit(train_end_date)),
                "Train",
            )
            .when(F.col("Year") == validation_year, "Validation")
            .when(F.col("Year") == test_year, "Test")
            .otherwise("OutsideConfiguredSplit"),
        )
        .withColumn(
            "ReliabilityFeatureScope",
            F.lit("TRAINING_HISTORY_ONLY"),
        )
    )
