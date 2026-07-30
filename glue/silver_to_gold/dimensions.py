"""Gold dimensions and Fact table creation."""

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
            + 0.20
            * (
                1
                - F.coalesce(cancellation_rate, F.lit(0.0))
            )
            + 0.10
            * (
                1
                - F.coalesce(diversion_rate, F.lit(0.0))
            )
        ),
        2,
    )


def build_dim_date(gold_base_df: DataFrame) -> DataFrame:
    return (
        gold_base_df
        .select(
            "DateKey",
            "FlightDate",
            "Year",
            "Quarter",
            "Month",
            "DayofMonth",
            "DayOfWeek",
            "WeekendIndicator",
            "SeasonIndicator",
            "YearMonth",
        )
        .dropDuplicates(["DateKey"])
    )


def build_dim_airline(gold_base_df: DataFrame) -> DataFrame:
    marketing = gold_base_df.select(
        F.col("MarketingAirlineKey").alias("AirlineKey"),
        F.col("Marketing_Airline_Network").alias("AirlineCode"),
        F.col("MarketingAirlineName").alias("AirlineName"),
        F.col("MarketingAirlineLabel").alias("AirlineLabel"),
    )

    operating = gold_base_df.select(
        F.col("OperatingAirlineKey").alias("AirlineKey"),
        F.col("Operating_Airline").alias("AirlineCode"),
        F.col("OperatingAirlineName").alias("AirlineName"),
        F.col("OperatingAirlineLabel").alias("AirlineLabel"),
    )

    master = (
        marketing
        .unionByName(operating)
        .filter(F.col("AirlineCode").isNotNull())
        .groupBy("AirlineKey", "AirlineCode")
        .agg(
            F.first("AirlineName", ignorenulls=True).alias(
                "AirlineName"
            ),
            F.first("AirlineLabel", ignorenulls=True).alias(
                "AirlineLabel"
            ),
        )
    )

    stats = (
        gold_base_df
        .groupBy(
            F.col("MarketingAirlineKey").alias("AirlineKey")
        )
        .agg(
            F.count("*").alias("FlightCount"),
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
            "ReliabilityScore",
            _reliability_score(
                F.col("OnTimeRate"),
                F.col("CancellationRate"),
                F.col("DiversionRate"),
            ),
        )
    )

    return (
        master
        .join(stats, on="AirlineKey", how="left")
        .select(
            "AirlineKey",
            "AirlineCode",
            "AirlineName",
            "AirlineLabel",
            F.coalesce(
                F.col("FlightCount"),
                F.lit(0),
            ).alias("FlightCount"),
            F.round("OnTimeRate", 4).alias("OnTimeRate"),
            F.round("CancellationRate", 4).alias(
                "CancellationRate"
            ),
            F.round("DiversionRate", 4).alias("DiversionRate"),
            "ReliabilityScore",
        )
    )


def build_dim_airport(gold_base_df: DataFrame) -> DataFrame:
    origin = gold_base_df.select(
        F.col("OriginAirportKey").alias("AirportKey"),
        F.col("Origin").alias("AirportCode"),
        F.col("OriginCityName").alias("CityName"),
        F.col("OriginState").alias("StateCode"),
        F.col("OriginStateName").alias("StateName"),
        F.col("OriginRegion").alias("Region"),
    )

    destination = gold_base_df.select(
        F.col("DestAirportKey").alias("AirportKey"),
        F.col("Dest").alias("AirportCode"),
        F.col("DestCityName").alias("CityName"),
        F.col("DestState").alias("StateCode"),
        F.col("DestStateName").alias("StateName"),
        F.col("DestRegion").alias("Region"),
    )

    master = (
        origin
        .unionByName(destination)
        .groupBy("AirportKey", "AirportCode")
        .agg(
            F.first("CityName", ignorenulls=True).alias("CityName"),
            F.first("StateCode", ignorenulls=True).alias(
                "StateCode"
            ),
            F.first("StateName", ignorenulls=True).alias(
                "StateName"
            ),
            F.first("Region", ignorenulls=True).alias("Region"),
        )
    )

    departure_stats = (
        gold_base_df
        .groupBy(
            F.col("OriginAirportKey").alias("AirportKey")
        )
        .agg(
            F.count("*").alias("DepartureFlightCount"),
            F.avg(
                F.when(
                    (F.col("Cancelled") == 0)
                    & F.col("DepDelay").isNotNull(),
                    F.when(
                        F.col("DepDelay") <= 15,
                        1.0,
                    ).otherwise(0.0),
                )
            ).alias("DepartureOnTimeRate"),
            F.avg(
                F.when(
                    F.col("Cancelled") == 1,
                    1.0,
                ).otherwise(0.0)
            ).alias("DepartureCancellationRate"),
            F.avg(
                F.when(
                    F.col("Diverted") == 1,
                    1.0,
                ).otherwise(0.0)
            ).alias("DepartureDiversionRate"),
        )
        .withColumn(
            "DepartureReliabilityScore",
            _reliability_score(
                F.col("DepartureOnTimeRate"),
                F.col("DepartureCancellationRate"),
                F.col("DepartureDiversionRate"),
            ),
        )
    )

    arrival_stats = (
        gold_base_df
        .groupBy(
            F.col("DestAirportKey").alias("AirportKey")
        )
        .agg(
            F.count("*").alias("ArrivalFlightCount"),
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
            ).alias("ArrivalOnTimeRate"),
            F.round(
                F.avg("ArrDelay"),
                2,
            ).alias("AverageArrivalDelay"),
        )
        .withColumn(
            "ArrivalReliabilityScore",
            F.round(
                100
                * F.coalesce(
                    F.col("ArrivalOnTimeRate"),
                    F.lit(0.0),
                ),
                2,
            ),
        )
    )

    return (
        master
        .join(departure_stats, on="AirportKey", how="left")
        .join(arrival_stats, on="AirportKey", how="left")
        .select(
            "AirportKey",
            "AirportCode",
            "CityName",
            "StateCode",
            "StateName",
            "Region",
            F.coalesce(
                F.col("DepartureFlightCount"),
                F.lit(0),
            ).alias("DepartureFlightCount"),
            F.coalesce(
                F.col("ArrivalFlightCount"),
                F.lit(0),
            ).alias("ArrivalFlightCount"),
            F.round(
                "DepartureOnTimeRate",
                4,
            ).alias("DepartureOnTimeRate"),
            F.round(
                "ArrivalOnTimeRate",
                4,
            ).alias("ArrivalOnTimeRate"),
            F.round(
                "DepartureCancellationRate",
                4,
            ).alias("DepartureCancellationRate"),
            F.round(
                "DepartureDiversionRate",
                4,
            ).alias("DepartureDiversionRate"),
            "AverageArrivalDelay",
            "DepartureReliabilityScore",
            "ArrivalReliabilityScore",
        )
    )


def build_dim_route(gold_base_df: DataFrame) -> DataFrame:
    return (
        gold_base_df
        .groupBy(
            "RouteKey",
            "Route",
            "Origin",
            "Dest",
            "StatePair",
        )
        .agg(
            F.count("*").alias("FlightCount"),
            F.round(F.avg("ArrDelay"), 2).alias("AverageDelay"),
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
            "ReliabilityScore",
            _reliability_score(
                F.col("OnTimeRate"),
                F.col("CancellationRate"),
                F.col("DiversionRate"),
            ),
        )
        .select(
            "RouteKey",
            "Route",
            "Origin",
            "Dest",
            "StatePair",
            "FlightCount",
            "AverageDelay",
            F.round("OnTimeRate", 4).alias("OnTimeRate"),
            F.round("CancellationRate", 4).alias(
                "CancellationRate"
            ),
            F.round("DiversionRate", 4).alias("DiversionRate"),
            "ReliabilityScore",
        )
    )


def build_fact_flights(gold_base_df: DataFrame) -> DataFrame:
    columns = [
        "FlightKey",
        "DateKey",
        "MarketingAirlineKey",
        "OperatingAirlineKey",
        "OriginAirportKey",
        "DestAirportKey",
        "RouteKey",
        "Year",
        "Month",
        "FlightDate",
        "Flight_Number_Marketing_Airline",
        "Operated_or_Branded_Code_Share_Partners",
        "CRSDepTime",
        "CRSArrTime",
        "ScheduledElapsedTimeMinutes",
        "DepartureHour",
        "ArrivalHour",
        "DeparturePeriod",
        "ArrivalPeriod",
        "PeakHourIndicator",
        "WeekendIndicator",
        "SeasonIndicator",
        "Distance",
        "AirTime",
        "TaxiOut",
        "TaxiIn",
        "DepDelay",
        "ArrDelay",
        "DepDel15",
        "ArrDel15",
        "HasDepDelay",
        "HasArrDelay",
        "ArrDelayNullExceptionFlag",
        "Cancelled",
        "Diverted",
        "CompletedFlightFlag",
        "FlightStatus",
        "DelayCategory",
        "DeparturePerformanceType",
        "ArrivalPerformanceType",
        "DistanceCategory",
        "FlightDurationCategory",
        "FlightTimeCompleteFlag",
        "TotalFlightTimeMinutes",
        "AverageFlightSpeedMph",
        "CarrierDelay",
        "WeatherDelay",
        "NASDelay",
        "SecurityDelay",
        "LateAircraftDelay",
        "PrimaryDelayCause",
        "TotalRecordedCauseDelay",
        "DelayCauseFlag",
        "CodeshareFlag",
        "IntraStateRouteFlag",
    ]
    return gold_base_df.select(*columns)
