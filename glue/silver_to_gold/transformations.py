"""Silver-to-Gold row-level feature engineering."""

from functools import reduce

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from constants import AIRLINE_NAMES, REQUIRED_COLUMNS, STATE_TO_REGION


def validate_source_columns(df: DataFrame) -> None:
    missing = sorted(set(REQUIRED_COLUMNS) - set(df.columns))
    if missing:
        raise ValueError(
            "Silver dataset is missing required columns: " + ", ".join(missing)
        )


def select_source_columns(df: DataFrame) -> DataFrame:
    validate_source_columns(df)
    return df.select(*REQUIRED_COLUMNS)


def _create_map(mapping):
    expressions = []
    for key, value in mapping.items():
        expressions.extend([F.lit(key), F.lit(value)])
    return F.create_map(*expressions)


def add_hhmm_features(
    df: DataFrame,
    source_column: str,
    prefix: str,
) -> DataFrame:
    normalized = F.when(
        F.col(source_column) == 2400,
        F.lit(0),
    ).otherwise(F.col(source_column))

    hour = F.floor(normalized / 100).cast("int")
    minute = (normalized % 100).cast("int")

    valid = normalized.isNotNull() & hour.between(0, 23) & minute.between(0, 59)

    return (
        df.withColumn(
            "{}Hour".format(prefix),
            F.when(valid, hour).otherwise(F.lit(None).cast("int")),
        )
        .withColumn(
            "{}Minute".format(prefix),
            F.when(valid, minute).otherwise(F.lit(None).cast("int")),
        )
        .withColumn(
            "{}TimeHHMM".format(prefix),
            F.when(
                valid,
                F.format_string("%02d:%02d", hour, minute),
            ),
        )
    )


def add_time_features(df: DataFrame) -> DataFrame:
    result = add_hhmm_features(df, "CRSDepTime", "Departure")
    result = add_hhmm_features(result, "CRSArrTime", "Arrival")

    scheduled_difference = (
        (
            F.col("ArrivalHour") * 60
            + F.col("ArrivalMinute")
            - F.col("DepartureHour") * 60
            - F.col("DepartureMinute")
            + 1440
        )
        % 1440
    ).cast("int")

    return result.withColumn(
        "ScheduledElapsedTimeMinutes",
        scheduled_difference,
    )


def add_calendar_features(df: DataFrame) -> DataFrame:
    return (
        df.withColumn(
            "WeekendIndicator",
            F.when(F.col("DayOfWeek").isin(6, 7), 1).otherwise(0),
        )
        .withColumn(
            "SeasonIndicator",
            F.when(F.col("Month").isin(12, 1, 2), "Winter")
            .when(F.col("Month").isin(3, 4, 5), "Spring")
            .when(F.col("Month").isin(6, 7, 8), "Summer")
            .when(F.col("Month").isin(9, 10, 11), "Fall")
            .otherwise("Unknown"),
        )
        .withColumn(
            "YearMonth",
            F.concat_ws(
                "-",
                F.col("Year").cast("string"),
                F.lpad(F.col("Month").cast("string"), 2, "0"),
            ),
        )
        .withColumn(
            "FlightYearMonth",
            F.to_date(
                F.concat_ws(
                    "-",
                    F.col("Year").cast("string"),
                    F.lpad(F.col("Month").cast("string"), 2, "0"),
                    F.lit("01"),
                )
            ),
        )
    )


def add_period_features(df: DataFrame) -> DataFrame:
    return (
        df.withColumn(
            "DeparturePeriod",
            F.when(F.col("DepartureHour").between(5, 11), "Morning")
            .when(
                F.col("DepartureHour").between(12, 16),
                "Afternoon",
            )
            .when(F.col("DepartureHour").between(17, 21), "Evening")
            .when(F.col("DepartureHour").isNotNull(), "Night")
            .otherwise("Unknown"),
        )
        .withColumn(
            "ArrivalPeriod",
            F.when(F.col("ArrivalHour").between(5, 11), "Morning")
            .when(F.col("ArrivalHour").between(12, 16), "Afternoon")
            .when(F.col("ArrivalHour").between(17, 21), "Evening")
            .when(F.col("ArrivalHour").isNotNull(), "Night")
            .otherwise("Unknown"),
        )
        .withColumn(
            "PeakHourIndicator",
            F.when(
                F.col("DepartureHour").between(6, 9)
                | F.col("DepartureHour").between(16, 19),
                1,
            ).otherwise(0),
        )
    )


def add_route_and_key_features(df: DataFrame) -> DataFrame:
    return (
        df.withColumn(
            "Route",
            F.concat_ws("-", F.col("Origin"), F.col("Dest")),
        )
        .withColumn(
            "StatePair",
            F.concat_ws(
                "-",
                F.col("OriginState"),
                F.col("DestState"),
            ),
        )
        .withColumn(
            "IntraStateRouteFlag",
            F.when(
                F.col("OriginState").isNotNull()
                & F.col("DestState").isNotNull()
                & (F.col("OriginState") == F.col("DestState")),
                1,
            ).otherwise(0),
        )
        .withColumn(
            "FlightKey",
            F.concat_ws(
                "|",
                F.date_format("FlightDate", "yyyy-MM-dd"),
                F.coalesce(
                    F.col("Marketing_Airline_Network"),
                    F.lit("UNK"),
                ),
                F.coalesce(
                    F.col("Flight_Number_Marketing_Airline").cast("string"),
                    F.lit("UNK"),
                ),
                F.coalesce(F.col("Origin"), F.lit("UNK")),
                F.coalesce(F.col("Dest"), F.lit("UNK")),
                F.lpad(
                    F.coalesce(
                        F.col("CRSDepTime").cast("string"),
                        F.lit("0"),
                    ),
                    4,
                    "0",
                ),
            ),
        )
    )


def add_operational_features(df: DataFrame) -> DataFrame:
    return (
        df.withColumn(
            "FlightTimeCompleteFlag",
            F.when(
                F.col("AirTime").isNotNull()
                & F.col("TaxiOut").isNotNull()
                & F.col("TaxiIn").isNotNull(),
                1,
            ).otherwise(0),
        )
        .withColumn(
            "TotalFlightTimeMinutes",
            F.when(
                F.col("FlightTimeCompleteFlag") == 1,
                F.col("AirTime") + F.col("TaxiOut") + F.col("TaxiIn"),
            ).otherwise(F.lit(None).cast("int")),
        )
        .withColumn(
            "AverageFlightSpeedMph",
            F.when(
                F.col("Distance").isNotNull()
                & F.col("AirTime").isNotNull()
                & (F.col("AirTime") > 0),
                F.round(
                    F.col("Distance") / F.col("AirTime") * 60,
                    2,
                ),
            ),
        )
        .withColumn(
            "FlightDurationCategory",
            F.when(
                F.col("TotalFlightTimeMinutes").isNull(),
                "Not Available",
            )
            .when(F.col("TotalFlightTimeMinutes") < 120, "Short")
            .when(F.col("TotalFlightTimeMinutes") < 240, "Medium")
            .otherwise("Long"),
        )
        .withColumn(
            "DistanceCategory",
            F.when(F.col("Distance").isNull(), "Not Available")
            .when(F.col("Distance") < 500, "Short Haul")
            .when(F.col("Distance") < 1500, "Medium Haul")
            .otherwise("Long Haul"),
        )
    )


def add_delay_features(df: DataFrame) -> DataFrame:
    return (
        df.withColumn(
            "HasDepDelay",
            F.when(F.col("DepDelay").isNotNull(), 1).otherwise(0),
        )
        .withColumn(
            "HasArrDelay",
            F.when(F.col("ArrDelay").isNotNull(), 1).otherwise(0),
        )
        .withColumn(
            "ArrDelayNullExceptionFlag",
            F.when(
                F.col("ArrDelay").isNull()
                & (F.coalesce(F.col("Cancelled"), F.lit(0)) == 0)
                & (F.coalesce(F.col("Diverted"), F.lit(0)) == 0),
                1,
            ).otherwise(0),
        )
        .withColumn(
            "DeparturePerformanceType",
            F.when(F.col("DepDelay").isNull(), "Not Available")
            .when(F.col("DepDelay") < 0, "Early")
            .when(F.col("DepDelay") == 0, "On Time")
            .otherwise("Delayed"),
        )
        .withColumn(
            "ArrivalPerformanceType",
            F.when(F.col("ArrDelay").isNull(), "Not Available")
            .when(F.col("ArrDelay") < 0, "Early")
            .when(F.col("ArrDelay") == 0, "On Time")
            .otherwise("Delayed"),
        )
        .withColumn(
            "DelayCategory",
            F.when(F.col("ArrDelay").isNull(), "Not Available")
            .when(F.col("ArrDelay") <= 15, "On Time")
            .when(F.col("ArrDelay") <= 60, "Minor Delay")
            .otherwise("Major Delay"),
        )
    )


def add_delay_cause_features(df: DataFrame) -> DataFrame:
    cause_columns = [
        "CarrierDelay",
        "WeatherDelay",
        "NASDelay",
        "SecurityDelay",
        "LateAircraftDelay",
    ]
    result = df

    for column_name in cause_columns:
        result = result.withColumn(
            "{}Filled".format(column_name),
            F.coalesce(F.col(column_name), F.lit(0)).cast("int"),
        )

    total_expression = reduce(
        lambda left, right: left + right,
        [F.col("{}Filled".format(column_name)) for column_name in cause_columns],
    )

    result = result.withColumn(
        "TotalRecordedCauseDelay",
        total_expression,
    ).withColumn(
        "DelayCauseFlag",
        F.when(F.col("TotalRecordedCauseDelay") > 0, 1).otherwise(0),
    )

    greatest_cause = F.greatest(
        *[F.col("{}Filled".format(column_name)) for column_name in cause_columns]
    )

    return result.withColumn(
        "PrimaryDelayCause",
        F.when(F.col("TotalRecordedCauseDelay") == 0, "None")
        .when(F.col("CarrierDelayFilled") == greatest_cause, "Carrier")
        .when(F.col("WeatherDelayFilled") == greatest_cause, "Weather")
        .when(F.col("NASDelayFilled") == greatest_cause, "NAS")
        .when(F.col("SecurityDelayFilled") == greatest_cause, "Security")
        .when(
            F.col("LateAircraftDelayFilled") == greatest_cause,
            "Late Aircraft",
        )
        .otherwise("Unknown"),
    )


def add_status_features(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "FlightStatus",
        F.when(F.col("Cancelled") == 1, "Cancelled")
        .when(F.col("Diverted") == 1, "Diverted")
        .when(
            (F.col("Cancelled") == 0) & (F.col("Diverted") == 0),
            "Completed",
        )
        .otherwise("Unknown"),
    ).withColumn(
        "CompletedFlightFlag",
        F.when(
            (F.col("Cancelled") == 0) & (F.col("Diverted") == 0),
            1,
        ).otherwise(0),
    )


def add_airline_features(df: DataFrame) -> DataFrame:
    airline_map = _create_map(AIRLINE_NAMES)

    return (
        df.withColumn(
            "CodeshareFlag",
            F.when(
                F.upper(
                    F.coalesce(
                        F.col("Operated_or_Branded_Code_Share_Partners"),
                        F.lit(""),
                    )
                ).contains("CODESHARE"),
                1,
            )
            .when(
                F.col("Marketing_Airline_Network").isNotNull()
                & F.col("Operating_Airline").isNotNull()
                & (F.col("Marketing_Airline_Network") != F.col("Operating_Airline")),
                1,
            )
            .otherwise(0),
        )
        .withColumn(
            "MarketingAirlineName",
            F.coalesce(
                airline_map[F.col("Marketing_Airline_Network")],
                F.lit("Unknown Airline"),
            ),
        )
        .withColumn(
            "OperatingAirlineName",
            F.coalesce(
                airline_map[F.col("Operating_Airline")],
                F.lit("Unknown Airline"),
            ),
        )
        .withColumn(
            "MarketingAirlineLabel",
            F.concat_ws(
                " - ",
                F.col("Marketing_Airline_Network"),
                F.col("MarketingAirlineName"),
            ),
        )
        .withColumn(
            "OperatingAirlineLabel",
            F.concat_ws(
                " - ",
                F.col("Operating_Airline"),
                F.col("OperatingAirlineName"),
            ),
        )
    )


def add_region_features(df: DataFrame) -> DataFrame:
    region_map = _create_map(STATE_TO_REGION)
    return df.withColumn(
        "OriginRegion",
        F.coalesce(
            region_map[F.col("OriginState")],
            F.lit("Unknown"),
        ),
    ).withColumn(
        "DestRegion",
        F.coalesce(
            region_map[F.col("DestState")],
            F.lit("Unknown"),
        ),
    )


def add_dimension_keys(df: DataFrame) -> DataFrame:
    return (
        df.withColumn(
            "DateKey",
            F.date_format("FlightDate", "yyyyMMdd").cast("int"),
        )
        .withColumn(
            "MarketingAirlineKey",
            F.sha2(
                F.coalesce(
                    F.col("Marketing_Airline_Network"),
                    F.lit("UNK"),
                ),
                256,
            ),
        )
        .withColumn(
            "OperatingAirlineKey",
            F.sha2(
                F.coalesce(
                    F.col("Operating_Airline"),
                    F.lit("UNK"),
                ),
                256,
            ),
        )
        .withColumn(
            "OriginAirportKey",
            F.sha2(
                F.coalesce(F.col("Origin"), F.lit("UNK")),
                256,
            ),
        )
        .withColumn(
            "DestAirportKey",
            F.sha2(
                F.coalesce(F.col("Dest"), F.lit("UNK")),
                256,
            ),
        )
        .withColumn(
            "RouteKey",
            F.sha2(
                F.coalesce(F.col("Route"), F.lit("UNK-UNK")),
                256,
            ),
        )
    )


def build_gold_base(silver_df: DataFrame) -> DataFrame:
    """
    Build the complete row-level Gold feature set.

    localCheckpoint is intentionally not used. It stores blocks on executor
    local storage and becomes unrecoverable when an executor is lost.
    The caller repartitions and persists the completed Gold base once.
    """
    result = select_source_columns(silver_df)

    result = add_time_features(result)
    result = add_calendar_features(result)
    result = add_period_features(result)

    result = add_route_and_key_features(result)
    result = add_operational_features(result)
    result = add_delay_features(result)
    result = add_delay_cause_features(result)

    result = add_status_features(result)
    result = add_airline_features(result)
    result = add_region_features(result)
    result = add_dimension_keys(result)

    return result
