"""Dashboard-oriented aggregate tables for Power BI and Tableau.

The functions in this module implement the logic from the dashboard notebooks
without reading the Gold tables back from S3. The DataFrames already created
by the Silver-to-Gold pipeline are reused directly.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def _safe_percentage(numerator, denominator):
    """Return a percentage rounded to two decimals without divide-by-zero."""
    return F.when(
        denominator > 0,
        F.round(numerator * F.lit(100.0) / denominator, 2),
    ).otherwise(F.lit(0.0))


def build_delay_analytics(
    fact_flights_df: DataFrame,
    dim_airline_df: DataFrame,
) -> DataFrame:
    """Build the delay-analysis table used by the delay dashboard.

    Grain:
        One row per Year-Month-Airline-PrimaryDelayCause-DelayCategory-Season.
    """
    airline_lookup_df = dim_airline_df.select(
        "AirlineKey",
        "AirlineCode",
        "AirlineName",
        "AirlineLabel",
    )

    reporting_df = (
        fact_flights_df.alias("f")
        .join(
            airline_lookup_df.alias("a"),
            F.col("f.MarketingAirlineKey") == F.col("a.AirlineKey"),
            "left",
        )
        .select(
            F.col("f.Year").alias("Year"),
            F.col("f.Month").alias("Month"),
            F.col("f.MarketingAirlineKey").alias("MarketingAirlineKey"),
            F.col("a.AirlineCode").alias("AirlineCode"),
            F.col("a.AirlineName").alias("AirlineName"),
            F.col("a.AirlineLabel").alias("AirlineLabel"),
            F.col("f.PrimaryDelayCause").alias("PrimaryDelayCause"),
            F.col("f.DelayCategory").alias("DelayCategory"),
            F.col("f.SeasonIndicator").alias("SeasonIndicator"),
            F.col("f.ArrDel15").alias("ArrDel15"),
            F.col("f.Cancelled").alias("Cancelled"),
            F.col("f.Diverted").alias("Diverted"),
            F.col("f.ArrDelay").alias("ArrDelay"),
            F.col("f.DepDelay").alias("DepDelay"),
            F.col("f.CarrierDelay").alias("CarrierDelay"),
            F.col("f.WeatherDelay").alias("WeatherDelay"),
            F.col("f.NASDelay").alias("NASDelay"),
            F.col("f.SecurityDelay").alias("SecurityDelay"),
            F.col("f.LateAircraftDelay").alias("LateAircraftDelay"),
        )
    )

    aggregated_df = reporting_df.groupBy(
        "Year",
        "Month",
        "MarketingAirlineKey",
        "AirlineCode",
        "AirlineName",
        "AirlineLabel",
        "PrimaryDelayCause",
        "DelayCategory",
        "SeasonIndicator",
    ).agg(
        F.count("*").alias("TotalFlights"),
        F.sum(F.when(F.col("ArrDel15") == 0, 1).otherwise(0)).alias("OnTimeFlights"),
        F.sum(F.when(F.col("ArrDel15") == 1, 1).otherwise(0)).alias("DelayedFlights"),
        F.sum(F.when(F.col("Cancelled") == 1, 1).otherwise(0)).alias(
            "CancelledFlights"
        ),
        F.sum(F.when(F.col("Diverted") == 1, 1).otherwise(0)).alias("DivertedFlights"),
        F.round(F.avg("ArrDelay"), 2).alias("AvgArrivalDelay"),
        F.round(F.avg("DepDelay"), 2).alias("AvgDepartureDelay"),
        F.round(F.avg("CarrierDelay"), 2).alias("AvgCarrierDelay"),
        F.round(F.avg("WeatherDelay"), 2).alias("AvgWeatherDelay"),
        F.round(F.avg("NASDelay"), 2).alias("AvgNASDelay"),
        F.round(F.avg("SecurityDelay"), 2).alias("AvgSecurityDelay"),
        F.round(F.avg("LateAircraftDelay"), 2).alias("AvgLateAircraftDelay"),
        F.coalesce(
            F.sum("CarrierDelay"),
            F.lit(0),
        ).alias("CarrierDelayMinutes"),
        F.coalesce(
            F.sum("WeatherDelay"),
            F.lit(0),
        ).alias("WeatherDelayMinutes"),
        F.coalesce(
            F.sum("NASDelay"),
            F.lit(0),
        ).alias("NASDelayMinutes"),
        F.coalesce(
            F.sum("SecurityDelay"),
            F.lit(0),
        ).alias("SecurityDelayMinutes"),
        F.coalesce(
            F.sum("LateAircraftDelay"),
            F.lit(0),
        ).alias("LateAircraftDelayMinutes"),
        F.coalesce(
            F.sum("ArrDelay"),
            F.lit(0),
        ).alias("TotalArrivalDelayMinutes"),
        F.coalesce(
            F.sum("DepDelay"),
            F.lit(0),
        ).alias("TotalDepartureDelayMinutes"),
        F.sum(
            F.when(
                (F.col("ArrDelay") >= 0) & (F.col("ArrDelay") < 15),
                1,
            ).otherwise(0)
        ).alias("Delay_0_15"),
        F.sum(
            F.when(
                (F.col("ArrDelay") >= 15) & (F.col("ArrDelay") < 30),
                1,
            ).otherwise(0)
        ).alias("Delay_15_30"),
        F.sum(
            F.when(
                (F.col("ArrDelay") >= 30) & (F.col("ArrDelay") < 60),
                1,
            ).otherwise(0)
        ).alias("Delay_30_60"),
        F.sum(
            F.when(
                (F.col("ArrDelay") >= 60) & (F.col("ArrDelay") < 120),
                1,
            ).otherwise(0)
        ).alias("Delay_60_120"),
        F.sum(F.when(F.col("ArrDelay") >= 120, 1).otherwise(0)).alias("Delay_120_Plus"),
    )

    contribution_window = Window.partitionBy(
        "Year",
        "Month",
        "MarketingAirlineKey",
    )

    total_delayed_in_window = F.sum("DelayedFlights").over(contribution_window)

    return (
        aggregated_df.withColumn(
            "YearMonth",
            F.concat_ws(
                "-",
                F.col("Year").cast("string"),
                F.lpad(F.col("Month").cast("string"), 2, "0"),
            ),
        )
        .withColumn(
            "DelayRate",
            _safe_percentage(
                F.col("DelayedFlights"),
                F.col("TotalFlights"),
            ),
        )
        .withColumn(
            "OnTimePercentage",
            _safe_percentage(
                F.col("OnTimeFlights"),
                F.col("TotalFlights"),
            ),
        )
        .withColumn(
            "CancellationRate",
            _safe_percentage(
                F.col("CancelledFlights"),
                F.col("TotalFlights"),
            ),
        )
        .withColumn(
            "DiversionRate",
            _safe_percentage(
                F.col("DivertedFlights"),
                F.col("TotalFlights"),
            ),
        )
        .withColumn(
            "DelayContributionPercent",
            _safe_percentage(
                F.col("DelayedFlights"),
                total_delayed_in_window,
            ),
        )
    )


def build_reliability_analytics(
    fact_flights_df: DataFrame,
    dim_airline_df: DataFrame,
    dim_airport_df: DataFrame,
    dim_route_df: DataFrame,
) -> DataFrame:
    """Build the airline-airport-route reliability dashboard table.

    Grain:
        One row per Year-Month-Season-Airline-Route-Origin-Destination.
    """
    airline = dim_airline_df.alias("a")
    origin = dim_airport_df.alias("o")
    destination = dim_airport_df.alias("d")
    route = dim_route_df.alias("r")
    fact = fact_flights_df.alias("f")

    master_df = (
        fact.join(
            airline,
            F.col("f.MarketingAirlineKey") == F.col("a.AirlineKey"),
            "left",
        )
        .join(
            origin,
            F.col("f.OriginAirportKey") == F.col("o.AirportKey"),
            "left",
        )
        .join(
            destination,
            F.col("f.DestAirportKey") == F.col("d.AirportKey"),
            "left",
        )
        .join(
            route,
            F.col("f.RouteKey") == F.col("r.RouteKey"),
            "left",
        )
        .select(
            # Time and Fact identifiers
            F.col("f.Year").alias("Year"),
            F.col("f.Month").alias("Month"),
            F.col("f.SeasonIndicator").alias("SeasonIndicator"),
            F.col("f.MarketingAirlineKey").alias("MarketingAirlineKey"),
            F.col("f.OriginAirportKey").alias("OriginAirportKey"),
            F.col("f.DestAirportKey").alias("DestAirportKey"),
            F.col("f.RouteKey").alias("RouteKey"),
            # Flight measures
            F.col("f.CompletedFlightFlag").alias("CompletedFlightFlag"),
            F.col("f.Cancelled").alias("Cancelled"),
            F.col("f.Diverted").alias("Diverted"),
            F.col("f.ArrDel15").alias("ArrDel15"),
            F.col("f.ArrDelay").alias("ArrDelay"),
            F.col("f.DepDelay").alias("DepDelay"),
            F.col("f.CarrierDelay").alias("CarrierDelay"),
            F.col("f.WeatherDelay").alias("WeatherDelay"),
            F.col("f.NASDelay").alias("NASDelay"),
            F.col("f.SecurityDelay").alias("SecurityDelay"),
            F.col("f.LateAircraftDelay").alias("LateAircraftDelay"),
            # Airline dimension
            F.col("a.AirlineCode").alias("AirlineCode"),
            F.col("a.AirlineName").alias("AirlineName"),
            F.col("a.AirlineLabel").alias("AirlineLabel"),
            F.col("a.FlightCount").alias("AirlineFlightCount"),
            F.col("a.OnTimeRate").alias("AirlineOnTimeRate"),
            F.col("a.CancellationRate").alias("AirlineCancellationRate"),
            F.col("a.DiversionRate").alias("AirlineDiversionRate"),
            F.col("a.ReliabilityScore").alias("AirlineReliabilityScore"),
            # Origin airport dimension
            F.col("o.AirportCode").alias("OriginAirportCode"),
            F.col("o.CityName").alias("OriginCity"),
            F.col("o.StateCode").alias("OriginStateCode"),
            F.col("o.StateName").alias("OriginState"),
            F.col("o.Region").alias("OriginRegion"),
            F.col("o.DepartureFlightCount").alias("OriginDepartureFlightCount"),
            F.col("o.ArrivalFlightCount").alias("OriginArrivalFlightCount"),
            F.col("o.DepartureOnTimeRate").alias("OriginDepartureOnTimeRate"),
            F.col("o.ArrivalOnTimeRate").alias("OriginArrivalOnTimeRate"),
            F.col("o.DepartureCancellationRate").alias(
                "OriginDepartureCancellationRate"
            ),
            F.col("o.DepartureDiversionRate").alias("OriginDepartureDiversionRate"),
            F.col("o.AverageArrivalDelay").alias("OriginAverageArrivalDelay"),
            F.col("o.DepartureReliabilityScore").alias(
                "OriginDepartureReliabilityScore"
            ),
            F.col("o.ArrivalReliabilityScore").alias("OriginArrivalReliabilityScore"),
            # Destination airport dimension
            F.col("d.AirportCode").alias("DestAirportCode"),
            F.col("d.CityName").alias("DestCity"),
            F.col("d.StateCode").alias("DestStateCode"),
            F.col("d.StateName").alias("DestState"),
            F.col("d.Region").alias("DestRegion"),
            F.col("d.DepartureFlightCount").alias("DestDepartureFlightCount"),
            F.col("d.ArrivalFlightCount").alias("DestArrivalFlightCount"),
            F.col("d.DepartureOnTimeRate").alias("DestDepartureOnTimeRate"),
            F.col("d.ArrivalOnTimeRate").alias("DestArrivalOnTimeRate"),
            F.col("d.DepartureCancellationRate").alias("DestDepartureCancellationRate"),
            F.col("d.DepartureDiversionRate").alias("DestDepartureDiversionRate"),
            F.col("d.AverageArrivalDelay").alias("DestAverageArrivalDelay"),
            F.col("d.DepartureReliabilityScore").alias("DestDepartureReliabilityScore"),
            F.col("d.ArrivalReliabilityScore").alias("DestArrivalReliabilityScore"),
            # Route dimension
            F.col("r.Route").alias("Route"),
            F.col("r.Origin").alias("RouteOrigin"),
            F.col("r.Dest").alias("RouteDestination"),
            F.col("r.StatePair").alias("StatePair"),
            F.col("r.FlightCount").alias("RouteFlightCount"),
            F.col("r.AverageDelay").alias("RouteAverageDelay"),
            F.col("r.OnTimeRate").alias("RouteOnTimeRate"),
            F.col("r.CancellationRate").alias("RouteCancellationRate"),
            F.col("r.DiversionRate").alias("RouteDiversionRate"),
            F.col("r.ReliabilityScore").alias("RouteReliabilityScore"),
        )
    )

    aggregated_df = master_df.groupBy(
        "Year",
        "Month",
        "SeasonIndicator",
        "MarketingAirlineKey",
        "AirlineCode",
        "AirlineName",
        "AirlineLabel",
        "RouteKey",
        "Route",
        "RouteOrigin",
        "RouteDestination",
        "StatePair",
        "OriginAirportKey",
        "OriginAirportCode",
        "OriginCity",
        "OriginStateCode",
        "OriginState",
        "OriginRegion",
        "DestAirportKey",
        "DestAirportCode",
        "DestCity",
        "DestStateCode",
        "DestState",
        "DestRegion",
    ).agg(
        # Airline descriptive reliability
        F.first("AirlineFlightCount").alias("AirlineFlightCount"),
        F.first("AirlineOnTimeRate").alias("AirlineOnTimeRate"),
        F.first("AirlineCancellationRate").alias("AirlineCancellationRate"),
        F.first("AirlineDiversionRate").alias("AirlineDiversionRate"),
        F.first("AirlineReliabilityScore").alias("AirlineReliabilityScore"),
        # Route descriptive reliability
        F.first("RouteFlightCount").alias("RouteFlightCount"),
        F.first("RouteAverageDelay").alias("RouteAverageDelay"),
        F.first("RouteOnTimeRate").alias("RouteOnTimeRate"),
        F.first("RouteCancellationRate").alias("RouteCancellationRate"),
        F.first("RouteDiversionRate").alias("RouteDiversionRate"),
        F.first("RouteReliabilityScore").alias("RouteReliabilityScore"),
        # Origin airport descriptive reliability
        F.first("OriginDepartureFlightCount").alias("OriginDepartureFlightCount"),
        F.first("OriginArrivalFlightCount").alias("OriginArrivalFlightCount"),
        F.first("OriginDepartureOnTimeRate").alias("OriginDepartureOnTimeRate"),
        F.first("OriginArrivalOnTimeRate").alias("OriginArrivalOnTimeRate"),
        F.first("OriginDepartureCancellationRate").alias(
            "OriginDepartureCancellationRate"
        ),
        F.first("OriginDepartureDiversionRate").alias("OriginDepartureDiversionRate"),
        F.first("OriginAverageArrivalDelay").alias("OriginAverageArrivalDelay"),
        F.first("OriginDepartureReliabilityScore").alias(
            "OriginDepartureReliabilityScore"
        ),
        F.first("OriginArrivalReliabilityScore").alias("OriginArrivalReliabilityScore"),
        # Destination airport descriptive reliability
        F.first("DestDepartureFlightCount").alias("DestDepartureFlightCount"),
        F.first("DestArrivalFlightCount").alias("DestArrivalFlightCount"),
        F.first("DestDepartureOnTimeRate").alias("DestDepartureOnTimeRate"),
        F.first("DestArrivalOnTimeRate").alias("DestArrivalOnTimeRate"),
        F.first("DestDepartureCancellationRate").alias("DestDepartureCancellationRate"),
        F.first("DestDepartureDiversionRate").alias("DestDepartureDiversionRate"),
        F.first("DestAverageArrivalDelay").alias("DestAverageArrivalDelay"),
        F.first("DestDepartureReliabilityScore").alias("DestDepartureReliabilityScore"),
        F.first("DestArrivalReliabilityScore").alias("DestArrivalReliabilityScore"),
        # Monthly flight KPIs
        F.count("*").alias("TotalFlights"),
        F.sum(
            F.when(
                F.col("CompletedFlightFlag") == 1,
                1,
            ).otherwise(0)
        ).alias("CompletedFlights"),
        F.sum(F.when(F.col("Cancelled") == 1, 1).otherwise(0)).alias(
            "CancelledFlights"
        ),
        F.sum(F.when(F.col("Diverted") == 1, 1).otherwise(0)).alias("DivertedFlights"),
        F.sum(F.when(F.col("ArrDel15") == 0, 1).otherwise(0)).alias("OnTimeFlights"),
        F.sum(F.when(F.col("ArrDel15") == 1, 1).otherwise(0)).alias("DelayedFlights"),
        F.round(F.avg("ArrDelay"), 2).alias("AvgArrivalDelay"),
        F.round(F.avg("DepDelay"), 2).alias("AvgDepartureDelay"),
        F.round(F.avg("CarrierDelay"), 2).alias("AvgCarrierDelay"),
        F.round(F.avg("WeatherDelay"), 2).alias("AvgWeatherDelay"),
        F.round(F.avg("NASDelay"), 2).alias("AvgNASDelay"),
        F.round(F.avg("SecurityDelay"), 2).alias("AvgSecurityDelay"),
        F.round(F.avg("LateAircraftDelay"), 2).alias("AvgLateAircraftDelay"),
        F.coalesce(
            F.sum("CarrierDelay"),
            F.lit(0),
        ).alias("CarrierDelayMinutes"),
        F.coalesce(
            F.sum("WeatherDelay"),
            F.lit(0),
        ).alias("WeatherDelayMinutes"),
        F.coalesce(
            F.sum("NASDelay"),
            F.lit(0),
        ).alias("NASDelayMinutes"),
        F.coalesce(
            F.sum("SecurityDelay"),
            F.lit(0),
        ).alias("SecurityDelayMinutes"),
        F.coalesce(
            F.sum("LateAircraftDelay"),
            F.lit(0),
        ).alias("LateAircraftDelayMinutes"),
    )

    return (
        aggregated_df.withColumn(
            "YearMonth",
            F.concat_ws(
                "-",
                F.col("Year").cast("string"),
                F.lpad(F.col("Month").cast("string"), 2, "0"),
            ),
        )
        .withColumn(
            "OnTimePercentage",
            _safe_percentage(
                F.col("OnTimeFlights"),
                F.col("TotalFlights"),
            ),
        )
        .withColumn(
            "DelayRate",
            _safe_percentage(
                F.col("DelayedFlights"),
                F.col("TotalFlights"),
            ),
        )
        .withColumn(
            "CalculatedCancellationRate",
            _safe_percentage(
                F.col("CancelledFlights"),
                F.col("TotalFlights"),
            ),
        )
        .withColumn(
            "CalculatedDiversionRate",
            _safe_percentage(
                F.col("DivertedFlights"),
                F.col("TotalFlights"),
            ),
        )
        .withColumn(
            "CompletionRate",
            _safe_percentage(
                F.col("CompletedFlights"),
                F.col("TotalFlights"),
            ),
        )
    )


def build_visualization_tables(
    fact_flights_df: DataFrame,
    dim_airline_df: DataFrame,
    dim_airport_df: DataFrame,
    dim_route_df: DataFrame,
):
    """Return all visualization DataFrames."""
    delay_analytics_df = build_delay_analytics(
        fact_flights_df=fact_flights_df,
        dim_airline_df=dim_airline_df,
    )

    reliability_analytics_df = build_reliability_analytics(
        fact_flights_df=fact_flights_df,
        dim_airline_df=dim_airline_df,
        dim_airport_df=dim_airport_df,
        dim_route_df=dim_route_df,
    )

    return delay_analytics_df, reliability_analytics_df
