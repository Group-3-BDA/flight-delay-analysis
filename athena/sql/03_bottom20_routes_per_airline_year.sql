CREATE OR REPLACE VIEW bottom20_routes_per_airline_year AS

WITH ranked_routes AS
(
    SELECT
        Year,
        AirlineCode,
        AirlineName,
        AirlineLabel,

        Route,
        RouteOrigin,
        RouteDestination,
        StatePair,

        SUM(RouteFlightCount) AS TotalFlights,

        ROUND(AVG(RouteAverageDelay), 2) AS AvgDelay,
        ROUND(AVG(RouteOnTimeRate), 2) AS OnTimeRate,
        ROUND(AVG(RouteCancellationRate), 2) AS CancellationRate,
        ROUND(AVG(RouteDiversionRate), 2) AS DiversionRate,

        ROUND(
            SUM(RouteReliabilityScore * RouteFlightCount)
            /
            NULLIF(SUM(RouteFlightCount), 0),
            2
        ) AS RouteReliabilityScore,

        ROW_NUMBER() OVER
        (
            PARTITION BY Year, AirlineCode
            ORDER BY
                ROUND(
                    SUM(RouteReliabilityScore * RouteFlightCount)
                    /
                    NULLIF(SUM(RouteFlightCount), 0),
                    2
                ) ASC,
                SUM(RouteFlightCount) DESC
        ) AS RouteRank

    FROM viz_reliability_analytics

    GROUP BY
        Year,
        AirlineCode,
        AirlineName,
        AirlineLabel,
        Route,
        RouteOrigin,
        RouteDestination,
        StatePair
)

SELECT *
FROM ranked_routes
WHERE RouteRank <= 20;
