CREATE OR REPLACE VIEW flight_gold.airline_kpi_summary_view AS

SELECT
    Year,

    AirlineCode,
    AirlineName,
    AirlineLabel,

    SUM(TotalFlights) AS TotalFlights,
    SUM(OnTimeFlights) AS OnTimeFlights,
    SUM(DelayedFlights) AS DelayedFlights,
    SUM(CancelledFlights) AS CancelledFlights,
    SUM(DivertedFlights) AS DivertedFlights,

    COUNT(DISTINCT RouteKey) AS ActiveRoutes,

    ROUND(AVG(AirlineReliabilityScore), 2) AS ReliabilityScore,

    ROUND(AVG(AirlineOnTimeRate), 4) AS OnTimeRate,
    ROUND(AVG(AirlineCancellationRate), 4) AS CancellationRate,
    ROUND(AVG(AirlineDiversionRate), 4) AS DiversionRate

FROM flight_gold.viz_reliability_analytics

GROUP BY

    Year,
    AirlineCode,
    AirlineName,
    AirlineLabel;
