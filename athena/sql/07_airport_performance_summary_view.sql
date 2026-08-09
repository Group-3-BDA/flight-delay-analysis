CREATE OR REPLACE VIEW airport_performance_summary_view AS

SELECT

    Year,

    OriginAirportKey,
    OriginAirportCode,
    OriginCity,
    OriginState,
    OriginRegion,

    SUM(OriginDepartureFlightCount) AS DepartureFlights,
    SUM(OriginArrivalFlightCount) AS ArrivalFlights,

    --------------------------------------------------------------------
    -- Departure Reliability
    --------------------------------------------------------------------
    ROUND(
        SUM(OriginDepartureReliabilityScore * OriginDepartureFlightCount)
        /
        NULLIF(SUM(OriginDepartureFlightCount),0),
        2
    ) AS DepartureReliabilityScore,

    --------------------------------------------------------------------
    -- Arrival Reliability
    --------------------------------------------------------------------
    ROUND(
        SUM(OriginArrivalReliabilityScore * OriginArrivalFlightCount)
        /
        NULLIF(SUM(OriginArrivalFlightCount),0),
        2
    ) AS ArrivalReliabilityScore,

    --------------------------------------------------------------------
    -- Overall Reliability
    --------------------------------------------------------------------
    ROUND(
        (
            SUM(OriginDepartureReliabilityScore * OriginDepartureFlightCount)
            +
            SUM(OriginArrivalReliabilityScore * OriginArrivalFlightCount)
        )
        /
        NULLIF(
            SUM(OriginDepartureFlightCount)
            +
            SUM(OriginArrivalFlightCount),
            0
        ),
        2
    ) AS OverallReliabilityScore,

    --------------------------------------------------------------------
    -- Departure On-Time Rate
    --------------------------------------------------------------------
    ROUND(
        SUM(OriginDepartureOnTimeRate * OriginDepartureFlightCount)
        /
        NULLIF(SUM(OriginDepartureFlightCount),0),
        4
    ) AS DepartureOnTimeRate,

    --------------------------------------------------------------------
    -- Arrival On-Time Rate
    --------------------------------------------------------------------
    ROUND(
        SUM(OriginArrivalOnTimeRate * OriginArrivalFlightCount)
        /
        NULLIF(SUM(OriginArrivalFlightCount),0),
        4
    ) AS ArrivalOnTimeRate,

    --------------------------------------------------------------------
    -- Overall On-Time Rate
    --------------------------------------------------------------------
    ROUND(
        (
            SUM(OriginDepartureOnTimeRate * OriginDepartureFlightCount)
            +
            SUM(OriginArrivalOnTimeRate * OriginArrivalFlightCount)
        )
        /
        NULLIF(
            SUM(OriginDepartureFlightCount)
            +
            SUM(OriginArrivalFlightCount),
            0
        ),
        4
    ) AS OverallOnTimeRate,

    --------------------------------------------------------------------
    -- Departure Cancellation Rate
    --------------------------------------------------------------------
    ROUND(
        SUM(OriginDepartureCancellationRate * OriginDepartureFlightCount)
        /
        NULLIF(SUM(OriginDepartureFlightCount),0),
        4
    ) AS DepartureCancellationRate,

    --------------------------------------------------------------------
    -- Departure Diversion Rate
    --------------------------------------------------------------------
    ROUND(
        SUM(OriginDepartureDiversionRate * OriginDepartureFlightCount)
        /
        NULLIF(SUM(OriginDepartureFlightCount),0),
        4
    ) AS DepartureDiversionRate,

    --------------------------------------------------------------------
    -- Average Arrival Delay
    --------------------------------------------------------------------
    ROUND(AVG(OriginAverageArrivalDelay),2) AS AvgArrivalDelay,

    --------------------------------------------------------------------
    -- Flight Statistics
    --------------------------------------------------------------------
    SUM(TotalFlights) AS TotalFlights,
    SUM(OnTimeFlights) AS OnTimeFlights,
    SUM(DelayedFlights) AS DelayedFlights,
    SUM(CancelledFlights) AS CancelledFlights,
    SUM(DivertedFlights) AS DivertedFlights,

    ROUND(SUM(OnTimeFlights)*1.0/SUM(TotalFlights),4) AS OnTimeRate,
    ROUND(SUM(DelayedFlights)*1.0/SUM(TotalFlights),4) AS DelayRate,
    ROUND(SUM(CancelledFlights)*1.0/SUM(TotalFlights),4) AS CancellationRate,
    ROUND(SUM(DivertedFlights)*1.0/SUM(TotalFlights),4) AS DiversionRate

FROM viz_reliability_analytics

GROUP BY

    Year,
    OriginAirportKey,
    OriginAirportCode,
    OriginCity,
    OriginState,
    OriginRegion;
--------------------------------------------------------------------------------------------
AIRLINE PERFORMANCE
--------------------------------------------------------------------------------------------
