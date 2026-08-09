CREATE OR REPLACE VIEW airline_reliability_status_view_new AS

-- On-Time Flights
SELECT
    Year,
    AirlineCode,
    AirlineName,
    AirlineLabel,
    'On-Time Flights' AS FlightStatus,

    SUM(OnTimeFlights) AS FlightCount,

    ROUND(
        (
            0.7 * AVG(AirlineOnTimeRate)
            +
            0.2 * (1 - AVG(AirlineDiversionRate))
            +
            0.1 * (1 - AVG(AirlineCancellationRate))
        ),
        4
    ) AS ReliabilityScore,

    ROUND(AVG(AirlineOnTimeRate),4) AS StatusRate

FROM viz_reliability_analytics

GROUP BY
    Year,
    AirlineCode,
    AirlineName,
    AirlineLabel

UNION ALL

-- Delayed Flights
SELECT
    Year,
    AirlineCode,
    AirlineName,
    AirlineLabel,
    'Delayed Flights' AS FlightStatus,

    SUM(DelayedFlights) AS FlightCount,

    ROUND(
        (
            0.7 * AVG(AirlineOnTimeRate)
            +
            0.2 * (1 - AVG(AirlineDiversionRate))
            +
            0.1 * (1 - AVG(AirlineCancellationRate))
        ),
        4
    ) AS ReliabilityScore,

    ROUND(1 - AVG(AirlineOnTimeRate),4) AS StatusRate

FROM viz_reliability_analytics

GROUP BY
    Year,
    AirlineCode,
    AirlineName,
    AirlineLabel

UNION ALL

-- Cancelled Flights
SELECT
    Year,
    AirlineCode,
    AirlineName,
    AirlineLabel,
    'Cancelled Flights' AS FlightStatus,

    SUM(CancelledFlights) AS FlightCount,

    ROUND(
        (
            0.7 * AVG(AirlineOnTimeRate)
            +
            0.2 * (1 - AVG(AirlineDiversionRate))
            +
            0.1 * (1 - AVG(AirlineCancellationRate))
        ),
        4
    ) AS ReliabilityScore,

    ROUND(AVG(AirlineCancellationRate),4) AS StatusRate

FROM viz_reliability_analytics

GROUP BY
    Year,
    AirlineCode,
    AirlineName,
    AirlineLabel

UNION ALL

-- Diverted Flights
SELECT
    Year,
    AirlineCode,
    AirlineName,
    AirlineLabel,
    'Diverted Flights' AS FlightStatus,

    SUM(DivertedFlights) AS FlightCount,

    ROUND(
        (
            0.7 * AVG(AirlineOnTimeRate)
            +
            0.2 * (1 - AVG(AirlineDiversionRate))
            +
            0.1 * (1 - AVG(AirlineCancellationRate))
        ),
        4
    ) AS ReliabilityScore,

    ROUND(AVG(AirlineDiversionRate),4) AS StatusRate

FROM viz_reliability_analytics

GROUP BY
    Year,
    AirlineCode,
    AirlineName,
    AirlineLabel;

--------------------------------------------------------------------------------------
ROUTE RELIABILITY
--------------------------------------------------------------------------------------
