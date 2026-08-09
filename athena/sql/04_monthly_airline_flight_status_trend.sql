CREATE OR REPLACE VIEW monthly_airline_flight_status_trend AS

-- On-Time Flights
SELECT
    Year,
    Month,
    AirlineCode,
    AirlineName,
    AirlineLabel,
    'On-Time Flights' AS FlightStatus,
    SUM(OnTimeFlights) AS FlightCount
FROM viz_reliability_analytics
GROUP BY
    Year,
    Month,
    AirlineCode,
    AirlineName,
    AirlineLabel

UNION ALL

-- Delayed Flights
SELECT
    Year,
    Month,
    AirlineCode,
    AirlineName,
    AirlineLabel,
    'Delayed Flights' AS FlightStatus,
    SUM(DelayedFlights) AS FlightCount
FROM viz_reliability_analytics
GROUP BY
    Year,
    Month,
    AirlineCode,
    AirlineName,
    AirlineLabel

UNION ALL

-- Cancelled Flights
SELECT
    Year,
    Month,
    AirlineCode,
    AirlineName,
    AirlineLabel,
    'Cancelled Flights' AS FlightStatus,
    SUM(CancelledFlights) AS FlightCount
FROM viz_reliability_analytics
GROUP BY
    Year,
    Month,
    AirlineCode,
    AirlineName,
    AirlineLabel

UNION ALL

-- Diverted Flights
SELECT
    Year,
    Month,
    AirlineCode,
    AirlineName,
    AirlineLabel,
    'Diverted Flights' AS FlightStatus,
    SUM(DivertedFlights) AS FlightCount
FROM viz_reliability_analytics
GROUP BY
    Year,
    Month,
    AirlineCode,
    AirlineName,
    AirlineLabel;
---------------------------------------------------------
AIRPORT PERFORMANCE:
--------------------------------------------------------
