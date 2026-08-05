CREATE OR REPLACE VIEW flight_gold.seasonality_airline_flight_status_trend AS

-- On-Time Flights
SELECT
    Year,
    SeasonIndicator,
    AirlineCode,
    AirlineName,
    AirlineLabel,
    'On-Time Flights' AS FlightStatus,
    SUM(OnTimeFlights) AS FlightCount
FROM flight_gold.viz_reliability_analytics
GROUP BY
    Year,
    SeasonIndicator,
    AirlineCode,
    AirlineName,
    AirlineLabel

UNION ALL

-- Delayed Flights
SELECT
    Year,
    SeasonIndicator,
    AirlineCode,
    AirlineName,
    AirlineLabel,
    'Delayed Flights' AS FlightStatus,
    SUM(DelayedFlights) AS FlightCount
FROM flight_gold.viz_reliability_analytics
GROUP BY
    Year,
    SeasonIndicator,
    AirlineCode,
    AirlineName,
    AirlineLabel

UNION ALL

-- Cancelled Flights
SELECT
    Year,
    SeasonIndicator,
    AirlineCode,
    AirlineName,
    AirlineLabel,
    'Cancelled Flights' AS FlightStatus,
    SUM(CancelledFlights) AS FlightCount
FROM flight_gold.viz_reliability_analytics
GROUP BY
    Year,
    SeasonIndicator,
    AirlineCode,
    AirlineName,
    AirlineLabel

UNION ALL

-- Diverted Flights
SELECT
    Year,
    SeasonIndicator,
    AirlineCode,
    AirlineName,
    AirlineLabel,
    'Diverted Flights' AS FlightStatus,
    SUM(DivertedFlights) AS FlightCount
FROM flight_gold.viz_reliability_analytics
GROUP BY
    Year,
    SeasonIndicator,
    AirlineCode,
    AirlineName,
    AirlineLabel;
