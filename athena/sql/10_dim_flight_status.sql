CREATE OR REPLACE VIEW dim_flight_status AS
SELECT 'On-Time Flights' AS FlightStatus
UNION ALL
SELECT 'Delayed Flights'
UNION ALL
SELECT 'Cancelled Flights'
UNION ALL
SELECT 'Diverted Flights';
