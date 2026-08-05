CREATE OR REPLACE VIEW flight_gold.dim_airline_slicer AS

SELECT DISTINCT
    AirlineCode,
    AirlineName,
    AirlineLabel
FROM flight_gold.viz_reliability_analytics
ORDER BY AirlineName;
