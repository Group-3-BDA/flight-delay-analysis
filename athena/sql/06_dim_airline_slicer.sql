CREATE OR REPLACE VIEW dim_airline_slicer AS

SELECT DISTINCT
    AirlineCode,
    AirlineName,
    AirlineLabel
FROM viz_reliability_analytics
ORDER BY AirlineName;
