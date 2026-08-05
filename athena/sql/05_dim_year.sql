CREATE OR REPLACE VIEW flight_gold.dim_year AS

SELECT DISTINCT
    Year
FROM flight_gold.viz_reliability_analytics
ORDER BY Year;
