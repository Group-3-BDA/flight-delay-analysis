CREATE OR REPLACE VIEW dim_year AS

SELECT DISTINCT
    Year
FROM viz_reliability_analytics
ORDER BY Year;
