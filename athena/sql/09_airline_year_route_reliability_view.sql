CREATE OR REPLACE VIEW "airline_year_route_reliability_view" AS 
WITH
  route_metrics AS (
   SELECT
     year
   , airlinecode
   , airlinename
   , airlinelabel
   , routekey
   , route
   , routeorigin
   , routedestination
   , statepair
   , originairportcode
   , origincity
   , originstatecode
   , originstate
   , destairportcode
   , destcity
   , deststatecode
   , deststate
   , SUM(totalflights) totalflights
   , SUM(ontimeflights) ontimeflights
   , SUM(delayedflights) delayedflights
   , SUM(cancelledflights) cancelledflights
   , SUM(divertedflights) divertedflights
   , SUM(completedflights) completedflights
   , ROUND(((1E2 * SUM(ontimeflights)) / NULLIF(SUM(totalflights), 0)), 2) ontimepercentage
   , ROUND(((1E2 * SUM(delayedflights)) / NULLIF(SUM(totalflights), 0)), 2) delayrate
   , ROUND(((1E2 * SUM(cancelledflights)) / NULLIF(SUM(totalflights), 0)), 2) cancellationrate
   , ROUND(((1E2 * SUM(divertedflights)) / NULLIF(SUM(totalflights), 0)), 2) diversionrate
   , ROUND(((1E2 * SUM(completedflights)) / NULLIF(SUM(totalflights), 0)), 2) completionrate
   , ROUND(AVG(avgarrivaldelay), 2) avgarrivaldelay
   , ROUND(AVG(avgdeparturedelay), 2) avgdeparturedelay
   , ROUND((1E2 * (((7E-1 * (SUM(ontimeflights) / NULLIF(SUM(totalflights), 0))) + (2E-1 * (1 - (SUM(cancelledflights) / NULLIF(SUM(totalflights), 0))))) + (1E-1 * (1 - (SUM(divertedflights) / NULLIF(SUM(totalflights), 0)))))), 2) reliabilityscore
   FROM
     viz_reliability_analytics
   GROUP BY year, airlinecode, airlinename, airlinelabel, routekey, route, routeorigin, routedestination, statepair, originairportcode, origincity, originstatecode, originstate, destairportcode, destcity, deststatecode, deststate
) 
SELECT
  *
, DENSE_RANK() OVER (PARTITION BY airlinecode, year ORDER BY reliabilityscore DESC) reliability_rank
, DENSE_RANK() OVER (PARTITION BY airlinecode, year ORDER BY reliabilityscore ASC) bottom_reliability_rank
FROM
  route_metrics
