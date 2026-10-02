-- 1. Create weather table stored as RCFile format
CREATE TABLE IF NOT EXISTS weather_rc (
    region STRING,
    season STRING,
    rainfall DOUBLE
)
STORED AS RCFILE;

-- Load data into the RCFile table from existing raw table
INSERT OVERWRITE TABLE weather_rc 
SELECT region, season, rainfall FROM weather_raw;

-- 2. Add custom Java UDF JAR and register temporary function
ADD JAR /path/to/YieldToRainfallUDF.jar;
CREATE TEMPORARY FUNCTION calc_ratio AS 'com.bda.udf.YieldToRainfallUDF';

-- 3. JOIN tables on (region, season), apply UDF, and aggregate average by region
SELECT 
    y.region, 
    AVG(calc_ratio(y.yield, w.rainfall)) AS avg_yield_rainfall_ratio
FROM yield_data y
JOIN weather_rc w 
  ON y.region = w.region AND y.season = w.season
GROUP BY y.region;
