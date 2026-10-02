-- 1. Load datasets from HDFS
yield_data = LOAD '/hdfs/path/yield_data' USING PigStorage(',') 
    AS (region:chararray, season:chararray, yield:double);

weather_data = LOAD '/hdfs/path/weather_data' USING PigStorage(',') 
    AS (region:chararray, season:chararray, rainfall:double);

-- 2. JOIN relations on (region, season)
joined_data = JOIN yield_data BY (region, season), weather_data BY (region, season);

-- 3. Compute yield-to-rainfall ratio inside FOREACH ... GENERATE
calculated_ratio = FOREACH joined_data GENERATE 
    yield_data::region AS region,
    (weather_data::rainfall > 0 ? (yield_data::yield / weather_data::rainfall) : 0.0) AS ratio;

-- 4. GROUP BY region and calculate average ratio
grouped_by_region = GROUP calculated_ratio BY region;
final_result = FOREACH grouped_by_region GENERATE 
    group AS region, 
    AVG(calculated_ratio.ratio) AS avg_yield_rainfall_ratio;

DUMP final_result;
