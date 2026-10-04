-- ====================================================================
-- STUDENT 2: SPARK SQL ANALYTICS & ANOMALY DETECTION QUERIES
-- Apache Spark SQL Queries for Weather Metric Transformation
-- ====================================================================

-- 1. Daily Aggregations with 7-Day and 30-Day Moving Averages (Window Functions)
WITH daily_summaries AS (
    SELECT 
        city,
        region,
        `date`,
        ROUND(AVG(temperature), 2) AS avg_temp,
        ROUND(MIN(temperature), 2) AS min_temp,
        ROUND(MAX(temperature), 2) AS max_temp,
        ROUND(AVG(humidity), 2) AS avg_humidity,
        ROUND(SUM(precipitation), 2) AS total_precipitation,
        ROUND(AVG(wind_speed), 2) AS avg_wind_speed
    FROM weather_db.raw_weather
    GROUP BY city, region, `date`
)
SELECT 
    city,
    region,
    `date`,
    avg_temp,
    min_temp,
    max_temp,
    avg_humidity,
    total_precipitation,
    -- 7-Day Moving Average Window Calculation
    ROUND(AVG(avg_temp) OVER (
        PARTITION BY city 
        ORDER BY `date` 
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ), 2) AS moving_avg_7d,
    -- 30-Day Moving Average Window Calculation
    ROUND(AVG(avg_temp) OVER (
        PARTITION BY city 
        ORDER BY `date` 
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ), 2) AS moving_avg_30d
FROM daily_summaries
ORDER BY city, `date`;

-- 2. Anomaly Threshold Detection (Statistical Z-Score Thresholds for Extreme Weather)
WITH city_stats AS (
    SELECT 
        city,
        AVG(temperature) AS city_mean_temp,
        STDDEV(temperature) AS city_std_temp
    FROM weather_db.raw_weather
    GROUP BY city
)
SELECT 
    w.city,
    w.region,
    w.timestamp,
    w.`date`,
    w.temperature,
    s.city_mean_temp,
    ROUND((w.temperature - s.city_mean_temp) / NULLIF(s.city_std_temp, 0), 2) AS z_score,
    CASE 
        WHEN (w.temperature - s.city_mean_temp) / NULLIF(s.city_std_temp, 0) > 2.0 THEN 'EXTREME HEATWAVE'
        WHEN (w.temperature - s.city_mean_temp) / NULLIF(s.city_std_temp, 0) < -2.0 THEN 'EXTREME COLD SNAP'
        ELSE 'NORMAL'
    END AS anomaly_status
FROM weather_db.raw_weather w
JOIN city_stats s ON w.city = s.city;

-- 3. Regional Climate Summary Aggregations
SELECT 
    region,
    COUNT(DISTINCT city) as total_cities,
    ROUND(AVG(temperature), 2) as region_avg_temp,
    ROUND(MIN(temperature), 2) as region_min_temp,
    ROUND(MAX(temperature), 2) as region_max_temp,
    ROUND(AVG(humidity), 2) as region_avg_humidity,
    ROUND(SUM(precipitation), 2) as region_total_precipitation
FROM weather_db.raw_weather
GROUP BY region
ORDER BY region_avg_temp DESC;
