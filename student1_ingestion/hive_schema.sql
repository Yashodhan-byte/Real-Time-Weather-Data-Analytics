-- ====================================================================
-- STUDENT 1: HIVE DDL SCHEMAS FOR WEATHER DATA ANALYTICS PIPELINE
-- Hive / Spark SQL Table Definitions & External Partitioning
-- ====================================================================

CREATE DATABASE IF NOT EXISTS weather_db;
USE weather_db;

-- 1. Raw Partitioned External Hive Table (Points to HDFS Partitioned CSV files)
CREATE EXTERNAL TABLE IF NOT EXISTS weather_db.raw_weather (
    city STRING,
    country STRING,
    latitude DOUBLE,
    longitude DOUBLE,
    timestamp STRING,
    `date` STRING,
    day STRING,
    hour INT,
    temperature DOUBLE,
    humidity DOUBLE,
    wind_speed DOUBLE,
    precipitation DOUBLE,
    surface_pressure DOUBLE
)
PARTITIONED BY (
    region STRING,
    year INT,
    month STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/hive/warehouse/weather_db.db/raw_weather';

-- Recover partitions automatically in Hive
MSCK REPAIR TABLE weather_db.raw_weather;

-- 2. Managed Hive Table for Spark SQL Processed Analytics (ORC / Parquet format for fast querying)
CREATE TABLE IF NOT EXISTS weather_db.daily_analytics (
    city STRING,
    region STRING,
    `date` STRING,
    avg_temp DOUBLE,
    min_temp DOUBLE,
    max_temp DOUBLE,
    moving_avg_7d DOUBLE,
    moving_avg_30d DOUBLE,
    total_precipitation DOUBLE,
    avg_humidity DOUBLE,
    temp_anomaly_flag STRING,
    z_score DOUBLE
)
STORED AS PARQUET;
