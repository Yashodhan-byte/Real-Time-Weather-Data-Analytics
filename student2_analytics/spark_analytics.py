import os
import sys
import pandas as pd
import numpy as np

def run_pyspark_analytics(raw_csv_path="data/raw/weather_raw.csv", output_dir="data/processed"):
    """
    Execute Spark SQL / PySpark Analytics Pipeline:
    1. Daily Aggregations with 7-day and 30-day Moving Averages.
    2. Temperature Anomaly Detection (Z-Score thresholding).
    3. Regional Climate Summary Aggregation.
    """
    print("=" * 60)
    print("STUDENT 2: Starting Spark SQL / PySpark Analytics Pipeline...")
    print("=" * 60)

    os.makedirs(output_dir, exist_ok=True)
    spark_success = False

    # Attempt native PySpark execution
    try:
        from pyspark.sql import SparkSession
        from pyspark.sql import functions as F
        from pyspark.sql.window import Window
        
        print("[Spark Engine] Initializing PySpark Session...")
        spark = SparkSession.builder \
            .appName("RealTimeWeatherAnalytics") \
            .master("local[*]") \
            .config("spark.sql.shuffle.partitions", "4") \
            .getOrCreate()
        
        spark.sparkContext.setLogLevel("WARN")

        # Load raw data into PySpark DataFrame
        df_spark = spark.read.csv(raw_csv_path, header=True, inferSchema=True)
        df_spark.createOrReplaceTempView("weather_raw")

        # 1. Daily Aggregations & Window Moving Averages
        daily_df = df_spark.groupBy("city", "region", "date").agg(
            F.round(F.avg("temperature"), 2).alias("avg_temp"),
            F.round(F.min("temperature"), 2).alias("min_temp"),
            F.round(F.max("temperature"), 2).alias("max_temp"),
            F.round(F.avg("humidity"), 2).alias("avg_humidity"),
            F.round(F.sum("precipitation"), 2).alias("total_precipitation"),
            F.round(F.avg("wind_speed"), 2).alias("avg_wind_speed")
        )

        window_7d = Window.partitionBy("city").orderBy("date").rowsBetween(-6, 0)
        window_30d = Window.partitionBy("city").orderBy("date").rowsBetween(-29, 0)

        daily_analytics_spark = daily_df.withColumn(
            "moving_avg_7d", F.round(F.avg("avg_temp").over(window_7d), 2)
        ).withColumn(
            "moving_avg_30d", F.round(F.avg("avg_temp").over(window_30d), 2)
        )

        # 2. Temperature Anomaly Detection (Z-Score calculation)
        city_stats = df_spark.groupBy("city").agg(
            F.avg("temperature").alias("city_mean_temp"),
            F.stddev("temperature").alias("city_std_temp")
        )

        anomaly_spark = df_spark.join(city_stats, "city") \
            .withColumn("z_score", F.round((F.col("temperature") - F.col("city_mean_temp")) / F.col("city_std_temp"), 2)) \
            .withColumn("anomaly_status", F.when(F.col("z_score") > 2.0, "EXTREME HEATWAVE")
                                          .when(F.col("z_score") < -2.0, "EXTREME COLD SNAP")
                                          .otherwise("NORMAL"))

        # 3. Regional Aggregations
        regional_spark = df_spark.groupBy("region").agg(
            F.countDistinct("city").alias("total_cities"),
            F.round(F.avg("temperature"), 2).alias("region_avg_temp"),
            F.round(F.min("temperature"), 2).alias("region_min_temp"),
            F.round(F.max("temperature"), 2).alias("region_max_temp"),
            F.round(F.avg("humidity"), 2).alias("region_avg_humidity"),
            F.round(F.sum("precipitation"), 2).alias("region_total_precipitation")
        )

        # Export outputs
        daily_pd = daily_analytics_spark.toPandas()
        anomaly_pd = anomaly_spark.toPandas()
        regional_pd = regional_spark.toPandas()

        daily_pd.to_csv(os.path.join(output_dir, "daily_analytics.csv"), index=False)
        anomaly_pd.to_csv(os.path.join(output_dir, "anomaly_analytics.csv"), index=False)
        regional_pd.to_csv(os.path.join(output_dir, "regional_analytics.csv"), index=False)

        spark.stop()
        spark_success = True
        print("[PySpark Analytics] Successfully computed and saved metrics via PySpark engine.")

    except Exception as e:
        print(f"[Spark Warning] Native PySpark session unavailable/skipped ({e}). Falling back to Pandas Spark-SQL analytical simulation engine...")

    if not spark_success:
        # Fallback Python / Pandas Analytical Engine producing exact same schema & calculations
        df = pd.read_csv(raw_csv_path)

        # 1. Daily Analytics & Moving Averages
        daily_pd = df.groupby(["city", "region", "date"]).agg(
            avg_temp=("temperature", "mean"),
            min_temp=("temperature", "min"),
            max_temp=("temperature", "max"),
            avg_humidity=("humidity", "mean"),
            total_precipitation=("precipitation", "sum"),
            avg_wind_speed=("wind_speed", "mean")
        ).reset_index()

        daily_pd = daily_pd.sort_values(["city", "date"])
        daily_pd["avg_temp"] = daily_pd["avg_temp"].round(2)
        daily_pd["min_temp"] = daily_pd["min_temp"].round(2)
        daily_pd["max_temp"] = daily_pd["max_temp"].round(2)
        daily_pd["avg_humidity"] = daily_pd["avg_humidity"].round(2)
        daily_pd["total_precipitation"] = daily_pd["total_precipitation"].round(2)
        daily_pd["avg_wind_speed"] = daily_pd["avg_wind_speed"].round(2)

        # Calculate 7-day and 30-day moving averages per city
        daily_pd["moving_avg_7d"] = daily_pd.groupby("city")["avg_temp"].transform(
            lambda x: x.rolling(7, min_periods=1).mean().round(2)
        )
        daily_pd["moving_avg_30d"] = daily_pd.groupby("city")["avg_temp"].transform(
            lambda x: x.rolling(30, min_periods=1).mean().round(2)
        )

        # 2. Temperature Anomaly Detection (Z-Score)
        df_stats = df.groupby("city")["temperature"].agg(["mean", "std"]).reset_index()
        df_stats.columns = ["city", "city_mean_temp", "city_std_temp"]
        
        anomaly_pd = pd.merge(df, df_stats, on="city")
        anomaly_pd["z_score"] = ((anomaly_pd["temperature"] - anomaly_pd["city_mean_temp"]) / anomaly_pd["city_std_temp"].replace(0, np.nan)).round(2)
        
        conditions = [
            anomaly_pd["z_score"] > 2.0,
            anomaly_pd["z_score"] < -2.0
        ]
        choices = ["EXTREME HEATWAVE", "EXTREME COLD SNAP"]
        anomaly_pd["anomaly_status"] = np.select(conditions, choices, default="NORMAL")

        # 3. Regional Aggregations
        regional_pd = df.groupby("region").agg(
            total_cities=("city", "nunique"),
            region_avg_temp=("temperature", lambda x: round(x.mean(), 2)),
            region_min_temp=("temperature", lambda x: round(x.min(), 2)),
            region_max_temp=("temperature", lambda x: round(x.max(), 2)),
            region_avg_humidity=("humidity", lambda x: round(x.mean(), 2)),
            region_total_precipitation=("precipitation", lambda x: round(x.sum(), 2))
        ).reset_index()

        # Save processed outputs
        daily_pd.to_csv(os.path.join(output_dir, "daily_analytics.csv"), index=False)
        anomaly_pd.to_csv(os.path.join(output_dir, "anomaly_analytics.csv"), index=False)
        regional_pd.to_csv(os.path.join(output_dir, "regional_analytics.csv"), index=False)

        print("[Fallback Analytics Engine] Successfully computed analytics and exported CSV output datasets.")

    print(f"[Analytics Storage] Results generated in directory: {output_dir}")
    print(f"  - daily_analytics.csv: {len(daily_pd)} records")
    print(f"  - anomaly_analytics.csv: {len(anomaly_pd)} records")
    print(f"  - regional_analytics.csv: {len(regional_pd)} records")

    return output_dir

if __name__ == "__main__":
    run_pyspark_analytics()
