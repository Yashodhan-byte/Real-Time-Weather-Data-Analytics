import os
import shutil
import pandas as pd

def store_in_hive_hdfs_format(csv_file_path, base_hdfs_dir="data/hdfs_hive"):
    """
    Simulate HDFS Hive external partitioned table directory layout:
    hdfs://warehouse/weather/raw_weather/region=.../year=YYYY/month=MM/
    """
    print("=" * 60)
    print("STUDENT 1: Partitioning Data into Hive / HDFS Directory Structure...")
    print("=" * 60)

    df = pd.read_csv(csv_file_path)
    table_dir = os.path.join(base_hdfs_dir, "raw_weather")
    
    if os.path.exists(table_dir):
        shutil.rmtree(table_dir)
        
    os.makedirs(table_dir, exist_ok=True)

    partition_count = 0
    grouped = df.groupby(["region", "year", "month"])
    for (region, year, month), group in grouped:
        # Standard Hive partition naming convention
        partition_path = os.path.join(table_dir, f"region={region}", f"year={year}", f"month={month:02d}")
        os.makedirs(partition_path, exist_ok=True)
        
        # Save partitioned chunk CSV
        file_path = os.path.join(partition_path, "data.csv")
        group.to_csv(file_path, index=False)
        partition_count += 1

    print(f"[HDFS/Hive Partitioning] Successfully organized dataset into {partition_count} partitions in: {table_dir}")
    return table_dir

if __name__ == "__main__":
    import sys
    raw_path = sys.argv[1] if len(sys.argv) > 1 else "data/raw/weather_raw.csv"
    store_in_hive_hdfs_format(raw_path)
