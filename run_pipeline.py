import os
import sys
import subprocess
import time

def run_cmd(description, command):
    print(f"\n[PIPELINE STEP] {description}")
    print(f"Executing: {command}")
    res = subprocess.run(command, shell=True)
    if res.returncode != 0:
        print(f"[ERROR] Step failed: {description}")
        return False
    return True

def main():
    print("=" * 70)
    print("REAL-TIME WEATHER DATA ANALYTICS PIPELINE")
    print("   Tech Stack: Python + Spark SQL / PySpark + Hive DDL + Power BI / Dashboard")
    print("=" * 70)

    base_dir = os.path.dirname(os.path.abspath(__file__))
    python_exe = sys.executable

    # Step 1: Data Ingestion (Student 1)
    s1_ingest = os.path.join(base_dir, "student1_ingestion", "ingest_weather.py")
    if not run_cmd("Student 1: Ingesting Data from Open-Meteo API...", f'"{python_exe}" "{s1_ingest}"'):
        sys.exit(1)

    # Step 2: HDFS / Hive Partitioning (Student 1)
    s1_store = os.path.join(base_dir, "student1_ingestion", "store_hdfs_hive.py")
    raw_csv = os.path.join(base_dir, "data", "raw", "weather_raw.csv")
    if not run_cmd("Student 1: Partitioning Data into Hive/HDFS directory structure...", f'"{python_exe}" "{s1_store}" "{raw_csv}"'):
        sys.exit(1)

    # Step 3: Spark SQL & PySpark Analytics Engine (Student 2)
    s2_analytics = os.path.join(base_dir, "student2_analytics", "spark_analytics.py")
    if not run_cmd("Student 2: Executing PySpark / Spark SQL Analytics...", f'"{python_exe}" "{s2_analytics}"'):
        sys.exit(1)

    print("\n" + "=" * 70)
    print("[SUCCESS] PIPELINE EXECUTION COMPLETE!")
    print("   Data processed and exported to data/processed/")
    print("=" * 70)

    # Step 4: Start Web Analytics Dashboard (Student 3)
    s3_dashboard = os.path.join(base_dir, "student3_visualization", "dashboard_app.py")
    print("\n[STUDENT 3] Launching Interactive Weather Analytics Dashboard...")
    print("Access the dashboard in your browser at: http://localhost:5000\n")
    
    subprocess.run(f'"{python_exe}" "{s3_dashboard}"', shell=True)

if __name__ == "__main__":
    main()
