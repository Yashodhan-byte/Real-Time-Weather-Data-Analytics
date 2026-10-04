import os
import sys
import json
import time
import requests
import pandas as pd
from datetime import datetime, timedelta

# Import central configuration
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config

# Target cities across Japan's prefectures & regions (22 station nodes across 8 Japanese regions)
CITIES = [
    # Kanto Region (Greater Tokyo Area)
    {"city": "Tokyo", "prefecture": "Tokyo", "region": "Kanto", "lat": 35.6762, "lon": 139.6503, "country": "Japan"},
    {"city": "Yokohama", "prefecture": "Kanagawa", "region": "Kanto", "lat": 35.4437, "lon": 139.6380, "country": "Japan"},
    {"city": "Chiba", "prefecture": "Chiba", "region": "Kanto", "lat": 35.6074, "lon": 140.1065, "country": "Japan"},

    # Kansai Region
    {"city": "Osaka", "prefecture": "Osaka", "region": "Kansai", "lat": 34.6937, "lon": 135.5023, "country": "Japan"},
    {"city": "Kyoto", "prefecture": "Kyoto", "region": "Kansai", "lat": 35.0116, "lon": 135.7681, "country": "Japan"},
    {"city": "Kobe", "prefecture": "Hyogo", "region": "Kansai", "lat": 34.6901, "lon": 135.1955, "country": "Japan"},

    # Hokkaido Region
    {"city": "Sapporo", "prefecture": "Hokkaido", "region": "Hokkaido", "lat": 43.0618, "lon": 141.3545, "country": "Japan"},
    {"city": "Asahikawa", "prefecture": "Hokkaido", "region": "Hokkaido", "lat": 43.7706, "lon": 142.3648, "country": "Japan"},
    {"city": "Hakodate", "prefecture": "Hokkaido", "region": "Hokkaido", "lat": 41.7687, "lon": 140.7288, "country": "Japan"},

    # Tohoku Region
    {"city": "Sendai", "prefecture": "Miyagi", "region": "Tohoku", "lat": 38.2682, "lon": 140.8694, "country": "Japan"},
    {"city": "Aomori", "prefecture": "Aomori", "region": "Tohoku", "lat": 40.8244, "lon": 140.7400, "country": "Japan"},
    {"city": "Akita", "prefecture": "Akita", "region": "Tohoku", "lat": 39.7186, "lon": 140.1024, "country": "Japan"},

    # Chubu Region
    {"city": "Nagoya", "prefecture": "Aichi", "region": "Chubu", "lat": 35.1815, "lon": 136.9066, "country": "Japan"},
    {"city": "Niigata", "prefecture": "Niigata", "region": "Chubu", "lat": 37.9162, "lon": 139.0364, "country": "Japan"},
    {"city": "Kanazawa", "prefecture": "Ishikawa", "region": "Chubu", "lat": 36.5613, "lon": 136.6562, "country": "Japan"},

    # Chugoku Region
    {"city": "Hiroshima", "prefecture": "Hiroshima", "region": "Chugoku", "lat": 34.3853, "lon": 132.4553, "country": "Japan"},
    {"city": "Okayama", "prefecture": "Okayama", "region": "Chugoku", "lat": 34.6617, "lon": 133.9350, "country": "Japan"},

    # Shikoku Region
    {"city": "Matsuyama", "prefecture": "Ehime", "region": "Shikoku", "lat": 33.8392, "lon": 132.7657, "country": "Japan"},
    {"city": "Takamatsu", "prefecture": "Kagawa", "region": "Shikoku", "lat": 34.3402, "lon": 134.0433, "country": "Japan"},

    # Kyushu & Okinawa Region
    {"city": "Fukuoka", "prefecture": "Fukuoka", "region": "Kyushu & Okinawa", "lat": 33.5904, "lon": 130.4017, "country": "Japan"},
    {"city": "Kagoshima", "prefecture": "Kagoshima", "region": "Kyushu & Okinawa", "lat": 31.5966, "lon": 130.5571, "country": "Japan"},
    {"city": "Naha", "prefecture": "Okinawa", "region": "Kyushu & Okinawa", "lat": 26.2124, "lon": 127.6809, "country": "Japan"}
]

def fetch_city_weather(lat, lon, city_name="", past_days=30):
    params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": "temperature_2m,relative_humidity_2m,wind_speed_10m,precipitation,surface_pressure",
        "past_days": past_days,
        "forecast_days": 3,
        "timezone": "UTC"
    }
    for attempt in range(2):
        try:
            res = requests.get(config.OPENMETEO_BASE_URL, params=params, timeout=6)
            res.raise_for_status()
            return res.json()
        except Exception as e:
            if attempt == 1:
                raise e
            time.sleep(0.3)

def run_ingestion(output_base_dir="data"):
    print("=" * 60)
    print(f"STUDENT 1: Fetching Weather Data for {len(CITIES)} Cities (3+ per Country)...")
    print("=" * 60)
    
    all_records = []
    raw_responses = {}

    for c in CITIES:
        city_name = c["city"]
        print(f"[Ingest] Fetching weather data for {city_name}, {c['country']} ({c['region']})...")
        try:
            data = fetch_city_weather(c["lat"], c["lon"], city_name=city_name)
            raw_responses[city_name] = data
            
            hourly = data.get("hourly", {})
            timestamps = hourly.get("time", [])
            temps = hourly.get("temperature_2m", [])
            humidities = hourly.get("relative_humidity_2m", [])
            wind_speeds = hourly.get("wind_speed_10m", [])
            precips = hourly.get("precipitation", [])
            pressures = hourly.get("surface_pressure", [])

            for i in range(len(timestamps)):
                ts_str = timestamps[i]
                dt = datetime.strptime(ts_str, "%Y-%m-%dT%H:%M")
                
                record = {
                    "city": city_name,
                    "prefecture": c.get("prefecture", "Japan"),
                    "country": c["country"],
                    "region": c["region"],
                    "latitude": c["lat"],
                    "longitude": c["lon"],
                    "timestamp": ts_str,
                    "date": dt.strftime("%Y-%m-%d"),
                    "year": dt.year,
                    "month": f"{dt.month:02d}",
                    "day": f"{dt.day:02d}",
                    "hour": dt.hour,
                    "temperature": temps[i] if i < len(temps) else None,
                    "humidity": humidities[i] if i < len(humidities) else None,
                    "wind_speed": wind_speeds[i] if i < len(wind_speeds) else None,
                    "precipitation": precips[i] if i < len(precips) else None,
                    "surface_pressure": pressures[i] if i < len(pressures) else None
                }
                all_records.append(record)
            
            time.sleep(0.05)
        except Exception as e:
            print(f"[Error] Failed to fetch data for {city_name}: {e}")

    df = pd.DataFrame(all_records)
    
    raw_dir = os.path.join(output_base_dir, "raw")
    os.makedirs(raw_dir, exist_ok=True)
    
    raw_json_path = os.path.join(raw_dir, "weather_raw.json")
    with open(raw_json_path, "w", encoding="utf-8") as f:
        json.dump(raw_responses, f, indent=2)
    
    raw_csv_path = os.path.join(raw_dir, "weather_raw.csv")
    df.to_csv(raw_csv_path, index=False)
    print(f"[Storage] Saved raw JSON and CSV dataset ({len(df)} records for {len(CITIES)} cities) to: {raw_csv_path}")

    return df, raw_csv_path

if __name__ == "__main__":
    run_ingestion()
