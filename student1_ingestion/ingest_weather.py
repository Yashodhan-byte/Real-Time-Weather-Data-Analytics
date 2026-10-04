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

# Expanded target cities (at least 3 cities per country across 7 major countries)
CITIES = [
    # Japan (Asia)
    {"city": "Tokyo", "country": "Japan", "region": "Asia", "lat": 35.6762, "lon": 139.6503},
    {"city": "Osaka", "country": "Japan", "region": "Asia", "lat": 34.6937, "lon": 135.5023},
    {"city": "Sapporo", "country": "Japan", "region": "Asia", "lat": 43.0618, "lon": 141.3545},

    # UK (Europe)
    {"city": "London", "country": "UK", "region": "Europe", "lat": 51.5074, "lon": -0.1278},
    {"city": "Manchester", "country": "UK", "region": "Europe", "lat": 53.4808, "lon": -2.2426},
    {"city": "Edinburgh", "country": "UK", "region": "Europe", "lat": 55.9533, "lon": -3.1883},

    # USA (North America)
    {"city": "New York", "country": "USA", "region": "North America", "lat": 40.7128, "lon": -74.0060},
    {"city": "Los Angeles", "country": "USA", "region": "North America", "lat": 34.0522, "lon": -118.2437},
    {"city": "Chicago", "country": "USA", "region": "North America", "lat": 41.8781, "lon": -87.6298},

    # Australia (Oceania)
    {"city": "Sydney", "country": "Australia", "region": "Oceania", "lat": -33.8688, "lon": 151.2093},
    {"city": "Melbourne", "country": "Australia", "region": "Oceania", "lat": -37.8136, "lon": 144.9631},
    {"city": "Brisbane", "country": "Australia", "region": "Oceania", "lat": -27.4698, "lon": 153.0251},

    # Egypt (Africa)
    {"city": "Cairo", "country": "Egypt", "region": "Africa", "lat": 30.0444, "lon": 31.2357},
    {"city": "Alexandria", "country": "Egypt", "region": "Africa", "lat": 31.2001, "lon": 29.9187},
    {"city": "Luxor", "country": "Egypt", "region": "Africa", "lat": 25.6872, "lon": 32.6396},

    # India (Asia)
    {"city": "Mumbai", "country": "India", "region": "Asia", "lat": 19.0760, "lon": 72.8777},
    {"city": "Delhi", "country": "India", "region": "Asia", "lat": 28.6139, "lon": 77.2090},
    {"city": "Bengaluru", "country": "India", "region": "Asia", "lat": 12.9716, "lon": 77.5946},

    # Brazil (South America)
    {"city": "São Paulo", "country": "Brazil", "region": "South America", "lat": -23.5505, "lon": -46.6333},
    {"city": "Rio de Janeiro", "country": "Brazil", "region": "South America", "lat": -22.9068, "lon": -43.1729},
    {"city": "Brasília", "country": "Brazil", "region": "South America", "lat": -15.7975, "lon": -47.8919}
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
