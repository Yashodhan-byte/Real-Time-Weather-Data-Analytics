import os

# Configuration settings for Real-Time Weather Data Analytics Pipeline

# Weather API Configuration
# Default: Open-Meteo API (Free, No Key Required)
# Optional: OpenWeatherMap / WeatherAPI / Custom Weather API Key
WEATHER_API_KEY = os.getenv("WEATHER_API_KEY", "DEMO_KEY_FREE_ACCESS")
WEATHER_API_PROVIDER = os.getenv("WEATHER_API_PROVIDER", "open-meteo") # Options: 'open-meteo', 'openweathermap', 'weatherapi'

# OpenWeatherMap API Endpoint (if using OpenWeatherMap)
OPENWEATHERMAP_BASE_URL = "https://api.openweathermap.org/data/2.5/weather"

# WeatherAPI Base URL (if using WeatherAPI.com)
WEATHERAPI_BASE_URL = "http://api.weatherapi.com/v1/history.json"

# Default Open-Meteo Endpoint
OPENMETEO_BASE_URL = "https://api.open-meteo.com/v1/forecast"
