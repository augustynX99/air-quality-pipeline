import requests
from datetime import datetime
from config import AIR_API_BASE 

 

def fetch_city_air_quality(latitude, longitude):
    """Fetch air quality data for given coordinates"""
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "hourly": "pm2_5,pm10,ozone,carbon_monoxide,nitrogen_dioxide,sulphur_dioxide,uv_index"
    }
    try:
        response = requests.get(AIR_API_BASE, params=params)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        print(f"🌐 API Error: {e}")
        return None
