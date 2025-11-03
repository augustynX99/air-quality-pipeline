from pymongo import MongoClient
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

# MongoDB connection (use .env variable or fallback IP)
MONGO_URI = os.getenv("MONGO_URI", "mongodb://10.42.4.228:27017")

client = MongoClient(MONGO_URI)
db = client["air_db"]
collection = db["air_qlty"]

# Define the base API endpoint
AIR_API_BASE = "https://air-quality-api.open-meteo.com/v1/air-quality"

cities = {
    "Nairobi": {"latitude": -1.286389, "longitude": 36.817223},
    "Mombasa": {"latitude": -4.043477, "longitude": 39.668206},
}




"""
https://air-quality-api.open-meteo.com/v1/air-quality?
latitude=-1.286389&longitude=36.817223&
hourly=pm2_5,pm10,ozone,carbon_monoxide,nitrogen_dioxide,sulphur_dioxide,uv_index
"""