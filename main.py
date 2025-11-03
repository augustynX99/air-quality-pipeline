
from config import cities
from extract import fetch_city_air_quality
from transform import process_city_data
from load import save_to_mongodb

def fetch_and_store_data():
    for city, cfg in cities.items():
        try:
            print(f"\n🌍 Fetching {city}")
            raw_data = fetch_city_air_quality(cfg["latitude"], cfg["longitude"])
            docs = process_city_data(city, cfg, raw_data)
            save_to_mongodb(docs)  # ✅ Now handled here
        except Exception as e:
            print(f"❌ Error processing {city}: {e}")

if __name__ == "__main__":
    fetch_and_store_data()
