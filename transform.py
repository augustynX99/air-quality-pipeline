

from datetime import datetime

def process_city_data(city_name, config, api_data):
    """Process and return structured air quality records for a city"""
    hourly_data = api_data.get("hourly", {})
    time_stamps = hourly_data.get("time", [])
    pollution_parameters = [
        "pm2_5", "pm10", "ozone", "carbon_monoxide",
        "nitrogen_dioxide", "sulphur_dioxide", "uv_index"
    ]

    docs = []
    for i, timestamp in enumerate(time_stamps):
        record = {
            "_id": f"{city_name}_{timestamp}",
            "city": city_name,
            "latitude": config["latitude"],
            "longitude": config["longitude"],
            "timestamp": timestamp,
            "ingestion_time": datetime.utcnow().isoformat(),
        }
        for p in pollution_parameters:
            values = hourly_data.get(p, [])
            record[p] = values[i] if i < len(values) else None
        docs.append(record)

    return docs  # ✅ Return, don’t save
