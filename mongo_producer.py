from kafka import KafkaProducer
from pymongo import MongoClient
import json
import time
import config
from config import collection
from datetime import datetime,timezone

# producer = KafkaProducer(
#     bootstrap_servers='localhost:9092',
#     value_serializer=lambda v: json.dumps(v).encode('utf-8')
# )


# print("🚀 Starting MongoDB → Kafka producer...")

# while True:
#     for doc in collection.find().limit(10):  # you can use change streams later
#         doc['_id'] = str(doc['_id'])  # make _id serializable
#         producer.send('mongo-air-quality', value=doc)
#         print("📤 Sent:", doc)
#         time.sleep(1)

#     time.sleep(10)


# 2️⃣ Setup Kafka Producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print("🚀 MongoDB → Kafka producer started...")

def fetch_latest_records(limit=10):
    """Fetch latest documents from MongoDB."""
    # Sort by ingestion_time descending to get most recent
    return list(collection.find().sort("ingestion_time", -1).limit(limit))

def send_to_kafka(records):
    """Send each MongoDB record to Kafka topic."""
    for record in records:
        # Convert MongoDB ObjectId to string if exists
        record["_id"] = str(record.get("_id"))
        #record["sent_time"] = datetime.utcnow().isoformat()
        record["sent_time"] = datetime.now(timezone.utc).isoformat()
        # Optionally update timestamp to current time
        #timestamp = data.get('timestamp', datetime.now(timezone.utc).isoformat())

        # Send record to Kafka topic
        producer.send("mongo-air-quality", value=record)
        print(f"✅ Sent to Kafka: {record.get('city')} at {record.get('timestamp')}")
        time.sleep(0.5)  # small delay to simulate streaming

# 3️⃣ Continuous streaming simulation
try:
    while True:
        latest_records = fetch_latest_records(limit=5)
        send_to_kafka(latest_records)
        time.sleep(10)  # fetch every 10 seconds

except KeyboardInterrupt:
    print("\n🛑 Stopping producer...")
    producer.close()