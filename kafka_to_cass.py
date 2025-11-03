from kafka import KafkaConsumer
from cassandra.cluster import Cluster
import json
from datetime import datetime,timezone

# 1️⃣ Setup Kafka consumer
consumer = KafkaConsumer(
    'mongo-air-quality',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

# 2️⃣ Connect to Cassandra
cluster = Cluster(['localhost'])
session = cluster.connect()

# 3️⃣ Create keyspace and table if not exist
session.execute("""
CREATE KEYSPACE IF NOT EXISTS air_data
WITH REPLICATION = { 'class': 'SimpleStrategy', 'replication_factor': 1 }
""")

session.execute("""
CREATE TABLE IF NOT EXISTS air_data.measurements (
    city text,
    timestamp text,
    pm2_5 float,
    pm10 float,
    ozone float,
    carbon_monoxide float,
    nitrogen_dioxide float,
    sulphur_dioxide float,
    uv_index float,
    latitude double,
    longitude double,
    ingestion_time text,
    PRIMARY KEY ((city), timestamp)
) WITH CLUSTERING ORDER BY (timestamp DESC)
""")

print("🎯 Kafka → Cassandra consumer running...")

# 4️⃣ Consume messages from Kafka topic continuously
for message in consumer:
    try:
        # Extract message data
        data = message.value
        
        # Extract individual fields
        city = data.get('city')
        #timestamp = data.get('timestamp', datetime.utcnow().isoformat())
        timestamp = data.get('timestamp', datetime.now(timezone.utc).isoformat())
        
        # Insert into Cassandra
        session.execute("""
            INSERT INTO air_data.measurements
            (city, timestamp, pm2_5, pm10, ozone, carbon_monoxide, 
             nitrogen_dioxide, sulphur_dioxide, uv_index, latitude, 
             longitude, ingestion_time)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            city,
            timestamp,
            data.get('pm2_5'),
            data.get('pm10'),
            data.get('ozone'),
            data.get('carbon_monoxide'),
            data.get('nitrogen_dioxide'),
            data.get('sulphur_dioxide'),
            data.get('uv_index'),
            data.get('latitude'),
            data.get('longitude'),
            data.get('ingestion_time')
        ))
        
        print(f"✅ Inserted record: {city} at {timestamp}")

    except Exception as e:
        print(f"✗ Error inserting into Cassandra: {e}")
