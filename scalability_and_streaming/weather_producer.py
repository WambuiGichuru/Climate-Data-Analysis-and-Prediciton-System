import json
import random
import time
from datetime import datetime
from kafka import KafkaProducer

# Kafka configuration
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# List of Kenyan counties
counties = [
    "Nairobi", "Mombasa", "Kisumu", "Nakuru", "Kiambu",
    "Machakos", "Kajiado", "Uasin Gishu", "Meru", "Kilifi"
]

def generate_weather_record():
    """Generate a random weather record for a county"""
    return {
        "county": random.choice(counties),
        "timestamp": datetime.now().isoformat(),
        "rainfall_mm": round(random.uniform(0, 50), 2),
        "temperature_c": round(random.uniform(15, 35), 1),
        "humidity": random.randint(30, 90)
    }

print("Starting weather data producer... Press Ctrl+C to stop.")
print(f"Sending data to Kafka topic: raw-weather-stream")

record_count = 0
try:
    while True:
        record = generate_weather_record()
        producer.send('raw-weather-stream', value=record)
        record_count += 1
        print(f"[{record_count}] Sent: {record}")
        time.sleep(1)  # Send one record per second
except KeyboardInterrupt:
    print(f"\nStopped. Total records sent: {record_count}")
    producer.close()
