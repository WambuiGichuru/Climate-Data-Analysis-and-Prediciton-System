import json
import random
import time
from datetime import datetime
from kafka import KafkaProducer
from concurrent.futures import ThreadPoolExecutor

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

counties = ["Nairobi", "Mombasa", "Kisumu", "Nakuru", "Kiambu", "Machakos", "Kajiado", "Uasin Gishu", "Meru", "Kilifi"]

def send_record():
    record = {
        "county": random.choice(counties),
        "timestamp": datetime.now().isoformat(),
        "rainfall_mm": round(random.uniform(0, 50), 2),
        "temperature_c": round(random.uniform(15, 35), 1),
        "humidity": random.randint(30, 90)
    }
    producer.send('raw-weather-stream', value=record)

print("Sending at high speed... Press Ctrl+C to stop")
start = time.time()
count = 0
try:
    with ThreadPoolExecutor(max_workers=10) as executor:
        while True:
            executor.submit(send_record)
            count += 1
            if count % 1000 == 0:
                elapsed = time.time() - start
                rate = count / elapsed
                print(f"Sent {count} records | Rate: {rate:.0f} records/sec")
except KeyboardInterrupt:
    elapsed = time.time() - start
    print(f"\nTotal: {count} records in {elapsed:.1f}s | Avg rate: {count/elapsed:.0f} rec/sec")
    producer.close()
