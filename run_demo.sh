#!/bin/bash
set -e

echo "Starting Kenya Rainfall Onset Demo..."
docker compose up -d

echo "Waiting for Kafka..."
for i in $(seq 1 12); do
    docker exec climate-kafka kafka-broker-api-versions \
        --bootstrap-server localhost:9092 > /dev/null 2>&1 && break
    echo "  attempt $i/12..."
    sleep 5
done

python src/streaming/topic_setup.py

nohup python src/streaming/kafka_producer.py > logs/producer.log 2>&1 &
echo $! >> .demo_pids

nohup python src/streaming/spark_consumer.py > logs/consumer.log 2>&1 &
echo $! >> .demo_pids

echo ""
echo "System running:"
echo "  Dashboard  -> http://localhost:8501"
echo "  API        -> http://localhost:8000"
echo "  Kafka UI   -> http://localhost:8080"
echo "  Logs       -> logs/producer.log and logs/consumer.log"
