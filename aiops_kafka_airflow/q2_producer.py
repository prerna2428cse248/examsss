from kafka import KafkaProducer
import json
import time

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda x: json.dumps(x).encode("utf-8")
)

for i in range(10):

    message = {
        "server_id": f"server{i+1}",
        "cpu_usage": 50 + i * 4,
        "memory_usage": 60 + i
    }

    producer.send(
        "server_metrics",
        value=message
    )

    print("Sent:", message)

    time.sleep(1)

producer.flush()
producer.close()
