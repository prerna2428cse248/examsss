import json

from kafka import KafkaConsumer

BROKER = "localhost:9092"
TOPIC = "server_metrics"


def main():
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BROKER,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="aiops-question-5",
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
    )

    anomaly_count = 0

    print("=" * 50)
    print("Integrated AIOps Kafka Monitoring System")
    print("=" * 50)
    print("Waiting for server metrics...")
    print("Press Ctrl+C to stop.\n")

    try:
        for message in consumer:
            data = message.value

            server_id = data["server_id"]
            cpu = data["cpu_usage"]

            print(
                f"Message received: {server_id} | "
                f"CPU: {cpu}%"
            )

            if cpu > 80:
                anomaly_count += 1
                print("ALERT: High CPU detected")
            else:
                print("Normal")

            print(f"Running anomaly count: {anomaly_count}")
            print("-" * 40)

    except KeyboardInterrupt:
        print("\n" + "=" * 50)
        print(f"Total anomalies detected: {anomaly_count}")
        print("=" * 50)
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
