# examsss
# Integrated AIOps + Kafka + Airflow Lab

This single codebase implements all 5 sample questions:

1. AIOps log anomaly detection
2. Kafka topic + producer
3. Python Kafka consumer
4. Airflow AIOps workflow
5. Integrated Kafka AIOps monitoring consumer

## Project structure

```text
aiops_kafka_airflow/
├── q1_anomaly_detection.py
├── q2_kafka_producer.py
├── q3_kafka_consumer.py
├── q5_aiops_monitor.py
├── requirements.txt
├── docker-compose.yml
├── data/
│   └── server_metrics.csv
└── airflow/
    └── dags/
        └── aiops_workflow.py
```

## 1. Install Python packages

```bash
pip install -r requirements.txt
```

For Question 2, 3 and 5, Kafka must be running.

## 2. Start Kafka

This project uses Kafka in KRaft mode through Docker Compose:

```bash
docker compose up -d
```

Check:

```bash
docker compose ps
```

Stop Kafka:

```bash
docker compose down
```

## 3. Question 1

Run:

```bash
python q1_anomaly_detection.py
```

It creates/reads `data/server_metrics.csv`, calculates statistics, detects anomalies and saves:

```text
data/anomaly_plot.png
```

The sample data is constructed to produce exactly 3 anomalous CPU records.

## 4. Question 2

Create the topic:

```bash
python q2_kafka_producer.py --create-topic
```

Send 10 messages:

```bash
python q2_kafka_producer.py
```

The producer prints every published message.

## 5. Question 3

Open a second terminal and run:

```bash
python q3_kafka_consumer.py
```

Then run the producer again:

```bash
python q2_kafka_producer.py
```

The consumer displays received metrics and prints an alert whenever CPU > 80.

## 6. Question 4 - Airflow

Set your Airflow home if needed:

```bash
export AIRFLOW_HOME=$PWD/airflow
```

Initialize the database:

```bash
airflow db migrate
```

Start the scheduler:

```bash
airflow scheduler
```

In another terminal:

```bash
airflow dags list
airflow dags trigger aiops_workflow
```

The DAG executes:

```text
collect_metrics
       ↓
process_metrics
       ↓
detect_anomaly
       ↓
generate_report
```

## 7. Question 5

Run:

```bash
python q5_aiops_monitor.py
```

Then publish messages:

```bash
python q2_kafka_producer.py
```

The monitor counts all messages with CPU > 80.

Press `Ctrl+C` to stop it and print the final anomaly count.

## Expected concepts demonstrated

- AIOps metrics and logs
- Basic descriptive statistics
- Threshold anomaly detection
- Kafka broker
- Kafka topic
- Producer
- Consumer
- Real-time monitoring
- Airflow DAG
- PythonOperator
- Task dependencies
- Integrated anomaly monitoring
