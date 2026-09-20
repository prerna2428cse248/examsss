AIOps + Kafka + Airflow Practical — Setup & Execution Reference

Purpose: remember exactly how this practical was set up, run, verified, and debugged in GitHub Codespaces so similar practicals can be solved without repeating the same mistakes.

1. Environment

Platform: GitHub Codespaces

Project:
/workspaces/examsss/aiops_kafka_airflow

OS/shell: Linux + bash

Python: 3.14.2

Docker: 29.8.0-1

Virtual environment:
.venv

Activate it:

cd /workspaces/examsss/aiops_kafka_airflow
source .venv/bin/activate

When the prompt starts with (.venv), the environment is active.

2. Final Project Structure

aiops_kafka_airflow/
├── q1_anomaly_detection.py
├── q2_topic.py
├── q2_producer.py
├── q3_consumer.py
├── q5_aiops_monitor.py
├── docker-compose.yml
├── requirements.txt
├── data/
│   └── server_metrics.csv
└── airflow/
    └── dags/
        └── aiops_workflow.py

The practical was intentionally kept as one codebase.

3. Installed Software / Packages

Kafka Python library

Initially:

ModuleNotFoundError: No module named 'kafka'

Fix:

pip install kafka-python-ng

Installed:

kafka-python-ng 2.2.3

Verify:

python -c "from kafka import KafkaProducer; print('Kafka library OK')"

Expected:

Kafka library OK

Airflow

Installed:

pip install "apache-airflow==3.3.2" --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.3.2/constraints-3.14.txt"

Verify:

airflow version
pip check

Expected:

3.3.2
No broken requirements found.

4. Kafka with Docker

docker-compose.yml used:

services:
  kafka:
    image: apache/kafka:4.0.1
    container_name: aiops-kafka
    ports:
      - "9092:9092"
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_LISTENERS: PLAINTEXT://:9092,CONTROLLER://:9093
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka:9093
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0
      KAFKA_NUM_PARTITIONS: 1

Start:

docker compose up -d

Check:

docker ps

Expected container:

aiops-kafka

Kafka port:

9092

5. Q1 — AIOps Log Anomaly Detection

Run:

python q1_anomaly_detection.py

Successful result:

Total records: 20
Anomalies detected: 3

Detected anomalies:

10:05  CPU=95  Memory=64  Response=410
10:12  CPU=97  Memory=70  Response=450
10:18  CPU=92  Memory=72  Response=430

Plot:

data/anomaly_plot.png

Status: COMPLETED.

Do not unnecessarily modify this working implementation.

6. Q2 — Kafka Topic + Producer

Faculty-provided producer was used.

q2_producer.py

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

Run:

python q2_producer.py

Each run sends 10 messages.

Important values:

server1  CPU 50%
...
server8  CPU 78%
server9  CPU 82%   <- anomaly
server10 CPU 86%   <- anomaly

q2_topic.py

from kafka.admin import KafkaAdminClient, NewTopic

admin = KafkaAdminClient(
    bootstrap_servers="localhost:9092"
)

topic = NewTopic(
    name="server_metrics",
    num_partitions=1,
    replication_factor=1
)

admin.create_topics(new_topics=[topic])

print("Topic created successfully!")

admin.close()

Run:

python q2_topic.py

If:

kafka.errors.TopicAlreadyExistsError

appears, the topic already exists. It is not a Kafka failure.

Topic:

server_metrics

Status: COMPLETED.

7. Q3 — Kafka Consumer

Faculty-provided consumer was used.

q3_consumer.py

from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    "server_metrics",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="aiops-monitor",
    value_deserializer=lambda value: json.loads(value.decode("utf-8"))
)

print("Waiting for messages...")

for message in consumer:

    data = message.value

    server = data["server_id"]
    cpu = data["cpu_usage"]
    memory = data["memory_usage"]

    print("\nReceived:")
    print("Server:", server)
    print("CPU:", cpu, "%")
    print("Memory:", memory, "%")

    if cpu > 80:
        print("ALERT: High CPU detected on", server)

Run:

python q3_consumer.py

Expected alerts:

ALERT: High CPU detected on server9
ALERT: High CPU detected on server10

Important Kafka lesson:

auto_offset_reset="earliest" plus a new consumer group can cause previously stored messages to be read. Therefore repeated server sequences can be normal.

Stop a continuous consumer with:

Ctrl+C

Status: COMPLETED.

8. Q4 — Airflow AIOps Workflow

DAG:

airflow/dags/aiops_workflow.py

Tasks:

collect_metrics
      ↓
process_metrics
      ↓
detect_anomaly
      ↓
generate_report

Sample metrics:

CPU = 87%
Memory = 65%
Response Time = 420 ms

Detection rule:

CPU > 80%

Result:

Anomaly detected: High CPU usage

DAG folder issue

Airflow initially looked at:

/home/codespace/airflow/dags

Project DAG was at:

/workspaces/examsss/aiops_kafka_airflow/airflow/dags

Fix:

export AIRFLOW__CORE__DAGS_FOLDER=/workspaces/examsss/aiops_kafka_airflow/airflow/dags

Verify:

echo $AIRFLOW__CORE__DAGS_FOLDER

DAG parse verification

python -c "from airflow.dag_processing.dagbag import DagBag; d=DagBag(dag_folder='/workspaces/examsss/aiops_kafka_airflow/airflow/dags'); print('DAGs:', list(d.dags.keys())); print('IMPORT ERRORS:', d.import_errors)"

Successful:

DAGs: ['aiops_workflow']
IMPORT ERRORS: {}

Run

airflow dags test aiops_workflow 2026-09-20

Successful report:

===== AIOps Report =====
Metrics collected successfully
Metrics processed successfully
Anomaly detection completed
========================

Final:

Dag run in success state
state=success

Graphviz warning:

Could not import graphviz.

This did not prevent DAG execution; it only affected graphical rendering.

Status: COMPLETED.

9. Q5 — Integrated AIOps Monitoring

Q5 extended Kafka monitoring with an anomaly counter.

Working behavior:

Consume server_metrics

Display server and CPU

If CPU > 80%, print alert

Increment anomaly count

Print running count

Successful examples:

Message received: server9 | CPU: 82%
ALERT: High CPU detected
Running anomaly count: 1

Message received: server10 | CPU: 86%
ALERT: High CPU detected
Running anomaly count: 2

The count eventually reached:

Running anomaly count: 6

This was correct because the producer had been run three times.

Each producer batch has two anomalies:

server9
server10

Therefore:

2 anomalies × 3 batches = 6

Stop:

Ctrl+C

Status: COMPLETED.

10. Problems We Encountered and Fixes

No module named 'kafka'

Fix:

source .venv/bin/activate
pip install kafka-python-ng

TopicAlreadyExistsError

Meaning:

server_metrics already exists

Do not recreate it unnecessarily.

Consumer reads old messages

Cause:

auto_offset_reset="earliest"

with a new consumer group.

Effect:
Previously stored Kafka messages can be processed.

Airflow showed no DAGs

Cause:
Airflow was using the default DAG directory rather than the project directory.

Fix:

export AIRFLOW__CORE__DAGS_FOLDER=/workspaces/examsss/aiops_kafka_airflow/airflow/dags

Graphviz warning

It did not break the DAG. Only graphical rendering was affected.

11. Standard Startup Procedure for Future Practicals

cd /workspaces/examsss/aiops_kafka_airflow
source .venv/bin/activate
python --version
airflow version
docker --version
pip check
docker compose up -d
docker ps

For Airflow:

export AIRFLOW__CORE__DAGS_FOLDER=/workspaces/examsss/aiops_kafka_airflow/airflow/dags

12. Recommended Approach for Similar Future Questions

Do not immediately rewrite the entire project.

Use:

1. Read the exact question.
2. Identify what is already installed.
3. Check Docker/Kafka/Airflow status.
4. Reuse the existing codebase.
5. Reuse faculty code when appropriate.
6. Add only the new requirement.
7. Run one component at a time.
8. Verify output.
9. Move to the next question only after verification.

Most important rule:

Do not change a component that is already working unless the new question actually requires the change.

13. Useful Command Cheat Sheet

cd /workspaces/examsss/aiops_kafka_airflow
source .venv/bin/activate
python --version
airflow version
docker --version
pip check

docker compose up -d
docker ps

python q1_anomaly_detection.py
python q2_topic.py
python q2_producer.py
python q3_consumer.py
python q5_aiops_monitor.py

airflow dags test aiops_workflow 2026-09-20

docker compose down

Stop a continuous Kafka consumer:

Ctrl+C

14. Final Verified State

Q1  Log anomaly detection             COMPLETED
Q2  Kafka topic + producer            COMPLETED
Q3  Kafka consumer + alerts           COMPLETED
Q4  Airflow AIOps workflow            COMPLETED
Q5  Integrated anomaly counter        COMPLETED

Everything was implemented and demonstrated in one GitHub Codespaces project.

15. Future Reference Checklist

Before installing anything again:

python --version
airflow version
docker --version
pip check
docker ps

Before modifying code, ask:

What is already working?

What exactly is new in the question?

Which faculty code should remain unchanged?

Can an existing topic be reused?

Is this a new Kafka producer/consumer or an extension?

Does the Airflow DAG need another task or a separate DAG?

Will a new consumer group read old Kafka messages?

Can the existing .venv and installed packages be reused?

This reference exists specifically to prevent repeating the setup/debugging mistakes from this practical.




-----------------------------------------------------------
# AIOps Kafka + Airflow Practical
## Complete Setup, Installation, Verification and Execution Guide

---

# 1. Enter the Project

Open the GitHub Codespace terminal and run:

```bash
cd /workspaces/examsss/aiops_kafka_airflow
```

Check the current directory:

```bash
pwd
```

Check project files:

```bash
ls
```

---

# 2. Check Environment Versions

Check Python:

```bash
python --version
```

Expected:

```text
Python 3.14.2
```

Check Docker:

```bash
docker --version
```

Expected:

```text
Docker version 29.8.0-1
```

Check Docker Compose:

```bash
docker compose version
```

---

# 3. Create Python Virtual Environment

Create the virtual environment:

```bash
python -m venv .venv
```

Activate it:

```bash
source .venv/bin/activate
```

Verify Python:

```bash
which python
python --version
```

The terminal should show:

```text
(.venv)
```

---

# 4. Install Kafka Python Library

Install:

```bash
pip install kafka-python-ng
```

Check installation:

```bash
pip show kafka-python-ng
```

Expected version:

```text
Version: 2.2.3
```

Verify that Kafka can be imported:

```bash
python -c "from kafka import KafkaProducer, KafkaConsumer; print('Kafka library OK')"
```

Expected:

```text
Kafka library OK
```

---

# 5. Start Kafka

Start Kafka using Docker Compose:

```bash
docker compose up -d
```

Check running containers:

```bash
docker ps
```

Kafka should appear as:

```text
aiops-kafka
```

Kafka is available at:

```text
localhost:9092
```

Kafka image used:

```text
apache/kafka:4.0.1
```

---

# 6. Q1 — AIOps Log Anomaly Detection

Run:

```bash
python q1_anomaly_detection.py
```

Expected important output:

```text
Total records: 20
```

and:

```text
Anomalies detected: 3
```

The graph should be generated at:

```text
data/anomaly_plot.png
```

---

# 7. Q2 — Create Kafka Topic

Run:

```bash
python q2_topic.py
```

First run should show:

```text
Topic created successfully!
```

The topic name is:

```text
server_metrics
```

If you get:

```text
TopicAlreadyExistsError
```

the topic already exists.

Do NOT recreate it.

Continue with the producer.

---

# 8. Q2 — Kafka Producer

Run:

```bash
python q2_producer.py
```

The producer sends 10 server metric messages.

The messages contain:

```text
server_id
cpu_usage
memory_usage
```

The CPU values include:

```text
server1  -> 50%
server2  -> 54%
server3  -> 58%
server4  -> 62%
server5  -> 66%
server6  -> 70%
server7  -> 74%
server8  -> 78%
server9  -> 82%
server10 -> 86%
```

Therefore server9 and server10 will trigger the high-CPU condition in Q3.

---

# 9. Q3 — Kafka Consumer

If you open a new terminal, first enter the project:

```bash
cd /workspaces/examsss/aiops_kafka_airflow
```

Activate the virtual environment:

```bash
source .venv/bin/activate
```

Verify Kafka:

```bash
python -c "from kafka import KafkaConsumer; print('Kafka library OK')"
```

Run the consumer:

```bash
python q3_consumer.py
```

The consumer displays the received server metrics.

For CPU greater than 80%, it prints:

```text
ALERT: High CPU detected on server9
```

and:

```text
ALERT: High CPU detected on server10
```

The consumer continuously waits for messages.

Stop it with:

```text
Ctrl+C
```

---

# 10. Q4 — Install Apache Airflow

Make sure the virtual environment is active:

```bash
source .venv/bin/activate
```

Install the exact Airflow version used:

```bash
pip install "apache-airflow==3.3.2" --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.3.2/constraints-3.14.txt"
```

Check Airflow:

```bash
airflow version
```

Expected:

```text
3.3.2
```

Check dependencies:

```bash
pip check
```

Expected:

```text
No broken requirements found.
```

---

# 11. Q4 — Initialize Airflow Database

Run:

```bash
airflow db migrate
```

Expected:

```text
Database migration done!
```

---

# 12. Q4 — Set Airflow DAG Folder

The project DAG is inside:

```text
/workspaces/examsss/aiops_kafka_airflow/airflow/dags
```

Set the DAG folder:

```bash
export AIRFLOW__CORE__DAGS_FOLDER=/workspaces/examsss/aiops_kafka_airflow/airflow/dags
```

Verify:

```bash
echo $AIRFLOW__CORE__DAGS_FOLDER
```

Expected:

```text
/workspaces/examsss/aiops_kafka_airflow/airflow/dags
```

---

# 13. Q4 — Verify Airflow DAG

Run:

```bash
python -c "from airflow.dag_processing.dagbag import DagBag; d=DagBag(dag_folder='/workspaces/examsss/aiops_kafka_airflow/airflow/dags'); print('DAGs:', list(d.dags.keys())); print('IMPORT ERRORS:', d.import_errors)"
```

Expected:

```text
DAGs: ['aiops_workflow']
IMPORT ERRORS: {}
```

---

# 14. Q4 — Run Airflow DAG

Run:

```bash
airflow dags test aiops_workflow 2026-09-20
```

The DAG contains these tasks:

```text
collect_metrics
        ↓
process_metrics
        ↓
detect_anomaly
        ↓
generate_report
```

Expected important output:

```text
Metrics collected
```

Metrics:

```text
CPU = 87%
Memory = 65%
Response Time = 420 ms
```

Then:

```text
Anomaly detected: High CPU usage
```

Finally:

```text
Dag run in success state
```

---

# 15. Q5 — Integrated AIOps Monitoring

Make sure Kafka is running:

```bash
docker ps
```

If Kafka is not running:

```bash
docker compose up -d
```

Make sure the virtual environment is active:

```bash
source .venv/bin/activate
```

Run:

```bash
python q5_aiops_monitor.py
```

The program:

- Consumes server metrics from Kafka.
- Checks CPU usage.
- Detects CPU greater than 80%.
- Prints high-CPU alerts.
- Maintains a running anomaly count.

Example:

```text
ALERT: High CPU detected
Running anomaly count: 1
```

Stop the monitoring program with:

```text
Ctrl+C
```

---

# 16. Complete Execution Order

Use this order when doing the practical from the beginning.

```bash
# Go to project
cd /workspaces/examsss/aiops_kafka_airflow

# Check environment
python --version
docker --version
docker compose version

# Create virtual environment
python -m venv .venv

# Activate virtual environment
source .venv/bin/activate

# Install Kafka library
pip install kafka-python-ng

# Verify Kafka library
pip show kafka-python-ng
python -c "from kafka import KafkaProducer, KafkaConsumer; print('Kafka library OK')"

# Start Kafka
docker compose up -d

# Verify Kafka
docker ps

# =========================
# Q1
# =========================

python q1_anomaly_detection.py

# =========================
# Q2 - Create Topic
# =========================

python q2_topic.py

# =========================
# Q2 - Producer
# =========================

python q2_producer.py

# =========================
# Q3 - Consumer
# =========================

python q3_consumer.py

# Stop consumer:
# Ctrl+C

# =========================
# Q4 - Install Airflow
# =========================

pip install "apache-airflow==3.3.2" --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.3.2/constraints-3.14.txt"

# Check Airflow
airflow version

# Check dependencies
pip check

# =========================
# Q4 - Airflow Database
# =========================

airflow db migrate

# =========================
# Q4 - DAG Folder
# =========================

export AIRFLOW__CORE__DAGS_FOLDER=/workspaces/examsss/aiops_kafka_airflow/airflow/dags

# Verify DAG folder
echo $AIRFLOW__CORE__DAGS_FOLDER

# =========================
# Q4 - Verify DAG
# =========================

python -c "from airflow.dag_processing.dagbag import DagBag; d=DagBag(dag_folder='/workspaces/examsss/aiops_kafka_airflow/airflow/dags'); print('DAGs:', list(d.dags.keys())); print('IMPORT ERRORS:', d.import_errors)"

# =========================
# Q4 - Run DAG
# =========================

airflow dags test aiops_workflow 2026-09-20

# =========================
# Q5
# =========================

python q5_aiops_monitor.py

# Stop monitoring:
# Ctrl+C
```

---

# 17. Installed Versions

```text
Python              3.14.2
Docker              29.8.0-1
Kafka Docker Image  apache/kafka:4.0.1
kafka-python-ng     2.2.3
Apache Airflow      3.3.2
```

---

# 18. Important Commands for a New Terminal

If you open a new Codespaces terminal during the practical:

```bash
cd /workspaces/examsss/aiops_kafka_airflow
```

Then:

```bash
source .venv/bin/activate
```

Then verify:

```bash
python --version
```

If working with Kafka:

```bash
docker ps
```

If Kafka is stopped:

```bash
docker compose up -d
```

---

# 19. Common Situation — Topic Already Exists

If:

```bash
python q2_topic.py
```

produces:

```text
TopicAlreadyExistsError
```

do not reinstall Kafka and do not delete anything.

The topic has already been created.

Simply run:

```bash
python q2_producer.py
```

---

# 20. Common Situation — Kafka Module Not Found

If you see:

```text
ModuleNotFoundError: No module named 'kafka'
```

activate the virtual environment:

```bash
source .venv/bin/activate
```

Then verify:

```bash
python -c "from kafka import KafkaProducer, KafkaConsumer; print('Kafka library OK')"
```

If the library is genuinely missing:

```bash
pip install kafka-python-ng
```

---

# 21. Common Situation — Airflow Cannot Find DAG

Set:

```bash
export AIRFLOW__CORE__DAGS_FOLDER=/workspaces/examsss/aiops_kafka_airflow/airflow/dags
```

Then verify:

```bash
python -c "from airflow.dag_processing.dagbag import DagBag; d=DagBag(dag_folder='/workspaces/examsss/aiops_kafka_airflow/airflow/dags'); print('DAGs:', list(d.dags.keys())); print('IMPORT ERRORS:', d.import_errors)"
```

Expected:

```text
DAGs: ['aiops_workflow']
IMPORT ERRORS: {}
```

---

# 22. Final Practical Flow

```text
GitHub Codespaces
        ↓
Enter project
        ↓
Create .venv
        ↓
Activate .venv
        ↓
Install kafka-python-ng
        ↓
Start Kafka with Docker
        ↓
Q1 — Anomaly Detection
        ↓
Q2 — Kafka Topic
        ↓
Q2 — Kafka Producer
        ↓
Q3 — Kafka Consumer
        ↓
Install Airflow
        ↓
Initialize Airflow DB
        ↓
Set DAG folder
        ↓
Verify DAG
        ↓
Q4 — Run Airflow DAG
        ↓
Q5 — Integrated AIOps Monitoring
```

# 23. Technologies Installed/Used

```text
Python
Docker
Docker Compose
Apache Kafka
kafka-python-ng
Apache Airflow
```

The practical is executed in this order:

```text
Q1 → Q2 → Q3 → Q4 → Q5
```