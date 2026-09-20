from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator


def collect_metrics(**context):
    metrics = {
        "cpu": 87,
        "memory": 65,
        "response_time": 420,
    }

    context["ti"].xcom_push(key="metrics", value=metrics)

    print("Metrics collected:")
    print(f"CPU = {metrics['cpu']}%")
    print(f"Memory = {metrics['memory']}%")
    print(f"Response Time = {metrics['response_time']} ms")


def process_metrics(**context):
    metrics = context["ti"].xcom_pull(
        task_ids="collect_metrics",
        key="metrics",
    )

    print("Processing metrics...")
    print(f"CPU: {metrics['cpu']}%")
    print(f"Memory: {metrics['memory']}%")
    print(f"Response Time: {metrics['response_time']} ms")

    context["ti"].xcom_push(key="processed", value=True)


def detect_anomaly(**context):
    metrics = context["ti"].xcom_pull(
        task_ids="collect_metrics",
        key="metrics",
    )

    if metrics["cpu"] > 80:
        result = "Anomaly detected: High CPU usage"
    else:
        result = "No anomaly detected"

    print(result)
    context["ti"].xcom_push(key="anomaly_result", value=result)


def generate_report(**context):
    print("===== AIOps Report =====")
    print("Metrics collected successfully")
    print("Metrics processed successfully")
    print("Anomaly detection completed")
    print("========================")


with DAG(
    dag_id="aiops_workflow",
    start_date=datetime(2026, 9, 20),
    schedule=None,
    catchup=False,
    tags=["aiops", "kafka", "monitoring"],
) as dag:

    collect_metrics_task = PythonOperator(
        task_id="collect_metrics",
        python_callable=collect_metrics,
    )

    process_metrics_task = PythonOperator(
        task_id="process_metrics",
        python_callable=process_metrics,
    )

    detect_anomaly_task = PythonOperator(
        task_id="detect_anomaly",
        python_callable=detect_anomaly,
    )

    generate_report_task = PythonOperator(
        task_id="generate_report",
        python_callable=generate_report,
    )

    collect_metrics_task >> process_metrics_task >> detect_anomaly_task >> generate_report_task
