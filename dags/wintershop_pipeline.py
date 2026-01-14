from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="wintershop_ingestion_every_10min",
    start_date=datetime(2026, 1, 14),
    schedule="*/10 * * * *",   # toutes les 10 minutes (cron 5 champs)
    catchup=False,
    max_active_runs=1,         # évite les overlaps si ça dure > 10 min
    tags=["wintershop"],
) as dag:

    ingest_bronze = BashOperator(
        task_id="ingest_bronze",
        bash_command="python /opt/airflow/project/airflow/ingestion.py",
    )
