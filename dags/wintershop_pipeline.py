from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator

# Chemin sûr (existe sur airflow-worker)
DBT_DIR = "/opt/airflow/project"

# Mets "prod" uniquement si ton profiles.yml contient bien outputs: prod
DBT_TARGET = "dev"

with DAG(
    dag_id="wintershop_ingestion_every_10min",
    start_date=datetime(2026, 1, 14),
    schedule="*/2 * * * *",  # toutes les 10 minutes (cron 5 champs)
    catchup=False,
    max_active_runs=1,
    tags=["wintershop"],
) as dag:

    ingest_bronze = BashOperator(
        task_id="ingest_bronze",
        bash_command="python /opt/airflow/project/airflow/ingestion.py",
    )

    dbt_run = BashOperator(    
        task_id='dbt_run', 
        bash_command='cd /opt/airflow/project && dbt run --target prod',   
        )

    # Dépendance : ingestion -> dbt
    ingest_bronze >> dbt_run
