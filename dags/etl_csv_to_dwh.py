from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="etl_csv_to_dwh",
    description="ETL: CSV -> Spark -> MySQL",
    # schedule_interval="*/5 * * * *",  # cada 5 minutos
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args
) as dag:

    extract = BashOperator(
        task_id="extract",
        bash_command="python /opt/etl/extract.py"
    )

    transform = BashOperator(
        task_id="transform",
        bash_command="python /opt/etl/transform.py"
    )

    load = BashOperator(
        task_id="load",
        bash_command="python /opt/etl/load.py"
    )

    extract >> transform >> load
