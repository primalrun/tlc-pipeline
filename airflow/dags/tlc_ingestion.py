"""
TLC Ingestion DAG

Downloads yellow taxi trip parquet data from the NYC TLC website
and triggers a PySpark transform job to clean and standardize it.
"""

import os
from datetime import datetime, timedelta

import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

TLC_BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
DATA_DIR = os.environ.get("AIRFLOW_DATA_DIR", "/opt/airflow/data")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def download_tripdata(**context):
    """Download yellow taxi parquet for the given month."""
    logical_date = context["logical_date"]
    year_month = logical_date.strftime("%Y-%m")

    filename = f"yellow_tripdata_{year_month}.parquet"
    url = f"{TLC_BASE_URL}/{filename}"

    raw_dir = os.path.join(DATA_DIR, "raw")
    os.makedirs(raw_dir, exist_ok=True)
    output_path = os.path.join(raw_dir, filename)

    response = requests.get(url, stream=True, timeout=300)
    response.raise_for_status()

    with open(output_path, "wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)

    file_size_mb = os.path.getsize(output_path) / (1024 * 1024)
    print(f"Downloaded {filename} ({file_size_mb:.1f} MB) to {output_path}")

    return output_path


with DAG(
    dag_id="tlc_ingestion",
    default_args=default_args,
    description="Download and transform NYC TLC yellow taxi trip data",
    schedule="@monthly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["tlc", "ingestion"],
) as dag:

    download_task = PythonOperator(
        task_id="download_tripdata",
        python_callable=download_tripdata,
    )

    transform_task = BashOperator(
        task_id="transform_tripdata",
        bash_command=(
            "spark-submit "
            "--master spark://spark-master:7077 "
            "/opt/spark-jobs/transform_trips.py "
            "--input /data/raw "
            "--output /data/processed "
            "--year-month {{ logical_date.strftime('%Y-%m') }}"
        ),
    )

    download_task >> transform_task
