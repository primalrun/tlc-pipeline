"""
TLC Ingestion DAG

Downloads yellow taxi trip parquet data from the NYC TLC website,
uploads it to S3 raw bucket, and triggers a PySpark transform job.
"""

import os
from datetime import datetime, timedelta

import boto3
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

TLC_BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
DATA_DIR = os.environ.get("AIRFLOW_DATA_DIR", "/opt/airflow/data")
S3_BUCKET_RAW = os.environ.get("S3_BUCKET_RAW", "tlc-pipeline-raw")
S3_BUCKET_PROCESSED = os.environ.get("S3_BUCKET_PROCESSED", "tlc-pipeline-processed")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def download_and_upload_tripdata(**context):
    """Download yellow taxi parquet for the given month and upload to S3."""
    logical_date = context["logical_date"]
    year_month = logical_date.strftime("%Y-%m")

    filename = f"yellow_tripdata_{year_month}.parquet"
    url = f"{TLC_BASE_URL}/{filename}"
    s3_key = f"yellow_tripdata/{filename}"

    # Download to local temp file
    raw_dir = os.path.join(DATA_DIR, "raw")
    os.makedirs(raw_dir, exist_ok=True)
    local_path = os.path.join(raw_dir, filename)

    print(f"Downloading {url}")
    response = requests.get(url, stream=True, timeout=300)
    response.raise_for_status()

    with open(local_path, "wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)

    file_size_mb = os.path.getsize(local_path) / (1024 * 1024)
    print(f"Downloaded {filename} ({file_size_mb:.1f} MB)")

    # Upload to S3
    s3 = boto3.client("s3")
    print(f"Uploading to s3://{S3_BUCKET_RAW}/{s3_key}")
    s3.upload_file(local_path, S3_BUCKET_RAW, s3_key)
    print("Upload complete")

    return f"s3://{S3_BUCKET_RAW}/{s3_key}"


with DAG(
    dag_id="tlc_ingestion",
    default_args=default_args,
    description="Download, upload to S3, and transform NYC TLC yellow taxi trip data",
    schedule="@monthly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["tlc", "ingestion"],
) as dag:

    download_task = PythonOperator(
        task_id="download_tripdata",
        python_callable=download_and_upload_tripdata,
    )

    transform_task = BashOperator(
        task_id="transform_tripdata",
        bash_command=(
            "spark-submit "
            "--master spark://spark-master:7077 "
            "--conf spark.hadoop.fs.s3a.access.key=$AWS_ACCESS_KEY_ID "
            "--conf spark.hadoop.fs.s3a.secret.key=$AWS_SECRET_ACCESS_KEY "
            "--conf spark.hadoop.fs.s3a.endpoint=s3.amazonaws.com "
            "--conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem "
            "/opt/spark-jobs/transform_trips.py "
            "--input s3a://tlc-pipeline-raw/yellow_tripdata "
            "--output s3a://tlc-pipeline-processed/yellow_tripdata "
            "--year-month {{ logical_date.strftime('%Y-%m') }}"
        ),
    )

    download_task >> transform_task
