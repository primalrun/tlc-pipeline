"""
TLC Ingestion DAG

Monthly pipeline for NYC TLC yellow taxi trip data:
  1. Download monthly parquet from the TLC website and upload to S3 raw bucket
  2. Transform with PySpark (quality filters, column standardization, computed columns)
  3. Load processed parquet into Snowflake via COPY INTO (idempotent — pre-deletes existing rows)
  4. Run dbt (seed, run, test) to rebuild staging, fact, and aggregate models
"""

import os
from datetime import datetime, timedelta

import boto3
import requests
import snowflake.connector
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


def load_to_snowflake(**context):
    """COPY processed parquet for the given month from S3 into Snowflake."""
    logical_date = context["logical_date"]
    year = logical_date.year
    month = logical_date.month  # integer — matches Spark's partition dir (e.g. month=3)

    conn = snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "TLC_WH"),
        database="TLC",
        schema="RAW",
        role=os.environ.get("SNOWFLAKE_ROLE", "TRANSFORM_ROLE"),
    )
    try:
        cur = conn.cursor()

        # Delete existing rows for this month to ensure idempotency
        cur.execute(f"""
            DELETE FROM TLC.RAW.yellow_trips
            WHERE pickup_datetime >= '{year}-{month:02d}-01'::TIMESTAMP
              AND pickup_datetime < DATEADD(month, 1, '{year}-{month:02d}-01'::TIMESTAMP)
        """)
        print(f"Deleted existing rows for year={year} month={month}")

        sql = f"""
            COPY INTO TLC.RAW.yellow_trips
            FROM @TLC.RAW.TLC_PROCESSED_STAGE/year={year}/month={month}/
            PATTERN='.*\\.parquet'
            FILE_FORMAT = (TYPE = 'PARQUET')
            MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
            FORCE = TRUE
        """
        print(f"Running COPY INTO for year={year} month={month}")
        cur.execute(sql)
        results = cur.fetchall()
        rows_loaded = sum(r[3] for r in results) if results else 0
        files_loaded = len(results)
        print(f"Loaded {rows_loaded} rows from {files_loaded} file(s)")
    finally:
        conn.close()


with DAG(
    dag_id="tlc_ingestion",
    default_args=default_args,
    description="Download, transform, load to Snowflake, and run dbt for NYC TLC yellow taxi trip data",
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
            "--input s3a://$S3_BUCKET_RAW/yellow_tripdata "
            "--output s3a://$S3_BUCKET_PROCESSED/yellow_tripdata "
            "--year-month {{ logical_date.strftime('%Y-%m') }}"
        ),
    )

    load_task = PythonOperator(
        task_id="load_to_snowflake",
        python_callable=load_to_snowflake,
    )

    run_dbt_task = BashOperator(
        task_id="run_dbt",
        bash_command="cd /opt/dbt && dbt deps --quiet && dbt seed --log-path /tmp/dbt-logs && dbt run --log-path /tmp/dbt-logs && dbt test --log-path /tmp/dbt-logs",
        env={
            **os.environ,
            "DBT_PROFILES_DIR": "/opt/dbt",
            # dbt needs CREATE SCHEMA + CREATE TABLE/VIEW on TLC database
            "SNOWFLAKE_ROLE": "ACCOUNTADMIN",
        },
    )

    download_task >> transform_task >> load_task >> run_dbt_task
