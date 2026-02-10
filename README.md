# TLC Pipeline

Data engineering pipeline for NYC TLC (Taxi & Limousine Commission) trip data.

## Architecture

- **Ingestion**: Airflow DAG downloads yellow taxi parquet files from the TLC website
- **Transform**: PySpark job cleans and standardizes trip data
- **Storage**: S3 (raw/staging/processed) + Snowflake
- **Modeling**: dbt for dimensional modeling in Snowflake
- **Infrastructure**: Terraform for AWS and Snowflake resources

## Quick Start

```bash
# Copy and fill in environment variables
cp .env.example .env

# Build and start services
make build
make up

# Download sample data
make download-sample YEAR_MONTH=2024-01

# Run transform locally
make spark-local JOB=transform_trips ARGS="--input data/raw --output data/processed --year-month 2024-01"
```

## Services

| Service          | URL                    |
|------------------|------------------------|
| Airflow UI       | http://localhost:8080   |
| Spark Master UI  | http://localhost:8081   |

## Project Structure

```
airflow/dags/       — Airflow DAG definitions
spark/jobs/         — PySpark transform jobs
dbt_tlc/            — dbt project (Snowflake models)
terraform/          — Infrastructure as code
data/               — Local data directory (gitignored)
```
