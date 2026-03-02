# TLC Pipeline

End-to-end data engineering pipeline for NYC Taxi & Limousine Commission (TLC) yellow taxi trip data. Orchestrated with Airflow, transformed with PySpark, stored in Snowflake, and modeled with dbt. Infrastructure provisioned with Terraform.

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        Airflow DAG (monthly)                    │
│                                                                 │
│  ┌──────────────────┐   ┌──────────────────┐   ┌────────────┐  │
│  │ download_tripdata│──▶│transform_tripdata│──▶│load_to_    │  │
│  │  (PythonOperator)│   │ (BashOperator /  │   │snowflake   │  │
│  │                  │   │   PySpark)       │   │(Python-    │  │
│  └──────────────────┘   └──────────────────┘   │ Operator)  │  │
│           │                      │             └────────────┘  │
└───────────┼──────────────────────┼──────────────────┼──────────┘
            ▼                      ▼                  ▼
     ┌─────────────┐      ┌──────────────┐   ┌──────────────────┐
     │  S3 (raw)   │      │ S3(processed)│   │    Snowflake     │
     │yellow_tripda│      │yellow_tripda │   │ TLC.RAW.         │
     │ta/YYYY-MM   │      │ta/year=YYYY/ │   │ yellow_trips     │
     │ .parquet    │      │month=M/*.    │   └──────────────────┘
     └─────────────┘      │parquet       │             │
                          └──────────────┘             ▼
                                               ┌──────────────────┐
                                               │   dbt (manual)   │
                                               │ stg_yellow_trips │
                                               │ fct_trips        │
                                               │agg_trips_monthly │
                                               └──────────────────┘
```

## Tech Stack

| Layer           | Technology                          |
|-----------------|-------------------------------------|
| Orchestration   | Apache Airflow 2.9 (LocalExecutor)  |
| Compute         | Apache Spark 3.5.3 (standalone)     |
| Cloud storage   | AWS S3 (3 buckets)                  |
| Data warehouse  | Snowflake                           |
| Transformation  | dbt-snowflake 1.8                   |
| Infrastructure  | Terraform (AWS + Snowflake)         |
| Containers      | Docker Compose                      |

## Project Structure

```
├── airflow/
│   └── dags/
│       └── tlc_ingestion.py     # Main DAG (3 tasks)
├── spark/
│   └── jobs/
│       └── transform_trips.py   # PySpark transform job
├── dbt_tlc/
│   └── models/
│       ├── staging/             # stg_yellow_trips (view)
│       └── marts/               # fct_trips, agg_trips_monthly (tables)
├── terraform/
│   ├── main.tf                  # Provider config
│   ├── s3.tf                    # S3 buckets
│   ├── iam.tf                   # IAM roles (pipeline + Snowflake)
│   ├── snowflake.tf             # Snowflake resources + S3 integration
│   ├── outputs.tf
│   └── variables.tf
├── Dockerfile.airflow
├── Dockerfile.spark
├── docker-compose.yaml
└── Makefile
```

## Prerequisites

- Docker Desktop
- AWS CLI + account with programmatic access
- Snowflake account (free trial works)
- Terraform >= 1.6

## Setup

### 1. Configure environment

```bash
cp .env.example .env
```

Fill in `.env`:

| Variable | Description |
|---|---|
| `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` | IAM user credentials with S3 access |
| `SNOWFLAKE_ACCOUNT` | `{ORG}-{ACCOUNT}` identifier (e.g. `MYORG-ABC12345`) |
| `SNOWFLAKE_USER` / `SNOWFLAKE_PASSWORD` | Snowflake login |
| `SNOWFLAKE_ROLE` | Role for COPY INTO — `SYSADMIN` works for account owners |

### 2. Provision infrastructure with Terraform

The Snowflake S3 storage integration has a circular dependency (IAM role ↔ integration), so apply in two phases.

**Phase 1 — AWS resources + Snowflake (placeholder trust policy):**

```bash
cd terraform
cp terraform.tfvars.example terraform.tfvars  # fill in credentials
terraform init
terraform apply
```

Copy the two outputs into `terraform.tfvars`:

```hcl
snowflake_iam_user_arn = "<output: snowflake_integration_iam_user_arn>"
snowflake_external_id  = "<output: snowflake_integration_external_id>"
```

**Phase 2 — Update IAM trust policy + create external stage:**

```bash
terraform apply
```

**Create the Snowflake target table** (one-time):

```sql
USE DATABASE TLC;
USE SCHEMA RAW;

CREATE OR REPLACE TABLE yellow_trips (
    vendor_id             INTEGER,
    pickup_datetime       TIMESTAMP_NTZ,
    dropoff_datetime      TIMESTAMP_NTZ,
    passenger_count       FLOAT,
    trip_distance         FLOAT,
    ratecode_id           FLOAT,
    store_and_fwd_flag    TEXT,
    pu_location_id        INTEGER,
    do_location_id        INTEGER,
    payment_type          INTEGER,
    fare_amount           FLOAT,
    extra                 FLOAT,
    mta_tax               FLOAT,
    tip_amount            FLOAT,
    tolls_amount          FLOAT,
    improvement_surcharge FLOAT,
    total_amount          FLOAT,
    congestion_surcharge  FLOAT,
    airport_fee           FLOAT,
    trip_duration_minutes FLOAT,
    cost_per_mile         FLOAT,
    year                  INTEGER,
    month                 INTEGER
);
```

### 3. Start Docker services

```bash
make build
make up
```

| Service         | URL                   | Credentials    |
|-----------------|-----------------------|----------------|
| Airflow UI      | http://localhost:8080 | admin / admin  |
| Spark Master UI | http://localhost:8081 | —              |

### 4. Set up dbt

```bash
cd dbt_tlc
pip install dbt-snowflake==1.8.3
dbt deps
```

Export Snowflake credentials to your shell, then run:

```bash
export SNOWFLAKE_ACCOUNT=...
export SNOWFLAKE_USER=...
export SNOWFLAKE_PASSWORD=...
DBT_PROFILES_DIR=. dbt run
DBT_PROFILES_DIR=. dbt test
```

## Running the Pipeline

Trigger the DAG from the Airflow UI or CLI. Use a historical date — TLC publishes data ~2 months behind.

```bash
docker compose exec airflow-scheduler \
  airflow dags trigger tlc_ingestion -e 2024-06-01T00:00:00+00:00
```

### DAG Tasks

| Task | Operator | What it does |
|------|----------|-------------|
| `download_tripdata` | PythonOperator | Downloads `yellow_tripdata_YYYY-MM.parquet` from the TLC website and uploads it to `s3://tlc-pipeline-raw/yellow_tripdata/` |
| `transform_tripdata` | BashOperator | Submits `transform_trips.py` to the Spark cluster. Drops nulls, filters invalid rows, renames columns to snake_case, adds `trip_duration_minutes` and `cost_per_mile`, writes partitioned parquet to `s3://tlc-pipeline-processed/` |
| `load_to_snowflake` | PythonOperator | Runs `COPY INTO TLC.RAW.yellow_trips` from the S3 external stage, targeting the specific `year=YYYY/month=M/` partition for the run |

Typical runtimes for a single month (~3M rows):

```
download_tripdata   ~20s
transform_tripdata  ~65s
load_to_snowflake   ~20s
```

## PySpark Transform

`spark/jobs/transform_trips.py` applies the following to each month's raw parquet:

**Quality filters:**
- Drop rows with nulls in required columns (pickup/dropoff time, distance, fare, total)
- Remove negative fares or distances
- Remove trip distances > 500 miles
- Remove passenger count ≤ 0

**Column standardization:**
- Rename TLC schema columns to snake_case (e.g. `VendorID` → `vendor_id`, `Airport_fee` → `airport_fee`)

**Computed columns:**
- `trip_duration_minutes` — `(dropoff_unix - pickup_unix) / 60`
- `cost_per_mile` — `fare_amount / trip_distance` (null when distance is 0)

**Output:** snappy-compressed parquet partitioned by `year` / `month`.

## dbt Models

```
TLC.RAW.yellow_trips          (raw Snowflake table — loaded by DAG)
    └── TLC.STAGING.stg_yellow_trips   (view — cleans, adds vendor/payment labels)
            ├── TLC.MART.fct_trips          (table — adds tip_pct)
            └── TLC.MART.agg_trips_monthly  (table — monthly rollup by vendor + payment type)
```

`agg_trips_monthly` exposes: `trip_count`, `total_passengers`, `avg_trip_distance_miles`, `avg_trip_duration_minutes`, `avg_fare`, `avg_tip`, `avg_tip_pct`, `total_revenue`, `avg_cost_per_mile`.

## Makefile Reference

```bash
make build            # Build all Docker images
make up               # Start all services (detached)
make down             # Stop all services
make restart          # down + up
make logs             # Follow all container logs

make spark-submit JOB=transform_trips \
  ARGS="--input s3a://... --output s3a://... --year-month 2024-01"

make download-sample YEAR_MONTH=2024-01   # Download raw parquet locally
make clean                                 # Remove local data/raw and data/processed
```

## Data Source

NYC TLC trip record data: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

Files are published ~2 months after the trip month. Yellow taxi parquet files range from ~40–60 MB per month (~3M rows).
