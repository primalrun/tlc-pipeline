# TLC Pipeline

End-to-end data engineering pipeline for NYC Taxi & Limousine Commission (TLC) yellow taxi trip data. Orchestrated with Airflow, transformed with PySpark, stored in Snowflake, and modeled with dbt. Infrastructure provisioned with Terraform.

## Architecture

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                             Airflow DAG (monthly)                            │
│                                                                              │
│  ┌────────────────┐  ┌────────────────┐  ┌────────────────┐  ┌───────────┐  │
│  │download_tripdat│─▶│transform_tripda│─▶│ load_to_       │─▶│ run_dbt   │  │
│  │(PythonOperator)│  │(BashOperator / │  │ snowflake      │  │(BashOpera-│  │
│  │                │  │  PySpark)      │  │(PythonOperator)│  │  tor)     │  │
│  └────────────────┘  └────────────────┘  └────────────────┘  └───────────┘  │
│          │                   │                   │                   │       │
└──────────┼───────────────────┼───────────────────┼───────────────────┼───────┘
           ▼                   ▼                   ▼                   ▼
    ┌────────────┐     ┌──────────────┐   ┌──────────────────┐  ┌──────────────┐
    │  S3 (raw)  │     │S3 (processed)│   │    Snowflake     │  │  Snowflake   │
    │yellow_trip-│     │yellow_tripda-│   │ TLC.RAW.         │  │ stg_yellow_  │
    │data/YYYY-MM│     │ta/year=YYYY/ │   │ yellow_trips     │  │ trips        │
    │ .parquet   │     │month=M/*.    │   └──────────────────┘  │ fct_trips    │
    └────────────┘     │parquet       │                         │ agg_trips_   │
                       └──────────────┘                         │ monthly      │
                                                                └──────────────┘
```

## Tech Stack

| Layer           | Technology                          |
|-----------------|-------------------------------------|
| Orchestration   | Apache Airflow 2.9 (LocalExecutor)  |
| Compute         | Apache Spark 3.5.3 (standalone)     |
| Cloud storage   | AWS S3 (2 buckets)                  |
| Data warehouse  | Snowflake                           |
| Transformation  | dbt-snowflake 1.8                   |
| Infrastructure  | Terraform (AWS + Snowflake)         |
| Containers      | Docker Compose                      |

## Project Structure

```
├── airflow/
│   └── dags/
│       └── tlc_ingestion.py     # Main DAG (4 tasks)
├── spark/
│   └── jobs/
│       └── transform_trips.py   # PySpark transform job
├── dbt_tlc/
│   ├── models/
│   │   ├── staging/             # stg_yellow_trips (view)
│   │   └── marts/               # fct_trips, agg_trips_monthly (tables)
│   └── seeds/
│       └── taxi_zone_lookup.csv # TLC taxi zone → borough/zone name lookup
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
| `SNOWFLAKE_ROLE` | Role for COPY INTO — `TRANSFORM_ROLE` (created by Terraform) |
| `AIRFLOW__CORE__FERNET_KEY` | Fernet key for encrypting Airflow connections — generate with `python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"` |
| `AIRFLOW__WEBSERVER__SECRET_KEY` | Secret key for Airflow webserver sessions — any random string |

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
    cost_per_mile         FLOAT
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
| dbt Docs        | http://localhost:8082 | —              |

Start the dbt docs server with:

```bash
make dbt-docs
```

### 4. Set up dbt

dbt runs automatically inside the Airflow container as part of the `run_dbt` DAG task — no local installation needed for normal pipeline operation.

To run dbt manually outside of Docker (optional):

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
DBT_PROFILES_DIR=. dbt seed
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
| `run_dbt` | BashOperator | Runs `dbt deps`, `dbt seed` (loads taxi zone lookup), `dbt run` (rebuilds all models), and `dbt test` (10 data quality checks) |

The pipeline is idempotent — re-running for the same month deletes existing rows before loading, so no duplicates are produced.

Typical runtimes for a single month (~3M rows):

```
download_tripdata   ~16s
transform_tripdata  ~53s
load_to_snowflake   ~18s
run_dbt             ~48s
─────────────────────────
total               ~2.5 min
```

### On Scheduling

The DAG is defined with `schedule="@monthly"` and `catchup=False`, but this repo uses manual triggers rather than relying on the automatic schedule. There are two reasons:

1. **TLC data lag.** TLC publishes trip data ~2 months after the fact, so a monthly schedule firing on the 1st would consistently fail — the previous month's file doesn't exist yet. A production deployment would either delay the trigger (e.g. run on the 15th, two months back) or use a sensor that polls the TLC website for new file availability.

2. **Local Docker runtime.** Automatic scheduling only works if the stack is continuously running. On a dev laptop this isn't realistic; in production this DAG would run on a managed service such as Amazon MWAA or Google Cloud Composer.

The scheduling primitives are all in place (`@monthly`, `retries=2`, `retry_delay`, `catchup=False`) — the intent is that pointing this at a managed Airflow environment with the right trigger offset is a straightforward operational change, not an architectural one.

## PySpark Transform

`spark/jobs/transform_trips.py` applies the following to each month's raw parquet:

**Quality filters:**
- Drop rows with nulls in required columns (pickup/dropoff time, distance, fare, total)
- Remove negative fares or distances
- Remove trip distances > 500 miles
- Remove passenger count ≤ 0
- Remove rows where pickup date doesn't match the file's month (TLC files occasionally contain records from adjacent months)

**Column standardization:**
- Rename TLC schema columns to snake_case (e.g. `VendorID` → `vendor_id`, `Airport_fee` → `airport_fee`)

**Computed columns:**
- `trip_duration_minutes` — `(unix_timestamp(dropoff_datetime) - unix_timestamp(pickup_datetime)) / 60`
- `cost_per_mile` — `fare_amount / trip_distance` (null when distance is 0)

**Output:** snappy-compressed parquet written to `year=YYYY/month=M/` partition path in S3.

## dbt Models

```
TLC.RAW.yellow_trips                  (raw table — loaded by DAG)
    └── TLC.RAW_STAGING.stg_yellow_trips   (view — cleans, joins zone names, adds labels)
            ├── TLC.RAW_MART.fct_trips          (table — adds tip_pct, pickup/dropoff borough+zone)
            └── TLC.RAW_MART.agg_trips_monthly  (table — monthly rollup by vendor, payment type, borough)
```

Staging joins `taxi_zone_lookup` (dbt seed, 265 NYC taxi zones) to resolve `pu_location_id` and `do_location_id` into human-readable borough and zone names.

`agg_trips_monthly` exposes: `trip_count`, `total_passengers`, `avg_trip_distance_miles`, `avg_trip_duration_minutes`, `avg_fare`, `avg_tip`, `avg_tip_pct`, `total_revenue`, `avg_cost_per_mile`, grouped by `trip_month`, `vendor_name`, `payment_type_desc`, and `pu_borough`.

## Example Queries

**Row counts per month:**
```sql
SELECT DATE_TRUNC('month', pickup_datetime) AS month, COUNT(*) AS trips
FROM TLC.RAW.yellow_trips
GROUP BY 1
ORDER BY 1;
```

**Monthly revenue and average fare by payment type:**
```sql
SELECT trip_month, payment_type_desc, total_revenue, avg_fare, avg_tip_pct
FROM TLC.RAW_MART.agg_trips_monthly
ORDER BY trip_month, payment_type_desc;
```

**Busiest pickup zones:**
```sql
SELECT pu_borough, pu_zone, COUNT(*) AS trips, ROUND(AVG(fare_amount), 2) AS avg_fare
FROM TLC.RAW_MART.fct_trips
GROUP BY 1, 2
ORDER BY 3 DESC
LIMIT 20;
```

**Average tip percentage by vendor:**
```sql
SELECT vendor_name, ROUND(AVG(avg_tip_pct), 2) AS avg_tip_pct, SUM(total_revenue) AS total_revenue
FROM TLC.RAW_MART.agg_trips_monthly
GROUP BY 1
ORDER BY 2 DESC;
```

**Most common pickup-to-dropoff zone pairs:**
```sql
SELECT pu_zone, do_zone, COUNT(*) AS trips, ROUND(AVG(total_amount), 2) AS avg_total
FROM TLC.RAW_MART.fct_trips
GROUP BY 1, 2
ORDER BY 3 DESC
LIMIT 20;
```

**Average fare by pickup borough:**
```sql
SELECT pu_borough, COUNT(*) AS trips, ROUND(AVG(fare_amount), 2) AS avg_fare, ROUND(AVG(tip_pct), 2) AS avg_tip_pct
FROM TLC.RAW_MART.fct_trips
GROUP BY 1
ORDER BY 2 DESC;
```

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
make dbt-docs                              # Generate and serve dbt docs at http://localhost:8082
```

## Data Source

NYC TLC trip record data: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

Files are published ~2 months after the trip month. Yellow taxi parquet files range from ~40–60 MB per month (~3M rows).
