# TLC Pipeline Internals

Implementation details for contributors and anyone wanting to understand how the pipeline works under the hood. For setup and usage see [README.md](README.md).

## Configuration Files

`.env` is the single source of truth for all credentials and environment-specific values. Everything else reads from it.

```
.env
 ├── docker-compose.yaml     reads via ${VAR} syntax, injects into containers
 ├── terraform.tfvars        values copied in manually during infrastructure setup
 └── (containers)
       ├── tlc_ingestion.py  reads via os.environ
       └── profiles.yml      reads via dbt env_var()
```

## Files Used During a Pipeline Run

### Stack startup (`make up`)

| File | Purpose |
|---|---|
| `.env` | Source of all credentials and config |
| `docker-compose.yaml` | Starts all containers with env vars and volume mounts |
| `Dockerfile.airflow` | Builds Airflow image (Python deps, Java, S3A JARs, dbt) |
| `Dockerfile.spark` | Builds Spark image (S3A JARs) |

### Task 1 — `download_tripdata`

| File | Purpose |
|---|---|
| `airflow/dags/tlc_ingestion.py` | Airflow reads DAG definition; `download_and_upload_tripdata()` executes |

Actions:
- Reads `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `S3_BUCKET_RAW` from environment
- Downloads TLC parquet to `data/raw/yellow_tripdata_YYYY-MM.parquet` (host) / `/opt/airflow/data/raw/` (container)
- Uploads to `s3://tlc-pipeline-raw/yellow_tripdata/yellow_tripdata_YYYY-MM.parquet`

### Task 2 — `transform_tripdata`

| File | Purpose |
|---|---|
| `airflow/dags/tlc_ingestion.py` | BashOperator builds and runs the `spark-submit` command |
| `spark/jobs/transform_trips.py` | PySpark script executed by the Spark driver |

Actions:
- Driver runs inside the Airflow container; executors run on the `spark-worker` container
- Reads raw parquet from S3 raw bucket
- Applies quality filters (nulls, invalid fares/distances, passenger counts, out-of-range dates)
- Renames columns to snake_case, adds `trip_duration_minutes` and `cost_per_mile`
- Writes cleaned parquet to `s3://tlc-pipeline-processed/yellow_tripdata/year=YYYY/month=M/`

### Task 3 — `load_to_snowflake`

| File | Purpose |
|---|---|
| `airflow/dags/tlc_ingestion.py` | `load_to_snowflake()` executes |

Actions:
- Reads `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, `SNOWFLAKE_PASSWORD`, `SNOWFLAKE_WAREHOUSE`, `SNOWFLAKE_ROLE` from environment
- Deletes existing rows for the month from `TLC.RAW.yellow_trips` (idempotency)
- Runs `COPY INTO TLC.RAW.yellow_trips` from `@TLC.RAW.TLC_PROCESSED_STAGE/year=YYYY/month=M/` with `FORCE=TRUE`

### Task 4 — `run_dbt`

| File | Purpose |
|---|---|
| `airflow/dags/tlc_ingestion.py` | BashOperator runs dbt commands |
| `dbt_tlc/profiles.yml` | Snowflake connection config — reads credentials from environment |
| `dbt_tlc/dbt_project.yml` | dbt project config (model paths, schema names) |
| `dbt_tlc/seeds/taxi_zone_lookup.csv` | 265 NYC taxi zones loaded into `TLC.RAW.taxi_zone_lookup` |
| `dbt_tlc/models/staging/stg_yellow_trips.sql` | Rebuilt as view in `TLC.RAW_STAGING` |
| `dbt_tlc/models/staging/stg_yellow_trips.yml` | Column descriptions and tests for staging model |
| `dbt_tlc/models/staging/sources.yml` | Declares `TLC.RAW.yellow_trips` as a dbt source |
| `dbt_tlc/models/marts/fct_trips.sql` | Rebuilt as table in `TLC.RAW_MART` |
| `dbt_tlc/models/marts/agg_trips_monthly.sql` | Rebuilt as table in `TLC.RAW_MART` |
| `dbt_tlc/models/marts/marts.yml` | Column descriptions and tests for mart models |

Actions:
- `dbt deps` — installs dbt_utils package
- `dbt seed` — loads `taxi_zone_lookup.csv` into Snowflake
- `dbt run` — rebuilds all 3 models
- `dbt test` — runs 10 data quality tests

## Container Roles

| Container | Role | Stays running? |
|---|---|---|
| `postgres` | Airflow metadata database | Yes |
| `airflow-init` | One-time DB migration + admin user creation | No — exits after setup |
| `airflow-webserver` | Airflow UI at http://localhost:8080 | Yes |
| `airflow-scheduler` | Monitors DAGs, triggers tasks | Yes |
| `spark-master` | Spark cluster manager, UI at http://localhost:8081 | Yes |
| `spark-worker` | Provides 2 cores + 2GB memory for Spark executors | Yes |
| `dbt-docs` | Generates and serves dbt docs at http://localhost:8082 | On demand (`make dbt-docs`) |

## Spark Execution Model

```
spark-submit (runs in Airflow container)
      ↓  submits job to
Spark Master (spark-master:7077)
      ↓  allocates resources, assigns work to
Spark Worker (spark-worker container)
      ↓  launches executors
Executors  →  read partitions from S3, apply transforms, write parquet to S3
      ↑
Driver (Airflow container)  →  coordinates executors, builds execution plan
```

The driver runs in **client mode** inside the Airflow container — it plans the job and coordinates executors but the actual data processing happens on the Spark worker. Client mode is used because Spark standalone clusters do not support cluster deploy mode for Python jobs.

## Volume Mounts

All Airflow containers share these mounts, meaning changes to these files on the host take effect immediately without rebuilding:

| Host path | Container path | Purpose |
|---|---|---|
| `./airflow/dags` | `/opt/airflow/dags` | DAG files |
| `./spark/jobs` | `/opt/spark-jobs` | PySpark scripts |
| `./dbt_tlc` | `/opt/dbt` | dbt project |
| `./data` | `/opt/airflow/data` | Downloaded raw parquet files |

## Snowflake Schema Layout

```
TLC (database)
├── RAW (schema — created by Terraform)
│   ├── yellow_trips              table — loaded by COPY INTO
│   ├── taxi_zone_lookup          table — loaded by dbt seed
│   └── TLC_PROCESSED_STAGE       external stage — points to S3 processed bucket
├── RAW_STAGING (schema — created by dbt)
│   └── stg_yellow_trips          view — cleans raw data, joins zone lookup
└── RAW_MART (schema — created by dbt)
    ├── fct_trips                 table — fact table with all measures
    └── agg_trips_monthly         table — monthly rollup by vendor/payment/borough
```

## Terraform Two-Phase Apply

The Snowflake S3 storage integration requires a circular dependency between the IAM role and the integration itself:

- **Phase 1** — creates AWS resources and Snowflake resources with a placeholder IAM trust policy
- Snowflake outputs `storage_aws_iam_user_arn` and `storage_aws_external_id` after phase 1
- These values are copied into `terraform.tfvars`
- **Phase 2** — updates the IAM trust policy to trust Snowflake's IAM user and creates the external stage

This two-phase pattern is necessary because Snowflake generates its IAM user ARN dynamically when the storage integration is created — it can't be known in advance.
