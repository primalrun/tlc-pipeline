.PHONY: up down build restart logs spark-submit spark-local dbt-run download-sample clean

up:
	docker compose up -d

down:
	docker compose down

build:
	docker compose build

restart:
	docker compose down && docker compose up -d

logs:
	docker compose logs -f

# Run a PySpark job on the Spark cluster
# Usage: make spark-submit JOB=transform_trips ARGS="--input /data/raw --output /data/processed --year-month 2024-01"
JOB ?= transform_trips
ARGS ?=
spark-submit:
	docker compose exec spark-master spark-submit \
		--master spark://spark-master:7077 \
		/opt/spark-jobs/$(JOB).py $(ARGS)

# Run a PySpark job locally (no Docker)
spark-local:
	spark-submit spark/jobs/$(JOB).py $(ARGS)

# dbt commands
dbt-run:
	cd dbt_tlc && dbt run

dbt-test:
	cd dbt_tlc && dbt test

# Download sample TLC data
# Usage: make download-sample YEAR_MONTH=2024-01
YEAR_MONTH ?= 2024-01
download-sample:
	mkdir -p data/raw
	curl -o data/raw/yellow_tripdata_$(YEAR_MONTH).parquet \
		"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_$(YEAR_MONTH).parquet"

clean:
	rm -rf data/raw/* data/processed/*
