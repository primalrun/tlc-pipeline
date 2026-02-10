"""
PySpark Transform Job — TLC Yellow Taxi Trip Data

Reads raw parquet, applies data quality filters, standardizes column names,
adds computed columns, and writes cleaned parquet partitioned by year/month.
"""

import argparse
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

# Columns that must not be null
REQUIRED_COLUMNS = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "passenger_count",
    "trip_distance",
    "fare_amount",
    "total_amount",
]

# Map original column names to snake_case
COLUMN_RENAMES = {
    "VendorID": "vendor_id",
    "tpep_pickup_datetime": "pickup_datetime",
    "tpep_dropoff_datetime": "dropoff_datetime",
    "passenger_count": "passenger_count",
    "trip_distance": "trip_distance",
    "RatecodeID": "ratecode_id",
    "store_and_fwd_flag": "store_and_fwd_flag",
    "PULocationID": "pu_location_id",
    "DOLocationID": "do_location_id",
    "payment_type": "payment_type",
    "fare_amount": "fare_amount",
    "extra": "extra",
    "mta_tax": "mta_tax",
    "tip_amount": "tip_amount",
    "tolls_amount": "tolls_amount",
    "improvement_surcharge": "improvement_surcharge",
    "total_amount": "total_amount",
    "congestion_surcharge": "congestion_surcharge",
    "airport_fee": "airport_fee",
}


def parse_args():
    parser = argparse.ArgumentParser(description="Transform TLC trip data")
    parser.add_argument("--input", required=True, help="Input directory with raw parquet files")
    parser.add_argument("--output", required=True, help="Output directory for cleaned parquet")
    parser.add_argument("--year-month", required=True, help="Year-month to process (YYYY-MM)")
    return parser.parse_args()


def main():
    args = parse_args()
    year, month = args.year_month.split("-")

    spark = SparkSession.builder \
        .appName(f"tlc-transform-{args.year_month}") \
        .getOrCreate()

    # Read raw parquet
    input_path = f"{args.input}/yellow_tripdata_{args.year_month}.parquet"
    print(f"Reading from: {input_path}")
    df = spark.read.parquet(input_path)

    initial_count = df.count()
    print(f"Initial row count: {initial_count}")

    # Drop nulls on required columns
    df = df.dropna(subset=REQUIRED_COLUMNS)

    # Filter negative fares
    df = df.filter(F.col("fare_amount") >= 0)
    df = df.filter(F.col("total_amount") >= 0)

    # Filter impossible trip distances (negative or > 500 miles)
    df = df.filter((F.col("trip_distance") >= 0) & (F.col("trip_distance") <= 500))

    # Filter invalid passenger counts
    df = df.filter(F.col("passenger_count") > 0)

    filtered_count = df.count()
    print(f"After filtering: {filtered_count} ({initial_count - filtered_count} rows removed)")

    # Rename columns to snake_case
    for old_name, new_name in COLUMN_RENAMES.items():
        if old_name in df.columns:
            df = df.withColumnRenamed(old_name, new_name)

    # Add computed columns
    df = df.withColumn(
        "trip_duration_minutes",
        (F.col("dropoff_datetime").cast("long") - F.col("pickup_datetime").cast("long")) / 60.0
    )

    df = df.withColumn(
        "cost_per_mile",
        F.when(F.col("trip_distance") > 0, F.col("fare_amount") / F.col("trip_distance"))
         .otherwise(F.lit(None).cast(DoubleType()))
    )

    # Add partition columns
    df = df.withColumn("year", F.lit(int(year)))
    df = df.withColumn("month", F.lit(int(month)))

    # Write cleaned parquet partitioned by year/month
    output_path = args.output
    print(f"Writing to: {output_path}")

    df.write \
        .mode("overwrite") \
        .partitionBy("year", "month") \
        .parquet(output_path)

    print(f"Transform complete. Wrote {filtered_count} rows to {output_path}")

    spark.stop()


if __name__ == "__main__":
    main()
