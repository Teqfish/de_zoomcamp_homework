#!/usr/bin/env python3
import argparse
import os
from io import BytesIO

import dlt
import pandas as pd
import requests
from dlt.destinations import filesystem

PROJECT_ID = "de-zoomcamp-homework"

BUCKET = "de_zoomcamp_homework"
BASE_PREFIX = "ny_taxi_data"

# Keep DB dataset/schema stable
DATASET = "raw"

def gcs_url(mode: str) -> str:
    return f"gs://{BUCKET}/{BASE_PREFIX}/{mode}"


def stable_table_name(taxi: str) -> str:
    # Stable names for DuckDB & BigQuery
    return f"{taxi}_tripdata"


def gcs_table_name_with_range(taxi: str, year: int, start_month: int, end_month: int) -> str:
    # Only for GCS filenames/folders (human readable)
    # Example: green_tripdata_202401-202406
    return f"{taxi}_tripdata_{year}{start_month:02d}-{year}{end_month:02d}"


def taxi_urls(taxi: str, year: int, months: range) -> list[str]:
    prefix = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{taxi}_tripdata"
    return [f"{prefix}_{year}-{m:02d}.parquet" for m in months]


def make_resource(taxi: str, resource_name: str, write_disposition: str, year: int, start_month: int, end_month: int):
    """
    Factory that builds a dlt resource with a caller-controlled name.
    We use:
      - stable name for DB destinations
      - decorated name for GCS destination
    """
    @dlt.resource(name=resource_name, write_disposition=write_disposition)
    def _resource():
        for url in taxi_urls(taxi, year, range(start_month, end_month + 1)):
            r = requests.get(url, timeout=60)
            r.raise_for_status()
            df = pd.read_parquet(BytesIO(r.content))
            yield df

    return _resource()


def run_to_gcs(taxi: str, mode: str, year: int, start_month: int, end_month: int):
    """
    GCS naming convention:
      gs://<bucket>/<base_prefix>/<mode>/<dataset>/<stable_table>/<load_id>_<YYYYMM-YYYYMM>.parquet

    Folder stays: raw/<green_tripdata|yellow_tripdata>/
    Filename includes: load_id + date range
    """
    os.environ["DESTINATION__FILESYSTEM__BUCKET_URL"] = gcs_url(mode)

    gcs_name = gcs_table_name_with_range(taxi, year, start_month, end_month)

    pipe = dlt.pipeline(
        pipeline_name=f"ny_taxi_to_gcs_{mode}",
        destination=filesystem(layout="{schema_name}/{table_name}/{load_id}_{table_name}.{ext}"),
        dataset_name=DATASET,
    )

    gcs_resource = make_resource(
        taxi=taxi,
        resource_name=gcs_name,          # ONLY affects GCS
        write_disposition="append",      # doesn't really "append files"; it creates new load packages
        year=year,
        start_month=start_month,
        end_month=end_month,
    )

    info = pipe.run(gcs_resource, loader_file_format="parquet")
    print("GCS load result:", info)


def run_to_duckdb(taxi: str, mode: str, year: int, start_month: int, end_month: int):
    """
    DuckDB:
      dataset/schema = raw
      table         = green_tripdata OR yellow_tripdata
      disposition   = append
    """
    pipe = dlt.pipeline(
        pipeline_name=f"ny_taxi_to_duckdb_{mode}",
        destination="duckdb",
        dataset_name=DATASET,
    )

    db_resource = make_resource(
        taxi=taxi,
        resource_name=stable_table_name(taxi),  # stable table name
        write_disposition="append",
        year=year,
        start_month=start_month,
        end_month=end_month,
    )

    info = pipe.run(db_resource)
    print("DuckDB load result:", info)


def run_to_bigquery(taxi: str, mode: str, year: int, start_month: int, end_month: int):
    """
    BigQuery:
      de-zoomcamp-homework.raw.green_tripdata OR yellow_tripdata
      disposition = append
    """
    os.environ["DESTINATION__BIGQUERY__CREDENTIALS__PROJECT_ID"] = PROJECT_ID
    os.environ["DESTINATION__BIGQUERY__LOCATION"] = "EU"

    pipe = dlt.pipeline(
        pipeline_name=f"ny_taxi_to_bigquery_{mode}",
        destination="bigquery",
        dataset_name=DATASET,
    )

    bq_resource = make_resource(
        taxi=taxi,
        resource_name=stable_table_name(taxi),  # stable table name
        write_disposition="append",
        year=year,
        start_month=start_month,
        end_month=end_month,
    )

    info = pipe.run(bq_resource)
    print("BigQuery load result:", info)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=["dev", "prod"], required=True)
    ap.add_argument("--taxi", choices=["yellow", "green"], required=True)
    ap.add_argument("--year", type=int, default=2024)
    ap.add_argument("--start-month", type=int, default=1)
    ap.add_argument("--end-month", type=int, default=6)
    args = ap.parse_args()

    run_to_gcs(args.taxi, args.mode, args.year, args.start_month, args.end_month)

    if args.mode == "dev":
        run_to_duckdb(args.taxi, args.mode, args.year, args.start_month, args.end_month)
    else:
        run_to_bigquery(args.taxi, args.mode, args.year, args.start_month, args.end_month)


if __name__ == "__main__":
    main()
