#!/usr/bin/env python3
import argparse
import os
from io import BytesIO

import dlt
import pandas as pd
import requests
from dlt.destinations import filesystem


# -------------------------
# Config (naming convention)
# -------------------------
# BigQuery project ID (not display name)
PROJECT_ID = "de-zoomcamp-homework"

# GCS layout
BUCKET = "de_zoomcamp_homework"
BASE_PREFIX = "ny_taxi_data"  # bucket "folder" root for this project

# Naming convention:
#   BigQuery:  <PROJECT>.<DATASET>.<TABLE>
#   DuckDB:    <DB FILE>  schema=<DATASET> table=<TABLE>
DATASET = "ny_taxi_raw"
TABLE = "yellow_tripdata"


def gcs_url(mode: str) -> str:
    # gs://de_zoomcamp_homework/ny_taxi_data/dev  OR  .../prod
    return f"gs://{BUCKET}/{BASE_PREFIX}/{mode}"


def taxi_urls(year: int, months: range) -> list[str]:
    prefix = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata"
    return [f"{prefix}_{year}-{m:02d}.parquet" for m in months]


@dlt.resource(name=TABLE, write_disposition="replace")
def download_parquet(year: int = 2024, start_month: int = 1, end_month: int = 6):
    """Yields DataFrames month-by-month into a single logical table/resource."""
    for url in taxi_urls(year, range(start_month, end_month + 1)):
        r = requests.get(url, timeout=60)
        r.raise_for_status()
        df = pd.read_parquet(BytesIO(r.content))
        yield df


def run_to_gcs(mode: str, year: int, start_month: int, end_month: int):
    """
    Writes Parquet outputs to GCS under:
      gs://<bucket>/<base_prefix>/<mode>/<dataset>/<table>/<load_id>.parquet
    """
    # Tell dlt filesystem destination where to write
    os.environ["DESTINATION__FILESYSTEM__BUCKET_URL"] = gcs_url(mode)

    pipe = dlt.pipeline(
        pipeline_name=f"ny_taxi_to_gcs_{mode}",
        destination=filesystem(layout="{schema_name}/{table_name}/{load_id}.{ext}"),
        dataset_name=DATASET,  # becomes the "schema_name" folder in GCS layout
    )

    info = pipe.run(
        download_parquet(year=year, start_month=start_month, end_month=end_month),
        loader_file_format="parquet",
    )
    print("GCS load result:", info)


def run_to_duckdb(mode: str, year: int, start_month: int, end_month: int):
    """
    Loads the same data into a local DuckDB database (dev only) using:
      schema = ny_taxi_raw
      table  = yellow_tripdata
    """
    pipe = dlt.pipeline(
        pipeline_name=f"ny_taxi_to_duckdb_{mode}",
        destination="duckdb",
        dataset_name=DATASET,  # DuckDB schema
    )
    info = pipe.run(download_parquet(year=year, start_month=start_month, end_month=end_month))
    print("DuckDB load result:", info)


def run_to_bigquery(mode: str, year: int, start_month: int, end_month: int):
    """
    Loads the same data into BigQuery:
      de-zoomcamp-homework.ny_taxi_raw.yellow_tripdata

    Requires Google ADC credentials (e.g. GOOGLE_APPLICATION_CREDENTIALS set).
    """
    # Make the BigQuery project explicit (avoids ambiguity if multiple projects/creds exist)
    os.environ["DESTINATION__BIGQUERY__CREDENTIALS__PROJECT_ID"] = PROJECT_ID
    os.environ["DESTINATION__BIGQUERY__LOCATION"] = "EU"
    # If you created the dataset in EU, keep location consistent (optional but recommended)
    # os.environ["DESTINATION__BIGQUERY__LOCATION"] = "EU"

    pipe = dlt.pipeline(
        pipeline_name=f"ny_taxi_to_bigquery_{mode}",
        destination="bigquery",
        dataset_name=DATASET,  # BigQuery dataset
    )
    info = pipe.run(download_parquet(year=year, start_month=start_month, end_month=end_month))
    print("BigQuery load result:", info)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=["dev", "prod"], required=True)
    ap.add_argument("--year", type=int, default=2024)
    ap.add_argument("--start-month", type=int, default=1)
    ap.add_argument("--end-month", type=int, default=6)
    args = ap.parse_args()

    # Always write to GCS (dev/prod prefix)
    run_to_gcs(args.mode, args.year, args.start_month, args.end_month)

    # Then load to DB depending on mode
    if args.mode == "dev":
        run_to_duckdb(args.mode, args.year, args.start_month, args.end_month)
    else:
        run_to_bigquery(args.mode, args.year, args.start_month, args.end_month)


if __name__ == "__main__":
    main()
