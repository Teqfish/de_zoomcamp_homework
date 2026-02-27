"""@bruin

# TODO: Set the asset name (recommended pattern: schema.asset_name).
# - Convention in this module: use an `ingestion.` schema for raw ingestion tables.
name: ingestion.trips

# TODO: Set the asset type.
# Docs: https://getbruin.com/docs/bruin/assets/python
type: python

# TODO: Pick a Python image version (Bruin runs Python in isolated environments).
# Example: python:3.11
image: python:3.11

# TODO: Set the connection.
connection: duckdb-default

# TODO: Choose materialization (optional, but recommended).
# Bruin feature: Python materialization lets you return a DataFrame (or list[dict]) and Bruin loads it into your destination.
# This is usually the easiest way to build ingestion assets in Bruin.
# Alternative (advanced): you can skip Bruin Python materialization and write a "plain" Python asset that manually writes
# into DuckDB (or another destination) using your own client library and SQL. In that case:
# - you typically omit the `materialization:` block
# - you do NOT need a `materialize()` function; you just run Python code
# Docs: https://getbruin.com/docs/bruin/assets/python#materialization
materialization:
  # TODO: choose `table` or `view` (ingestion generally should be a table)
  type: table
  # TODO: pick a strategy.
  # suggested strategy: append
  strategy: append

# TODO: Define output columns (names + types) for metadata, lineage, and quality checks.
# Tip: mark stable identifiers as `primary_key: true` if you plan to use `merge` later.
# Docs: https://getbruin.com/docs/bruin/assets/columns
columns:
  - name: taxi_type
    type: string
    description: Taxi type in the source filename (yellow or green)
  - name: source_url
    type: string
    description: Full source URL of the downloaded parquet file
  - name: extracted_at
    type: timestamp
    description: UTC timestamp when the source file was ingested

@bruin"""

# TODO: Add imports needed for your ingestion (e.g., pandas, requests).
# - Put dependencies in the nearest `requirements.txt` (this template has one at the pipeline root).
# Docs: https://getbruin.com/docs/bruin/assets/python
import io
import json
import os
from datetime import datetime, timezone

import pandas as pd
import requests
from dateutil import parser
from dateutil.relativedelta import relativedelta


BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
DEFAULT_TAXI_TYPES = ["yellow", "green"]
EXPECTED_RAW_COLUMNS = [
    "VendorID",
    "tpep_pickup_datetime",
    "lpep_pickup_datetime",
    "pickup_datetime",
    "tpep_dropoff_datetime",
    "lpep_dropoff_datetime",
    "dropoff_datetime",
    "PULocationID",
    "DOLocationID",
    "payment_type",
    "passenger_count",
    "trip_distance",
    "fare_amount",
    "total_amount",
]


def month_starts(start_date: datetime, end_date: datetime) -> list[datetime]:
    current = start_date.replace(day=1)
    results = []
    while current < end_date:
        results.append(current)
        current = current + relativedelta(months=1)
    return results


# TODO: Only implement `materialize()` if you are using Bruin Python materialization.
# If you choose the manual-write approach (no `materialization:` block), remove this function and implement ingestion
# as a standard Python script instead.
def materialize():
    """
    TODO: Implement ingestion using Bruin runtime context.

    Required Bruin concepts to use here:
    - Built-in date window variables:
      - BRUIN_START_DATE / BRUIN_END_DATE (YYYY-MM-DD)
      - BRUIN_START_DATETIME / BRUIN_END_DATETIME (ISO datetime)
      Docs: https://getbruin.com/docs/bruin/assets/python#environment-variables
    - Pipeline variables:
      - Read JSON from BRUIN_VARS, e.g. `taxi_types`
      Docs: https://getbruin.com/docs/bruin/getting-started/pipeline-variables

    Design TODOs (keep logic minimal, focus on architecture):
    - Use start/end dates + `taxi_types` to generate a list of source endpoints for the run window.
    - Fetch data for each endpoint, parse into DataFrames, and concatenate.
    - Add a column like `extracted_at` for lineage/debugging (timestamp of extraction).
    - Prefer append-only in ingestion; handle duplicates in staging.
    """
    start_date = parser.parse(os.environ["BRUIN_START_DATE"])
    end_date = parser.parse(os.environ["BRUIN_END_DATE"])
    vars_payload = json.loads(os.environ.get("BRUIN_VARS", "{}"))
    taxi_types = vars_payload.get("taxi_types", DEFAULT_TAXI_TYPES)

    extraction_ts = datetime.now(timezone.utc).isoformat()
    all_frames = []

    for month_start in month_starts(start_date=start_date, end_date=end_date):
        month_label = month_start.strftime("%Y-%m")
        for taxi_type in taxi_types:
            source_url = f"{BASE_URL}/{taxi_type}_tripdata_{month_label}.parquet"
            response = requests.get(source_url, timeout=120)
            if response.status_code != 200:
                continue

            frame = pd.read_parquet(io.BytesIO(response.content))
            for column in EXPECTED_RAW_COLUMNS:
                if column not in frame.columns:
                    frame[column] = pd.NA
            frame["taxi_type"] = taxi_type
            frame["source_url"] = source_url
            frame["extracted_at"] = extraction_ts
            all_frames.append(frame)

    if not all_frames:
        return pd.DataFrame(columns=["taxi_type", "source_url", "extracted_at"])

    return pd.concat(all_frames, ignore_index=True)
