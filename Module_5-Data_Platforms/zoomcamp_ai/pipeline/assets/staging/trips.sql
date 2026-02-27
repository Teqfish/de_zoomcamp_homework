/* @bruin

# Docs:
# - Materialization: https://getbruin.com/docs/bruin/assets/materialization
# - Quality checks (built-ins): https://getbruin.com/docs/bruin/quality/available_checks
# - Custom checks: https://getbruin.com/docs/bruin/quality/custom

# TODO: Set the asset name (recommended: staging.trips).
name: staging.trips
# TODO: Set platform type.
# Docs: https://getbruin.com/docs/bruin/assets/sql
# suggested type: duckdb.sql
type: duckdb.sql

# TODO: Declare dependencies so `bruin run ... --downstream` and lineage work.
# Examples:
# depends:
#   - ingestion.trips
#   - ingestion.payment_lookup
depends:
  - ingestion.trips
  - ingestion.payment_lookup

# TODO: Choose time-based incremental processing if the dataset is naturally time-windowed.
# - This module expects you to use `time_interval` to reprocess only the requested window.
materialization:
  # What is materialization?
  # Materialization tells Bruin how to turn your SELECT query into a persisted dataset.
  # Docs: https://getbruin.com/docs/bruin/assets/materialization
  #
  # Materialization "type":
  # - table: persisted table
  # - view: persisted view (if the platform supports it)
  type: table
  # TODO: set a materialization strategy.
  # Docs: https://getbruin.com/docs/bruin/assets/materialization
  # suggested strategy: time_interval
  #
  # Incremental strategies (what does "incremental" mean?):
  # Incremental means you update only part of the destination instead of rebuilding everything every run.
  # In Bruin, this is controlled by `strategy` plus keys like `incremental_key` and `time_granularity`.
  #
  # Common strategies you can choose from (see docs for full list):
  # - create+replace (full rebuild)
  # - truncate+insert (full refresh without drop/create)
  # - append (insert new rows only)
  # - delete+insert (refresh partitions based on incremental_key values)
  # - merge (upsert based on primary key)
  # - time_interval (refresh rows within a time window)
  strategy: time_interval
  # TODO: set incremental_key to your event time column (DATE or TIMESTAMP).
  incremental_key: pickup_datetime
  # TODO: choose `date` vs `timestamp` based on the incremental_key type.
  time_granularity: timestamp

# TODO: Define output columns, mark primary keys, and add a few checks.
columns:
  - name: trip_id
    type: string
    description: Stable trip identifier built from raw trip attributes
    primary_key: true
    nullable: false
    checks:
      - name: not_null
      - name: unique
  - name: taxi_type
    type: string
    description: Taxi type of the trip
    checks:
      - name: not_null
  - name: vendor_id
    type: integer
    description: Taxi vendor identifier from source
    checks:
      - name: not_null
  - name: pickup_datetime
    type: timestamp
    description: Trip pickup timestamp
    checks:
      - name: not_null
  - name: dropoff_datetime
    type: timestamp
    description: Trip dropoff timestamp
    checks:
      - name: not_null
  - name: pickup_location_id
    type: integer
    description: Pickup location zone ID
    checks:
      - name: not_null
  - name: dropoff_location_id
    type: integer
    description: Dropoff location zone ID
    checks:
      - name: not_null
  - name: payment_type_id
    type: integer
    description: Numeric payment type code
    checks:
      - name: not_null
  - name: payment_type_name
    type: string
    description: Payment type from lookup table
    checks:
      - name: not_null
  - name: passenger_count
    type: double
    description: Number of passengers
    checks:
      - name: non_negative
  - name: trip_distance
    type: double
    description: Trip distance in miles
    checks:
      - name: non_negative
  - name: fare_amount
    type: double
    description: Metered fare amount
    checks:
      - name: non_negative
  - name: total_amount
    type: double
    description: Total charged amount
    checks:
      - name: non_negative

# TODO: Add one custom check that validates a staging invariant (uniqueness, ranges, etc.)
# Docs: https://getbruin.com/docs/bruin/quality/custom
custom_checks:
  - name: duplicate_trip_ids
    description: Ensures no duplicate trip_id rows are present after deduplication.
    query: |
      SELECT COUNT(*)
      FROM (
        SELECT trip_id
        FROM staging.trips
        GROUP BY 1
        HAVING COUNT(*) > 1
      )
    value: 0

@bruin */

-- TODO: Write the staging SELECT query.
--
-- Purpose of staging:
-- - Clean and normalize schema from ingestion
-- - Deduplicate records (important if ingestion uses append strategy)
-- - Enrich with lookup tables (JOINs)
-- - Filter invalid rows (null PKs, negative values, etc.)
--
-- Why filter by {{ start_datetime }} / {{ end_datetime }}?
-- When using `time_interval` strategy, Bruin:
--   1. DELETES rows where `incremental_key` falls within the run's time window
--   2. INSERTS the result of your query
-- Therefore, your query MUST filter to the same time window so only that subset is inserted.
-- If you don't filter, you'll insert ALL data but only delete the window's data = duplicates.

WITH normalized AS (
  SELECT
    taxi_type,
    TRY_CAST(VendorID AS INTEGER) AS vendor_id,
    TRY_CAST(
      COALESCE(tpep_pickup_datetime, lpep_pickup_datetime, pickup_datetime) AS TIMESTAMP
    ) AS pickup_datetime,
    TRY_CAST(
      COALESCE(tpep_dropoff_datetime, lpep_dropoff_datetime, dropoff_datetime) AS TIMESTAMP
    ) AS dropoff_datetime,
    TRY_CAST(PULocationID AS INTEGER) AS pickup_location_id,
    TRY_CAST(DOLocationID AS INTEGER) AS dropoff_location_id,
    TRY_CAST(payment_type AS INTEGER) AS payment_type_id,
    TRY_CAST(passenger_count AS DOUBLE) AS passenger_count,
    TRY_CAST(trip_distance AS DOUBLE) AS trip_distance,
    TRY_CAST(fare_amount AS DOUBLE) AS fare_amount,
    TRY_CAST(total_amount AS DOUBLE) AS total_amount,
    TRY_CAST(extracted_at AS TIMESTAMP) AS extracted_at
  FROM ingestion.trips
  WHERE TRY_CAST(COALESCE(tpep_pickup_datetime, lpep_pickup_datetime, pickup_datetime) AS TIMESTAMP) >= '{{ start_datetime }}'
    AND TRY_CAST(COALESCE(tpep_pickup_datetime, lpep_pickup_datetime, pickup_datetime) AS TIMESTAMP) < '{{ end_datetime }}'
),
filtered AS (
  SELECT *
  FROM normalized
  WHERE pickup_datetime IS NOT NULL
    AND dropoff_datetime IS NOT NULL
    AND pickup_location_id IS NOT NULL
    AND dropoff_location_id IS NOT NULL
    AND payment_type_id IS NOT NULL
    AND vendor_id IS NOT NULL
),
deduplicated AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY
        taxi_type,
        vendor_id,
        pickup_datetime,
        dropoff_datetime,
        pickup_location_id,
        dropoff_location_id,
        trip_distance,
        total_amount
      ORDER BY extracted_at DESC NULLS LAST
    ) AS row_num
  FROM filtered
)
SELECT
  MD5(
    CONCAT_WS(
      '||',
      taxi_type,
      CAST(vendor_id AS VARCHAR),
      CAST(pickup_datetime AS VARCHAR),
      CAST(dropoff_datetime AS VARCHAR),
      CAST(pickup_location_id AS VARCHAR),
      CAST(dropoff_location_id AS VARCHAR),
      CAST(COALESCE(trip_distance, 0) AS VARCHAR),
      CAST(COALESCE(total_amount, 0) AS VARCHAR)
    )
  ) AS trip_id,
  d.taxi_type,
  d.vendor_id,
  d.pickup_datetime,
  d.dropoff_datetime,
  d.pickup_location_id,
  d.dropoff_location_id,
  d.payment_type_id,
  COALESCE(pl.payment_type_name, 'unknown') AS payment_type_name,
  d.passenger_count,
  d.trip_distance,
  d.fare_amount,
  d.total_amount
FROM deduplicated d
LEFT JOIN ingestion.payment_lookup pl
  ON d.payment_type_id = pl.payment_type_id
WHERE d.row_num = 1
