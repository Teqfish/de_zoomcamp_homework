/* @bruin

# Docs:
# - SQL assets: https://getbruin.com/docs/bruin/assets/sql
# - Materialization: https://getbruin.com/docs/bruin/assets/materialization
# - Quality checks: https://getbruin.com/docs/bruin/quality/available_checks

# TODO: Set the asset name (recommended: reports.trips_report).
name: reports.trips_report

# TODO: Set platform type.
# Docs: https://getbruin.com/docs/bruin/assets/sql
# suggested type: duckdb.sql
type: duckdb.sql

# TODO: Declare dependency on the staging asset(s) this report reads from.
depends:
  - staging.trips

# TODO: Choose materialization strategy.
# For reports, `time_interval` is a good choice to rebuild only the relevant time window.
# Important: Use the same `incremental_key` as staging (e.g., pickup_datetime) for consistency.
materialization:
  type: table
  # suggested strategy: time_interval
  strategy: time_interval
  # TODO: set to your report's date column
  incremental_key: trip_date
  # TODO: set to `date` or `timestamp`
  time_granularity: date

# TODO: Define report columns + primary key(s) at your chosen level of aggregation.
columns:
  - name: taxi_type
    type: string
    description: Taxi type for the aggregated trips
    primary_key: true
    checks:
      - name: not_null
  - name: trip_date
    type: DATE
    description: Trip date derived from pickup timestamp
    primary_key: true
    checks:
      - name: not_null
  - name: payment_type_name
    type: string
    description: Payment type used for the trips
    primary_key: true
    checks:
      - name: not_null
  - name: trip_count
    type: BIGINT
    description: Number of trips in the group
    checks:
      - name: non_negative
  - name: total_amount_sum
    type: DOUBLE
    description: Sum of total_amount in the group
    checks:
      - name: non_negative
  - name: avg_trip_distance
    type: DOUBLE
    description: Average trip distance in miles for the group
    checks:
      - name: non_negative

@bruin */

-- Purpose of reports:
-- - Aggregate staging data for dashboards and analytics
-- Required Bruin concepts:
-- - Filter using `{{ start_datetime }}` / `{{ end_datetime }}` for incremental runs
-- - GROUP BY your dimension + date columns

SELECT
  DATE_TRUNC('day', pickup_datetime)::DATE AS trip_date,
  taxi_type,
  payment_type_name,
  COUNT(*) AS trip_count,
  SUM(COALESCE(total_amount, 0)) AS total_amount_sum,
  AVG(COALESCE(trip_distance, 0)) AS avg_trip_distance
FROM staging.trips
WHERE pickup_datetime >= '{{ start_datetime }}'
  AND pickup_datetime < '{{ end_datetime }}'
GROUP BY 1, 2, 3
