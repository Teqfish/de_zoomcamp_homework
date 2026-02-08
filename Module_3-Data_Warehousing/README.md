1. C: 20,332,093

2. B: 0 MB for the External Table and 155.12 MB for the Materialized Table

~~~~sql
CREATE OR REPLACE EXTERNAL TABLE `de-zoomcamp-homework.ny_taxi_raw.yellow_tripdata_ext`
OPTIONS (format = 'PARQUET',
uris = ['gs://de_zoomcamp_homework/ny_taxi_data/prod/ny_taxi_raw/ny_taxi_to_gcs_prod/yellow_tripdata/1770506633.983041.parquet']
);
~~~~

3. A: BigQuery is a columnar database, and it only scans the specific columns requested in the query. Querying two columns (PULocationID, DOLocationID) requires reading more data than querying one column (PULocationID), leading to a higher estimated number of bytes processed.

4. D: 8,333

~~~~sql
SELECT COUNT(tpep_pickup_datetime)
FROM de-zoomcamp-homework.ny_taxi_raw.yellow_tripdata
WHERE fare_amount = 0
~~~~

5. A: Partition by tpep_dropoff_datetime and Cluster on VendorID

~~~~sql
CREATE OR REPLACE TABLE `de-zoomcamp-homework.ny_taxi_raw.yellow_tripdata_optimized`
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID AS
SELECT *
FROM `de-zoomcamp-homework.ny_taxi_raw.yellow_tripdata`;
~~~~

6. B: 310.24 MB for non-partitioned table and 26.84 MB for the partitioned table

~~~~sql
SELECT DISTINCT vendor_id
FROM `de-zoomcamp-homework.ny_taxi_raw.yellow_tripdata_optimized`
WHERE
TIMESTAMP_TRUNC(tpep_dropoff_datetime, DAY) >= TIMESTAMP("2024-03-01")
AND TIMESTAMP_TRUNC(tpep_dropoff_datetime, DAY) <= TIMESTAMP("2024-03-15")
~~~~

7. C: GCP Bucket

8. B: False

9. Currently it says the data estimate is 0mb.  This is likely because BQ has cached the results of a recent identical full count to reduce repetitive overheads.  Exploring the job history confirms this.
