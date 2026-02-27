{{ config(materialized='view') }}

select
  *
from {{ source('raw', 'fhv_tripdata') }}
where dispatching_base_num is not null
