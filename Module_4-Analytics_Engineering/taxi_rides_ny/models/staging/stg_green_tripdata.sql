select *
from {{ source('prod', 'green_tripdata') }}
limit 10
