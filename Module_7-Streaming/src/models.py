import json
import pandas as pd
from dataclasses import dataclass


@dataclass
class Ride:
    PULocationID: int
    DOLocationID: int
    trip_distance: float
    lpep_pickup_datetime: str
    lpep_dropoff_datetime: str
    total_amount: float
    tip_amount: float
    passenger_count: int | None

def ride_from_row(row):
    return Ride(
        PULocationID=int(row['PULocationID']),
        DOLocationID=int(row['DOLocationID']),
        trip_distance=float(row['trip_distance']),
        lpep_pickup_datetime=str(row['lpep_pickup_datetime']),
        lpep_dropoff_datetime=str(row['lpep_dropoff_datetime']),
        total_amount=float(row['total_amount']),
        tip_amount=float(row['tip_amount']),
        passenger_count=(
            None if pd.isna(row['passenger_count']) else int(row['passenger_count'])
            ),
    )

def ride_deserializer(data):
    json_str = data.decode('utf-8')
    ride_dict = json.loads(json_str)
    return Ride(**ride_dict)
