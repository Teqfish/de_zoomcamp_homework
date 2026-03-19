import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from kafka import KafkaConsumer
from models import ride_deserializer

server = 'localhost:9092'
topic_name = 'green-trips'

consumer = KafkaConsumer(
    topic_name,
    bootstrap_servers=[server],
    auto_offset_reset='earliest',
    group_id='green-distance-counter',
    value_deserializer=ride_deserializer,
    consumer_timeout_ms=5000
)

count = 0
total_messages = 0

for message in consumer:
    ride = message.value
    total_messages += 1

    if ride.trip_distance > 5.0:
        count += 1

    if total_messages % 10000 == 0:
        print(f"Read {total_messages} messages so far...")

consumer.close()

print(f"Total messages read: {total_messages}")
print(f"Trips with trip_distance > 5.0: {count}")
