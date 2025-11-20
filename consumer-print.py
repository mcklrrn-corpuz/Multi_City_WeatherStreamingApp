# consumer-print.py
import json
from kafka import KafkaConsumer

consumer = KafkaConsumer(
    "spotify_stream",
    bootstrap_servers=["localhost:9092"],
    auto_offset_reset="latest",
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    consumer_timeout_ms=10000
)

if __name__ == "__main__":
    for msg in consumer:
        v = msg.value
        print(f"RECV: {v.get('timestamp')}  T={v.get('temperature_c')}  H={v.get('humidity')}  P={v.get('pressure_mb')}")
