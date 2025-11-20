import json
from kafka import KafkaConsumer
from pymongo import MongoClient
import certifi
import sys

# ================================
# CONFIGURATIONS (EDIT IF NEEDED)
# ================================
KAFKA_TOPIC = "streaming-data"        # must match producer
KAFKA_SERVER = "localhost:9092"       # Kafka broker

MONGO_URI = (
    "mongodb+srv://qmlbcorpuz_db_user:cpe032@"
    "groceryinventorysystem.rh8eact.mongodb.net/"
    "?retryWrites=true&w=majority"
)

DB_NAME = "WeatherDB"              
COLLECTION_NAME = "Readings"          
# ================================


def main():

    # 1️⃣ CONNECT TO MONGODB
    try:
        mongo_client = MongoClient(
            MONGO_URI,
            tls=True,
            tlsAllowInvalidCertificates=False,
            tlsCAFile=certifi.where()
        )
        db = mongo_client[DB_NAME]
        collection = db[COLLECTION_NAME]
        print(f"[MongoDB] Connected → Database: {DB_NAME}, Collection: {COLLECTION_NAME}")
    except Exception as e:
        print("[MongoDB ERROR] Cannot connect:", e)
        sys.exit(1)

    # 2️⃣ CONNECT TO KAFKA
    try:
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=KAFKA_SERVER,
            auto_offset_reset="latest",
            enable_auto_commit=True,
            value_deserializer=lambda x: json.loads(x.decode("utf-8"))
        )
        print(f"[Kafka] Connected → Topic: {KAFKA_TOPIC}")
    except Exception as e:
        print("[Kafka ERROR] Cannot connect:", e)
        sys.exit(1)

    # 3️⃣ START CONSUMING
    print("\n🔥 Starting Kafka → MongoDB Consumer (Multi-City Support)...")
    print("Press CTRL + C to stop.\n")

    try:
        for message in consumer:
            data = message.value
            print("Received:", data)

            # Ensure messages include 'location'
            if "location" not in data:
                print("[WARNING] Dropped message because no location field:", data)
                continue

            # Insert into MongoDB
            try:
                collection.insert_one(data)
                print(f"✔ Inserted ({data.get('location')}) into MongoDB")
            except Exception as insert_error:
                print("[MongoDB INSERT ERROR]", insert_error)
                continue

    except KeyboardInterrupt:
        print("\nConsumer stopped manually.")
    except Exception as e:
        print("[ERROR] Unexpected issue:", e)


if __name__ == "__main__":
    main()