"""
Historical Data Loader for Weather Streaming Dashboard
STUDENT PROJECT: Big Data - MongoDB Historical Loader

This module provides helper functions to:
1. Connect to MongoDB
2. Query historical weather data for dashboard use.
"""

from pymongo import MongoClient
import pandas as pd
import certifi
from datetime import datetime, timedelta


# ---------------------------------------------------------
# CONNECT TO MONGODB
# ---------------------------------------------------------
def connect_mongo(uri: str, db_name: str, collection: str):
    """
    Connect to MongoDB Atlas cluster and return collection handle.
    """
    client = MongoClient(
        uri,
        tls=True,
        tlsAllowInvalidCertificates=False,
        tlsCAFile=certifi.where()
    )

    db = client[db_name]
    return db[collection]


# ---------------------------------------------------------
# LOAD HISTORICAL WEATHER DATA
# ---------------------------------------------------------
def load_historical_data(uri: str, db_name: str, collection: str, time_range: str):
    """
    Load weather data from MongoDB based on time range.

    time_range options:
    - "1h"
    - "24h"
    - "7d"
    - "30d"

    Returns:
        pandas DataFrame with:
        timestamp, location, temperature_c, humidity,
        pressure_mb, wind_kph, condition
    """

    # Time conversion table
    minutes = {
        "1h": 60,
        "24h": 1440,
        "7d": 10080,
        "30d": 43200
    }

    # Compute cutoff datetime
    since_time = datetime.utcnow() - timedelta(minutes=minutes[time_range])

    # Mongo collection
    col = connect_mongo(uri, db_name, collection)

    # -----------------------------------------------------
    # MAIN DATETIME QUERY
    # First attempt: stored as real datetime
    # Second attempt: stored as ISO string
    # -----------------------------------------------------
# Normalize both datetime and ISO-strings
    since_str = since_time.isoformat()

    # Query ANY of the following:
    # 1. Proper Mongo datetime fields
    # 2. ISO string timestamps saved by producer
    docs = list(col.find({
        "$or": [
            {"timestamp": {"$gte": since_time}},     # datetime objects
            {"timestamp": {"$gte": since_str}},      # ISO strings with timezone
            {"timestamp": {"$gte": since_str + "Z"}} # ISO strings with Z suffix
        ]
    }))

    if not docs:
        return pd.DataFrame()  # return EMPTY DF (safer for Streamlit)

    # Convert to DataFrame
    df = pd.DataFrame(docs)
    df["_id"] = df["_id"].astype(str)

    # -----------------------------------------------------
    # TIMESTAMP CLEANUP
    # Handles:
    # - native MongoDB datetime
    # - ISO strings with or without "Z"
    # -----------------------------------------------------
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    df = df.dropna(subset=["timestamp"])

    # -----------------------------------------------------
    # SAFE FLOAT CONVERSIONS
    # -----------------------------------------------------
    numeric_fields = ["temperature_c", "humidity", "pressure_mb", "wind_kph"]

    for field in numeric_fields:
        if field in df.columns:
            df[field] = pd.to_numeric(df[field], errors="coerce")

    # Drop rows missing key metrics
    df = df.dropna(subset=["temperature_c", "humidity", "pressure_mb", "wind_kph"])

    # Ensure rows are sorted
    df = df.sort_values("timestamp")

    return df
