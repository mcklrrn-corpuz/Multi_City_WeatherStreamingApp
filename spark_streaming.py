"""
Enhanced Spark Streaming Application for Weather Data Processing
Implements: Kafka → Spark → MongoDB pipeline with aggregations and anomaly detection
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json,
    col,
    window,
    avg,
    max,
    min,
    stddev,
    count,
    when,
    current_timestamp,
    to_timestamp,
    concat,
    lit,
)
from pyspark.sql.types import StructType, StringType, DoubleType

# ================================
# CONFIGURATIONS
# ================================
MONGO_URI = "mongodb+srv://qmlbcorpuz_db_user:cpe032@groceryinventorysystem.rh8eact.mongodb.net"
DB_NAME = "WeatherDB"
COLLECTION_READINGS = "Readings"
COLLECTION_WINDOWED = "WindowedStats"
COLLECTION_ALERTS = "Alerts"

KAFKA_TOPIC = "streaming-data"
KAFKA_BOOTSTRAP = "localhost:9092"

# NOTE: Make sure to run with the Mongo Spark connector, e.g.:
# spark-submit \
#   --packages org.mongodb.spark:mongo-spark-connector_2.12:10.2.0 \
#   spark_streaming.py

# ================================
# SPARK SESSION INITIALIZATION
# ================================
print("Initializing Spark Session...")
spark = SparkSession.builder \
    .appName("KafkaWeatherSparkStream") \
    .config("spark.mongodb.write.connection.uri", MONGO_URI) \
    .config("spark.sql.streaming.checkpointLocation", "/tmp/spark_checkpoints") \
    .config("spark.sql.shuffle.partitions", "2") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
print("✓ Spark Session initialized")

# ================================
# SCHEMA DEFINITION
# ================================
schema = StructType() \
    .add("timestamp", StringType()) \
    .add("location", StringType()) \
    .add("temperature_c", DoubleType()) \
    .add("humidity", DoubleType()) \
    .add("pressure_mb", DoubleType()) \
    .add("wind_kph", DoubleType()) \
    .add("condition", StringType())

# ================================
# READ STREAM FROM KAFKA
# ================================
print(f"Connecting to Kafka at {KAFKA_BOOTSTRAP}, topic: {KAFKA_TOPIC}...")
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()

print("✓ Connected to Kafka stream")

# ================================
# PARSE JSON AND CONVERT TIMESTAMP
# ================================
json_df = kafka_df.selectExpr("CAST(value AS STRING) as json_value") \
    .select(from_json(col("json_value"), schema).alias("data")) \
    .select("data.*")

# Convert string timestamp to proper timestamp type for windowing
json_df = json_df.withColumn(
    "timestamp",
    to_timestamp(col("timestamp"))
)

# Drop records where timestamp failed to parse
json_df = json_df.filter(col("timestamp").isNotNull())

print("✓ JSON parsing configured")

# ================================
# STREAM 1: RAW DATA TO MONGODB
# ================================
print("Starting Stream 1: Raw Data → MongoDB Readings...")
query_raw = json_df.writeStream \
    .format("mongodb") \
    .option("database", DB_NAME) \
    .option("collection", COLLECTION_READINGS) \
    .option("checkpointLocation", "/tmp/weather_checkpoint_raw") \
    .outputMode("append") \
    .start()

print("✓ Stream 1 started (Raw Readings)")

# ================================
# STREAM 2: WINDOWED AGGREGATIONS
# ================================
print("Starting Stream 2: Windowed Aggregations → MongoDB WindowedStats...")

windowed_df = json_df \
    .withWatermark("timestamp", "10 minutes") \
    .groupBy(
        window(col("timestamp"), "5 minutes", "1 minute"),
        col("location")
    ) \
    .agg(
        avg("temperature_c").alias("avg_temp"),
        max("temperature_c").alias("max_temp"),
        min("temperature_c").alias("min_temp"),
        stddev("temperature_c").alias("temp_stddev"),
        avg("humidity").alias("avg_humidity"),
        max("humidity").alias("max_humidity"),
        min("humidity").alias("min_humidity"),
        avg("pressure_mb").alias("avg_pressure"),
        max("pressure_mb").alias("max_pressure"),
        min("pressure_mb").alias("min_pressure"),
        avg("wind_kph").alias("avg_wind"),
        count("*").alias("reading_count")
    ) \
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("location"),
        col("avg_temp"),
        col("max_temp"),
        col("min_temp"),
        col("temp_stddev"),
        col("avg_humidity"),
        col("max_humidity"),
        col("min_humidity"),
        col("avg_pressure"),
        col("max_pressure"),
        col("min_pressure"),
        col("avg_wind"),
        col("reading_count"),
        current_timestamp().alias("computed_at")
    )

query_windowed = windowed_df.writeStream \
    .format("mongodb") \
    .option("database", DB_NAME) \
    .option("collection", COLLECTION_WINDOWED) \
    .option("checkpointLocation", "/tmp/weather_checkpoint_windowed") \
    .outputMode("append") \
    .start()

print("✓ Stream 2 started (Windowed Statistics)")

# ================================
# STREAM 3: ANOMALY DETECTION
# ================================
print("Starting Stream 3: Anomaly Detection → MongoDB Alerts...")

anomalies_df = json_df.filter(
    (col("temperature_c") > 40) |
    (col("temperature_c") < -5) |
    (col("humidity") > 95) |
    (col("pressure_mb") < 980) |
    (col("pressure_mb") > 1040)
).withColumn(
    "alert_type",
    when(col("temperature_c") > 40, "HIGH_TEMP")
    .when(col("temperature_c") < -5, "LOW_TEMP")
    .when(col("humidity") > 95, "HIGH_HUMIDITY")
    .when(col("pressure_mb") < 980, "LOW_PRESSURE")
    .when(col("pressure_mb") > 1040, "HIGH_PRESSURE")
    .otherwise("UNKNOWN")
).withColumn(
    "severity",
    when((col("temperature_c") > 45) | (col("temperature_c") < -10), "CRITICAL")
    .when((col("humidity") > 98) | (col("pressure_mb") < 970), "CRITICAL")
    .otherwise("WARNING")
).withColumn(
    "alert_message",
    when(col("alert_type") == "HIGH_TEMP",
         concat(lit("High temperature detected: "), col("temperature_c"), lit(" °C")))
    .when(col("alert_type") == "LOW_TEMP",
         concat(lit("Low temperature detected: "), col("temperature_c"), lit(" °C")))
    .when(col("alert_type") == "HIGH_HUMIDITY",
         concat(lit("High humidity detected: "), col("humidity"), lit(" %")))
    .when(col("alert_type") == "LOW_PRESSURE",
         concat(lit("Low pressure detected: "), col("pressure_mb"), lit(" hPa")))
    .when(col("alert_type") == "HIGH_PRESSURE",
         concat(lit("High pressure detected: "), col("pressure_mb"), lit(" hPa")))
    .otherwise(lit("Unknown anomaly"))
).withColumn(
    "detected_at",
    current_timestamp()
)

query_alerts = anomalies_df.writeStream \
    .format("mongodb") \
    .option("database", DB_NAME) \
    .option("collection", COLLECTION_ALERTS) \
    .option("checkpointLocation", "/tmp/weather_checkpoint_alerts") \
    .outputMode("append") \
    .start()

print("✓ Stream 3 started (Anomaly Alerts)")

# ================================
# MONITORING
# ================================
print("\n" + "="*60)
print("🚀 SPARK STREAMING APPLICATION RUNNING")
print("="*60)
print(f"📊 Stream 1: Raw readings → {COLLECTION_READINGS}")
print(f"📈 Stream 2: 5-minute windows → {COLLECTION_WINDOWED}")
print(f"⚠️  Stream 3: Anomaly alerts → {COLLECTION_ALERTS}")
print("="*60)
print("\n✓ All streams started successfully!")
print("📡 Consuming from Kafka topic:", KAFKA_TOPIC)
print("💾 Writing to MongoDB:", DB_NAME)
print("\n⌨️  Press CTRL+C to stop...\n")

# ================================
# WAIT FOR TERMINATION
# ================================
try:
    # Wait for any query to terminate (keeps app alive)
    spark.streams.awaitAnyTermination()
except KeyboardInterrupt:
    print("\n\n Stopping Spark Streaming Application...")
    print("Stopping all active streams...")
    for q in spark.streams.active:
        print(f"  → Stopping {q.name or 'stream'}...")
        q.stop()
    print("✓ All streams stopped")
    spark.stop()
    print("✓ Spark session closed")
    print("Goodbye!")
