from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, DoubleType

# MongoDB config
MONGO_URI = "mongodb+srv://qmlbcorpuz_db_user:cpe032@groceryinventorysystem.rh8eact.mongodb.net"
DB_NAME = "WeatherDB"
COLLECTION_NAME = "Readings"

# Kafka config
KAFKA_TOPIC = "streaming-data"
KAFKA_BOOTSTRAP = "localhost:9092"

spark = SparkSession.builder \
    .appName("KafkaWeatherSparkStream") \
    .config("spark.mongodb.write.connection.uri", MONGO_URI) \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

schema = StructType() \
    .add("timestamp", StringType()) \
    .add("location", StringType()) \
    .add("temperature_c", DoubleType()) \
    .add("humidity", DoubleType()) \
    .add("pressure_mb", DoubleType()) \
    .add("wind_kph", DoubleType()) \
    .add("condition", StringType())

# Read stream from Kafka
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "latest") \
    .load()

json_df = kafka_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Write to MongoDB
query = json_df.writeStream \
    .format("mongodb") \
    .option("checkpointLocation", "/tmp/weather_checkpoint") \
    .option("forceDeleteTempCheckpointLocation", "true") \
    .option("database", DB_NAME) \
    .option("collection", COLLECTION_NAME) \
    .start()

query.awaitTermination()
