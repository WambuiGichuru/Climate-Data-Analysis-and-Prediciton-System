from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Create Spark session with Kafka packages for Spark 3.4.1
spark = SparkSession.builder \
    .appName("WeatherStreaming") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1") \
    .config("spark.sql.streaming.schemaInference", "true") \
    .config("spark.sql.adaptive.enabled", "false") \
    .getOrCreate()

# Define schema for incoming JSON data
schema = StructType([
    StructField("county", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("rainfall_mm", DoubleType(), True),
    StructField("temperature_c", DoubleType(), True),
    StructField("humidity", IntegerType(), True)
])

# Read from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "raw-weather-stream") \
    .option("startingOffsets", "latest") \
    .load() \
    .select(from_json(col("value").cast("string"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("timestamp", to_timestamp(col("timestamp")))

# Simple console output (without window for testing)
query = df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .start()

query.awaitTermination()
