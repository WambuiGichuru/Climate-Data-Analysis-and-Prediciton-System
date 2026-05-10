import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from dotenv import load_dotenv

# Load your new .env config
load_dotenv()

# 1. Initialize Spark for Streaming
spark = SparkSession.builder \
    .appName("Nairobi_Streaming_Analytics_R04") \
    .getOrCreate()

# 2. Define the Schema for the Kafka Pulse (Matching your Training Features)
# As R-04, you ensure these match the XGBoost input schema
def process_nairobi_stream(df):
    # Apply Watermarking to handle late data
    streaming_df = df.withWatermark("timestamp", "12 hours")
    
    # 72-Hour Sliding Window for Rainfall Onset detection
    # This is the core 'Speed Layer' requirement
    analytics_df = streaming_df.groupBy(
        F.window("timestamp", "72 hours", "6 hours"),
        "county"
    ).agg(
        F.sum("precipitation_mm").alias("total_rainfall_72h"),
        F.avg("temperature_c").alias("avg_temp_72h")
    )
    
    # Apply the 20mm Onset Threshold logic
    return analytics_df.withColumn(
        "onset_alert", 
        F.when(F.col("total_rainfall_72h") >= 20.0, "HIGH RISK").otherwise("LOW RISK")
    )

# 3. Deliverable: Validation Logic
# You must compare this 'Live' alert against Nairobi's historical 'Expected' baseline