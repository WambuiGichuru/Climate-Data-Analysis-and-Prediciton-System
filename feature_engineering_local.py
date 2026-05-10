"""
Feature Engineering for ERA5 Climate Data
R02 - Distributed Processing Engineer
Milestone 4
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# Initialize Spark session
spark = SparkSession.builder \
    .appName("ERA5_Feature_Engineering") \
    .config("spark.master", "local[*]") \
    .config("spark.sql.adaptive.enabled", "true") \
    .getOrCreate()

print("=" * 60)
print("MILESTONE 4: FEATURE ENGINEERING")
print("=" * 60)

# Read CSV
csv_path = "era5_data.csv"
print(f"\n1. Reading CSV file: {csv_path}")

df = spark.read.option("header", "true").option("inferSchema", "true").csv(csv_path)

print(f"   Row count: {df.count():,}")
print(f"   Columns: {df.columns}")

# Columns from your CSV
# ['lat', 'lon', 'number', 'time', 'pressure_hpa', 'valid_time', 'temperature_k', 'temperature_c', 'step_hours']

# Rename 'time' column to avoid conflict with PySpark's time function
df = df.withColumnRenamed("time", "time_str")

# Create date column from valid_time
df = df.withColumn("date", to_date(col("valid_time")))
df = df.withColumn("year", year(col("date")))
df = df.withColumn("month", month(col("date")))

print(f"\n2. Date extraction complete")

# Region encoding based on latitude
df = df.withColumn(
    "region",
    when(col("lat") >= -1, "Coastal")
    .when((col("lat") >= -5) & (col("lat") < -1), "Highland")
    .when((col("lat") >= -10) & (col("lat") < -5), "Lake")
    .otherwise("Arid")
)

df = df.withColumn("region_encoded",
    when(col("region") == "Coastal", 0)
    .when(col("region") == "Highland", 1)
    .when(col("region") == "Lake", 2)
    .otherwise(3)
)

print(f"3. Region encoding complete")

# Season one-hot encoding
df = df.withColumn("season",
    when(col("month").isin(3, 4, 5), "MAM")
    .when(col("month").isin(6, 7, 8), "JJA")
    .when(col("month").isin(9, 10, 11), "OND")
    .otherwise("DJF")
)

for season in ["MAM", "JJA", "OND", "DJF"]:
    df = df.withColumn(f"season_{season}", when(col("season") == season, 1).otherwise(0))

print(f"4. Season encoding complete")

# Rolling means (3-month precipitation, 12-month temperature)
# Note: Your CSV doesn't have a rainfall column. Using temperature for demonstration.
# If you have rainfall elsewhere, you can add it.

windowSpec3 = Window.partitionBy("lat", "lon").orderBy("date").rowsBetween(-2, 0)
windowSpec12 = Window.partitionBy("lat", "lon").orderBy("date").rowsBetween(-11, 0)

# Since no rainfall column exists, we'll skip precip_3mo_rolling
# But we can compute temp rolling means
df = df.withColumn("temp_12mo_rolling", avg(col("temperature_c")).over(windowSpec12))
print(f"5. Rolling means complete (12-month temperature)")

# Onset detection (using temperature > 25°C as proxy for onset)
# In reality, onset is rainfall-based. This is a placeholder.
df = df.withColumn("onset", when(col("temperature_c") > 25, col("month")).otherwise(lit(None)))
windowPrevYear = Window.partitionBy("lat", "lon").orderBy("date")
df = df.withColumn("onset_month_lag", lag("onset", 12).over(windowPrevYear))
print(f"6. Onset lag complete")

# Select final feature columns
feature_columns = [
    "lat", "lon",
    "region_encoded",
    "year", "month",
    "season_MAM", "season_JJA", "season_OND", "season_DJF",
    "temp_12mo_rolling",
    "onset_month_lag",
    "temperature_c"  # target variable
]

# Keep only columns that exist
existing_columns = [c for c in feature_columns if c in df.columns]
df_features = df.select(existing_columns)

print(f"\n7. Feature columns ({len(existing_columns)} total):")
for col in existing_columns:
    print(f"   - {col}")

# Save to Parquet
output_path = "ml_features/era5_features.parquet"
df_features.write.mode("overwrite").parquet(output_path)
print(f"\n8. Features saved to: {output_path}")

# Show sample
print("\n9. Sample of generated features:")
df_features.show(10, truncate=False)

# Summary
print(f"\n10. Summary Statistics:")
print(f"    Total rows: {df_features.count():,}")
print(f"    Features per row: {len(existing_columns)}")

print("\n" + "=" * 60)
print("FEATURE ENGINEERING COMPLETE")
print("=" * 60)

spark.stop()
