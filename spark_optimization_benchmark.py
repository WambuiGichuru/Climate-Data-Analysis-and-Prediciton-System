"""
Milestone 5: Spark Pipeline Optimization
R02 - Distributed Processing Engineer
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import time

print("=" * 60)
print("MILESTONE 5: SPARK OPTIMIZATION BENCHMARK")
print("=" * 60)

# Configuration variants
configs = [
    {"name": "Default (no cache, partitions=200)", "cache": False, "partitions": 200},
    {"name": "With cache, partitions=200", "cache": True, "partitions": 200},
    {"name": "With cache, partitions=50", "cache": True, "partitions": 50},
]

results = []

for config in configs:
    print(f"\n--- Testing: {config['name']} ---")
    
    # Create Spark session with config
    spark = SparkSession.builder \
        .appName(f"Optimization_{config['name']}") \
        .config("spark.master", "local[*]") \
        .config("spark.sql.shuffle.partitions", config['partitions']) \
        .config("spark.sql.adaptive.enabled", "false") \
        .getOrCreate()
    
    # Read data
    start = time.time()
    df = spark.read.parquet("ml_features/era5_features.parquet")
    read_time = time.time() - start
    print(f"  Read time: {read_time:.2f}s")
    
    # MAP phase (feature transformations)
    start_map = time.time()
    
    # Add derived features (this is the MAP phase)
    df = df.withColumn("temp_f", col("temperature_c") * 9/5 + 32)
    df = df.withColumn("temp_squared", col("temperature_c") ** 2)
    df = df.withColumn("temp_bucket", floor(col("temperature_c") / 10))
    
    # CACHE after MAP phase (if enabled)
    if config["cache"]:
        df = df.cache()
        # Force cache by counting
        df.count()
    
    map_time = time.time() - start_map
    print(f"  MAP phase time: {map_time:.2f}s")
    
    # REDUCE 1: Group by region
    start_reduce1 = time.time()
    result1 = df.groupBy("region_encoded").avg("temperature_c").collect()
    reduce1_time = time.time() - start_reduce1
    print(f"  REDUCE 1 (by region): {reduce1_time:.2f}s")
    
    # REDUCE 2: Group by month
    start_reduce2 = time.time()
    result2 = df.groupBy("month").avg("temperature_c").collect()
    reduce2_time = time.time() - start_reduce2
    print(f"  REDUCE 2 (by month): {reduce2_time:.2f}s")
    
    total_time = map_time + reduce1_time + reduce2_time
    
    results.append({
        "config": config["name"],
        "cache": config["cache"],
        "partitions": config["partitions"],
        "map_time": map_time,
        "reduce1_time": reduce1_time,
        "reduce2_time": reduce2_time,
        "total_time": total_time
    })
    
    spark.stop()
    print(f"  Total time: {total_time:.2f}s")

# Print summary table
print("\n" + "=" * 60)
print("OPTIMIZATION RESULTS")
print("=" * 60)
print(f"{'Configuration':<35} | {'Map (s)':<8} | {'Reduce1 (s)':<10} | {'Reduce2 (s)':<10} | {'Total (s)':<8}")
print("-" * 80)
for r in results:
    print(f"{r['config']:<35} | {r['map_time']:<8.2f} | {r['reduce1_time']:<10.2f} | {r['reduce2_time']:<10.2f} | {r['total_time']:<8.2f}")

# Calculate improvements
if len(results) >= 2:
    default_total = results[0]["total_time"]
    cache_total = results[1]["total_time"]
    tuned_total = results[2]["total_time"]
    
    print("\n" + "=" * 60)
    print("IMPROVEMENT SUMMARY")
    print("=" * 60)
    print(f"Default: {default_total:.2f}s")
    print(f"With cache: {cache_total:.2f}s ({(1 - cache_total/default_total)*100:.1f}% faster)")
    print(f"With cache + partitions=50: {tuned_total:.2f}s ({(1 - tuned_total/default_total)*100:.1f}% faster)")

print("\n✅ Benchmark complete. Add these numbers to your M5 report.")
