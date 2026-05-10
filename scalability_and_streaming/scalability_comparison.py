"""
Milestone 6: Scalability Comparison (M1 vs M2 vs M5)
R02 - Distributed Processing Engineer
"""

import matplotlib.pyplot as plt
import numpy as np

# M1: Local synthetic benchmark (from scalability_analysis.py)
# Data sizes and naive scan times
sizes_m1 = [10000, 50000, 100000, 500000, 1000000, 5000000]
times_m1 = [3.108, 12.052, 22.345, 108.037, 225.707, 1052.396]  # naive_ms

# M2: This would be Dataproc (placeholder for now)
# Note: Without GCP, we acknowledge this is incomplete
sizes_m2 = [10000, 50000, 100000, 500000, 1000000, 5000000]
times_m2 = [t * 0.8 for t in times_m1]  # Placeholder - 20% faster

# M5: Optimized Spark (from spark_optimization_benchmark.py)
# Using your actual M5 results (total times)
configs = ['Default', 'With cache', 'With cache + partitions=50']
times_m5 = [10.31, 8.13, 3.35]  # seconds

# Create figure
fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 5))

# Plot 1: M1 scalability (synthetic data)
ax1.plot(sizes_m1, times_m1, 'o-', label='M1: Naive Scan (local)', color='blue')
ax1.set_xlabel('Number of Rows')
ax1.set_ylabel('Time (ms)')
ax1.set_title('M1: Local Synthetic Benchmark')
ax1.set_xscale('log')
ax1.set_yscale('log')
ax1.grid(True)
ax1.legend()

# Plot 2: M5 optimization results
ax2.bar(configs, times_m5, color=['red', 'orange', 'green'])
ax2.set_xlabel('Configuration')
ax2.set_ylabel('Time (seconds)')
ax2.set_title('M5: Spark Optimization Results')
ax2.set_ylim(0, 12)

# Annotate improvement
ax2.text(2, 9, f"67.5% faster", ha='center', fontsize=10, 
         bbox=dict(facecolor='white', alpha=0.8))

plt.tight_layout()
plt.savefig('scalability_comparison.png', dpi=150)
print("✅ Scalability comparison chart saved as 'scalability_comparison.png'")
print("\n📊 M1 Results (5M rows): 1052.4ms (naive scan)")
print("📊 M5 Results (optimized): 3.35s total pipeline")
print("\n⚠️ Note: M2 (Dataproc) not run due to GCP billing requirements.")
