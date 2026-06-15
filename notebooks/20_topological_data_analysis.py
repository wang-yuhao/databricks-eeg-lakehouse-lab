# Databricks notebook source
# MAGIC %md
# MAGIC # Day 20: Topological Data Analysis (TDA) for EEG
# MAGIC 
# MAGIC ## 🎯 Learning Objectives
# MAGIC - Understand Topological Data Analysis fundamentals
# MAGIC - Apply persistent homology to EEG time series
# MAGIC - Extract topological features using Takens embedding
# MAGIC - Compute Betti numbers and persistence diagrams
# MAGIC - Detect sleep stage patterns via topology
# MAGIC - Build TDA feature store for ML models
# MAGIC 
# MAGIC ## 📋 Prerequisites
# MAGIC - Completed Days 1-19
# MAGIC - Understanding of signal processing
# MAGIC - Basic topology concepts

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 1: TDA Theory Overview
# MAGIC 
# MAGIC ### What is Topological Data Analysis?
# MAGIC 
# MAGIC TDA studies the shape of data using algebraic topology:
# MAGIC - **Persistent Homology**: Tracks topological features across scales
# MAGIC - **Betti Numbers**: Count topological features (components, holes, voids)
# MAGIC   - β₀: Connected components
# MAGIC   - β₁: Loops/cycles
# MAGIC   - β₂: Voids/cavities
# MAGIC - **Persistence Diagrams**: Visualize feature birth and death
# MAGIC 
# MAGIC ### Why TDA for EEG?
# MAGIC 
# MAGIC 1. **Shape-based analysis**: Captures dynamic brain states
# MAGIC 2. **Noise robust**: Topological features are stable
# MAGIC 3. **Multi-scale**: Persistent homology reveals patterns at different scales
# MAGIC 4. **Sleep research**: Different sleep stages have distinct topological signatures

# COMMAND ----------

# Install required packages
%pip install ripser persim scikit-tda

# COMMAND ----------

import numpy as np
import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.types import *

# TDA libraries
from ripser import ripser
from persim import plot_diagrams
import matplotlib.pyplot as plt
from sklearn.preprocessing import StandardScaler

# Configuration
CATALOG = "eeg_lakehouse"
SCHEMA = "gold"
TABLE_NAME = f"{CATALOG}.{SCHEMA}.tda_features"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 2: Takens Embedding
# MAGIC 
# MAGIC Transform 1D time series into point cloud using delay embedding:
# MAGIC 
# MAGIC Given time series x(t), create vectors:
# MAGIC ```
# v(t) = [x(t), x(t+τ), x(t+2τ), ..., x(t+(d-1)τ)]
# MAGIC ```
# MAGIC 
# MAGIC Where:
# MAGIC - d = embedding dimension
# MAGIC - τ = time delay

# COMMAND ----------

def takens_embedding(signal, dimension=3, delay=1):
    """
    Apply Takens delay embedding to time series
    
    Parameters:
    -----------
    signal : array-like
        1D time series
    dimension : int
        Embedding dimension (typical range: 2-10)
    delay : int
        Time delay in samples
    
    Returns:
    --------
    embedded : ndarray
        Point cloud in d-dimensional space
    """
    n = len(signal)
    m = n - (dimension - 1) * delay
    
    embedded = np.zeros((m, dimension))
    for i in range(dimension):
        embedded[:, i] = signal[i * delay:i * delay + m]
    
    return embedded

# Test embedding
test_signal = np.sin(np.linspace(0, 4 * np.pi, 100))
embedded = takens_embedding(test_signal, dimension=3, delay=5)

print(f"Original signal shape: {test_signal.shape}")
print(f"Embedded point cloud shape: {embedded.shape}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 3: Persistent Homology Computation
# MAGIC 
# MAGIC Ripser algorithm computes persistence diagrams efficiently:
# MAGIC 1. Build filtered simplicial complex (Vietoris-Rips)
# MAGIC 2. Track birth and death of topological features
# MAGIC 3. Generate persistence diagram

# COMMAND ----------

def compute_persistent_homology(point_cloud, maxdim=2, thresh=None):
    """
    Compute persistent homology using Ripser
    
    Parameters:
    -----------
    point_cloud : ndarray
        Point cloud data (n_points, n_dimensions)
    maxdim : int
        Maximum homology dimension to compute
    thresh : float
        Maximum distance threshold
    
    Returns:
    --------
    diagrams : dict
        Persistence diagrams for each dimension
    """
    result = ripser(point_cloud, maxdim=maxdim, thresh=thresh)
    return result['dgms']

# Compute persistence for test signal
diagrams = compute_persistent_homology(embedded, maxdim=1)

# Visualize persistence diagram
fig, ax = plt.subplots(figsize=(8, 6))
plot_diagrams(diagrams, show=False, ax=ax)
plt.title("Persistence Diagram - Test Signal")
plt.xlabel("Birth")
plt.ylabel("Death")
display(fig)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 4: Extract Topological Features
# MAGIC 
# MAGIC From persistence diagrams, extract numerical features:
# MAGIC - Betti numbers (feature counts)
# MAGIC - Total persistence
# MAGIC - Maximum persistence
# MAGIC - Persistence entropy

# COMMAND ----------

def extract_tda_features(diagrams):
    """
    Extract numerical features from persistence diagrams
    
    Parameters:
    -----------
    diagrams : list
        Persistence diagrams from Ripser
    
    Returns:
    --------
    features : dict
        Dictionary of topological features
    """
    features = {}
    
    for dim, dgm in enumerate(diagrams):
        # Remove infinite death times
        dgm_finite = dgm[dgm[:, 1] < np.inf]
        
        if len(dgm_finite) == 0:
            features[f'h{dim}_count'] = 0
            features[f'h{dim}_max_persistence'] = 0
            features[f'h{dim}_total_persistence'] = 0
            features[f'h{dim}_mean_persistence'] = 0
            continue
        
        # Persistence = death - birth
        persistence = dgm_finite[:, 1] - dgm_finite[:, 0]
        
        # Betti number (count of features)
        features[f'h{dim}_count'] = len(dgm_finite)
        
        # Maximum persistence
        features[f'h{dim}_max_persistence'] = float(np.max(persistence))
        
        # Total persistence
        features[f'h{dim}_total_persistence'] = float(np.sum(persistence))
        
        # Mean persistence
        features[f'h{dim}_mean_persistence'] = float(np.mean(persistence))
        
        # Persistence entropy
        if np.sum(persistence) > 0:
            p_normalized = persistence / np.sum(persistence)
            entropy = -np.sum(p_normalized * np.log(p_normalized + 1e-10))
            features[f'h{dim}_entropy'] = float(entropy)
        else:
            features[f'h{dim}_entropy'] = 0.0
    
    return features

# Test feature extraction
features = extract_tda_features(diagrams)
for key, value in features.items():
    print(f"{key}: {value:.4f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 5: EEG Signal TDA Pipeline
# MAGIC 
# MAGIC Apply TDA to real EEG epochs:
# MAGIC 1. Load preprocessed EEG epochs
# MAGIC 2. Apply Takens embedding per channel
# MAGIC 3. Compute persistent homology
# MAGIC 4. Extract features

# COMMAND ----------

# Load EEG data
eeg_df = spark.table(f"{CATALOG}.silver.eeg_preprocessed")

print(f"Total epochs: {eeg_df.count()}")
eeg_df.select("subject_id", "epoch_id", "channel_name", "sampling_rate").show(5)

# COMMAND ----------

def process_epoch_tda(signal_array, sampling_rate=256, 
                      embedding_dim=3, delay_ms=20):
    """
    Complete TDA pipeline for one EEG epoch
    
    Parameters:
    -----------
    signal_array : array
        EEG signal for one channel
    sampling_rate : int
        Sampling frequency in Hz
    embedding_dim : int
        Takens embedding dimension
    delay_ms : int
        Time delay in milliseconds
    
    Returns:
    --------
    features : dict
        Topological features
    """
    # Convert delay from ms to samples
    delay_samples = int((delay_ms / 1000) * sampling_rate)
    
    # Apply Takens embedding
    embedded = takens_embedding(signal_array, 
                                dimension=embedding_dim, 
                                delay=delay_samples)
    
    # Normalize point cloud
    scaler = StandardScaler()
    embedded_norm = scaler.fit_transform(embedded)
    
    # Compute persistent homology
    diagrams = compute_persistent_homology(embedded_norm, maxdim=1)
    
    # Extract features
    features = extract_tda_features(diagrams)
    
    return features

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 6: Distributed TDA with Spark UDFs

# COMMAND ----------

# Define schema for TDA features
tda_schema = StructType([
    StructField("h0_count", IntegerType(), True),
    StructField("h0_max_persistence", DoubleType(), True),
    StructField("h0_total_persistence", DoubleType(), True),
    StructField("h0_mean_persistence", DoubleType(), True),
    StructField("h0_entropy", DoubleType(), True),
    StructField("h1_count", IntegerType(), True),
    StructField("h1_max_persistence", DoubleType(), True),
    StructField("h1_total_persistence", DoubleType(), True),
    StructField("h1_mean_persistence", DoubleType(), True),
    StructField("h1_entropy", DoubleType(), True),
])

# Create UDF
@udf(returnType=tda_schema)
def compute_tda_features_udf(signal_array, sampling_rate):
    """Spark UDF for TDA feature extraction"""
    try:
        features = process_epoch_tda(signal_array, sampling_rate)
        return (
            features.get('h0_count', 0),
            features.get('h0_max_persistence', 0.0),
            features.get('h0_total_persistence', 0.0),
            features.get('h0_mean_persistence', 0.0),
            features.get('h0_entropy', 0.0),
            features.get('h1_count', 0),
            features.get('h1_max_persistence', 0.0),
            features.get('h1_total_persistence', 0.0),
            features.get('h1_mean_persistence', 0.0),
            features.get('h1_entropy', 0.0),
        )
    except Exception as e:
        print(f"Error processing epoch: {str(e)}")
        return (0, 0.0, 0.0, 0.0, 0.0, 0, 0.0, 0.0, 0.0, 0.0)

# COMMAND ----------

# Apply TDA to all epochs
tda_features_df = (eeg_df
    .withColumn("tda_features", 
                compute_tda_features_udf(F.col("signal"), F.col("sampling_rate")))
    .select(
        "subject_id",
        "epoch_id",
        "channel_name",
        "sleep_stage",
        "tda_features.*",
        F.current_timestamp().alias("computed_timestamp")
    )
)

# Show results
tda_features_df.show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 7: Sleep Stage Analysis
# MAGIC 
# MAGIC Compare topological features across sleep stages

# COMMAND ----------

# Aggregate TDA features by sleep stage
sleep_stage_tda = (tda_features_df
    .groupBy("sleep_stage")
    .agg(
        F.avg("h0_count").alias("avg_h0_count"),
        F.avg("h1_count").alias("avg_h1_count"),
        F.avg("h0_max_persistence").alias("avg_h0_max_pers"),
        F.avg("h1_max_persistence").alias("avg_h1_max_pers"),
        F.count("*").alias("epoch_count")
    )
    .orderBy("sleep_stage")
)

sleep_stage_tda.show()

# COMMAND ----------

# Visualize TDA features by sleep stage
import matplotlib.pyplot as plt
import seaborn as sns

stage_data = sleep_stage_tda.toPandas()

fig, axes = plt.subplots(2, 2, figsize=(14, 10))

# H0 count (connected components)
ax = axes[0, 0]
ax.bar(stage_data['sleep_stage'], stage_data['avg_h0_count'])
ax.set_title('H0: Connected Components by Sleep Stage')
ax.set_xlabel('Sleep Stage')
ax.set_ylabel('Average Count')

# H1 count (loops)
ax = axes[0, 1]
ax.bar(stage_data['sleep_stage'], stage_data['avg_h1_count'])
ax.set_title('H1: Loops by Sleep Stage')
ax.set_xlabel('Sleep Stage')
ax.set_ylabel('Average Count')

# H0 persistence
ax = axes[1, 0]
ax.bar(stage_data['sleep_stage'], stage_data['avg_h0_max_pers'])
ax.set_title('H0: Maximum Persistence by Sleep Stage')
ax.set_xlabel('Sleep Stage')
ax.set_ylabel('Persistence')

# H1 persistence
ax = axes[1, 1]
ax.bar(stage_data['sleep_stage'], stage_data['avg_h1_max_pers'])
ax.set_title('H1: Maximum Persistence by Sleep Stage')
ax.set_xlabel('Sleep Stage')
ax.set_ylabel('Persistence')

plt.tight_layout()
display(fig)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 8: Memory Consolidation Pattern Analysis
# MAGIC 
# MAGIC Study topological changes during memory-critical sleep phases

# COMMAND ----------

# Focus on slow-wave sleep (SWS) and REM
memory_stages = tda_features_df.filter(
    F.col("sleep_stage").isin(["N3", "REM"])
)

# Time series of topological features
time_series_tda = (memory_stages
    .groupBy("subject_id", "epoch_id", "sleep_stage")
    .agg(
        F.avg("h0_count").alias("h0_count"),
        F.avg("h1_count").alias("h1_count"),
        F.avg("h1_max_persistence").alias("h1_persistence")
    )
    .orderBy("subject_id", "epoch_id")
)

time_series_tda.show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 9: Save TDA Features to Feature Store

# COMMAND ----------

# Create TDA feature table
(tda_features_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(TABLE_NAME)
)

print(f"✅ TDA features saved to {TABLE_NAME}")

# Display table statistics
spark.sql(f"""
    DESCRIBE DETAIL {TABLE_NAME}
""").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC 
# MAGIC In this notebook, you learned:
# MAGIC 
# MAGIC ✅ Topological Data Analysis fundamentals
# MAGIC ✅ Takens delay embedding for time series
# MAGIC ✅ Persistent homology computation with Ripser
# MAGIC ✅ Topological feature extraction (Betti numbers, persistence)
# MAGIC ✅ Sleep stage topology characterization
# MAGIC ✅ Memory consolidation pattern analysis
# MAGIC ✅ Scalable TDA pipelines with Spark UDFs
# MAGIC ✅ TDA feature store for ML models
# MAGIC 
# MAGIC ### Key Insights:
# MAGIC - Different sleep stages have distinct topological signatures
# MAGIC - H0 features capture signal connectivity patterns
# MAGIC - H1 features detect cyclic/oscillatory behavior
# MAGIC - TDA provides robust, noise-resistant features
# MAGIC 
# MAGIC ### Next Steps:
# MAGIC - **Day 21**: Integrate TDA features into end-to-end production pipeline
# MAGIC - Combine spectral, connectivity, and TDA features
# MAGIC - Deploy ML models using comprehensive feature set
# MAGIC 
# MAGIC ### Research Applications:
# MAGIC - Sleep disorder detection using topological biomarkers
# MAGIC - Memory consolidation research
# MAGIC - Brain state transition analysis
# MAGIC - Clinical EEG interpretation

# COMMAND ----------

# MAGIC %md
# MAGIC ## References
# MAGIC 
# MAGIC 1. Takens, F. (1981). Detecting strange attractors in turbulence
# MAGIC 2. Edelsbrunner & Harer (2010). Computational Topology
# MAGIC 3. Perea et al. (2015). Sliding Windows and Persistence
# MAGIC 4. Saggar et al. (2018). TDA for neuroimaging data
