"""Configuration dataclasses for the EEG Lakehouse Lab.

This module centralizes all configuration — paths, catalog names, Spark settings —
so that changing an environment (dev → staging → prod) requires editing exactly one file.

Exam relevance: Demonstrates production config management; Unity Catalog FQN helpers
are directly tested in the UC governance exam domain.
"""

from dataclasses import dataclass, field
from typing import Optional
import os


# ---------------------------------------------------------------------------
# Environment detection
# ---------------------------------------------------------------------------

def _env() -> str:
    """Return current environment from DATABRICKS_ENV env var, default 'dev'."""
    return os.getenv("DATABRICKS_ENV", "dev")


# ---------------------------------------------------------------------------
# Path configuration
# ---------------------------------------------------------------------------

@dataclass
class PathConfig:
    """File system and Unity Catalog Volume paths for raw EEG data."""

    # Local dev paths (override these in Databricks via environment variables)
    local_edf_dir: str = field(
        default_factory=lambda: os.getenv(
            "EEG_LOCAL_DIR", "/tmp/sleep-edf"
        )
    )

    # Databricks Unity Catalog Volume path for raw EDF files
    # Format: /Volumes/<catalog>/<schema>/<volume_name>/<path>
    volume_edf_dir: str = field(
        default_factory=lambda: os.getenv(
            "EEG_VOLUME_DIR",
            "/Volumes/eeg_lakehouse/bronze/raw_edf"
        )
    )

    # Auto Loader checkpoint location (must be a cloud-backed path)
    autoloader_checkpoint: str = field(
        default_factory=lambda: os.getenv(
            "AUTOLOADER_CHECKPOINT",
            "/Volumes/eeg_lakehouse/bronze/_checkpoints/edf_ingestion"
        )
    )

    # Schema inference location for Auto Loader
    autoloader_schema_location: str = field(
        default_factory=lambda: os.getenv(
            "AUTOLOADER_SCHEMA_LOCATION",
            "/Volumes/eeg_lakehouse/bronze/_schema/edf_schema"
        )
    )

    @property
    def edf_source_path(self) -> str:
        """Return EDF source path: Volume path on Databricks, local path otherwise."""
        if os.getenv("DATABRICKS_RUNTIME_VERSION"):
            return self.volume_edf_dir
        return self.local_edf_dir


# ---------------------------------------------------------------------------
# Unity Catalog configuration
# ---------------------------------------------------------------------------

@dataclass
class CatalogConfig:
    """Unity Catalog catalog, schema, and table names.

    FQN (Fully Qualified Name) format: <catalog>.<schema>.<table>
    This pattern maps directly to Unity Catalog exam questions.
    """

    catalog: str = "eeg_lakehouse"

    # Layer schemas
    bronze_schema: str = "bronze"
    silver_schema: str = "silver"
    gold_schema: str = "gold"

    # Bronze tables
    bronze_edf_table: str = "raw_eeg_files"
    bronze_metadata_table: str = "subject_metadata"

    # Silver tables
    silver_epochs_table: str = "cleaned_epochs"
    silver_spindles_table: str = "spindle_events"
    silver_so_table: str = "slow_oscillation_events"
    silver_pac_table: str = "pac_windows"

    # Gold tables
    gold_features_table: str = "tda_features"
    gold_ml_table: str = "ml_ready_features"

    def fqn(self, schema: str, table: str) -> str:
        """Return a fully qualified Unity Catalog table name.

        Example:
            cfg.fqn(cfg.bronze_schema, cfg.bronze_edf_table)
            # → 'eeg_lakehouse.bronze.raw_eeg_files'
        """
        return f"{self.catalog}.{schema}.{table}"

    @property
    def bronze_edf_fqn(self) -> str:
        return self.fqn(self.bronze_schema, self.bronze_edf_table)

    @property
    def bronze_metadata_fqn(self) -> str:
        return self.fqn(self.bronze_schema, self.bronze_metadata_table)

    @property
    def silver_epochs_fqn(self) -> str:
        return self.fqn(self.silver_schema, self.silver_epochs_table)

    @property
    def silver_spindles_fqn(self) -> str:
        return self.fqn(self.silver_schema, self.silver_spindles_table)

    @property
    def silver_so_fqn(self) -> str:
        return self.fqn(self.silver_schema, self.silver_so_table)

    @property
    def gold_features_fqn(self) -> str:
        return self.fqn(self.gold_schema, self.gold_features_table)


# ---------------------------------------------------------------------------
# Spark / processing configuration
# ---------------------------------------------------------------------------

@dataclass
class SparkConfig:
    """Spark and Databricks runtime configuration.

    AQE (Adaptive Query Execution) settings are critical for exam Domain 5:
    Performance & Cost Optimization. AQE dynamically re-optimizes query plans
    at runtime based on accurate partition statistics.
    """

    # Adaptive Query Execution — ON by default in Databricks 10.4+
    aqe_enabled: bool = True

    # Coalesce small shuffle partitions after joins/aggregations
    # AQE will reduce from spark.sql.shuffle.partitions (default 200)
    aqe_coalesce_partitions: bool = True

    # Broadcast join threshold — tables smaller than this are auto-broadcast
    # Default: 10MB. Increase for small EEG metadata lookup tables.
    broadcast_threshold_bytes: int = 50 * 1024 * 1024  # 50 MB

    # Shuffle partitions — reduce for small datasets to avoid excessive overhead
    shuffle_partitions: int = 200  # Override to ~8 for local dev

    def to_spark_conf(self) -> dict:
        """Return Spark configuration as key-value dict for SparkSession.builder.config()."""
        return {
            "spark.sql.adaptive.enabled": str(self.aqe_enabled).lower(),
            "spark.sql.adaptive.coalescePartitions.enabled": str(self.aqe_coalesce_partitions).lower(),
            "spark.sql.autoBroadcastJoinThreshold": str(self.broadcast_threshold_bytes),
            "spark.sql.shuffle.partitions": str(self.shuffle_partitions),
            # Delta-specific: enable optimized writes (auto-compact small files)
            "spark.databricks.delta.optimizeWrite.enabled": "true",
            # Enable change data feed for incremental downstream consumption
            "spark.databricks.delta.properties.defaults.enableChangeDataFeed": "true",
        }


# ---------------------------------------------------------------------------
# EEG Signal processing configuration
# ---------------------------------------------------------------------------

@dataclass
class EEGConfig:
    """EEG signal processing parameters.

    These map to YASA and MNE-Python processing settings used in the Silver layer.
    """

    # Preprocessing
    bandpass_low_hz: float = 0.5
    bandpass_high_hz: float = 40.0
    notch_freq_hz: float = 50.0  # European power line frequency
    target_sample_rate_hz: int = 100

    # Epoch windows
    epoch_duration_sec: int = 30  # Standard sleep scoring epoch
    tda_window_sec: int = 5       # Sliding window for TDA point cloud construction
    tda_overlap_ratio: float = 0.5  # 50% overlap → 2.5 sec step

    # Spindle detection (YASA defaults, validated in literature)
    spindle_freq_low_hz: float = 11.0
    spindle_freq_high_hz: float = 16.0
    spindle_min_duration_sec: float = 0.5
    spindle_max_duration_sec: float = 3.0

    # Slow oscillation detection (YASA defaults)
    so_neg_peak_threshold_uv: float = -40.0  # negative peak amplitude threshold
    so_freq_low_hz: float = 0.5
    so_freq_high_hz: float = 1.0

    # TDA parameters
    ripser_max_dimension: int = 2  # Compute H0, H1, H2
    vietoris_rips_max_edge: Optional[float] = None  # None → auto from data


# ---------------------------------------------------------------------------
# Composite application config
# ---------------------------------------------------------------------------

@dataclass
class AppConfig:
    """Composite configuration object — pass this everywhere in the pipeline.

    Usage example::

        from src.utils.config import AppConfig
        cfg = AppConfig()
        spark.read.format("delta").table(cfg.catalog.bronze_edf_fqn)
    """

    env: str = field(default_factory=_env)
    paths: PathConfig = field(default_factory=PathConfig)
    catalog: CatalogConfig = field(default_factory=CatalogConfig)
    spark: SparkConfig = field(default_factory=SparkConfig)
    eeg: EEGConfig = field(default_factory=EEGConfig)


# Module-level default config (singleton pattern for convenience)
DEFAULT_CONFIG = AppConfig()
