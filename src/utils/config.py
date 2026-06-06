"""Pipeline configuration for the EEG Lakehouse Lab.

All paths, table names, and Spark settings are defined here.
Never hardcode catalog/schema/table names in pipeline code — always import from this module.

Exam link: Demonstrates environment-aware config, essential for Databricks bundle deployments.
Research link: Enables swapping between local (Delta OSS) and cloud (UC) without code changes.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
import os


class Env(str, Enum):
    DEV = "dev"
    STAGING = "staging"
    PROD = "prod"


@dataclass
class UCConfig:
    """Unity Catalog object hierarchy.

    Three-level namespace: catalog.schema.table
    Exam domain: Data Governance (Unity Catalog) — ~9% of exam.
    """
    catalog: str = "eeg_lakehouse"
    bronze_schema: str = "bronze"
    silver_schema: str = "silver"
    gold_schema: str = "gold"
    volume_name: str = "raw_eeg_files"  # UC Volume for raw EDF files

    def bronze_table(self, name: str) -> str:
        """Return fully qualified Bronze table name: catalog.bronze.name"""
        return f"{self.catalog}.{self.bronze_schema}.{name}"

    def silver_table(self, name: str) -> str:
        """Return fully qualified Silver table name: catalog.silver.name"""
        return f"{self.catalog}.{self.silver_schema}.{name}"

    def gold_table(self, name: str) -> str:
        """Return fully qualified Gold table name: catalog.gold.name"""
        return f"{self.catalog}.{self.gold_schema}.{name}"

    def volume_path(self, env: Env = Env.DEV) -> str:
        """Return UC Volume path for raw EDF files."""
        return f"/Volumes/{self.catalog}/{self.bronze_schema}/{self.volume_name}"


@dataclass
class PathConfig:
    """File system paths — switches between local and ADLS based on environment.

    Local mode uses a relative ./data/ directory for unit tests and local Spark.
    Production mode uses ADLS Gen2 via Unity Catalog external location.
    """
    env: Env = Env.DEV

    # Raw EDF source — override with ADLS abfss:// path in production
    raw_edf_source: str = field(default="")
    checkpoint_base: str = field(default="")
    schema_location_base: str = field(default="")

    def __post_init__(self) -> None:
        if not self.raw_edf_source:
            if self.env == Env.PROD:
                # Production: ADLS Gen2 path (fill in your storage account)
                self.raw_edf_source = (
                    "abfss://eeg-raw@<storage_account>.dfs.core.windows.net/sleep-edf/"
                )
            else:
                # Dev/local: relative path — works with Delta OSS + local Spark
                self.raw_edf_source = os.path.join(
                    os.path.dirname(__file__), "..", "..", "data", "raw"
                )

        if not self.checkpoint_base:
            self.checkpoint_base = (
                "/dbfs/checkpoints/eeg_lakehouse"
                if self.env == Env.PROD
                else "/tmp/eeg_lakehouse_checkpoints"
            )

        if not self.schema_location_base:
            self.schema_location_base = (
                "/dbfs/schema_locations/eeg_lakehouse"
                if self.env == Env.PROD
                else "/tmp/eeg_lakehouse_schemas"
            )

    def checkpoint(self, name: str) -> str:
        return f"{self.checkpoint_base}/{name}"

    def schema_location(self, name: str) -> str:
        return f"{self.schema_location_base}/{name}"


@dataclass
class SparkConfig:
    """Spark / Delta configuration hints.

    Exam link: Performance optimization domain — AQE, broadcast, shuffle partitions.
    """
    # Adaptive Query Execution — on by default in Databricks 10.4+
    aqe_enabled: bool = True
    # Number of shuffle partitions — default 200 is too high for small EEG batches
    shuffle_partitions: int = 8
    # Broadcast join threshold (bytes) — 10 MB is Databricks default
    broadcast_threshold_bytes: int = 10 * 1024 * 1024  # 10 MB
    # Delta auto-optimize
    auto_optimize_enabled: bool = True
    auto_compact_enabled: bool = True

    def apply(self, spark) -> None:
        """Apply all Spark config settings to an active SparkSession."""
        spark.conf.set("spark.sql.adaptive.enabled", str(self.aqe_enabled).lower())
        spark.conf.set("spark.sql.shuffle.partitions", str(self.shuffle_partitions))
        spark.conf.set(
            "spark.sql.autoBroadcastJoinThreshold",
            str(self.broadcast_threshold_bytes),
        )
        spark.conf.set(
            "spark.databricks.delta.optimizeWrite.enabled",
            str(self.auto_optimize_enabled).lower(),
        )
        spark.conf.set(
            "spark.databricks.delta.autoCompact.enabled",
            str(self.auto_compact_enabled).lower(),
        )


@dataclass
class PipelineConfig:
    """Top-level config object — compose all sub-configs here.

    Usage::

        from src.utils.config import PipelineConfig, Env

        cfg = PipelineConfig(env=Env.PROD)
        bronze_table = cfg.uc.bronze_table("eeg_raw")  # -> eeg_lakehouse.bronze.eeg_raw
        cfg.spark.apply(spark)                          # -> sets all Spark conf
    """
    env: Env = Env.DEV
    uc: UCConfig = field(default_factory=UCConfig)
    paths: PathConfig = field(default_factory=lambda: PathConfig())
    spark: SparkConfig = field(default_factory=SparkConfig)

    def __post_init__(self) -> None:
        # Propagate env to sub-configs
        self.paths = PathConfig(env=self.env)

    @classmethod
    def from_env(cls) -> "PipelineConfig":
        """Construct config from DATABRICKS_ENV environment variable."""
        env_str = os.getenv("DATABRICKS_ENV", "dev").lower()
        return cls(env=Env(env_str))


# Table name constants — import these directly in pipeline code
BRONZE_EEG_RAW = "eeg_raw"           # Bronze: raw EDF binary + metadata
BRONZE_EEG_METADATA = "eeg_metadata" # Bronze: subject/session metadata CSV
SILVER_EEG_PREPROCESSED = "eeg_preprocessed"  # Silver: cleaned EEG epochs
SILVER_SPINDLE_EVENTS = "spindle_events"       # Silver: YASA spindle catalog
SILVER_SO_EVENTS = "so_events"                 # Silver: YASA slow-oscillation catalog
SILVER_TDA_FEATURES = "tda_features"           # Silver: per-window topological features
GOLD_FEATURE_TABLE = "eeg_gold_features"       # Gold: model-ready feature table
GOLD_ML_PREDICTIONS = "eeg_ml_predictions"     # Gold: model predictions
