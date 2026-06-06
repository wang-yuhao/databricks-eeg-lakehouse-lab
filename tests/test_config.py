"""Day 1 tests: validate PipelineConfig construction and table naming."""

from src.utils.config import (
    PipelineConfig,
    Env,
    UCConfig,
    BRONZE_EEG_RAW,
    SILVER_SPINDLE_EVENTS,
    GOLD_FEATURE_TABLE,
)


def test_default_config_creates_successfully():
    cfg = PipelineConfig()
    assert cfg.env == Env.DEV


def test_uc_fully_qualified_names():
    uc = UCConfig(catalog="eeg_lakehouse", bronze_schema="bronze")
    assert uc.bronze_table(BRONZE_EEG_RAW) == "eeg_lakehouse.bronze.eeg_raw"
    assert uc.silver_table(SILVER_SPINDLE_EVENTS) == "eeg_lakehouse.silver.spindle_events"
    assert uc.gold_table(GOLD_FEATURE_TABLE) == "eeg_lakehouse.gold.eeg_gold_features"


def test_prod_config_uses_adls_path():
    cfg = PipelineConfig(env=Env.PROD)
    assert "abfss://" in cfg.paths.raw_edf_source


def test_dev_config_uses_local_path():
    cfg = PipelineConfig(env=Env.DEV)
    # Local path should not be an abfss:// URL
    assert not cfg.paths.raw_edf_source.startswith("abfss://")


def test_checkpoint_path_namespaced():
    cfg = PipelineConfig(env=Env.DEV)
    cp = cfg.paths.checkpoint("bronze_eeg")
    assert "bronze_eeg" in cp


def test_spark_config_defaults():
    cfg = PipelineConfig()
    assert cfg.spark.aqe_enabled is True
    assert cfg.spark.shuffle_partitions == 8
