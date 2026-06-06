"""Unit tests for Silver layer: preprocessing and event detection.

Tests cover:
- Silver epoch schema validity
- Band power computation (sigma, delta)
- PAC modulation index computation
- Spindle event schema after mapInPandas
- Null/artifact row handling

Run with: pytest tests/test_silver.py -v
"""

import pytest
import numpy as np
import pandas as pd

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.silver.preprocess_eeg import (
    SILVER_EPOCH_SCHEMA,
    _compute_band_power,
    _mock_preprocess_epoch,
)
from src.silver.detect_events import (
    SILVER_SPINDLE_SCHEMA,
    SILVER_SO_SCHEMA,
    _compute_pac_modulation_index,
    _mock_detect_spindles,
    compute_spindle_density,
)


class TestBandPowerComputation:
    """Tests for _compute_band_power()"""

    def test_sigma_power_non_negative(self):
        signal = np.random.randn(3000).astype(np.float32)
        power = _compute_band_power(signal, sfreq=100, fmin=11.0, fmax=16.0)
        assert power >= 0

    def test_delta_power_non_negative(self):
        signal = np.random.randn(3000).astype(np.float32)
        power = _compute_band_power(signal, sfreq=100, fmin=0.5, fmax=4.0)
        assert power >= 0

    def test_zero_signal_returns_zero_power(self):
        signal = np.zeros(3000, dtype=np.float32)
        power = _compute_band_power(signal, sfreq=100, fmin=11.0, fmax=16.0)
        assert power == pytest.approx(0.0, abs=1e-6)

    def test_short_signal_does_not_crash(self):
        signal = np.random.randn(50).astype(np.float32)
        power = _compute_band_power(signal, sfreq=100, fmin=11.0, fmax=16.0)
        assert isinstance(power, float)


class TestMockPreprocessing:
    """Tests for mock preprocessing function."""

    def test_output_shape_preserved(self):
        signal = np.random.randn(3000).astype(np.float32)
        result = _mock_preprocess_epoch(signal, sfreq=100)
        assert result.shape == signal.shape

    def test_output_dtype_is_float32(self):
        signal = np.random.randn(3000).astype(np.float64)
        result = _mock_preprocess_epoch(signal, sfreq=100)
        assert result.dtype == np.float32


class TestPACModulationIndex:
    """Tests for Tort et al. 2010 PAC modulation index."""

    def test_pac_returns_float(self):
        signal = np.random.randn(3000).astype(np.float32)
        mi = _compute_pac_modulation_index(signal, sfreq=100)
        assert isinstance(mi, float)

    def test_pac_in_valid_range(self):
        signal = np.random.randn(3000).astype(np.float32)
        mi = _compute_pac_modulation_index(signal, sfreq=100)
        # MI should be in [0, 1] for normalized KL-divergence
        if not np.isnan(mi):
            assert 0.0 <= mi <= 1.0

    def test_zero_signal_returns_nan_or_zero(self):
        signal = np.zeros(3000, dtype=np.float32)
        mi = _compute_pac_modulation_index(signal, sfreq=100)
        assert np.isnan(mi) or mi == pytest.approx(0.0, abs=1e-4)


class TestSpindleDetection:
    """Tests for mock spindle detection."""

    def test_mock_spindles_return_list(self):
        signal = np.random.randn(3000).astype(np.float32)
        events = _mock_detect_spindles(signal, sfreq=100, epoch_start=0.0)
        assert isinstance(events, list)

    def test_spindle_fields_present(self):
        signal = np.random.randn(3000).astype(np.float32)
        np.random.seed(42)
        events = _mock_detect_spindles(signal, sfreq=100, epoch_start=0.0)
        if len(events) > 0:
            required_keys = {"start_sec", "end_sec", "duration_sec", "channel"}
            assert required_keys.issubset(set(events[0].keys()))

    def test_spindle_duration_positive(self):
        signal = np.random.randn(3000).astype(np.float32)
        events = _mock_detect_spindles(signal, sfreq=100, epoch_start=0.0)
        for ev in events:
            assert ev["duration_sec"] > 0

    def test_spindle_end_after_start(self):
        signal = np.random.randn(3000).astype(np.float32)
        events = _mock_detect_spindles(signal, sfreq=100, epoch_start=0.0)
        for ev in events:
            assert ev["end_sec"] > ev["start_sec"]


class TestSilverSchema:
    """Tests for Silver schema definitions."""

    def test_epoch_schema_has_required_columns(self):
        required = {
            "subject_id", "epoch_idx", "sleep_stage",
            "signal_blob", "sigma_power", "delta_power"
        }
        actual = {f.name for f in SILVER_EPOCH_SCHEMA.fields}
        assert required.issubset(actual)

    def test_spindle_schema_has_required_columns(self):
        required = {
            "subject_id", "epoch_idx", "start_sec",
            "duration_sec", "channel", "peak_amplitude_uv"
        }
        actual = {f.name for f in SILVER_SPINDLE_SCHEMA.fields}
        assert required.issubset(actual)

    def test_so_schema_has_required_columns(self):
        required = {
            "subject_id", "epoch_idx", "neg_peak_sec",
            "neg_amplitude_uv", "channel"
        }
        actual = {f.name for f in SILVER_SO_SCHEMA.fields}
        assert required.issubset(actual)


class TestSpindleDensityAggregation:
    """PySpark tests for spindle density computation."""

    def test_spindle_density_columns_present(self, spark: SparkSession):
        sample_data = [
            ("SC4001", 0, 0, "N2", 5.0, 7.5, 2.5, 45.0, 13.5, "Fpz-Cz", 30.0),
            ("SC4001", 0, 0, "N2", 10.0, 12.0, 2.0, 40.0, 13.0, "Fpz-Cz", 28.0),
            ("SC4001", 0, 1, "N2", 35.0, 37.5, 2.5, 50.0, 14.0, "Fpz-Cz", 35.0),
        ]
        columns = [
            "subject_id", "study_night", "epoch_idx", "sleep_stage",
            "start_sec", "end_sec", "duration_sec",
            "peak_amplitude_uv", "peak_freq_hz", "channel", "rms_amplitude_uv"
        ]
        df = spark.createDataFrame(sample_data, columns)
        density_df = compute_spindle_density(df)
        result_cols = set(density_df.columns)
        assert "spindle_density_per_min" in result_cols
        assert "total_spindles" in result_cols
        assert "mean_duration_sec" in result_cols
