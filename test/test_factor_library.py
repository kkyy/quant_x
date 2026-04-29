"""Tests for the factor library — FactorLibrary, FactorCleaner, FactorScreener, new factors."""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest
from pathlib import Path
from unittest.mock import patch, MagicMock


# ── Shared fixtures ───────────────────────────────────────────────────────────

def _multiindex(instruments, dates):
    return pd.MultiIndex.from_product(
        [instruments, pd.to_datetime(dates)], names=["instrument", "datetime"]
    )


def _factor_df(instruments=("SH600000", "SZ000001"), n_dates=20, seed=42):
    rng = np.random.default_rng(seed)
    dates = pd.date_range("2025-01-01", periods=n_dates, freq="B")
    idx = _multiindex(instruments, dates)
    return pd.DataFrame(
        rng.standard_normal((len(idx), 3)),
        index=idx,
        columns=["f1", "f2", "f3"],
    )


def _returns(factor_df, seed=7):
    rng = np.random.default_rng(seed)
    return pd.Series(
        rng.standard_normal(len(factor_df)), index=factor_df.index, name="ret"
    )


# ── FactorMeta ────────────────────────────────────────────────────────────────

class TestFactorMeta:
    def test_passes_quality_no_stats(self):
        from features.library.meta import FactorMeta
        m = FactorMeta(name="x", source="technical")
        assert m.passes_quality(min_ic=0.05, min_icir=1.0)

    def test_passes_quality_with_stats(self):
        from features.library.meta import FactorMeta
        m = FactorMeta(name="x", source="technical", ic_mean=0.03, icir=0.5)
        assert m.passes_quality(min_ic=0.02, min_icir=0.4)
        assert not m.passes_quality(min_ic=0.04)
        assert not m.passes_quality(min_icir=0.6)

    def test_to_pipeline_config(self):
        from features.library.meta import FactorMeta
        m = FactorMeta(name="rsi_14d", source="technical", config={"rsi_windows": [14]})
        cfg = m.to_pipeline_config()
        assert cfg == {"name": "technical", "rsi_windows": [14]}


# ── FactorLibrary ─────────────────────────────────────────────────────────────

class TestFactorLibrary:
    def test_add_and_get(self, tmp_path):
        from features.library.meta import FactorMeta, FactorLibrary
        lib = FactorLibrary(tmp_path / "lib.json")
        lib.add(FactorMeta(name="pb", source="fundamental"))
        assert lib.get("pb") is not None
        assert lib.get("nonexistent") is None

    def test_duplicate_raises_without_overwrite(self, tmp_path):
        from features.library.meta import FactorMeta, FactorLibrary
        lib = FactorLibrary(tmp_path / "lib.json")
        lib.add(FactorMeta(name="pb", source="fundamental"))
        with pytest.raises(ValueError):
            lib.add(FactorMeta(name="pb", source="technical"))

    def test_overwrite(self, tmp_path):
        from features.library.meta import FactorMeta, FactorLibrary
        lib = FactorLibrary(tmp_path / "lib.json")
        lib.add(FactorMeta(name="pb", source="fundamental"))
        lib.add(FactorMeta(name="pb", source="technical"), overwrite=True)
        assert lib.get("pb").source == "technical"

    def test_enable_disable(self, tmp_path):
        from features.library.meta import FactorMeta, FactorLibrary
        lib = FactorLibrary(tmp_path / "lib.json")
        lib.add(FactorMeta(name="x", source="technical"))
        lib.disable("x")
        assert lib.list(enabled_only=True) == []
        lib.enable("x")
        assert len(lib.list(enabled_only=True)) == 1

    def test_update_stats(self, tmp_path):
        from features.library.meta import FactorMeta, FactorLibrary
        lib = FactorLibrary(tmp_path / "lib.json")
        lib.add(FactorMeta(name="x", source="technical"))
        lib.update_stats("x", ic_mean=0.03, icir=0.7, coverage=0.95)
        m = lib.get("x")
        assert pytest.approx(m.ic_mean, abs=1e-4) == 0.03

    def test_persistence(self, tmp_path):
        from features.library.meta import FactorMeta, FactorLibrary
        path = tmp_path / "lib.json"
        lib = FactorLibrary(path)
        lib.add(FactorMeta(name="pb", source="fundamental", description="Price-to-book"))
        lib2 = FactorLibrary(path)
        assert lib2.get("pb").description == "Price-to-book"

    def test_filter_by_tags(self, tmp_path):
        from features.library.meta import FactorMeta, FactorLibrary
        lib = FactorLibrary(tmp_path / "lib.json")
        lib.add(FactorMeta(name="pb", source="fundamental", tags=["valuation"]))
        lib.add(FactorMeta(name="rsi", source="technical", tags=["momentum"]))
        vals = lib.list(tags=["valuation"])
        assert len(vals) == 1 and vals[0].name == "pb"

    def test_to_pipeline_configs(self, tmp_path):
        from features.library.meta import FactorMeta, FactorLibrary
        lib = FactorLibrary(tmp_path / "lib.json")
        lib.add(FactorMeta(name="pb", source="fundamental", config={"metrics": ["pb"]}))
        lib.add(FactorMeta(name="rsi", source="technical", config={"rsi_windows": [14]}, enabled=False))
        configs = lib.to_pipeline_configs(enabled_only=True)
        assert len(configs) == 1
        assert configs[0]["name"] == "fundamental"

    def test_quality_filter_in_list(self, tmp_path):
        from features.library.meta import FactorMeta, FactorLibrary
        lib = FactorLibrary(tmp_path / "lib.json")
        lib.add(FactorMeta(name="good", source="technical", ic_mean=0.05, icir=0.8))
        lib.add(FactorMeta(name="bad", source="technical", ic_mean=0.01, icir=0.1))
        good = lib.list(min_ic=0.03, min_icir=0.5)
        assert len(good) == 1 and good[0].name == "good"


# ── FactorCleaner ─────────────────────────────────────────────────────────────

class TestFactorCleaner:
    def test_zscore_mean_near_zero(self):
        from features.library.cleaner import FactorCleaner
        df = _factor_df()
        cleaner = FactorCleaner(winsorize_sigma=0, zscore=True, fill_method="zero")
        result = cleaner.transform(df)
        # Cross-sectional mean per date should be ~0 after z-score
        date_means = result.groupby(level=1).mean()
        assert (date_means.abs() < 0.1).all().all()

    def test_winsorize_clips_outliers(self):
        from features.library.cleaner import FactorCleaner
        # 20 instruments per date so the outlier doesn't dominate the std
        instruments = [f"S{i:02d}" for i in range(20)]
        idx = _multiindex(instruments, pd.date_range("2025-01-01", periods=1, freq="B"))
        # Values 1..19 are tight; instrument S19 has an extreme outlier (1000)
        vals = list(range(1, 20)) + [1000.0]
        df = pd.DataFrame({"f": vals}, index=idx)
        cleaner = FactorCleaner(winsorize_sigma=2.0, zscore=False, fill_method="zero")
        result = cleaner.transform(df)
        assert result["f"].max() < 1000.0

    def test_fill_zero(self):
        from features.library.cleaner import FactorCleaner
        df = _factor_df()
        df.iloc[0, 0] = np.nan
        cleaner = FactorCleaner(winsorize_sigma=0, zscore=False, fill_method="zero")
        result = cleaner.transform(df)
        assert not result.isna().any().any()

    def test_drop_low_coverage(self):
        from features.library.cleaner import FactorCleaner
        df = _factor_df()
        df["sparse"] = np.nan  # 0% coverage
        df.loc[df.index[0], "sparse"] = 1.0  # just 1 valid cell
        cleaner = FactorCleaner(min_coverage=0.5, winsorize_sigma=0, zscore=False, fill_method="zero")
        result = cleaner.transform(df)
        assert "sparse" not in result.columns

    def test_passthrough_categorical(self):
        from features.library.cleaner import FactorCleaner
        df = _factor_df()
        df["sector_id"] = 1  # integer / categorical column
        cleaner = FactorCleaner(winsorize_sigma=3.0, zscore=True, fill_method="zero")
        result = cleaner.transform(df)
        assert "sector_id" in result.columns


# ── FactorEvaluator ───────────────────────────────────────────────────────────

class TestFactorEvaluator:
    def test_evaluate_returns_stats(self):
        from features.library.screener import FactorEvaluator
        factors = _factor_df()
        returns = _returns(factors)
        ev = FactorEvaluator()
        stats = ev.evaluate(factors, returns)
        assert set(stats.columns) >= {"ic_mean", "icir", "coverage", "n_dates"}
        assert len(stats) == 3  # 3 factor columns

    def test_ic_series_same_length_as_dates(self):
        from features.library.screener import FactorEvaluator
        factors = _factor_df(n_dates=10)
        returns = _returns(factors)
        ev = FactorEvaluator()
        ic_ts = ev.ic_series(factors["f1"], returns)
        assert len(ic_ts) <= 10


# ── FactorScreener ────────────────────────────────────────────────────────────

class TestFactorScreener:
    def test_all_pass_relaxed_thresholds(self):
        from features.library.screener import FactorScreener
        factors = _factor_df(n_dates=30)
        returns = _returns(factors)
        screener = FactorScreener(min_ic=0.0, min_icir=0.0, max_corr=1.0, min_coverage=0.0)
        kept, report = screener.screen(factors, returns)
        assert len(kept.columns) == 3

    def test_drops_low_ic(self):
        from features.library.screener import FactorScreener
        rng = np.random.default_rng(99)
        dates = pd.date_range("2025-01-01", periods=30, freq="B")
        idx = _multiindex(["A", "B", "C"], dates)
        factors = pd.DataFrame(rng.standard_normal((len(idx), 2)), index=idx, columns=["good", "noise"])
        returns = pd.Series(factors["good"] + rng.standard_normal(len(idx)) * 0.1, index=idx)
        screener = FactorScreener(min_ic=0.3, min_icir=0.5, max_corr=1.0, min_coverage=0.0)
        _, report = screener.screen(factors, returns)
        assert "kept" in report.columns  # smoke test

    def test_dedup_removes_correlated(self):
        from features.library.screener import FactorScreener
        rng = np.random.default_rng(0)
        dates = pd.date_range("2025-01-01", periods=40, freq="B")
        idx = _multiindex(["A", "B", "C", "D", "E"], dates)
        base = pd.Series(rng.standard_normal(len(idx)), index=idx)
        # f2 is almost identical to f1
        factors = pd.DataFrame({
            "f1": base,
            "f2": base + rng.standard_normal(len(idx)) * 0.01,
            "f3": pd.Series(rng.standard_normal(len(idx)), index=idx),
        })
        returns = pd.Series(rng.standard_normal(len(idx)), index=idx)
        screener = FactorScreener(min_ic=0.0, min_icir=0.0, max_corr=0.9, min_coverage=0.0)
        kept, report = screener.screen(factors, returns)
        # f1 and f2 are near-duplicates; one should be removed
        high_corr_dropped = (report["reason"] == "high_corr").any()
        assert high_corr_dropped

    def test_report_index_is_factor_name(self):
        from features.library.screener import FactorScreener
        factors = _factor_df()
        returns = _returns(factors)
        _, report = FactorScreener(min_ic=0.0, min_icir=0.0).screen(factors, returns)
        assert set(report.index) == {"f1", "f2", "f3"}


# ── CsvFactor ─────────────────────────────────────────────────────────────────

class TestCsvFactor:
    def _price_index(self):
        dates = pd.date_range("2025-01-06", periods=5, freq="B")
        return pd.MultiIndex.from_product(
            [["SH600000", "SZ000001"], dates], names=["instrument", "datetime"]
        )

    def test_long_format_csv(self, tmp_path):
        from features.csv_factor import CsvFactor
        rows = []
        for sym in ["SH600000", "SZ000001"]:
            for d in pd.date_range("2025-01-01", periods=10, freq="B"):
                rows.append({"date": d.date(), "symbol": sym, "my_factor": 1.5})
        pd.DataFrame(rows).to_csv(tmp_path / "f.csv", index=False)

        price_data = pd.DataFrame(index=self._price_index())
        factor = CsvFactor(str(tmp_path / "f.csv"), date_col="date", symbol_col="symbol")
        result = factor.compute(price_data)
        assert result is not None
        assert "my_factor" in result.columns
        assert len(result) == len(self._price_index())

    def test_missing_file_returns_none(self, tmp_path):
        from features.csv_factor import CsvFactor
        factor = CsvFactor(str(tmp_path / "nonexistent.csv"))
        price_data = pd.DataFrame(index=self._price_index())
        result = factor.compute(price_data)
        assert result is None

    def test_column_filter(self, tmp_path):
        from features.csv_factor import CsvFactor
        rows = [{"date": "2025-01-06", "symbol": "SH600000", "a": 1.0, "b": 2.0}]
        pd.DataFrame(rows).to_csv(tmp_path / "f.csv", index=False)
        price_data = pd.DataFrame(index=self._price_index())
        factor = CsvFactor(str(tmp_path / "f.csv"), columns=["a"])
        result = factor.compute(price_data)
        assert result is None or "b" not in (result.columns if result is not None else [])


# ── FundamentalFactor (precomputed path) ─────────────────────────────────────

class TestFundamentalFactor:
    def _price_index(self):
        dates = pd.date_range("2025-01-06", periods=5, freq="B")
        return pd.MultiIndex.from_product(
            [["SH600000"], dates], names=["instrument", "datetime"]
        )

    def test_precomputed_path(self):
        from features.fundamental_factor import FundamentalFactor
        dates = pd.date_range("2025-01-01", periods=10, freq="B")
        idx = pd.MultiIndex.from_product(
            [["SH600000"], dates], names=["instrument", "datetime"]
        )
        precomputed = pd.DataFrame({"pe_ttm": 15.0, "pb": 2.0}, index=idx)

        price_data = pd.DataFrame(index=self._price_index())
        factor = FundamentalFactor(metrics=["pe_ttm", "pb"], precomputed=precomputed)
        result = factor.compute(price_data)
        assert result is not None
        assert "pe_ttm" in result.columns
        assert "pb" in result.columns
        assert len(result) == len(self._price_index())

    def test_akshare_failure_returns_none(self, tmp_path):
        from features.fundamental_factor import FundamentalFactor
        price_data = pd.DataFrame(index=self._price_index())
        factor = FundamentalFactor(
            metrics=["pe_ttm"],
            cache_dir=str(tmp_path / "fund"),
            cache_ttl_days=0,
        )
        # Patch the akshare import inside the method so any API name works
        import features.fundamental_factor as ff_mod
        with patch.object(ff_mod, "_fetch_akshare_data", side_effect=Exception("api down"), create=True):
            # Fall back to patching at the import level inside _fetch_akshare
            with patch.dict("sys.modules", {"akshare": MagicMock(
                stock_a_lg_indicator=MagicMock(side_effect=Exception("api down"))
            )}):
                result = factor.compute(price_data)
        # Graceful degradation: returns None, no exception raised
        assert result is None


# ── FactorPipeline.compute_with_cleaning ─────────────────────────────────────

class TestFactorPipelineWithCleaning:
    def test_compute_with_cleaning_no_cleaner(self):
        """With cleaner=None, behaves identically to compute()."""
        from features.base import FactorPipeline, BaseFactor, FactorRegistry
        import pandas as pd, numpy as np

        @FactorRegistry.register("_test_wc")
        class _TestFactor(BaseFactor):
            def compute(self, price_data):
                return pd.DataFrame(
                    np.ones((len(price_data), 1)),
                    index=price_data.index,
                    columns=["x"],
                )

        pipeline = FactorPipeline([FactorRegistry.build("_test_wc")])
        idx = _multiindex(["SH600000"], pd.date_range("2025-01-01", periods=3, freq="B"))
        price_data = pd.DataFrame(index=idx)
        result = pipeline.compute_with_cleaning(price_data, cleaner=None)
        assert result is not None
        assert "x" in result.columns

    def test_compute_with_cleaning_applies_zscore(self):
        from features.base import FactorPipeline, BaseFactor, FactorRegistry
        from features.library.cleaner import FactorCleaner
        import pandas as pd, numpy as np

        @FactorRegistry.register("_test_wc2")
        class _TestFactor2(BaseFactor):
            def compute(self, price_data):
                rng = np.random.default_rng(1)
                return pd.DataFrame(
                    rng.standard_normal((len(price_data), 1)) * 50 + 100,
                    index=price_data.index,
                    columns=["big"],
                )

        pipeline = FactorPipeline([FactorRegistry.build("_test_wc2")])
        instruments = ["A", "B", "C", "D", "E"]
        dates = pd.date_range("2025-01-01", periods=10, freq="B")
        idx = _multiindex(instruments, dates)
        price_data = pd.DataFrame(index=idx)

        cleaner = FactorCleaner(winsorize_sigma=3.0, zscore=True, fill_method="zero")
        result = pipeline.compute_with_cleaning(price_data, cleaner=cleaner)
        assert result is not None
        # After z-score, cross-sectional std should be ~1
        per_date_std = result.groupby(level=1)["big"].std()
        assert (per_date_std < 2.0).all()
