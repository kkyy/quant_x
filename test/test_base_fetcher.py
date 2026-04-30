from pathlib import Path

import pytest
from quant_ex.data.fetchers.base import BaseDataFetcher


class ConcreteFetcher(BaseDataFetcher):
    """Minimal concrete impl for testing."""
    def fetch(self, symbols, start_date, end_date):
        return None

    def refresh_cache(self, symbols):
        pass


def test_cannot_instantiate_abc():
    with pytest.raises(TypeError):
        BaseDataFetcher(cache_dir="/tmp", cache_ttl_days=1)


def test_concrete_subclass_instantiates():
    f = ConcreteFetcher(cache_dir="/tmp", cache_ttl_days=1)
    assert f.cache_dir == Path("/tmp")
    assert f.cache_ttl_days == 1


def test_is_cache_fresh_missing_file(tmp_path):
    f = ConcreteFetcher(cache_dir=str(tmp_path), cache_ttl_days=1)
    assert f._is_cache_fresh(tmp_path / "nonexistent.csv") is False


def test_is_cache_fresh_within_ttl(tmp_path):
    f = ConcreteFetcher(cache_dir=str(tmp_path), cache_ttl_days=7)
    cache_file = tmp_path / "data.csv"
    cache_file.write_text("x")
    assert f._is_cache_fresh(cache_file) is True


def test_is_cache_fresh_expired(tmp_path):
    import time
    f = ConcreteFetcher(cache_dir=str(tmp_path), cache_ttl_days=0)
    cache_file = tmp_path / "data.csv"
    cache_file.write_text("x")
    # TTL=0 means always stale
    assert f._is_cache_fresh(cache_file) is False


def test_to_bare_code():
    assert BaseDataFetcher.to_bare_code("SH600000") == "600000"


def test_to_qlib_symbol():
    assert BaseDataFetcher.to_qlib_symbol("600000", "SH") == "SH600000"


def test_infer_exchange():
    assert BaseDataFetcher.infer_exchange("600000") == "SH"
    assert BaseDataFetcher.infer_exchange("000001") == "SZ"
    assert BaseDataFetcher.infer_exchange("300001") == "SZ"
