from datetime import datetime
from pathlib import Path

import pytest
from quant_ex.data.fetchers.base import BaseDataFetcher


class ConcreteFetcher(BaseDataFetcher):
    """Minimal concrete impl for testing."""
    def fetch(self, _symbols, _start_date, _end_date):
        return None

    def refresh_cache(self, _symbols):
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
    from datetime import timedelta
    import os
    f = ConcreteFetcher(cache_dir=str(tmp_path), cache_ttl_days=7)
    cache_file = tmp_path / "data.csv"
    cache_file.write_text("x")
    # Set mtime to 3 days ago to actually test TTL boundary
    old_mtime = (datetime.now() - timedelta(days=3)).timestamp()
    os.utime(cache_file, (old_mtime, old_mtime))
    assert f._is_cache_fresh(cache_file) is True


def test_is_cache_fresh_expired(tmp_path):
    from datetime import timedelta
    import os
    f = ConcreteFetcher(cache_dir=str(tmp_path), cache_ttl_days=1)
    cache_file = tmp_path / "data.csv"
    cache_file.write_text("x")
    # Set mtime to 2 days ago (past TTL=1)
    old_mtime = (datetime.now() - timedelta(days=2)).timestamp()
    os.utime(cache_file, (old_mtime, old_mtime))
    assert f._is_cache_fresh(cache_file) is False


def test_to_bare_code():
    assert BaseDataFetcher.to_bare_code("SH600000") == "600000"


def test_to_qlib_symbol():
    assert BaseDataFetcher.to_qlib_symbol("600000", "SH") == "SH600000"


def test_to_exchange():
    assert BaseDataFetcher.to_exchange("SH600000") == "SH"
    assert BaseDataFetcher.to_exchange("SZ000001") == "SZ"
    assert BaseDataFetcher.to_exchange("BJ430047") == "BJ"


def test_infer_exchange():
    assert BaseDataFetcher.infer_exchange("600000") == "SH"
    assert BaseDataFetcher.infer_exchange("900001") == "SH"
    assert BaseDataFetcher.infer_exchange("430047") == "BJ"
    assert BaseDataFetcher.infer_exchange("000001") == "SZ"
    assert BaseDataFetcher.infer_exchange("300001") == "SZ"
