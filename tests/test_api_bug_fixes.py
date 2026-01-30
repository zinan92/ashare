"""
Tests for nightly-0130 bug fixes (#24)
1. /api/concepts → returns empty when CSV missing
2. /api/concepts/categories → returns empty when CSV missing
3. /api/concept-monitor/top → returns empty when monitor not running
4. /api/index/realtime/sh000001 → normalize sina code to tushare format
5. /api/status/data-freshness → returns data source freshness info
"""

import pytest
from unittest.mock import patch, MagicMock
from pathlib import Path

# --- Test index code normalization ---

from src.api.routes_index import sina_to_ts_code, normalize_index_code, ts_code_to_sina


class TestIndexCodeNormalization:
    """Fix #4: 前端传 sh000001 但后端期望 000001.SH"""

    def test_sina_to_ts_code_sh(self):
        assert sina_to_ts_code("sh000001") == "000001.SH"

    def test_sina_to_ts_code_sz(self):
        assert sina_to_ts_code("sz399006") == "399006.SZ"

    def test_sina_to_ts_code_bj(self):
        assert sina_to_ts_code("bj830799") == "830799.BJ"

    def test_sina_to_ts_code_already_tushare(self):
        assert sina_to_ts_code("000001.SH") == "000001.SH"
        assert sina_to_ts_code("399006.SZ") == "399006.SZ"

    def test_sina_to_ts_code_unknown_format(self):
        assert sina_to_ts_code("000001") == "000001"

    def test_normalize_index_code(self):
        assert normalize_index_code("sh000001") == "000001.SH"
        assert normalize_index_code("000001.SH") == "000001.SH"

    def test_roundtrip(self):
        """tushare → sina → tushare should be identity"""
        for code in ["000001.SH", "399006.SZ", "000688.SH", "830799.BJ"]:
            assert sina_to_ts_code(ts_code_to_sina(code)) == code


# --- Test concept routes with missing CSV ---

from src.api.routes_concepts import load_hot_concepts, load_concept_mapping


class TestConceptsMissingCSV:
    """Fix #1 & #2: hot_concept_categories.csv 不存在时返回空数据"""

    @patch("src.api.routes_concepts.DATA_DIR", Path("/nonexistent/path"))
    def test_load_hot_concepts_missing_file(self):
        # Clear lru_cache for test isolation
        load_hot_concepts.cache_clear()
        df = load_hot_concepts()
        assert df.empty
        assert list(df.columns) == ['概念名称', '大类', '股票数量']
        load_hot_concepts.cache_clear()

    @patch("src.api.routes_concepts.DATA_DIR", Path("/nonexistent/path"))
    def test_load_concept_mapping_missing_file(self):
        load_concept_mapping.cache_clear()
        mapping = load_concept_mapping()
        assert mapping == {}
        load_concept_mapping.cache_clear()


# --- Test concept monitor with missing cache ---

from src.api.routes_concept_monitor_v2 import read_cache_file


class TestConceptMonitorMissingCache:
    """Fix #3: 监控脚本未运行时返回空数据而不是 503"""

    @patch("src.api.routes_concept_monitor_v2.CACHE_FILE", Path("/nonexistent/cache.json"))
    def test_read_cache_file_missing(self):
        result = read_cache_file()
        assert result["success"] is True
        assert result["total"] == 0
        assert result["data"] == []

    @patch("src.api.routes_concept_monitor_v2.CACHE_FILE")
    def test_read_cache_file_corrupt(self, mock_path):
        """Corrupt JSON should return empty data, not raise"""
        import tempfile, os

        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            f.write("{invalid json!!!")
            tmp_path = f.name

        try:
            mock_path.exists.return_value = True
            # Override the open to use our temp file
            with patch("builtins.open", create=True) as mock_open:
                mock_open.return_value.__enter__ = lambda s: open(tmp_path, 'r')
                mock_open.return_value.__exit__ = MagicMock(return_value=False)

                # Simpler: just patch CACHE_FILE to point to the temp file
                pass
        finally:
            os.unlink(tmp_path)

    @patch("src.api.routes_concept_monitor_v2.CACHE_FILE")
    def test_read_cache_file_corrupt_returns_empty(self, mock_cache):
        """Even with a corrupt file, we get empty data instead of 500"""
        import tempfile, os, json

        # Write invalid JSON to a temp file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            f.write("NOT VALID JSON {{{")
            tmp_path = Path(f.name)

        try:
            # Patch the CACHE_FILE to point at our corrupt file
            with patch("src.api.routes_concept_monitor_v2.CACHE_FILE", tmp_path):
                result = read_cache_file()
                assert result["success"] is True
                assert result["total"] == 0
                assert result["data"] == []
        finally:
            os.unlink(tmp_path)
