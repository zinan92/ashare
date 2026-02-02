"""
Tests for Crypto WebSocket real-time data module
Tests cover: TickerSnapshot, MiniKline, CryptoWebSocketManager, message handling
"""
import time
import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

from src.services.crypto_ws import (
    TickerSnapshot,
    MiniKline,
    CryptoWebSocketManager,
    get_crypto_ws_manager,
    DEFAULT_SYMBOLS,
    STALE_THRESHOLD,
)


# ── TickerSnapshot Tests ──

class TestTickerSnapshot:
    def test_basic_creation(self):
        ts = TickerSnapshot(
            symbol="BTCUSDT",
            price=75000.0,
            change_24h=-3000.0,
            change_pct_24h=-3.85,
            high_24h=78000.0,
            low_24h=74000.0,
            volume_24h=50000.0,
            quote_volume_24h=3750000000.0,
            open_price=78000.0,
            trades_count=1000000,
            last_update=time.time(),
        )
        assert ts.price == 75000.0
        assert ts.base_symbol == "BTC"
        assert not ts.is_stale

    def test_stale_detection(self):
        ts = TickerSnapshot(
            symbol="ETHUSDT",
            price=2200.0,
            change_24h=-200.0,
            change_pct_24h=-8.3,
            high_24h=2400.0,
            low_24h=2100.0,
            volume_24h=100000.0,
            quote_volume_24h=220000000.0,
            open_price=2400.0,
            trades_count=500000,
            last_update=time.time() - STALE_THRESHOLD - 10,
        )
        assert ts.is_stale

    def test_fresh_not_stale(self):
        ts = TickerSnapshot(
            symbol="SOLUSDT",
            price=100.0,
            change_24h=-5.0,
            change_pct_24h=-4.76,
            high_24h=110.0,
            low_24h=95.0,
            volume_24h=200000.0,
            quote_volume_24h=20000000.0,
            open_price=105.0,
            trades_count=100000,
            last_update=time.time(),
        )
        assert not ts.is_stale

    def test_to_dict(self):
        ts = TickerSnapshot(
            symbol="BTCUSDT",
            price=75000.0,
            change_24h=-3000.0,
            change_pct_24h=-3.85,
            high_24h=78000.0,
            low_24h=74000.0,
            volume_24h=50000.0,
            quote_volume_24h=3750000000.0,
            open_price=78000.0,
            trades_count=1000000,
            last_update=time.time(),
        )
        d = ts.to_dict()
        assert d["symbol"] == "BTC"
        assert d["pair"] == "BTCUSDT"
        assert d["price"] == 75000.0
        assert d["source"] == "binance_ws"
        assert "last_update" in d
        assert isinstance(d["is_stale"], bool)

    def test_base_symbol_extraction(self):
        cases = [
            ("BTCUSDT", "BTC"),
            ("ETHUSDT", "ETH"),
            ("SOLUSDT", "SOL"),
            ("DOGEUSDT", "DOGE"),
        ]
        for symbol, expected in cases:
            ts = TickerSnapshot(
                symbol=symbol, price=0, change_24h=0, change_pct_24h=0,
                high_24h=0, low_24h=0, volume_24h=0, quote_volume_24h=0,
                open_price=0, trades_count=0, last_update=time.time(),
            )
            assert ts.base_symbol == expected


# ── MiniKline Tests ──

class TestMiniKline:
    def test_basic_creation(self):
        kline = MiniKline(
            symbol="BTCUSDT",
            interval="1m",
            open_time=1700000000000,
            close_time=1700000059999,
            open=75000.0,
            high=75100.0,
            low=74900.0,
            close=75050.0,
            volume=10.5,
            is_closed=True,
        )
        assert kline.close == 75050.0
        assert kline.is_closed

    def test_to_dict(self):
        kline = MiniKline(
            symbol="ETHUSDT",
            interval="5m",
            open_time=1700000000000,
            close_time=1700000299999,
            open=2200.0,
            high=2210.0,
            low=2195.0,
            close=2205.0,
            volume=500.0,
            is_closed=False,
        )
        d = kline.to_dict()
        assert d["symbol"] == "ETH"
        assert d["pair"] == "ETHUSDT"
        assert d["interval"] == "5m"
        assert d["is_closed"] is False


# ── CryptoWebSocketManager Tests ──

class TestCryptoWebSocketManager:
    def test_default_initialization(self):
        mgr = CryptoWebSocketManager()
        assert len(mgr.symbols) == len(DEFAULT_SYMBOLS)
        assert not mgr.is_running
        assert not mgr.is_connected

    def test_custom_symbols(self):
        mgr = CryptoWebSocketManager(symbols=["BTCUSDT", "ETHUSDT"])
        assert len(mgr.symbols) == 2

    def test_custom_kline_intervals(self):
        mgr = CryptoWebSocketManager(kline_intervals=["1m", "5m", "15m"])
        assert mgr.kline_intervals == ["1m", "5m", "15m"]

    def test_stream_names_ticker_only(self):
        mgr = CryptoWebSocketManager(
            symbols=["BTCUSDT", "ETHUSDT"],
            kline_intervals=[],
        )
        streams = mgr._build_stream_names()
        assert "btcusdt@ticker" in streams
        assert "ethusdt@ticker" in streams
        assert len(streams) == 2

    def test_stream_names_with_klines(self):
        mgr = CryptoWebSocketManager(
            symbols=["BTCUSDT"],
            kline_intervals=["1m", "5m"],
        )
        streams = mgr._build_stream_names()
        assert "btcusdt@ticker" in streams
        assert "btcusdt@kline_1m" in streams
        assert "btcusdt@kline_5m" in streams
        assert len(streams) == 3

    def test_get_ticker_not_found(self):
        mgr = CryptoWebSocketManager()
        assert mgr.get_ticker("BTC") is None
        assert mgr.get_ticker("NONEXIST") is None

    def test_get_ticker_with_usdt_suffix(self):
        mgr = CryptoWebSocketManager()
        # Manually inject a ticker
        mgr._tickers["BTCUSDT"] = TickerSnapshot(
            symbol="BTCUSDT", price=75000.0, change_24h=-3000.0,
            change_pct_24h=-3.85, high_24h=78000.0, low_24h=74000.0,
            volume_24h=50000.0, quote_volume_24h=3750000000.0,
            open_price=78000.0, trades_count=1000000, last_update=time.time(),
        )
        # All forms should work (get_ticker does .upper() internally)
        assert mgr.get_ticker("BTC") is not None
        assert mgr.get_ticker("BTCUSDT") is not None
        assert mgr.get_ticker("btc") is not None  # .upper() handles lowercase
        assert mgr.get_ticker("NONEXIST") is None

    def test_get_all_tickers_empty(self):
        mgr = CryptoWebSocketManager()
        assert mgr.get_all_tickers() == {}
        assert mgr.get_all_tickers_list() == []

    def test_get_all_tickers_sorted_by_volume(self):
        mgr = CryptoWebSocketManager()
        now = time.time()
        mgr._tickers["ETHUSDT"] = TickerSnapshot(
            symbol="ETHUSDT", price=2200.0, change_24h=0, change_pct_24h=0,
            high_24h=0, low_24h=0, volume_24h=0, quote_volume_24h=1000000.0,
            open_price=0, trades_count=0, last_update=now,
        )
        mgr._tickers["BTCUSDT"] = TickerSnapshot(
            symbol="BTCUSDT", price=75000.0, change_24h=0, change_pct_24h=0,
            high_24h=0, low_24h=0, volume_24h=0, quote_volume_24h=5000000.0,
            open_price=0, trades_count=0, last_update=now,
        )
        result = mgr.get_all_tickers_list()
        assert len(result) == 2
        assert result[0]["symbol"] == "BTC"  # Higher volume first
        assert result[1]["symbol"] == "ETH"

    def test_get_status_initial(self):
        mgr = CryptoWebSocketManager()
        status = mgr.get_status()
        assert status["running"] is False
        assert status["connected"] is False
        assert status["tickers_cached"] == 0
        assert status["message_count"] == 0

    def test_handle_ticker_message(self):
        mgr = CryptoWebSocketManager()
        ticker_data = {
            "e": "24hrTicker",
            "s": "BTCUSDT",
            "c": "75000.50",
            "p": "-3000.00",
            "P": "-3.85",
            "h": "78000.00",
            "l": "74000.00",
            "v": "50000.00",
            "q": "3750000000.00",
            "o": "78000.50",
            "n": 1000000,
        }
        mgr._handle_ticker(ticker_data)

        assert "BTCUSDT" in mgr._tickers
        t = mgr._tickers["BTCUSDT"]
        assert t.price == 75000.50
        assert t.change_pct_24h == -3.85
        assert t.trades_count == 1000000

    def test_handle_kline_message(self):
        mgr = CryptoWebSocketManager()
        kline_data = {
            "e": "kline",
            "k": {
                "s": "BTCUSDT",
                "i": "1m",
                "t": 1700000000000,
                "T": 1700000059999,
                "o": "75000.00",
                "h": "75100.00",
                "l": "74900.00",
                "c": "75050.00",
                "v": "10.5",
                "x": True,
            },
        }
        mgr._handle_kline(kline_data)

        key = "BTCUSDT_1m"
        assert key in mgr._latest_klines
        k = mgr._latest_klines[key]
        assert k.close == 75050.0
        assert k.is_closed is True

    def test_handle_ticker_callback(self):
        callback_results = []
        mgr = CryptoWebSocketManager(on_ticker=lambda t: callback_results.append(t))

        ticker_data = {
            "e": "24hrTicker",
            "s": "ETHUSDT",
            "c": "2200.00",
            "p": "-200.00",
            "P": "-8.3",
            "h": "2400.00",
            "l": "2100.00",
            "v": "100000.00",
            "q": "220000000.00",
            "o": "2400.00",
            "n": 500000,
        }
        mgr._handle_ticker(ticker_data)

        assert len(callback_results) == 1
        assert callback_results[0].symbol == "ETHUSDT"

    def test_handle_kline_callback_only_on_close(self):
        callback_results = []
        mgr = CryptoWebSocketManager(on_kline=lambda k: callback_results.append(k))

        # Not closed — no callback
        kline_open = {
            "e": "kline",
            "k": {
                "s": "BTCUSDT", "i": "1m",
                "t": 0, "T": 0,
                "o": "75000", "h": "75100", "l": "74900", "c": "75050",
                "v": "10", "x": False,
            },
        }
        mgr._handle_kline(kline_open)
        assert len(callback_results) == 0

        # Closed — callback fires
        kline_closed = {
            "e": "kline",
            "k": {
                "s": "BTCUSDT", "i": "1m",
                "t": 0, "T": 0,
                "o": "75000", "h": "75100", "l": "74900", "c": "75050",
                "v": "10", "x": True,
            },
        }
        mgr._handle_kline(kline_closed)
        assert len(callback_results) == 1

    def test_handle_invalid_ticker_data(self):
        """Should not crash on bad data"""
        mgr = CryptoWebSocketManager()
        mgr._handle_ticker({"e": "24hrTicker"})  # missing fields
        mgr._handle_ticker({"e": "24hrTicker", "s": ""})  # empty symbol
        mgr._handle_ticker({"e": "24hrTicker", "s": "BTCUSDT", "c": "not_a_number"})
        # Should not crash — just log warnings

    def test_handle_invalid_kline_data(self):
        """Should not crash on bad data"""
        mgr = CryptoWebSocketManager()
        mgr._handle_kline({"e": "kline"})  # missing k
        mgr._handle_kline({"e": "kline", "k": {}})  # empty k
        # Should not crash

    @pytest.mark.asyncio
    async def test_handle_message_routing(self):
        mgr = CryptoWebSocketManager()

        # Ticker message
        await mgr._handle_message({
            "stream": "btcusdt@ticker",
            "data": {
                "e": "24hrTicker",
                "s": "BTCUSDT",
                "c": "75000", "p": "0", "P": "0",
                "h": "0", "l": "0", "v": "0", "q": "0",
                "o": "0", "n": 0,
            },
        })
        assert "BTCUSDT" in mgr._tickers

        # Kline message
        await mgr._handle_message({
            "stream": "btcusdt@kline_1m",
            "data": {
                "e": "kline",
                "k": {
                    "s": "BTCUSDT", "i": "1m",
                    "t": 0, "T": 0,
                    "o": "75000", "h": "75100", "l": "74900", "c": "75050",
                    "v": "10", "x": False,
                },
            },
        })
        assert "BTCUSDT_1m" in mgr._latest_klines

    @pytest.mark.asyncio
    async def test_handle_message_empty_data(self):
        """Empty data should be ignored"""
        mgr = CryptoWebSocketManager()
        await mgr._handle_message({"stream": "test", "data": {}})
        await mgr._handle_message({"stream": "test"})
        assert len(mgr._tickers) == 0

    def test_get_kline(self):
        mgr = CryptoWebSocketManager()
        assert mgr.get_kline("BTC", "1m") is None

        mgr._latest_klines["BTCUSDT_1m"] = MiniKline(
            symbol="BTCUSDT", interval="1m",
            open_time=0, close_time=0,
            open=75000, high=75100, low=74900, close=75050,
            volume=10, is_closed=False,
        )
        assert mgr.get_kline("BTC", "1m") is not None
        assert mgr.get_kline("BTCUSDT", "1m") is not None
        assert mgr.get_kline("ETH", "1m") is None


# ── Singleton Tests ──

class TestSingleton:
    def test_get_manager_returns_same_instance(self):
        # Reset
        import src.services.crypto_ws as mod
        mod._ws_manager = None

        m1 = get_crypto_ws_manager()
        m2 = get_crypto_ws_manager()
        assert m1 is m2

        # Cleanup
        mod._ws_manager = None


# ── API Route Tests (using TestClient) ──

class TestCryptoWSRoutes:
    """Test the FastAPI routes"""

    @pytest.fixture
    def client(self):
        from fastapi.testclient import TestClient
        from src.api.routes_crypto_ws import router
        from fastapi import FastAPI

        app = FastAPI()
        app.include_router(router, prefix="/crypto")

        return TestClient(app)

    def test_ws_status(self, client):
        resp = client.get("/crypto/ws/status")
        assert resp.status_code == 200
        data = resp.json()
        assert "running" in data
        assert "connected" in data
        assert "tickers_cached" in data

    def test_realtime_not_connected(self, client):
        """Should return 503 when WS not connected"""
        resp = client.get("/crypto/realtime")
        assert resp.status_code == 503

    def test_realtime_symbol_not_found(self, client):
        resp = client.get("/crypto/realtime/NONEXIST")
        assert resp.status_code == 404
