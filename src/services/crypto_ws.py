"""
Crypto WebSocket 实时数据流
连接 Binance WebSocket API，维护实时价格缓存
支持自动重连、心跳、多流订阅
"""
import asyncio
import json
import logging
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any, Set, Callable
from dataclasses import dataclass, field

import websockets
from websockets.exceptions import ConnectionClosed, InvalidStatusCode

logger = logging.getLogger(__name__)

# Binance WebSocket endpoints
BINANCE_WS_BASE = "wss://stream.binance.com:9443"
BINANCE_WS_COMBINED = f"{BINANCE_WS_BASE}/stream"

# Default symbols to track
DEFAULT_SYMBOLS = [
    "BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "DOGEUSDT",
    "ADAUSDT", "SUIUSDT", "LINKUSDT", "DOTUSDT", "AVAXUSDT",
    "MATICUSDT", "UNIUSDT", "LTCUSDT", "BCHUSDT", "XLMUSDT",
]

# Reconnect settings
INITIAL_RECONNECT_DELAY = 1.0
MAX_RECONNECT_DELAY = 60.0
RECONNECT_BACKOFF = 2.0
PING_INTERVAL = 30  # seconds
STALE_THRESHOLD = 120  # seconds - mark data stale if no update in 2min


@dataclass
class TickerSnapshot:
    """Real-time ticker data for a single symbol"""
    symbol: str
    price: float
    change_24h: float  # absolute
    change_pct_24h: float  # percentage
    high_24h: float
    low_24h: float
    volume_24h: float  # base asset volume
    quote_volume_24h: float  # quote asset (USDT) volume
    open_price: float
    trades_count: int
    last_update: float  # unix timestamp
    source: str = "binance_ws"

    @property
    def is_stale(self) -> bool:
        return (time.time() - self.last_update) > STALE_THRESHOLD

    @property
    def base_symbol(self) -> str:
        """Extract base symbol (BTC from BTCUSDT)"""
        return self.symbol.replace("USDT", "")

    def to_dict(self) -> Dict[str, Any]:
        return {
            "symbol": self.base_symbol,
            "pair": self.symbol,
            "price": self.price,
            "change_24h": self.change_24h,
            "change_pct_24h": self.change_pct_24h,
            "high_24h": self.high_24h,
            "low_24h": self.low_24h,
            "volume_24h": self.volume_24h,
            "quote_volume_24h": self.quote_volume_24h,
            "open_price": self.open_price,
            "trades_count": self.trades_count,
            "last_update": datetime.fromtimestamp(
                self.last_update, tz=timezone.utc
            ).isoformat(),
            "is_stale": self.is_stale,
            "source": self.source,
        }


@dataclass
class MiniKline:
    """Mini kline (candlestick) from WebSocket"""
    symbol: str
    interval: str
    open_time: int
    close_time: int
    open: float
    high: float
    low: float
    close: float
    volume: float
    is_closed: bool  # Whether this kline is final
    last_update: float = field(default_factory=time.time)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "symbol": self.symbol.replace("USDT", ""),
            "pair": self.symbol,
            "interval": self.interval,
            "open_time": self.open_time,
            "close_time": self.close_time,
            "open": self.open,
            "high": self.high,
            "low": self.low,
            "close": self.close,
            "volume": self.volume,
            "is_closed": self.is_closed,
        }


class CryptoWebSocketManager:
    """
    Manages Binance WebSocket connections for real-time crypto data.
    
    Features:
    - Combined stream subscription (multiple symbols in one connection)
    - Auto-reconnect with exponential backoff
    - In-memory ticker cache
    - Optional kline streaming
    - Health monitoring
    """

    def __init__(
        self,
        symbols: Optional[List[str]] = None,
        kline_intervals: Optional[List[str]] = None,
        on_ticker: Optional[Callable] = None,
        on_kline: Optional[Callable] = None,
    ):
        self.symbols = symbols or DEFAULT_SYMBOLS
        self.kline_intervals = kline_intervals or []  # e.g., ["1m", "5m"]
        self.on_ticker = on_ticker  # callback(TickerSnapshot)
        self.on_kline = on_kline  # callback(MiniKline)

        # State
        self._tickers: Dict[str, TickerSnapshot] = {}
        self._latest_klines: Dict[str, MiniKline] = {}  # key: "BTCUSDT_1m"
        self._ws = None
        self._running = False
        self._reconnect_delay = INITIAL_RECONNECT_DELAY
        self._connected = False
        self._connect_time: Optional[float] = None
        self._message_count = 0
        self._last_message_time: Optional[float] = None
        self._task: Optional[asyncio.Task] = None

    # ── Public API ──

    @property
    def is_running(self) -> bool:
        return self._running

    @property
    def is_connected(self) -> bool:
        return self._connected and self._ws is not None

    def get_ticker(self, symbol: str) -> Optional[TickerSnapshot]:
        """Get latest ticker for a symbol (e.g., 'BTCUSDT' or 'BTC')"""
        pair = symbol.upper()
        if not pair.endswith("USDT"):
            pair = f"{pair}USDT"
        return self._tickers.get(pair)

    def get_all_tickers(self) -> Dict[str, TickerSnapshot]:
        """Get all cached tickers"""
        return dict(self._tickers)

    def get_all_tickers_list(self) -> List[Dict[str, Any]]:
        """Get all tickers as list of dicts, sorted by market cap proxy (volume)"""
        tickers = sorted(
            self._tickers.values(),
            key=lambda t: t.quote_volume_24h,
            reverse=True,
        )
        return [t.to_dict() for t in tickers]

    def get_kline(self, symbol: str, interval: str) -> Optional[MiniKline]:
        """Get latest kline for symbol+interval"""
        pair = symbol.upper()
        if not pair.endswith("USDT"):
            pair = f"{pair}USDT"
        key = f"{pair}_{interval}"
        return self._latest_klines.get(key)

    def get_status(self) -> Dict[str, Any]:
        """Health/status info"""
        uptime = (time.time() - self._connect_time) if self._connect_time else 0
        stale_count = sum(1 for t in self._tickers.values() if t.is_stale)
        return {
            "running": self._running,
            "connected": self._connected,
            "symbols_count": len(self.symbols),
            "tickers_cached": len(self._tickers),
            "stale_tickers": stale_count,
            "kline_intervals": self.kline_intervals,
            "message_count": self._message_count,
            "uptime_seconds": round(uptime, 1),
            "last_message": (
                datetime.fromtimestamp(self._last_message_time, tz=timezone.utc).isoformat()
                if self._last_message_time
                else None
            ),
            "reconnect_delay": self._reconnect_delay,
        }

    # ── Lifecycle ──

    async def start(self):
        """Start the WebSocket manager (non-blocking)"""
        if self._running:
            logger.warning("WebSocket manager already running")
            return
        self._running = True
        self._task = asyncio.create_task(self._run_loop())
        logger.info(
            f"Crypto WS manager started: {len(self.symbols)} symbols, "
            f"kline intervals: {self.kline_intervals or 'none'}"
        )

    async def stop(self):
        """Stop the WebSocket manager gracefully"""
        self._running = False
        if self._ws:
            await self._ws.close()
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        self._connected = False
        logger.info("Crypto WS manager stopped")

    # ── Internal ──

    def _build_stream_names(self) -> List[str]:
        """Build combined stream subscription list"""
        streams = []
        for sym in self.symbols:
            sym_lower = sym.lower()
            # 24h ticker
            streams.append(f"{sym_lower}@ticker")
            # Klines
            for interval in self.kline_intervals:
                streams.append(f"{sym_lower}@kline_{interval}")
        return streams

    async def _run_loop(self):
        """Main connection loop with auto-reconnect"""
        while self._running:
            try:
                await self._connect_and_listen()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"WS connection error: {e}")

            if not self._running:
                break

            # Exponential backoff
            logger.info(
                f"Reconnecting in {self._reconnect_delay:.1f}s..."
            )
            await asyncio.sleep(self._reconnect_delay)
            self._reconnect_delay = min(
                self._reconnect_delay * RECONNECT_BACKOFF,
                MAX_RECONNECT_DELAY,
            )

    async def _connect_and_listen(self):
        """Connect to Binance combined stream and process messages"""
        streams = self._build_stream_names()
        stream_param = "/".join(streams)
        url = f"{BINANCE_WS_COMBINED}?streams={stream_param}"

        logger.info(f"Connecting to Binance WS ({len(streams)} streams)...")

        async with websockets.connect(
            url,
            ping_interval=PING_INTERVAL,
            ping_timeout=PING_INTERVAL * 2,
            close_timeout=5,
            max_size=2**20,  # 1MB
        ) as ws:
            self._ws = ws
            self._connected = True
            self._connect_time = time.time()
            self._reconnect_delay = INITIAL_RECONNECT_DELAY  # Reset backoff
            logger.info(f"Connected to Binance WS ✓ ({len(streams)} streams)")

            try:
                async for raw_msg in ws:
                    if not self._running:
                        break
                    try:
                        msg = json.loads(raw_msg)
                        self._message_count += 1
                        self._last_message_time = time.time()
                        await self._handle_message(msg)
                    except json.JSONDecodeError:
                        logger.warning(f"Invalid JSON from WS: {raw_msg[:100]}")
                    except Exception as e:
                        logger.error(f"Error processing WS message: {e}")
            except ConnectionClosed as e:
                logger.warning(f"WS connection closed: code={e.code} reason={e.reason}")
            finally:
                self._connected = False
                self._ws = None

    async def _handle_message(self, msg: Dict[str, Any]):
        """Route incoming message to handler"""
        stream = msg.get("stream", "")
        data = msg.get("data", {})

        if not data:
            return

        event_type = data.get("e", "")

        if event_type == "24hrTicker":
            self._handle_ticker(data)
        elif event_type == "kline":
            self._handle_kline(data)

    def _handle_ticker(self, data: Dict[str, Any]):
        """Process 24hr ticker update"""
        try:
            symbol = data.get("s", "")
            if not symbol:
                return

            snapshot = TickerSnapshot(
                symbol=symbol,
                price=float(data.get("c", 0)),  # close/last price
                change_24h=float(data.get("p", 0)),  # price change
                change_pct_24h=float(data.get("P", 0)),  # price change %
                high_24h=float(data.get("h", 0)),
                low_24h=float(data.get("l", 0)),
                volume_24h=float(data.get("v", 0)),  # base volume
                quote_volume_24h=float(data.get("q", 0)),  # quote volume
                open_price=float(data.get("o", 0)),
                trades_count=int(data.get("n", 0)),
                last_update=time.time(),
            )

            self._tickers[symbol] = snapshot

            # Callback
            if self.on_ticker:
                try:
                    self.on_ticker(snapshot)
                except Exception as e:
                    logger.error(f"Ticker callback error: {e}")

        except (ValueError, TypeError) as e:
            logger.warning(f"Invalid ticker data: {e}")

    def _handle_kline(self, data: Dict[str, Any]):
        """Process kline update"""
        try:
            k = data.get("k", {})
            if not k:
                return

            symbol = k.get("s", "")
            interval = k.get("i", "")

            kline = MiniKline(
                symbol=symbol,
                interval=interval,
                open_time=k.get("t", 0),
                close_time=k.get("T", 0),
                open=float(k.get("o", 0)),
                high=float(k.get("h", 0)),
                low=float(k.get("l", 0)),
                close=float(k.get("c", 0)),
                volume=float(k.get("v", 0)),
                is_closed=k.get("x", False),
            )

            key = f"{symbol}_{interval}"
            self._latest_klines[key] = kline

            # Callback
            if self.on_kline and kline.is_closed:
                try:
                    self.on_kline(kline)
                except Exception as e:
                    logger.error(f"Kline callback error: {e}")

        except (ValueError, TypeError) as e:
            logger.warning(f"Invalid kline data: {e}")


# ── Singleton ──

_ws_manager: Optional[CryptoWebSocketManager] = None


def get_crypto_ws_manager() -> CryptoWebSocketManager:
    """Get or create the global WebSocket manager"""
    global _ws_manager
    if _ws_manager is None:
        _ws_manager = CryptoWebSocketManager(
            kline_intervals=["1m"],  # Track 1m klines by default
        )
    return _ws_manager


async def start_crypto_ws():
    """Start the global WebSocket manager"""
    manager = get_crypto_ws_manager()
    if not manager.is_running:
        await manager.start()
    return manager


async def stop_crypto_ws():
    """Stop the global WebSocket manager"""
    global _ws_manager
    if _ws_manager and _ws_manager.is_running:
        await _ws_manager.stop()
