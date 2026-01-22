"""
K线截图生成服务
使用 mplfinance 生成K线图片，供 Claude Code 进行形态分析

重构说明:
- 使用 Repository 模式替代 SessionLocal()
- 支持依赖注入用于测试
- 向后兼容：无参数调用时自动创建 session
"""

import os
import time
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Dict, Any

import pandas as pd
import mplfinance as mpf
import matplotlib.pyplot as plt
import matplotlib
from sqlalchemy.orm import Session

from src.database import SessionLocal
from src.repositories.kline_repository import KlineRepository
from src.repositories.symbol_repository import SymbolRepository

# 设置中文字体
matplotlib.rcParams['font.sans-serif'] = ['PingFang SC', 'Heiti SC', 'STHeiti', 'SimHei', 'Arial Unicode MS']
matplotlib.rcParams['axes.unicode_minus'] = False
from src.models import Watchlist, SymbolMetadata, Kline, KlineTimeframe, SymbolType
from src.utils.logging import get_logger

logger = get_logger(__name__)

# 关闭 matplotlib 的交互模式，避免弹窗
plt.switch_backend('Agg')


class ScreenshotService:
    """
    K线截图生成服务

    重构后支持:
    - 依赖注入 Session（用于测试）
    - 向后兼容：无参数调用时自动创建 session
    """

    # 深色主题样式
    CHART_STYLE = {
        "base_mpl_style": "dark_background",
        "marketcolors": mpf.make_marketcolors(
            up="#ff4d4d",      # 上涨红色 (A股习惯)
            down="#00d4aa",    # 下跌绿色
            edge="inherit",
            wick="inherit",
            volume="inherit",
        ),
        "facecolor": "#1a1a2e",
        "edgecolor": "#1a1a2e",
        "gridcolor": "#2d2d44",
        "gridstyle": "--",
        "gridaxis": "both",
        "y_on_right": True,
        "rc": {
            "axes.labelcolor": "#cccccc",
            "axes.titlecolor": "#ffffff",
            "xtick.color": "#888888",
            "ytick.color": "#888888",
            "figure.facecolor": "#1a1a2e",
            "axes.facecolor": "#1a1a2e",
            "savefig.facecolor": "#1a1a2e",
            "font.sans-serif": ["PingFang SC", "Heiti SC", "STHeiti", "SimHei", "Arial Unicode MS", "DejaVu Sans"],
            "axes.unicode_minus": False,
        }
    }

    # 均线颜色
    MA_COLORS = ["#f39c12", "#3498db", "#9b59b6", "#1abc9c"]  # MA5/10/20/60

    def __init__(
        self,
        session: Optional[Session] = None,
        kline_repo: Optional[KlineRepository] = None,
        symbol_repo: Optional[SymbolRepository] = None,
        output_base_dir: str = "data/screenshots"
    ):
        """
        初始化截图服务

        Args:
            session: 数据库会话（可选，用于依赖注入）
            kline_repo: K线数据仓库（可选）
            symbol_repo: 标的数据仓库（可选）
            output_base_dir: 截图输出基础目录
        """
        self.output_base_dir = Path(output_base_dir)
        self.style = mpf.make_mpf_style(**self.CHART_STYLE)

        # 支持三种初始化方式：
        # 1. 注入 repositories（最推荐，用于测试）
        # 2. 注入 session（次推荐）
        # 3. 自动创建 session（向后兼容）
        if kline_repo and symbol_repo:
            self.kline_repo = kline_repo
            self.symbol_repo = symbol_repo
            self.session = kline_repo.session
            self._owns_session = False
        elif session:
            self.session = session
            self.kline_repo = KlineRepository(session)
            self.symbol_repo = SymbolRepository(session)
            self._owns_session = False
        else:
            self.session = SessionLocal()
            self.kline_repo = KlineRepository(self.session)
            self.symbol_repo = SymbolRepository(self.session)
            self._owns_session = True

    @classmethod
    def create_with_session(cls, session: Session, output_base_dir: str = "data/screenshots") -> "ScreenshotService":
        """使用现有session创建服务的工厂方法"""
        return cls(session=session, output_base_dir=output_base_dir)

    def __del__(self):
        """确保session在对象销毁时关闭"""
        if (
            hasattr(self, "_owns_session")
            and self._owns_session
            and hasattr(self, "session")
        ):
            self.session.close()

    def _ensure_output_dir(self, date_str: Optional[str] = None) -> Path:
        """确保输出目录存在"""
        if date_str is None:
            date_str = datetime.now().strftime("%Y-%m-%d")

        output_dir = self.output_base_dir / date_str
        output_dir.mkdir(parents=True, exist_ok=True)
        return output_dir

    def _get_watchlist_tickers(self) -> List[tuple]:
        """获取自选股列表 (ticker, name)"""
        # 使用实例的 session
        results = (
            self.session.query(Watchlist.ticker, SymbolMetadata.name)
            .join(SymbolMetadata, Watchlist.ticker == SymbolMetadata.ticker)
            .order_by(Watchlist.added_at.desc())
            .all()
        )
        return [(r[0], r[1] or r[0]) for r in results]

    def _get_kline_data(
        self,
        ticker: str,
        timeframe: str = "day",
        limit: int = 120
    ) -> Optional[pd.DataFrame]:
        """
        获取K线数据并转换为 mplfinance 格式

        Args:
            ticker: 股票代码
            timeframe: 时间周期
            limit: K线数量

        Returns:
            DataFrame with DatetimeIndex and OHLCV columns
        """
        # 映射 timeframe
        tf_map = {
            "day": KlineTimeframe.DAY,
            "30m": KlineTimeframe.MINS_30,
            "5m": KlineTimeframe.MINS_5,
            "1m": KlineTimeframe.MINS_1,
        }
        kline_tf = tf_map.get(timeframe, KlineTimeframe.DAY)

        try:
            # 使用 repository 查询K线数据
            klines = self.kline_repo.find_by_symbol_and_timeframe(
                symbol_code=ticker,
                symbol_type=SymbolType.STOCK,
                timeframe=kline_tf,
                limit=limit,
                order_desc=True
            )

            if not klines:
                logger.warning(f"{ticker} 没有K线数据")
                return None

            # 转换为DataFrame
            data = []
            for k in klines:
                data.append({
                    "Date": k.trade_time,
                    "Open": float(k.open) if k.open else 0,
                    "High": float(k.high) if k.high else 0,
                    "Low": float(k.low) if k.low else 0,
                    "Close": float(k.close) if k.close else 0,
                    "Volume": float(k.volume) if k.volume else 0,
                })

            df = pd.DataFrame(data)
            df["Date"] = pd.to_datetime(df["Date"])
            df = df.set_index("Date")
            df = df.sort_index()  # 按时间正序排列

            # 计算均线
            df["MA5"] = df["Close"].rolling(window=5).mean()
            df["MA10"] = df["Close"].rolling(window=10).mean()
            df["MA20"] = df["Close"].rolling(window=20).mean()
            df["MA60"] = df["Close"].rolling(window=60).mean()

            # 计算MACD
            exp1 = df["Close"].ewm(span=12, adjust=False).mean()
            exp2 = df["Close"].ewm(span=26, adjust=False).mean()
            df["DIF"] = exp1 - exp2
            df["DEA"] = df["DIF"].ewm(span=9, adjust=False).mean()
            df["MACD"] = (df["DIF"] - df["DEA"]) * 2

            return df

        except Exception as e:
            logger.error(f"{ticker} 获取K线数据失败: {e}")
            return None

    def generate_chart(
        self,
        ticker: str,
        name: str,
        timeframe: str = "day",
        limit: int = 120,
        include_volume: bool = True,
        include_macd: bool = True,
        output_dir: Optional[Path] = None,
    ) -> Optional[str]:
        """
        生成单只股票的K线截图

        Args:
            ticker: 股票代码
            name: 股票名称
            timeframe: 时间周期
            limit: K线数量
            include_volume: 是否包含成交量
            include_macd: 是否包含MACD
            output_dir: 输出目录

        Returns:
            生成的文件路径，失败返回None
        """
        # 获取K线数据
        df = self._get_kline_data(ticker, timeframe, limit)
        if df is None or df.empty:
            return None

        # 准备输出目录和文件名
        if output_dir is None:
            output_dir = self._ensure_output_dir()

        # 清理文件名中的特殊字符
        safe_name = name.replace("/", "_").replace("\\", "_").replace(" ", "_")
        filename = f"{ticker}_{safe_name}_{timeframe}.png"
        filepath = output_dir / filename

        try:
            # 准备均线
            ma_plots = []
            for i, period in enumerate([5, 10, 20, 60]):
                col = f"MA{period}"
                if col in df.columns and df[col].notna().any():
                    ma_plots.append(
                        mpf.make_addplot(
                            df[col],
                            color=self.MA_COLORS[i],
                            width=0.8,
                            panel=0,
                        )
                    )

            # 准备MACD
            if include_macd and "DIF" in df.columns:
                # MACD柱状图颜色
                macd_colors = ["#ff4d4d" if v >= 0 else "#00d4aa" for v in df["MACD"].fillna(0)]

                ma_plots.extend([
                    mpf.make_addplot(df["DIF"], panel=2, color="#f39c12", width=0.8, ylabel="MACD"),
                    mpf.make_addplot(df["DEA"], panel=2, color="#3498db", width=0.8),
                    mpf.make_addplot(df["MACD"], panel=2, type="bar", color=macd_colors, width=0.6),
                ])

            # 计算涨跌幅
            if len(df) >= 2:
                last_close = df["Close"].iloc[-1]
                prev_close = df["Close"].iloc[-2]
                change_pct = (last_close - prev_close) / prev_close * 100 if prev_close else 0
                price_str = f"¥{last_close:,.2f}  {change_pct:+.2f}%"
            else:
                price_str = ""

            # 标题
            tf_name = {"day": "日线", "week": "周线", "30m": "30分钟"}.get(timeframe, timeframe)
            title = f"{name} ({ticker}) {tf_name}  {price_str}"

            # 生成图表
            fig, axes = mpf.plot(
                df,
                type="candle",
                style=self.style,
                title=title,
                volume=include_volume,
                addplot=ma_plots if ma_plots else None,
                figsize=(12, 8),
                panel_ratios=(6, 2, 2) if include_macd else (6, 2),
                returnfig=True,
                warn_too_much_data=1000,
            )

            # 保存图片
            fig.savefig(
                filepath,
                dpi=100,
                bbox_inches="tight",
                facecolor=self.CHART_STYLE["facecolor"],
                edgecolor="none",
            )
            plt.close(fig)

            logger.debug(f"生成截图: {filepath}")
            return str(filepath)

        except Exception as e:
            logger.error(f"{ticker} 生成截图失败: {e}")
            return None

    def batch_generate(
        self,
        scope: str = "watchlist",
        tickers: Optional[List[str]] = None,
        timeframe: str = "day",
        limit: int = 120,
        include_volume: bool = True,
        include_macd: bool = True,
    ) -> Dict[str, Any]:
        """
        批量生成K线截图

        Args:
            scope: "watchlist" 或 "custom"
            tickers: 自定义股票列表 (scope=custom时使用)
            timeframe: 时间周期
            limit: K线数量
            include_volume: 是否包含成交量
            include_macd: 是否包含MACD

        Returns:
            生成结果统计
        """
        start_time = time.time()

        # 获取股票列表
        if scope == "watchlist":
            stock_list = self._get_watchlist_tickers()
        elif tickers:
            # 使用 repository 查询股票名称
            stock_list = []
            for t in tickers:
                meta = self.symbol_repo.find_by_ticker(t)
                name = meta.name if meta else t
                stock_list.append((t, name))
        else:
            return {
                "success": False,
                "error": "请指定 scope=watchlist 或提供 tickers 列表"
            }

        if not stock_list:
            return {
                "success": False,
                "error": "没有找到股票"
            }

        # 准备输出目录
        output_dir = self._ensure_output_dir()

        logger.info(f"开始批量生成截图: {len(stock_list)} 只股票")

        # 逐个生成
        generated_files = []
        failed_tickers = []

        for i, (ticker, name) in enumerate(stock_list):
            filepath = self.generate_chart(
                ticker=ticker,
                name=name,
                timeframe=timeframe,
                limit=limit,
                include_volume=include_volume,
                include_macd=include_macd,
                output_dir=output_dir,
            )

            if filepath:
                generated_files.append(os.path.basename(filepath))
            else:
                failed_tickers.append(ticker)

            # 每20个打印一次进度
            if (i + 1) % 20 == 0:
                logger.info(f"进度: {i + 1}/{len(stock_list)}")

        duration = time.time() - start_time

        result = {
            "success": True,
            "total": len(stock_list),
            "generated": len(generated_files),
            "failed": len(failed_tickers),
            "failed_tickers": failed_tickers,
            "output_dir": str(output_dir),
            "duration_seconds": round(duration, 1),
            "files": generated_files,
        }

        logger.info(
            f"批量截图完成: 成功 {result['generated']}/{result['total']}, "
            f"耗时 {result['duration_seconds']}秒"
        )

        return result

    def list_screenshots(
        self,
        date_str: Optional[str] = None,
        timeframe: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        获取截图列表

        Args:
            date_str: 日期字符串 (YYYY-MM-DD)，默认今天
            timeframe: 筛选时间周期

        Returns:
            截图列表信息
        """
        if date_str is None:
            date_str = datetime.now().strftime("%Y-%m-%d")

        dir_path = self.output_base_dir / date_str

        if not dir_path.exists():
            return {
                "date": date_str,
                "count": 0,
                "directory": str(dir_path),
                "files": [],
            }

        files = []
        for f in dir_path.glob("*.png"):
            # 解析文件名: {ticker}_{name}_{timeframe}.png
            parts = f.stem.rsplit("_", 1)
            if len(parts) == 2:
                ticker_name, tf = parts
                ticker_parts = ticker_name.split("_", 1)
                ticker = ticker_parts[0]
                name = ticker_parts[1] if len(ticker_parts) > 1 else ticker
            else:
                ticker = f.stem
                name = ticker
                tf = "unknown"

            # 筛选时间周期
            if timeframe and tf != timeframe:
                continue

            stat = f.stat()
            files.append({
                "filename": f.name,
                "ticker": ticker,
                "name": name,
                "timeframe": tf,
                "size_kb": round(stat.st_size / 1024, 1),
                "created_at": datetime.fromtimestamp(stat.st_ctime).isoformat(),
                "path": str(f),
            })

        # 按ticker排序
        files.sort(key=lambda x: x["ticker"])

        return {
            "date": date_str,
            "timeframe": timeframe,
            "count": len(files),
            "directory": str(dir_path),
            "files": files,
        }

    def get_latest_directory(self) -> Optional[Dict[str, Any]]:
        """获取最新的截图目录"""
        if not self.output_base_dir.exists():
            return None

        # 找到最新的日期目录
        dirs = [d for d in self.output_base_dir.iterdir() if d.is_dir()]
        if not dirs:
            return None

        latest_dir = max(dirs, key=lambda d: d.name)
        file_count = len(list(latest_dir.glob("*.png")))

        return {
            "date": latest_dir.name,
            "directory": str(latest_dir),
            "count": file_count,
        }
