/**
 * 股票 K线卡片
 * 使用统一的 KlineChart 组件 (Lightweight Charts)
 */

import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import type { Timeframe } from "../types/timeframe";
import type { SymbolMeta } from "../types/symbol";
import type { MAConfig } from "../types/chartConfig";
import { fetchCandles } from "../hooks/useCandles";
import { useMemo, useState, useRef } from "react";
import { apiFetch, REFRESH_INTERVALS } from "../utils/api";
import { KlineEvaluationForm } from "./KlineEvaluationForm";
import { KlineChart, type KlineDataPoint } from "./charts";
import { getBoardInfo } from "../utils/boardUtils";

interface RealtimePrice {
  ticker: string;
  price: number;
  change: number;
  changePercent: number;
  lastUpdate: string;
}

interface Props {
  symbol: SymbolMeta;
  timeframe: Timeframe;
  maConfig: MAConfig;
  onStockClick?: (ticker: string) => void;
  realtimePrice?: RealtimePrice;
  klineLimit?: number;
}

export function CandleCard({ symbol, timeframe, maConfig, onStockClick, realtimePrice, klineLimit = 120 }: Props) {
  const [isExpanded, setIsExpanded] = useState(false);
  const [isInWatchlist, setIsInWatchlist] = useState(false);
  const cardRef = useRef<HTMLElement>(null);
  const queryClient = useQueryClient();
  const industry = symbol.industryLv1 || "未知";

  const { data } = useQuery({
    queryKey: ["candles", symbol.ticker, timeframe, klineLimit],
    queryFn: () => fetchCandles(symbol.ticker, timeframe, klineLimit),
    staleTime: 1000 * 60 * 10
  });

  // 检查是否已在自选中（使用不带后缀的ticker）
  const tickerForCheck = symbol.ticker.split('.')[0];
  const { data: watchlistStatus } = useQuery({
    queryKey: ["watchlist-check", tickerForCheck],
    queryFn: async () => {
      const response = await apiFetch(`/api/watchlist/check/${tickerForCheck}`);
      if (!response.ok) return { in_watchlist: false };
      return response.json();
    },
    staleTime: REFRESH_INTERVALS.watchlist
  });

  // 更新本地状态
  useMemo(() => {
    if (watchlistStatus) {
      setIsInWatchlist(watchlistStatus.in_watchlist);
    }
  }, [watchlistStatus]);

  const addToWatchlist = useMutation({
    mutationFn: async () => {
      const tickerForApi = symbol.ticker.split('.')[0];
      const response = await apiFetch("/api/watchlist", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ ticker: tickerForApi })
      });
      if (!response.ok) {
        const error = await response.json();
        throw new Error(error.detail || "添加失败");
      }
      return response.json();
    },
    onSuccess: () => {
      setIsInWatchlist(true);
      queryClient.invalidateQueries({ queryKey: ["watchlist"] });
      queryClient.invalidateQueries({ queryKey: ["watchlist-check", tickerForCheck] });
    }
  });

  // 获取最后一个交易日的收盘价
  const lastClose = useMemo(() => {
    if (!data || data.candles.length === 0) return null;
    return data.candles[data.candles.length - 1].close;
  }, [data]);

  // 计算昨日和今日涨跌幅
  const priceChanges = useMemo(() => {
    if (!data || data.candles.length < 2) return null;

    const candles = data.candles;
    const len = candles.length;

    let yesterdayChange = null;
    if (len >= 3) {
      const yesterdayClose = candles[len - 2].close;
      const prevClose = candles[len - 3].close;
      yesterdayChange = ((yesterdayClose - prevClose) / prevClose) * 100;
    }

    let todayChange = null;
    if (realtimePrice) {
      todayChange = realtimePrice.changePercent;
    } else {
      const yesterdayClose = candles[len - 2].close;
      const todayClose = candles[len - 1].close;
      todayChange = ((todayClose - yesterdayClose) / yesterdayClose) * 100;
    }

    return { yesterdayChange, todayChange };
  }, [data, realtimePrice]);

  // 获取最后一根K线的日期
  const lastCandleDate = useMemo(() => {
    if (realtimePrice) {
      const today = new Date();
      const yyyy = today.getFullYear();
      const mm = String(today.getMonth() + 1).padStart(2, '0');
      const dd = String(today.getDate()).padStart(2, '0');
      return `${yyyy}-${mm}-${dd}`;
    }

    if (!data || data.candles.length === 0) return null;
    const lastCandle = data.candles[data.candles.length - 1];
    const date = new Date(lastCandle.timestamp);
    const yyyy = date.getFullYear();
    const mm = String(date.getMonth() + 1).padStart(2, '0');
    const dd = String(date.getDate()).padStart(2, '0');
    return `${yyyy}-${mm}-${dd}`;
  }, [data, realtimePrice]);

  // 转换数据格式为 KlineChart 需要的格式
  const chartData: KlineDataPoint[] = useMemo(() => {
    if (!data || !data.candles) return [];
    const isIntraday = timeframe !== "day";
    return data.candles.map(c => ({
      date: isIntraday
        ? String(Math.floor(new Date(c.timestamp).getTime() / 1000) + 8 * 3600)
        : new Date(c.timestamp).toISOString().slice(0, 10).replace(/-/g, ''),
      open: c.open,
      high: c.high,
      low: c.low,
      close: c.close,
      volume: c.volume || 0,
    }));
  }, [data, timeframe]);

  // 准备K线数据用于评估表单
  const klineDataForEval = useMemo(() => {
    if (!data) return null;
    return {
      ticker: symbol.ticker,
      timeframe,
      candles: data.candles.map(c => ({
        timestamp: c.timestamp,
        open: c.open,
        high: c.high,
        low: c.low,
        close: c.close,
        volume: c.volume,
        ma5: c.ma5,
        ma10: c.ma10,
        ma20: c.ma20,
        ma50: c.ma50,
      }))
    };
  }, [data, symbol.ticker, timeframe]);

  // 获取K线最后日期
  const klineEndDateForEval = useMemo(() => {
    if (!data || data.candles.length === 0) return "";
    const lastCandle = data.candles[data.candles.length - 1];
    const date = new Date(lastCandle.timestamp);
    return date.toISOString().split('T')[0];
  }, [data]);

  return (
    <article
      ref={cardRef}
      className={`candle-card ${isExpanded ? "candle-card--expanded" : ""}`}
      onDoubleClick={() => {
        if (onStockClick) {
          onStockClick(symbol.ticker);
        } else {
          setIsExpanded((value) => !value);
        }
      }}
    >
      <header className="candle-card__header">
        <div>
          <h3 className="candle-card__title">
            {symbol.name}
            {(() => {
              const boardInfo = getBoardInfo(symbol.ticker);
              return boardInfo.label ? (
                <span className={`board-tag ${boardInfo.className}`}>{boardInfo.label}</span>
              ) : null;
            })()}
            {symbol.totalMv && (
              <span className="candle-card__mv">{(symbol.totalMv / 1e4).toFixed(0)}亿</span>
            )}
            {realtimePrice ? (
              <span className={`candle-card__price candle-card__price--${realtimePrice.change >= 0 ? 'up' : 'down'}`}>
                ¥{realtimePrice.price.toFixed(2)}
                <span className="candle-card__price-change">
                  {realtimePrice.change >= 0 ? '+' : ''}{realtimePrice.changePercent.toFixed(2)}%
                </span>
              </span>
            ) : lastClose !== null && (
              <span className="candle-card__price">¥{lastClose.toFixed(2)}</span>
            )}
            {lastCandleDate && (
              <span className="candle-card__date">
                {lastCandleDate}
              </span>
            )}
          </h3>
          <p className="candle-card__subtitle">
            {symbol.ticker}
            {realtimePrice && (
              <span className="candle-card__live-indicator">🔴 Live {realtimePrice.lastUpdate}</span>
            )}
          </p>
          {priceChanges && (
            <div className="candle-card__changes">
              {priceChanges.todayChange !== null && (
                <span className={`candle-card__change-badge candle-card__change-badge--${priceChanges.todayChange >= 0 ? 'up' : 'down'}`}>
                  今日: {priceChanges.todayChange >= 0 ? '+' : ''}{priceChanges.todayChange.toFixed(2)}%
                </span>
              )}
              {priceChanges.yesterdayChange !== null && (
                <span className={`candle-card__change-badge candle-card__change-badge--${priceChanges.yesterdayChange >= 0 ? 'up' : 'down'}`}>
                  昨日: {priceChanges.yesterdayChange >= 0 ? '+' : ''}{priceChanges.yesterdayChange.toFixed(2)}%
                </span>
              )}
            </div>
          )}
        </div>
        <div className="candle-card__metrics">
          {symbol.peTtm !== null && symbol.peTtm !== undefined && (
            <span className="candle-card__badge">
              PE {symbol.peTtm.toFixed(2)}
            </span>
          )}
        </div>
      </header>

      <section className="candle-card__chart">
        {chartData.length > 0 ? (
          <KlineChart
            data={chartData}
            height={320}
            showVolume={true}
            showMACD={true}
            maConfig={maConfig}
            compact={true}
          />
        ) : (
          <div className="candle-card__loading">加载中...</div>
        )}
      </section>

      <footer className="candle-card__footer">
        <div className="candle-card__category-info">
          <span className="candle-card__industry">{industry}</span>
        </div>
        <button
          className={`candle-card__watchlist-btn ${isInWatchlist ? "candle-card__watchlist-btn--added" : ""}`}
          onClick={(e) => {
            e.stopPropagation();
            if (!isInWatchlist) {
              addToWatchlist.mutate();
            }
          }}
          disabled={addToWatchlist.isPending || isInWatchlist}
        >
          {addToWatchlist.isPending ? "添加中..." : isInWatchlist ? "✓ 已自选" : "➕ 自选"}
        </button>
      </footer>

      {/* K线标注评估表单 */}
      <KlineEvaluationForm
        ticker={symbol.ticker}
        stockName={symbol.name}
        timeframe={timeframe}
        klineEndDate={klineEndDateForEval}
        priceAtEval={lastClose}
        klineData={klineDataForEval}
        chartRef={cardRef}
      />
    </article>
  );
}
