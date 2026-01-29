/**
 * 股票详情页
 * 使用统一的 KlineChart 组件 (Lightweight Charts)
 */

import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import type { Timeframe } from "../types/timeframe";
import type { MAConfig } from "../types/chartConfig";
import { MA_COLORS } from "../types/chartConfig";
import { fetchCandles } from "../hooks/useCandles";
import { fetchSymbols } from "../hooks/useSymbols";
import { useMemo, useState } from "react";
import { useRealtimePrice } from "../hooks/useRealtimePrice";
import { KlineChart, type KlineDataPoint } from "./charts";
import { apiFetch, REFRESH_INTERVALS } from "../utils/api";

interface Props {
  ticker: string;
  maConfig: MAConfig;
  onMAConfigChange: (config: MAConfig) => void;
  klineLimit?: number;
  onKlineLimitChange?: (limit: number) => void;
}

export function StockDetail({ ticker, maConfig, onMAConfigChange, klineLimit = 120, onKlineLimitChange }: Props) {
  // 获取股票元数据
  const { data: allSymbols } = useQuery({
    queryKey: ["symbols"],
    queryFn: fetchSymbols,
    staleTime: 1000 * 60 * 60
  });

  const symbol = useMemo(() => {
    if (!allSymbols) return undefined;
    // 尝试精确匹配
    let found = allSymbols.find((s) => s.ticker === ticker);
    if (!found) {
      // 尝试去除后缀匹配（如 000001.SZ -> 000001）
      const tickerWithoutSuffix = ticker.split('.')[0];
      found = allSymbols.find((s) => s.ticker === tickerWithoutSuffix);
    }
    return found;
  }, [allSymbols, ticker]);

  // Fetch real-time price (30 minutes interval, only during market hours)
  const realtimePrices = useRealtimePrice({
    tickers: [ticker],
    interval: 1800000, // 30 minutes
    enabled: true
  });

  const realtimePrice = realtimePrices.get(ticker);

  // 使用不带后缀的ticker查询K线（API要求不带后缀）
  const tickerForApi = ticker.split('.')[0];

  // 自选相关状态和查询
  const [isInWatchlist, setIsInWatchlist] = useState(false);
  const queryClient = useQueryClient();

  const { data: watchlistStatus } = useQuery({
    queryKey: ["watchlist-check", tickerForApi],
    queryFn: async () => {
      const response = await apiFetch(`/api/watchlist/check/${tickerForApi}`);
      if (!response.ok) return { in_watchlist: false };
      return response.json();
    },
    staleTime: REFRESH_INTERVALS.watchlist
  });

  useMemo(() => {
    if (watchlistStatus) {
      setIsInWatchlist(watchlistStatus.in_watchlist);
    }
  }, [watchlistStatus]);

  const addToWatchlist = useMutation({
    mutationFn: async () => {
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
      queryClient.invalidateQueries({ queryKey: ["watchlist-check", tickerForApi] });
    }
  });

  const removeFromWatchlist = useMutation({
    mutationFn: async () => {
      const response = await apiFetch(`/api/watchlist/${tickerForApi}`, {
        method: "DELETE"
      });
      if (!response.ok) {
        const error = await response.json();
        throw new Error(error.detail || "移除失败");
      }
      return response.json();
    },
    onSuccess: () => {
      setIsInWatchlist(false);
      queryClient.invalidateQueries({ queryKey: ["watchlist"] });
      queryClient.invalidateQueries({ queryKey: ["watchlist-check", tickerForApi] });
    }
  });

  // 获取股票的所有概念板块
  const { data: conceptsData } = useQuery({
    queryKey: ["ticker-concepts", tickerForApi],
    queryFn: async () => {
      const response = await fetch(`/api/concepts/by-ticker/${tickerForApi}`);
      if (!response.ok) return { concepts: [] };
      return response.json();
    },
    staleTime: 1000 * 60 * 60, // 1小时缓存
  });

  // 获取可用赛道列表
  const { data: availableSectorsData } = useQuery<{ sectors: string[] }>({
    queryKey: ["available-sectors"],
    queryFn: async () => {
      const response = await apiFetch("/api/sectors/list/available");
      if (!response.ok) return { sectors: [] };
      return response.json();
    },
    staleTime: 1000 * 60 * 60,
  });

  const availableSectors = availableSectorsData?.sectors ?? [];

  // 获取股票当前赛道
  const { data: sectorData } = useQuery({
    queryKey: ["sector", tickerForApi],
    queryFn: async () => {
      const response = await apiFetch(`/api/sectors/${tickerForApi}`);
      if (!response.ok) return { sector: null };
      return response.json();
    },
    staleTime: 1000 * 60 * 5,
  });

  // 更新赛道
  const updateSector = useMutation({
    mutationFn: async (sector: string) => {
      const response = await apiFetch(`/api/sectors/${tickerForApi}`, {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ sector }),
      });
      if (!response.ok) {
        const error = await response.json();
        throw new Error(error.detail || "更新失败");
      }
      return response.json();
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["sector", tickerForApi] });
      queryClient.invalidateQueries({ queryKey: ["sectors-all"] });
    },
  });

  // 获取日线和30分钟K线数据
  const { data: dayData } = useQuery({
    queryKey: ["candles", tickerForApi, "day", klineLimit],
    queryFn: () => fetchCandles(tickerForApi, "day" as Timeframe, klineLimit),
    staleTime: 1000 * 60 * 10,
    enabled: !!symbol
  });

  const { data: mins30Data } = useQuery({
    queryKey: ["candles", tickerForApi, "30m", klineLimit],
    queryFn: () => fetchCandles(tickerForApi, "30m" as Timeframe, klineLimit),
    staleTime: 1000 * 60 * 5,
    enabled: !!symbol
  });

  // 转换日线数据格式
  const dayChartData: KlineDataPoint[] = useMemo(() => {
    if (!dayData || !dayData.candles) return [];
    return dayData.candles.map(c => ({
      date: new Date(c.timestamp).toISOString().slice(0, 10).replace(/-/g, ''),
      open: c.open,
      high: c.high,
      low: c.low,
      close: c.close,
      volume: c.volume || 0,
    }));
  }, [dayData]);

  // 转换30分钟数据格式 - 使用时间戳确保唯一性
  const mins30ChartData: KlineDataPoint[] = useMemo(() => {
    if (!mins30Data || !mins30Data.candles) return [];
    return mins30Data.candles.map(c => ({
      date: String(Math.floor(new Date(c.timestamp).getTime() / 1000) + 8 * 3600),
      open: c.open,
      high: c.high,
      low: c.low,
      close: c.close,
      volume: c.volume || 0,
    }));
  }, [mins30Data]);

  if (!symbol) {
    return <div className="stock-detail__loading">加载股票信息...</div>;
  }

  return (
    <div className="stock-detail">
      {/* 控制面板 */}
      <div className="stock-detail__controls">
        <div className="stock-detail__ma-buttons">
          {(["ma5", "ma10", "ma20", "ma30", "ma50"] as const).map((key) => (
            <button
              key={key}
              className={`stock-detail__ma-btn ${maConfig[key] ? "stock-detail__ma-btn--active" : ""}`}
              style={{ borderColor: maConfig[key] ? MA_COLORS[key] : undefined }}
              onClick={() => onMAConfigChange({ ...maConfig, [key]: !maConfig[key] })}
            >
              <span className="stock-detail__ma-dot" style={{ backgroundColor: MA_COLORS[key] }} />
              {key.toUpperCase()}
            </button>
          ))}
        </div>
        {/* K线数量控制 */}
        {onKlineLimitChange && (
          <div className="stock-detail__kline-control">
            <span className="stock-detail__kline-label">K线数量:</span>
            <input
              type="range"
              min="50"
              max="500"
              step="10"
              value={klineLimit}
              onChange={(e) => onKlineLimitChange(parseInt(e.target.value))}
              className="stock-detail__kline-slider"
            />
            <span className="stock-detail__kline-value">{klineLimit}</span>
            <div className="stock-detail__kline-presets">
              {[100, 200, 300, 500].map((v) => (
                <button
                  key={v}
                  className={`stock-detail__kline-preset ${klineLimit === v ? 'stock-detail__kline-preset--active' : ''}`}
                  onClick={() => onKlineLimitChange(v)}
                >
                  {v}
                </button>
              ))}
            </div>
          </div>
        )}
      </div>

      {/* 股票基本信息 */}
      <div className="stock-detail__info">
        <div className="stock-detail__header">
          <div>
            <h2 className="stock-detail__name">
              {symbol.name}
              {realtimePrice && (
                <span className={`stock-detail__realtime-price stock-detail__realtime-price--${realtimePrice.change >= 0 ? 'up' : 'down'}`}>
                  ¥{realtimePrice.price.toFixed(2)}
                  <span className="stock-detail__price-change">
                    {realtimePrice.change >= 0 ? '+' : ''}{realtimePrice.changePercent.toFixed(2)}%
                  </span>
                </span>
              )}
            </h2>
            <p className="stock-detail__ticker">
              {symbol.ticker}
              {realtimePrice && (
                <span className="stock-detail__live-badge">🔴 Live {realtimePrice.lastUpdate}</span>
              )}
            </p>
            <button
              className={`stock-detail__watchlist-btn ${isInWatchlist ? "stock-detail__watchlist-btn--added" : ""}`}
              onClick={() => {
                if (isInWatchlist) {
                  removeFromWatchlist.mutate();
                } else {
                  addToWatchlist.mutate();
                }
              }}
              disabled={addToWatchlist.isPending || removeFromWatchlist.isPending}
            >
              {addToWatchlist.isPending ? "添加中..." :
               removeFromWatchlist.isPending ? "移除中..." :
               isInWatchlist ? "✓ 已自选" : "➕ 加入自选"}
            </button>
          </div>
          <div className="stock-detail__metrics">
            {symbol.totalMv && (
              <div className="stock-detail__metric">
                <span className="stock-detail__metric-label">总市值</span>
                <span className="stock-detail__metric-value">
                  {(symbol.totalMv / 1e4).toFixed(0)}亿
                </span>
              </div>
            )}
            {symbol.circMv && (
              <div className="stock-detail__metric">
                <span className="stock-detail__metric-label">流通市值</span>
                <span className="stock-detail__metric-value">
                  {(symbol.circMv / 1e4).toFixed(0)}亿
                </span>
              </div>
            )}
            {symbol.peTtm !== null && symbol.peTtm !== undefined && (
              <div className="stock-detail__metric">
                <span className="stock-detail__metric-label">PE(TTM)</span>
                <span className="stock-detail__metric-value">
                  {symbol.peTtm.toFixed(2)}
                </span>
              </div>
            )}
            {symbol.pb !== null && symbol.pb !== undefined && (
              <div className="stock-detail__metric">
                <span className="stock-detail__metric-label">PB</span>
                <span className="stock-detail__metric-value">
                  {symbol.pb.toFixed(2)}
                </span>
              </div>
            )}
            {symbol.listDate && (
              <div className="stock-detail__metric">
                <span className="stock-detail__metric-label">上市日期</span>
                <span className="stock-detail__metric-value">
                  {symbol.listDate.slice(0, 4)}-{symbol.listDate.slice(4, 6)}-
                  {symbol.listDate.slice(6, 8)}
                </span>
              </div>
            )}
          </div>
        </div>

        {/* 分类信息 */}
        <div className="stock-detail__classifications">
          {symbol.industryLv1 && (
            <div className="stock-detail__classification-item">
              <span className="stock-detail__classification-label">行业</span>
              <span className="stock-detail__classification-value stock-detail__classification-value--industry">
                {symbol.industryLv1}
              </span>
            </div>
          )}
          {/* 赛道分类 */}
          <div className="stock-detail__classification-item">
            <span className="stock-detail__classification-label">赛道</span>
            <select
              className="stock-detail__sector-select"
              value={sectorData?.sector || ""}
              onChange={(e) => updateSector.mutate(e.target.value)}
              disabled={updateSector.isPending}
            >
              <option value="">未分类</option>
              {availableSectors.map((sector) => (
                <option key={sector} value={sector}>
                  {sector}
                </option>
              ))}
            </select>
            {updateSector.isPending && (
              <span className="stock-detail__sector-updating">保存中...</span>
            )}
          </div>
        </div>

        {/* 概念板块 - 使用ticker_to_concepts.csv的完整数据 */}
        {conceptsData && conceptsData.concepts && conceptsData.concepts.length > 0 && (
          <div className="stock-detail__concepts">
            <h3 className="stock-detail__concepts-title">
              概念板块 <span className="stock-detail__concepts-count">({conceptsData.count}个)</span>
            </h3>
            <div className="stock-detail__concepts-list">
              {conceptsData.concepts.map((concept: string) => (
                <span key={concept} className="stock-detail__concept-tag">
                  {concept}
                </span>
              ))}
            </div>
          </div>
        )}

        {/* 公司信息 */}
        {(symbol.introduction || symbol.mainBusiness || symbol.businessScope) && (
          <div className="stock-detail__company-info">
            <h3 className="stock-detail__section-title">公司信息</h3>

            {/* 基本信息 */}
            {(symbol.setupDate || symbol.regCapital || symbol.province || symbol.city || symbol.website) && (
              <div className="stock-detail__company-basic">
                {symbol.setupDate && (
                  <div className="stock-detail__company-item">
                    <span className="stock-detail__company-label">成立日期:</span>
                    <span className="stock-detail__company-value">{symbol.setupDate}</span>
                  </div>
                )}
                {symbol.regCapital && (
                  <div className="stock-detail__company-item">
                    <span className="stock-detail__company-label">注册资本:</span>
                    <span className="stock-detail__company-value">{symbol.regCapital.toFixed(0)}万元</span>
                  </div>
                )}
                {(symbol.province || symbol.city) && (
                  <div className="stock-detail__company-item">
                    <span className="stock-detail__company-label">公司地址:</span>
                    <span className="stock-detail__company-value">
                      {symbol.province}{symbol.city && symbol.province !== symbol.city ? ` ${symbol.city}` : ''}
                    </span>
                  </div>
                )}
                {symbol.chairman && (
                  <div className="stock-detail__company-item">
                    <span className="stock-detail__company-label">法人代表:</span>
                    <span className="stock-detail__company-value">{symbol.chairman}</span>
                  </div>
                )}
                {symbol.manager && (
                  <div className="stock-detail__company-item">
                    <span className="stock-detail__company-label">总经理:</span>
                    <span className="stock-detail__company-value">{symbol.manager}</span>
                  </div>
                )}
                {symbol.website && (
                  <div className="stock-detail__company-item">
                    <span className="stock-detail__company-label">公司网站:</span>
                    <a
                      href={symbol.website.startsWith('http') ? symbol.website : `http://${symbol.website}`}
                      target="_blank"
                      rel="noopener noreferrer"
                      className="stock-detail__company-link"
                    >
                      {symbol.website}
                    </a>
                  </div>
                )}
              </div>
            )}

            {/* 公司介绍 */}
            {symbol.introduction && (
              <div className="stock-detail__company-section">
                <h4 className="stock-detail__company-subtitle">公司介绍</h4>
                <p className="stock-detail__company-text">{symbol.introduction}</p>
              </div>
            )}

            {/* 主要业务 */}
            {symbol.mainBusiness && (
              <div className="stock-detail__company-section">
                <h4 className="stock-detail__company-subtitle">主要业务</h4>
                <p className="stock-detail__company-text">{symbol.mainBusiness}</p>
              </div>
            )}

            {/* 经营范围 */}
            {symbol.businessScope && (
              <div className="stock-detail__company-section">
                <h4 className="stock-detail__company-subtitle">经营范围</h4>
                <p className="stock-detail__company-text">{symbol.businessScope}</p>
              </div>
            )}
          </div>
        )}
      </div>

      {/* K线图 */}
      <div className="stock-detail__charts">
        <p className="stock-detail__zoom-hint">滚轮缩放 | 拖动平移</p>
        <div className="stock-detail__chart">
          {dayChartData.length > 0 ? (
            <KlineChart
              data={dayChartData}
              height={650}
              showVolume={true}
              showMACD={true}
              maConfig={maConfig}
              title="日线"
            />
          ) : (
            <div className="stock-detail__chart-loading">加载日线...</div>
          )}
        </div>
        <div className="stock-detail__chart">
          {mins30ChartData.length > 0 ? (
            <KlineChart
              data={mins30ChartData}
              height={650}
              showVolume={true}
              showMACD={true}
              maConfig={maConfig}
              title="30分钟"
            />
          ) : (
            <div className="stock-detail__chart-loading">加载30分钟...</div>
          )}
        </div>
      </div>
    </div>
  );
}
