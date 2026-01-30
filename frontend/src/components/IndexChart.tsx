/**
 * 指数图表组件 - 双K线布局（日线 + 30分钟）
 * 使用统一的 KlineChart 组件
 */

import { useMemo, useState, useEffect } from "react";
import { useQuery } from "@tanstack/react-query";
import { apiFetch, REFRESH_INTERVALS } from "../utils/api";
import type { MAConfig } from "../types/chartConfig";
import { KlineChart, type KlineDataPoint } from "./charts";
import { UpdateTimeIndicator } from "./UpdateTimeIndicator";
import { isMarketOpen } from "../hooks/useRealtimePrice";

interface KlineData {
  date: string;
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
  amount: number;
  dif: number;
  dea: number;
  macd: number;
}

interface Kline30mData {
  datetime: number;
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
  amount: number;
  dif: number;
  dea: number;
  macd: number;
}

interface IndexKlineResponse {
  ts_code: string;
  name: string;
  count: number;
  latest: {
    date: string;
    close: number;
    open: number;
    high: number;
    low: number;
    change: number;
    change_pct: number;
    volume: number;
    amount: number;
  };
  klines: KlineData[];
}

interface IndexKline30mResponse {
  ts_code: string;
  name: string;
  count: number;
  klines: Kline30mData[];
}

interface IndexRealtimeResponse {
  ts_code: string;
  name: string;
  price: number;
  change: number;
  change_pct: number;
  volume: number;
  amount: number;
  last_update: string;
}

interface IndexQuoteResponse {
  ts_code: string;
  name: string;
  trade_date: string;
  price: {
    close: number;
    open: number;
    high: number;
    low: number;
    prev_close: number;
    change: number;
    change_pct: number;
    amplitude: number;
  };
  volume: {
    vol: number;
    amount: number;
  };
  indicators: {
    total_mv: number | null;
    float_mv: number | null;
    pe: number | null;
    pe_ttm: number | null;
    pb: number | null;
    turnover_rate: number | null;
  };
  market_stats: {
    up_count: number;
    down_count: number;
    flat_count: number;
  };
}

async function fetchIndexKline(tsCode: string): Promise<IndexKlineResponse> {
  const response = await apiFetch(`/api/index/kline/${tsCode}?limit=120`);
  if (!response.ok) throw new Error("Failed to fetch index kline");
  return response.json();
}

async function fetchIndexKline30m(tsCode: string): Promise<IndexKline30mResponse> {
  const response = await apiFetch(`/api/index/kline30m/${tsCode}?limit=120`);
  if (!response.ok) throw new Error("Failed to fetch index 30m kline");
  return response.json();
}

async function fetchIndexRealtime(tsCode: string): Promise<IndexRealtimeResponse> {
  const response = await apiFetch(`/api/index/realtime/${tsCode}`);
  if (!response.ok) throw new Error("Failed to fetch index realtime");
  return response.json();
}

async function fetchIndexQuote(tsCode: string): Promise<IndexQuoteResponse> {
  const response = await apiFetch(`/api/index/quote/${tsCode}`);
  if (!response.ok) throw new Error("Failed to fetch index quote");
  return response.json();
}

interface Props {
  tsCode?: string;
  maConfig: MAConfig;
  onMAConfigChange: (config: MAConfig) => void;
  /** Hide the indicator tables (成交/价格/涨跌) below the chart */
  hideIndicators?: boolean;
}

export function IndexChart({ tsCode = "000001.SH", maConfig, hideIndicators = false }: Props) {
  const [lastUpdate, setLastUpdate] = useState<string>("");

  // 获取实时数据（收盘后停止轮询）
  const { data: realtimeData } = useQuery({
    queryKey: ["index-realtime", tsCode],
    queryFn: () => fetchIndexRealtime(tsCode),
    staleTime: 1000 * 30,
    refetchInterval: isMarketOpen() ? 1000 * 30 : false, // 收盘后停止轮询
  });

  // 获取日线数据（收盘后停止轮询）
  const { data: klineData, isLoading: klineLoading } = useQuery({
    queryKey: ["index-kline", tsCode],
    queryFn: () => fetchIndexKline(tsCode),
    staleTime: REFRESH_INTERVALS.boards,
    refetchInterval: isMarketOpen() ? REFRESH_INTERVALS.boards : false, // 收盘后停止轮询
  });

  // 获取30分钟数据（收盘后停止轮询）
  const { data: kline30mData } = useQuery({
    queryKey: ["index-kline30m", tsCode],
    queryFn: () => fetchIndexKline30m(tsCode),
    staleTime: 1000 * 60 * 5,
    refetchInterval: isMarketOpen() ? 1000 * 60 * 5 : false, // 收盘后停止轮询
  });

  // 获取行情详情（收盘后停止轮询）
  const { data: quoteData } = useQuery({
    queryKey: ["index-quote", tsCode],
    queryFn: () => fetchIndexQuote(tsCode),
    staleTime: REFRESH_INTERVALS.boards,
    refetchInterval: isMarketOpen() ? REFRESH_INTERVALS.boards : false, // 收盘后停止轮询
  });

  // 上证指数时获取深证和北证数据计算三市成交额
  const isShanghai = tsCode === "000001.SH";

  const { data: szQuote } = useQuery({
    queryKey: ["index-quote", "399001.SZ"],
    queryFn: () => fetchIndexQuote("399001.SZ"),
    staleTime: REFRESH_INTERVALS.boards,
    enabled: isShanghai,
  });

  const { data: bjQuote } = useQuery({
    queryKey: ["index-quote", "899050.BJ"],
    queryFn: () => fetchIndexQuote("899050.BJ"),
    staleTime: REFRESH_INTERVALS.boards,
    enabled: isShanghai,
  });

  // 获取深证和北证 kline 用于计算上日三市成交额
  const { data: szKline } = useQuery({
    queryKey: ["index-kline", "399001.SZ"],
    queryFn: () => fetchIndexKline("399001.SZ"),
    staleTime: REFRESH_INTERVALS.boards,
    enabled: isShanghai,
  });

  const { data: bjKline } = useQuery({
    queryKey: ["index-kline", "899050.BJ"],
    queryFn: () => fetchIndexKline("899050.BJ"),
    staleTime: REFRESH_INTERVALS.boards,
    enabled: isShanghai,
  });

  // 更新时间戳
  useEffect(() => {
    if (realtimeData) {
      setLastUpdate(realtimeData.last_update);
    } else {
      setLastUpdate(new Date().toLocaleTimeString('zh-CN'));
    }
  }, [realtimeData, klineData, kline30mData]);

  // 计算三市成交额 (API 返回单位是千万，除以 10 得到亿)
  const threeMarketAmount = isShanghai && quoteData && szQuote && bjQuote
    ? (quoteData.volume.amount + szQuote.volume.amount + bjQuote.volume.amount) / 10
    : null;

  // 获取上日成交额
  const getYesterdayAmount = (klines: KlineData[] | undefined) => {
    if (!klines || klines.length < 2) return null;
    return klines[klines.length - 2].amount;
  };

  const yesterdayAmount = klineData ? getYesterdayAmount(klineData.klines) : null;

  // 上日三市成交额
  const threeMarketYesterdayAmount = isShanghai && klineData && szKline && bjKline
    ? ((getYesterdayAmount(klineData.klines) ?? 0) +
       (getYesterdayAmount(szKline.klines) ?? 0) +
       (getYesterdayAmount(bjKline.klines) ?? 0)) / 100000
    : null;

  // 今日涨跌幅（优先使用实时数据）
  const { price, change, changePct, prevDayChangePct } = useMemo(() => {
    if (realtimeData) {
      let prevDayChangePct = null;
      if (klineData && klineData.klines.length >= 3) {
        const klines = klineData.klines;
        const prevClose = klines[klines.length - 2].close;
        const prev2Close = klines[klines.length - 3].close;
        prevDayChangePct = ((prevClose - prev2Close) / prev2Close) * 100;
      }
      return {
        price: realtimeData.price,
        change: realtimeData.change,
        changePct: realtimeData.change_pct,
        prevDayChangePct
      };
    }

    if (klineData && klineData.klines.length >= 2) {
      const klines = klineData.klines;
      const lastClose = klines[klines.length - 1].close;
      const prevClose = klines[klines.length - 2].close;
      const change = lastClose - prevClose;
      const changePct = (change / prevClose) * 100;

      let prevDayChangePct = null;
      if (klines.length >= 3) {
        const prev2Close = klines[klines.length - 3].close;
        prevDayChangePct = ((prevClose - prev2Close) / prev2Close) * 100;
      }

      return { price: lastClose, change, changePct, prevDayChangePct };
    }

    return { price: 0, change: 0, changePct: 0, prevDayChangePct: null };
  }, [realtimeData, klineData]);

  const isPositive = changePct >= 0;

  // 转换日线数据格式（含今日合成K线）
  const dailyChartData: KlineDataPoint[] = useMemo(() => {
    if (!klineData || !klineData.klines) return [];
    const base = klineData.klines.map(k => ({
      date: String(k.date),
      open: k.open,
      high: k.high,
      low: k.low,
      close: k.close,
      volume: k.volume,
    }));

    // 用实时/行情数据合成今日K线（盘中日线还没入库时）
    if (quoteData && quoteData.price && quoteData.price.open > 0) {
      const todayDate = quoteData.trade_date || new Date().toISOString().slice(0, 10).replace(/-/g, "");
      const lastDate = base.length > 0 ? base[base.length - 1].date : "";
      if (todayDate !== lastDate) {
        base.push({
          date: todayDate,
          open: quoteData.price.open,
          high: quoteData.price.high,
          low: quoteData.price.low,
          close: realtimeData?.price ?? quoteData.price.close,
          volume: quoteData.volume?.vol ?? 0,
        });
      }
    }

    return base;
  }, [klineData, quoteData, realtimeData]);

  // 转换30分钟数据格式
  const mins30ChartData: KlineDataPoint[] = useMemo(() => {
    if (!kline30mData || !kline30mData.klines) return [];
    return kline30mData.klines.map(k => ({
      date: String(k.datetime), // Unix时间戳，parseDate会处理
      open: k.open,
      high: k.high,
      low: k.low,
      close: k.close,
      volume: k.volume,
    }));
  }, [kline30mData]);

  if (klineLoading) {
    return (
      <div className="index-chart index-chart--loading">
        <div className="index-chart__spinner">加载中...</div>
      </div>
    );
  }

  return (
    <div className="index-chart">
      {/* Header */}
      <div className="index-chart__header">
        <div className="index-chart__title">
          <span className="index-chart__name">{klineData?.name || realtimeData?.name || "指数"}</span>
        </div>
        <div className="index-chart__meta">
          <span className={`index-chart__price ${isPositive ? "index-chart__price--up" : "index-chart__price--down"}`}>
            {price.toFixed(2)}
          </span>
          <span className={`index-chart__change ${isPositive ? "index-chart__change--up" : "index-chart__change--down"}`}>
            今 {isPositive ? "+" : ""}{changePct.toFixed(2)}%
          </span>
          {prevDayChangePct !== null && (
            <span className={`index-chart__prev-change ${prevDayChangePct >= 0 ? "index-chart__prev-change--up" : "index-chart__prev-change--down"}`}>
              昨 {prevDayChangePct >= 0 ? "+" : ""}{prevDayChangePct.toFixed(2)}%
            </span>
          )}
          <span className="index-chart__live">🔴 {lastUpdate}</span>
        </div>
      </div>

      {/* 双K线图区域 */}
      <div className="index-chart__charts">
        <div className="index-chart__chart-container">
          <div className="index-chart__chart-header">
            <div className="index-chart__chart-label">日线</div>
            <UpdateTimeIndicator section="index" timeframe="day" />
          </div>
          <div className="index-chart__chart">
            {dailyChartData.length > 0 ? (
              <KlineChart
                data={dailyChartData}
                height={280}
                showVolume={true}
                showMACD={true}
                maConfig={maConfig}
                compact={true}
              />
            ) : (
              <div className="index-chart__loading">加载日线...</div>
            )}
          </div>
        </div>
        <div className="index-chart__chart-container">
          <div className="index-chart__chart-header">
            <div className="index-chart__chart-label">30分钟</div>
            <UpdateTimeIndicator section="index" timeframe="30m" />
          </div>
          <div className="index-chart__chart">
            {mins30ChartData.length > 0 ? (
              <KlineChart
                data={mins30ChartData}
                height={280}
                showVolume={true}
                showMACD={true}
                maConfig={maConfig}
                compact={true}
              />
            ) : (
              <div className="index-chart__loading">加载30分钟...</div>
            )}
          </div>
        </div>
      </div>

      {/* 指标数据区域（放在K线下方） */}
      {quoteData && !hideIndicators && (
        <div className="index-chart__indicators">
          <div className="index-chart__indicator-group">
            <h4 className="index-chart__group-title">成交数据</h4>
            <div className="index-chart__indicator">
              <span className="index-chart__indicator-label">
                {isShanghai ? "三市成交额" : "成交额"}
              </span>
              <span className="index-chart__indicator-value">
                {isShanghai && threeMarketAmount !== null
                  ? threeMarketAmount.toFixed(0)
                  : (quoteData.volume.amount / 10).toFixed(0)}亿
              </span>
            </div>
            <div className="index-chart__indicator">
              <span className="index-chart__indicator-label">
                {isShanghai ? "上日三市" : "上日成交额"}
              </span>
              <span className="index-chart__indicator-value">
                {isShanghai && threeMarketYesterdayAmount !== null
                  ? threeMarketYesterdayAmount.toFixed(0)
                  : yesterdayAmount !== null
                    ? (yesterdayAmount / 100000).toFixed(0)
                    : "-"}亿
              </span>
            </div>
          </div>

          <div className="index-chart__indicator-group">
            <h4 className="index-chart__group-title">价格数据</h4>
            <div className="index-chart__indicator">
              <span className="index-chart__indicator-label">昨收</span>
              <span className="index-chart__indicator-value">{quoteData.price.prev_close.toFixed(2)}</span>
            </div>
            <div className="index-chart__indicator">
              <span className="index-chart__indicator-label">开盘</span>
              <span className="index-chart__indicator-value">{quoteData.price.open.toFixed(2)}</span>
            </div>
            <div className="index-chart__indicator">
              <span className="index-chart__indicator-label">最高</span>
              <span className="index-chart__indicator-value index-chart__indicator-value--up">{quoteData.price.high.toFixed(2)}</span>
            </div>
            <div className="index-chart__indicator">
              <span className="index-chart__indicator-label">最低</span>
              <span className="index-chart__indicator-value index-chart__indicator-value--down">{quoteData.price.low.toFixed(2)}</span>
            </div>
          </div>

          <div className="index-chart__indicator-group">
            <h4 className="index-chart__group-title">涨跌统计</h4>
            <div className="index-chart__indicator">
              <span className="index-chart__indicator-label">上涨</span>
              <span className="index-chart__indicator-value index-chart__indicator-value--up">
                {quoteData.market_stats.up_count}
              </span>
            </div>
            <div className="index-chart__indicator">
              <span className="index-chart__indicator-label">平盘</span>
              <span className="index-chart__indicator-value">
                {quoteData.market_stats.flat_count}
              </span>
            </div>
            <div className="index-chart__indicator">
              <span className="index-chart__indicator-label">下跌</span>
              <span className="index-chart__indicator-value index-chart__indicator-value--down">
                {quoteData.market_stats.down_count}
              </span>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
