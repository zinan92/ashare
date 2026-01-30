/**
 * 统一的 K线图组件 (使用 Lightweight Charts)
 *
 * 功能：
 * - K线图 (蜡烛图)
 * - 均线 (MA5, MA10, MA20, MA50)
 * - 成交量柱状图
 * - MACD 指标 (DIF, DEA, MACD柱)
 * - 支持缩放和平移
 */

import { useEffect, useRef, useState, useMemo } from "react";
import {
  createChart,
  CandlestickSeries,
  HistogramSeries,
  LineSeries,
  type IChartApi,
  type CandlestickData,
  type HistogramData,
  type LineData,
  type Time,
  type ISeriesApi,
} from "lightweight-charts";
import { calculateMA, calculateMACD, type MACDResult } from "../../utils/indicators";
import type { MAConfig } from "../../types/chartConfig";
import { MA_COLORS } from "../../types/chartConfig";

// K线数据格式
export interface KlineDataPoint {
  date: string;       // 日期 "YYYYMMDD" 或 "YYYYMMDD HHmm"
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
  amount?: number;
}

// 组件属性
export interface KlineChartProps {
  // 数据
  data: KlineDataPoint[];

  // 配置
  height?: number;
  showVolume?: boolean;
  showMACD?: boolean;
  maConfig?: MAConfig;

  // 样式
  title?: string;
  backgroundColor?: string;

  // 布局模式
  compact?: boolean;  // 紧凑模式，用于卡片
}

// 颜色常量
const COLORS = {
  up: "#ef5f7c",           // 上涨红色
  down: "#23c19f",         // 下跌绿色
  upTransparent: "transparent",  // 上涨空心
  grid: "rgba(255, 255, 255, 0.05)",
  text: "#8f9bbd",
  dif: "#f5d05e",          // DIF 黄色
  dea: "#67c8ff",          // DEA 蓝色
  volumeMa: "#f5d05e",     // 成交量均线
};

// 解析日期字符串为 Time 格式
function parseDate(dateStr: string): Time {
  const str = dateStr.toString();

  // 检查是否是 Unix 时间戳（纯数字且长度为10位左右）
  if (/^\d{9,11}$/.test(str)) {
    return parseInt(str, 10) as Time;
  }

  // 如果已经是 YYYY-MM-DD 格式，直接返回
  if (/^\d{4}-\d{2}-\d{2}$/.test(str)) {
    return str as Time;
  }

  // 处理 YYYYMMDD 格式 (8位数字，如 "20260129")
  if (/^\d{8}$/.test(str)) {
    const year = str.slice(0, 4);
    const month = str.slice(4, 6);
    const day = str.slice(6, 8);
    return `${year}-${month}-${day}` as Time;
  }

  // 其他格式：移除非数字字符后取前8位
  const cleanDate = str.replace(/\D/g, '').slice(0, 8);
  if (cleanDate.length === 8) {
    const year = cleanDate.slice(0, 4);
    const month = cleanDate.slice(4, 6);
    const day = cleanDate.slice(6, 8);
    return `${year}-${month}-${day}` as Time;
  }

  // 如果都不匹配，返回原始字符串
  console.warn(`Unable to parse date: ${str}`);
  return str as Time;
}

export function KlineChart({
  data,
  height = 400,
  showVolume = true,
  showMACD = true,
  maConfig = { ma5: true, ma10: true, ma20: false, ma30: false, ma50: false },
  title,
  backgroundColor = "#161b2b",
  compact = false,
}: KlineChartProps) {
  const containerRef = useRef<HTMLDivElement>(null);
  const chartRef = useRef<IChartApi | null>(null);
  const seriesRef = useRef<{
    candle?: ISeriesApi<"Candlestick">;
    ma5?: ISeriesApi<"Line">;
    ma10?: ISeriesApi<"Line">;
    ma20?: ISeriesApi<"Line">;
    ma30?: ISeriesApi<"Line">;
    ma50?: ISeriesApi<"Line">;
    volume?: ISeriesApi<"Histogram">;
    volumeMa5?: ISeriesApi<"Line">;
    macdHist?: ISeriesApi<"Histogram">;
    dif?: ISeriesApi<"Line">;
    dea?: ISeriesApi<"Line">;
  }>({});
  const [chartReady, setChartReady] = useState(false);

  // 计算布局区域
  const layout = useMemo(() => {
    if (compact) {
      // 紧凑模式：K线 60%, 成交量 20%, MACD 20%
      return {
        candle: { top: 0.02, bottom: showMACD ? 0.40 : (showVolume ? 0.25 : 0.02) },
        volume: { top: showMACD ? 0.62 : 0.78, bottom: showMACD ? 0.22 : 0.02 },
        macd: { top: 0.80, bottom: 0.02 },
      };
    } else {
      // 标准模式：K线 50%, 成交量 15%, MACD 33%
      return {
        candle: { top: 0.02, bottom: showMACD ? 0.50 : (showVolume ? 0.25 : 0.02) },
        volume: { top: showMACD ? 0.52 : 0.78, bottom: showMACD ? 0.35 : 0.02 },
        macd: { top: 0.67, bottom: 0.02 },
      };
    }
  }, [compact, showVolume, showMACD]);

  // 初始化图表
  useEffect(() => {
    const container = containerRef.current;
    if (!container) return;

    let chart: IChartApi | null = null;
    let resizeObserver: ResizeObserver | null = null;

    const initChart = () => {
      const { width, height: containerHeight } = container.getBoundingClientRect();
      if (width === 0 || containerHeight === 0) return;

      chart = createChart(container, {
        width,
        height: containerHeight,
        layout: {
          background: { color: backgroundColor },
          textColor: COLORS.text,
          fontSize: 10,
        },
        grid: {
          vertLines: { color: COLORS.grid },
          horzLines: { color: COLORS.grid },
        },
        crosshair: {
          mode: 1, // Normal mode
        },
        rightPriceScale: {
          borderColor: COLORS.grid,
          minimumWidth: 1,
          borderVisible: false,
        },
        timeScale: {
          borderColor: COLORS.grid,
          timeVisible: true,
        },
        handleScroll: {
          mouseWheel: true,
          pressedMouseMove: true,
          horzTouchDrag: true,
          vertTouchDrag: true,
        },
        handleScale: {
          axisPressedMouseMove: true,
          mouseWheel: true,
          pinch: true,
        },
      });

      chartRef.current = chart;

      // K线 series
      const candleSeries = chart.addSeries(CandlestickSeries, {
        upColor: COLORS.upTransparent,
        downColor: COLORS.down,
        borderUpColor: COLORS.up,
        borderDownColor: COLORS.down,
        wickUpColor: COLORS.up,
        wickDownColor: COLORS.down,
        priceFormat: {
          type: "price",
          precision: 2,
          minMove: 0.01,
        },
      });
      candleSeries.priceScale().applyOptions({
        scaleMargins: layout.candle,
      });

      // 均线 series
      const ma5Series = chart.addSeries(LineSeries, {
        color: MA_COLORS.ma5,
        lineWidth: 1,
        priceLineVisible: false,
        lastValueVisible: false,
      });
      const ma10Series = chart.addSeries(LineSeries, {
        color: MA_COLORS.ma10,
        lineWidth: 1,
        priceLineVisible: false,
        lastValueVisible: false,
      });
      const ma20Series = chart.addSeries(LineSeries, {
        color: MA_COLORS.ma20,
        lineWidth: 1,
        priceLineVisible: false,
        lastValueVisible: false,
      });
      const ma30Series = chart.addSeries(LineSeries, {
        color: MA_COLORS.ma30,
        lineWidth: 1,
        priceLineVisible: false,
        lastValueVisible: false,
      });
      const ma50Series = chart.addSeries(LineSeries, {
        color: MA_COLORS.ma50,
        lineWidth: 1,
        priceLineVisible: false,
        lastValueVisible: false,
      });

      // 成交量 series
      let volumeSeries: ISeriesApi<"Histogram"> | undefined;
      let volumeMa5Series: ISeriesApi<"Line"> | undefined;
      if (showVolume) {
        volumeSeries = chart.addSeries(HistogramSeries, {
          priceFormat: { type: "volume" },
          priceScaleId: "volume",
        });
        volumeSeries.priceScale().applyOptions({
          scaleMargins: layout.volume,
        });

        volumeMa5Series = chart.addSeries(LineSeries, {
          color: COLORS.volumeMa,
          lineWidth: 1,
          priceScaleId: "volume",
          priceLineVisible: false,
          lastValueVisible: false,
        });
      }

      // MACD series
      let macdHistSeries: ISeriesApi<"Histogram"> | undefined;
      let difSeries: ISeriesApi<"Line"> | undefined;
      let deaSeries: ISeriesApi<"Line"> | undefined;
      if (showMACD) {
        macdHistSeries = chart.addSeries(HistogramSeries, {
          priceScaleId: "macd",
          priceLineVisible: false,
          lastValueVisible: false,
        });
        macdHistSeries.priceScale().applyOptions({
          scaleMargins: layout.macd,
        });

        difSeries = chart.addSeries(LineSeries, {
          color: COLORS.dif,
          lineWidth: 1,
          priceScaleId: "macd",
          priceLineVisible: false,
          lastValueVisible: false,
        });

        deaSeries = chart.addSeries(LineSeries, {
          color: COLORS.dea,
          lineWidth: 1,
          priceScaleId: "macd",
          priceLineVisible: false,
          lastValueVisible: false,
        });
      }

      seriesRef.current = {
        candle: candleSeries,
        ma5: ma5Series,
        ma10: ma10Series,
        ma20: ma20Series,
        ma30: ma30Series,
        ma50: ma50Series,
        volume: volumeSeries,
        volumeMa5: volumeMa5Series,
        macdHist: macdHistSeries,
        dif: difSeries,
        dea: deaSeries,
      };

      setChartReady(true);
    };

    const handleResize = () => {
      const { width, height: containerHeight } = container.getBoundingClientRect();
      if (width > 0 && containerHeight > 0) {
        if (!chart) {
          initChart();
        } else {
          chart.applyOptions({ width, height: containerHeight });
        }
      }
    };

    initChart();

    resizeObserver = new ResizeObserver(handleResize);
    resizeObserver.observe(container);

    return () => {
      resizeObserver?.disconnect();
      if (chart) {
        chart.remove();
        chartRef.current = null;
        seriesRef.current = {};
        setChartReady(false);
      }
    };
  }, [backgroundColor, showVolume, showMACD, layout]);

  // 更新数据
  useEffect(() => {
    const s = seriesRef.current;
    if (!chartReady || !s.candle || !data || data.length === 0) return;

    // 准备数据
    const closes = data.map(d => d.close);
    const volumes = data.map(d => d.volume);

    // K线数据
    const candleData: CandlestickData<Time>[] = data.map(d => ({
      time: parseDate(d.date),
      open: d.open,
      high: d.high,
      low: d.low,
      close: d.close,
    }));
    s.candle.setData(candleData);

    // 计算并设置均线数据
    const buildLineData = (values: (number | null)[]): LineData<Time>[] =>
      data
        .map((d, i) => ({ time: parseDate(d.date), value: values[i] }))
        .filter((item): item is LineData<Time> => item.value !== null);

    const ma5Values = calculateMA(closes, 5);
    const ma10Values = calculateMA(closes, 10);
    const ma20Values = calculateMA(closes, 20);
    const ma30Values = calculateMA(closes, 30);
    const ma50Values = calculateMA(closes, 50);

    if (s.ma5) s.ma5.setData(maConfig.ma5 ? buildLineData(ma5Values) : []);
    if (s.ma10) s.ma10.setData(maConfig.ma10 ? buildLineData(ma10Values) : []);
    if (s.ma20) s.ma20.setData(maConfig.ma20 ? buildLineData(ma20Values) : []);
    if (s.ma30) s.ma30.setData(maConfig.ma30 ? buildLineData(ma30Values) : []);
    if (s.ma50) s.ma50.setData(maConfig.ma50 ? buildLineData(ma50Values) : []);

    // 成交量数据
    if (showVolume && s.volume) {
      const volumeData: HistogramData<Time>[] = data.map((d, i) => ({
        time: parseDate(d.date),
        value: d.volume,
        color: d.close >= d.open
          ? "rgba(239, 95, 124, 0.5)"
          : "rgba(35, 193, 159, 0.5)",
      }));
      s.volume.setData(volumeData);

      // 成交量 MA5
      if (s.volumeMa5) {
        const volMa5Values = calculateMA(volumes, 5);
        s.volumeMa5.setData(buildLineData(volMa5Values));
      }
    }

    // MACD 数据
    if (showMACD && s.macdHist && s.dif && s.dea) {
      const macdResult = calculateMACD(closes);

      const macdHistData: HistogramData<Time>[] = data
        .map((d, i) => {
          const value = macdResult.macd[i];
          if (value === null) return null;
          return {
            time: parseDate(d.date),
            value: value,
            color: value >= 0 ? "rgba(239, 95, 124, 0.8)" : "rgba(35, 193, 159, 0.8)",
          } as HistogramData<Time>;
        })
        .filter((item): item is HistogramData<Time> => item !== null);

      s.macdHist.setData(macdHistData);
      s.dif.setData(buildLineData(macdResult.dif));
      s.dea.setData(buildLineData(macdResult.dea));
    }

    // 适应可见范围
    chartRef.current?.timeScale().fitContent();
  }, [data, chartReady, maConfig, showVolume, showMACD]);

  return (
    <div className="kline-chart" style={{ position: "relative" }}>
      {title && (
        <div
          className="kline-chart__title"
          style={{
            position: "absolute",
            top: 8,
            left: "50%",
            transform: "translateX(-50%)",
            color: "#b8c2e1",
            fontSize: compact ? 12 : 14,
            fontWeight: 600,
            zIndex: 10,
          }}
        >
          {title}
        </div>
      )}
      <div ref={containerRef} style={{ width: "100%", height }} />
    </div>
  );
}

export default KlineChart;
