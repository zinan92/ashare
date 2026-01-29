import { useQuery } from "@tanstack/react-query";
import { apiFetch } from "../utils/api";
import "../styles/UpdateTimeIndicator.css";

interface UpdateTimesResponse {
  current_time: string;
  kline_times: {
    [key: string]: {
      symbol_type: string;
      timeframe: string;
      last_update: string | null;
    };
  };
  scheduled_jobs: {
    [key: string]: {
      description: string;
      next_run: string | null;
      applies_to: string[];
    };
  };
}

interface UpdateTimeIndicatorProps {
  section: "index" | "concept" | "watchlist" | "sector";
  timeframe?: "day" | "30m" | "both";
}

export function UpdateTimeIndicator({ section, timeframe = "both" }: UpdateTimeIndicatorProps) {
  const { data, isLoading } = useQuery<UpdateTimesResponse>({
    queryKey: ["update-times"],
    queryFn: async () => {
      const response = await apiFetch("/api/status/update-times");
      if (!response.ok) {
        throw new Error("Failed to fetch update times");
      }
      return response.json();
    },
    refetchInterval: 60000, // Refresh every minute
    staleTime: 30000,
  });

  if (isLoading || !data) {
    return null;
  }

  const formatTime = (isoString: string | null) => {
    if (!isoString) return "未知";
    const date = new Date(isoString);
    return date.toLocaleString("zh-CN", {
      month: "2-digit",
      day: "2-digit",
      hour: "2-digit",
      minute: "2-digit",
    });
  };

  const formatRelativeTime = (isoString: string | null) => {
    if (!isoString) return null;
    const date = new Date(isoString);
    const now = new Date(data.current_time);
    const diffMs = date.getTime() - now.getTime();
    const diffMins = Math.floor(diffMs / 60000);

    if (diffMins < 0) {
      const absMins = Math.abs(diffMins);
      if (absMins < 60) return `${absMins}分钟前`;
      const hours = Math.floor(absMins / 60);
      if (hours < 24) return `${hours}小时前`;
      return `${Math.floor(hours / 24)}天前`;
    } else {
      if (diffMins < 60) return `${diffMins}分钟后`;
      const hours = Math.floor(diffMins / 60);
      if (hours < 24) return `${hours}小时后`;
      return `${Math.floor(hours / 24)}天后`;
    }
  };

  // Determine which data keys to show based on section
  const sectionMap: { [key: string]: string } = {
    index: "index",
    concept: "concept",
    watchlist: "stock",
    sector: "stock",
  };

  const symbolType = sectionMap[section];

  const renderTimeframe = (tf: "day" | "30m") => {
    const key = `${symbolType}_${tf}`;
    const klineData = data.kline_times[key];
    const lastUpdate = klineData?.last_update;

    // Find next update time
    let nextUpdate: string | null = null;
    if (tf === "day") {
      nextUpdate = data.scheduled_jobs.daily_update?.next_run || null;
    } else if (tf === "30m") {
      nextUpdate = data.scheduled_jobs["30m_update"]?.next_run || null;
    }

    const relativeNext = formatRelativeTime(nextUpdate);
    const relativeLast = formatRelativeTime(lastUpdate);

    return (
      <div className="update-time-indicator__timeframe" key={tf}>
        <div className="update-time-indicator__times">
          <div className="update-time-indicator__time">
            <span className="update-time-indicator__label">最后更新:</span>
            <span className="update-time-indicator__value" title={lastUpdate || undefined}>
              {relativeLast || formatTime(lastUpdate)}
            </span>
          </div>
          {nextUpdate && (
            <div className="update-time-indicator__time">
              <span className="update-time-indicator__label">下次更新:</span>
              <span className="update-time-indicator__value update-time-indicator__value--next" title={nextUpdate}>
                {relativeNext || formatTime(nextUpdate)}
              </span>
            </div>
          )}
        </div>
      </div>
    );
  };

  return (
    <div className="update-time-indicator">
      {timeframe === "both" ? (
        <>
          {renderTimeframe("30m")}
          {renderTimeframe("day")}
        </>
      ) : (
        renderTimeframe(timeframe)
      )}
    </div>
  );
}
