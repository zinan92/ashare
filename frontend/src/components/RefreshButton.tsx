import { useState } from "react";
import { useQueryClient } from "@tanstack/react-query";
import { apiFetch } from "../utils/api";

interface RefreshButtonProps {
  onRefreshComplete?: () => void;
}

export function RefreshButton({ onRefreshComplete }: RefreshButtonProps) {
  const queryClient = useQueryClient();
  const [isRefreshing, setIsRefreshing] = useState(false);
  const [progress, setProgress] = useState(0);
  const [message, setMessage] = useState("");
  const [jobId, setJobId] = useState<string | null>(null);

  const handleRefresh = async () => {
    setIsRefreshing(true);
    setProgress(0);
    setMessage("刷新中...");

    try {
      const response = await apiFetch("/api/tasks/refresh", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({})
      });

      if (response.ok) {
        const data = await response.json();
        const newJobId = data.job_id;
        setJobId(newJobId);
        pollStatus(newJobId);
      } else {
        const data = await response.json();
        throw new Error(data.detail || "刷新失败");
      }
    } catch (error) {
      setMessage(`✗ ${error instanceof Error ? error.message : String(error)}`);
      setTimeout(() => setMessage(""), 5000);
    }
  };

  const pollStatus = async (id: string) => {
    const timer = setInterval(async () => {
      try {
        const res = await apiFetch(`/api/tasks/refresh/${id}`);
        if (!res.ok) {
          throw new Error("状态查询失败");
        }
        const data = await res.json();
        setProgress(typeof data.progress === "number" ? data.progress : 0);
        if (data.status === "completed") {
          clearInterval(timer);
          setMessage("✓ 刷新完成");
          setIsRefreshing(false);

          // Invalidate all queries to refetch fresh data
          queryClient.invalidateQueries();

          onRefreshComplete?.();
          setTimeout(() => setMessage(""), 3000);
        } else if (data.status === "failed") {
          clearInterval(timer);
          setMessage(data.message || "刷新失败");
          setIsRefreshing(false);
        }
      } catch (err) {
        clearInterval(timer);
        setMessage(`✗ ${err instanceof Error ? err.message : String(err)}`);
        setIsRefreshing(false);
      }
    }, 1000);
  };

  return (
    <div className="refresh-button-container">
      <button
        onClick={handleRefresh}
        disabled={isRefreshing}
        className={`topbar__button topbar__button--secondary ${isRefreshing ? 'topbar__button--loading' : ''}`}
        title="刷新所有数据"
      >
        {isRefreshing ? "刷新中..." : "🔄 刷新数据"}
      </button>
      {isRefreshing && (
        <div className="refresh-progress">
          <div
            className="refresh-progress__bar"
            style={{ width: `${progress}%` }}
          />
          <span className="refresh-progress__text">
            {progress}% {message || ""}
          </span>
        </div>
      )}
      {message && (
        <span className={`refresh-message ${message.startsWith("✓") ? 'refresh-message--success' : message.startsWith("✗") ? 'refresh-message--error' : ''}`}>
          {message}
        </span>
      )}
    </div>
  );
}
