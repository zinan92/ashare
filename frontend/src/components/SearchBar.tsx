import { useState, useRef, useEffect } from "react";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { apiFetch, REFRESH_INTERVALS } from "../utils/api";
import { getBoardInfo } from "../utils/boardUtils";

interface SearchResult {
  ticker: string;
  name: string;
  industry: string;
  market: string;
  inWatchlist: boolean;
  totalMv: number | null;
  peTtm: number | null;
}

interface Props {
  onSelectStock: (ticker: string) => void;
}

async function searchSymbols(query: string): Promise<SearchResult[]> {
  if (!query || query.length < 1) {
    return [];
  }
  const response = await apiFetch(
    `/api/symbols/search?q=${encodeURIComponent(query)}`
  );
  if (!response.ok) {
    throw new Error("Search failed");
  }
  return response.json();
}

async function addToWatchlist(ticker: string): Promise<void> {
  const response = await apiFetch(`/api/watchlist`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ ticker }),
  });
  if (!response.ok) {
    const err = await response.json();
    throw new Error(err.detail || "添加失败");
  }
}

export function SearchBar({ onSelectStock }: Props) {
  const [query, setQuery] = useState("");
  const [showResults, setShowResults] = useState(false);
  const searchRef = useRef<HTMLDivElement>(null);
  const queryClient = useQueryClient();

  const { data: results, isLoading } = useQuery({
    queryKey: ["search", query],
    queryFn: () => searchSymbols(query),
    enabled: query.length >= 1,
    staleTime: REFRESH_INTERVALS.symbols,
  });

  const addMutation = useMutation({
    mutationFn: addToWatchlist,
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["search"] });
      queryClient.invalidateQueries({ queryKey: ["watchlist"] });
      queryClient.invalidateQueries({ queryKey: ["symbols"] });
    },
  });

  // Close results when clicking outside
  useEffect(() => {
    function handleClickOutside(event: MouseEvent) {
      if (
        searchRef.current &&
        !searchRef.current.contains(event.target as Node)
      ) {
        setShowResults(false);
      }
    }

    document.addEventListener("mousedown", handleClickOutside);
    return () => {
      document.removeEventListener("mousedown", handleClickOutside);
    };
  }, []);

  const handleSelectStock = (ticker: string, inWatchlist: boolean) => {
    if (inWatchlist) {
      setShowResults(false);
      setQuery("");
      onSelectStock(ticker);
    }
  };

  const handleAddToWatchlist = (
    e: React.MouseEvent,
    ticker: string
  ) => {
    e.stopPropagation();
    addMutation.mutate(ticker);
  };

  return (
    <div className="search-bar" ref={searchRef}>
      <input
        type="text"
        className="search-bar__input"
        placeholder="搜索股票代码或名称..."
        value={query}
        onChange={(e) => {
          setQuery(e.target.value);
          setShowResults(true);
        }}
        onFocus={() => setShowResults(true)}
      />
      {showResults && query.length >= 1 && (
        <div className="search-bar__results">
          {isLoading && (
            <div className="search-bar__loading">搜索中...</div>
          )}
          {!isLoading && results && results.length === 0 && (
            <div className="search-bar__empty">未找到匹配的股票</div>
          )}
          {!isLoading && results && results.length > 0 && (
            <ul className="search-bar__list">
              {results.map((stock) => {
                const boardInfo = getBoardInfo(stock.ticker);
                return (
                  <li
                    key={stock.ticker}
                    className={`search-bar__item ${stock.inWatchlist ? "" : "search-bar__item--not-watched"}`}
                    onClick={() =>
                      handleSelectStock(stock.ticker, stock.inWatchlist)
                    }
                  >
                    <div className="search-bar__item-main">
                      <span className="search-bar__item-name">
                        {stock.name}
                        {boardInfo.label && (
                          <span
                            className={`board-tag ${boardInfo.className}`}
                          >
                            {boardInfo.label}
                          </span>
                        )}
                      </span>
                      <span className="search-bar__item-ticker">
                        {stock.ticker}
                      </span>
                    </div>
                    <div className="search-bar__item-info">
                      {stock.industry && (
                        <span className="search-bar__item-industry">
                          {stock.industry}
                        </span>
                      )}
                      {stock.totalMv && (
                        <span className="search-bar__item-mv">
                          {(stock.totalMv / 1e4).toFixed(0)}亿
                        </span>
                      )}
                      {stock.peTtm !== null &&
                        stock.peTtm !== undefined && (
                          <span className="search-bar__item-pe">
                            PE {stock.peTtm.toFixed(1)}
                          </span>
                        )}
                      {stock.inWatchlist ? (
                        <span className="search-bar__item-tag search-bar__item-tag--watched">
                          ✓ 自选
                        </span>
                      ) : (
                        <button
                          className="search-bar__add-btn"
                          onClick={(e) =>
                            handleAddToWatchlist(e, stock.ticker)
                          }
                          disabled={addMutation.isPending}
                        >
                          + 加自选
                        </button>
                      )}
                    </div>
                  </li>
                );
              })}
            </ul>
          )}
        </div>
      )}
    </div>
  );
}
