"""
新闻快讯 API 路由
"""
from typing import List, Optional, Dict, Any
from fastapi import APIRouter, Query, HTTPException
from pydantic import BaseModel

from src.services.news import get_news_service, get_news_aggregator, get_alerts_service, get_external_service, get_smart_alert_system

router = APIRouter()


class NewsItem(BaseModel):
    source: str
    source_name: str
    title: str
    content: str
    time: str
    url: str = ""
    is_new: bool = False
    hash: str = ""


class NewsResponse(BaseModel):
    count: int
    news: List[NewsItem]


class StatsResponse(BaseModel):
    total_seen: int
    history_size: int
    keywords: List[str]
    exclude_keywords: List[str]


@router.get("/latest", response_model=NewsResponse)
async def get_latest_news(
    sources: str = Query("cls,ths", description="数据源，逗号分隔 (cls/ths/futu)"),
    limit: int = Query(30, ge=1, le=100, description="返回条数"),
    only_new: bool = Query(False, description="仅返回新消息"),
):
    """
    获取最新快讯
    
    - **sources**: 数据源，支持 cls(财联社), ths(同花顺), futu(富途)
    - **limit**: 返回条数限制
    - **only_new**: 仅返回未见过的新消息
    """
    aggregator = get_news_aggregator()
    source_list = [s.strip() for s in sources.split(',') if s.strip()]
    
    news = aggregator.fetch_latest(
        sources=source_list,
        limit=limit,
        only_new=only_new,
    )
    
    return NewsResponse(count=len(news), news=news)


@router.get("/alerts", response_model=NewsResponse)
async def get_news_alerts():
    """
    获取新快讯提醒（仅返回未见过的消息）
    适合定时轮询
    """
    aggregator = get_news_aggregator()
    news = aggregator.get_new_alerts()
    
    return NewsResponse(count=len(news), news=news)


@router.get("/source/{source}", response_model=NewsResponse)
async def get_news_by_source(
    source: str,
    limit: int = Query(20, ge=1, le=100),
):
    """
    获取指定数据源的快讯
    
    - **source**: cls / ths / futu
    """
    service = get_news_service()
    
    try:
        news = service.fetch_news(source=source, limit=limit)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    
    return NewsResponse(count=len(news), news=news)


@router.get("/stock/{symbol}", response_model=NewsResponse)
async def get_stock_news(
    symbol: str,
    limit: int = Query(20, ge=1, le=50),
):
    """
    获取个股新闻（东方财富）
    
    - **symbol**: 股票代码，如 000001, 600000
    """
    service = get_news_service()
    news = service.fetch_stock_news(symbol=symbol, limit=limit)
    
    return NewsResponse(count=len(news), news=news)


@router.post("/keywords")
async def set_keywords(
    include: Optional[List[str]] = None,
    exclude: Optional[List[str]] = None,
):
    """
    设置关键词过滤
    
    - **include**: 包含关键词列表（任一匹配即通过）
    - **exclude**: 排除关键词列表（任一匹配即排除）
    """
    aggregator = get_news_aggregator()
    aggregator.set_keywords(include=include, exclude=exclude)
    
    return {
        "status": "ok",
        "keywords": aggregator._keywords,
        "exclude_keywords": aggregator._exclude_keywords,
    }


@router.get("/stats", response_model=StatsResponse)
async def get_news_stats():
    """获取新闻服务统计信息"""
    aggregator = get_news_aggregator()
    return aggregator.get_stats()


@router.post("/clear")
async def clear_news_history():
    """清空新闻历史（重新开始追踪新消息）"""
    aggregator = get_news_aggregator()
    aggregator.clear_history()
    return {"status": "ok", "message": "History cleared"}


# ==================== 异动提醒 ====================

class AlertItem(BaseModel):
    type: str
    type_code: str
    time: str
    code: str
    name: str
    info: str = ""
    volume: Optional[float] = None
    price: Optional[float] = None
    change_pct: Optional[float] = None
    amount: Optional[float] = None


class AlertsResponse(BaseModel):
    type: str
    count: int
    alerts: List[AlertItem]


@router.get("/market-alerts/types")
async def get_alert_types():
    """获取所有支持的异动类型"""
    service = get_alerts_service()
    return {"types": service.get_all_types()}


@router.get("/market-alerts/{alert_type}", response_model=AlertsResponse)
async def get_market_alerts(
    alert_type: str,
):
    """
    获取指定类型的异动提醒
    
    - **alert_type**: 异动类型，如 大笔买入、大笔卖出、封涨停板 等
    """
    service = get_alerts_service()
    
    try:
        alerts = service.fetch_alerts(alert_type)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    
    return AlertsResponse(type=alert_type, count=len(alerts), alerts=alerts)


@router.get("/market-alerts")
async def get_market_alerts_summary():
    """
    获取异动摘要（大笔买入/卖出、涨停/跌停）
    """
    service = get_alerts_service()
    summary = service.fetch_summary()
    return summary


# ==================== Twitter + RSS ====================

@router.get("/external/status")
async def get_external_status():
    """获取外部信息源状态（Twitter/RSS可用性）"""
    service = get_external_service()
    return service.get_status()


@router.get("/twitter/timeline/{username}")
async def get_twitter_timeline(
    username: str,
    limit: int = Query(10, ge=1, le=50),
):
    """
    获取 Twitter 用户时间线
    
    - **username**: Twitter 用户名（不含@）
    """
    service = get_external_service()
    tweets = service.fetch_twitter_timeline(username, limit=limit)
    return {"username": username, "count": len(tweets), "tweets": tweets}


@router.get("/twitter/search")
async def search_twitter(
    q: str = Query(..., description="搜索关键词"),
    limit: int = Query(20, ge=1, le=50),
):
    """
    搜索 Twitter
    
    - **q**: 搜索关键词
    """
    service = get_external_service()
    tweets = service.search_twitter(q, limit=limit)
    return {"query": q, "count": len(tweets), "tweets": tweets}


@router.get("/rss")
async def get_rss_feed(
    url: str = Query(..., description="RSS feed URL"),
    name: str = Query("RSS Feed", description="Feed 名称"),
    limit: int = Query(10, ge=1, le=50),
):
    """
    获取 RSS feed
    
    - **url**: RSS feed URL
    - **name**: Feed 名称（用于显示）
    """
    service = get_external_service()
    items = service.fetch_rss_feed(url, name=name, limit=limit)
    return {"name": name, "url": url, "count": len(items), "items": items}


# ==================== 智能推送 ====================

@router.get("/smart-alerts/rules")
async def get_smart_alert_rules():
    """获取所有告警规则"""
    system = get_smart_alert_system()
    return {"rules": system.get_rules()}


@router.get("/smart-alerts/recent")
async def get_recent_smart_alerts(limit: int = Query(20, ge=1, le=100)):
    """获取最近触发的告警"""
    system = get_smart_alert_system()
    return {"alerts": system.get_recent_alerts(limit)}


@router.post("/smart-alerts/scan")
async def scan_for_alerts():
    """
    执行一次扫描，检查新闻和异动
    返回触发的告警
    """
    system = get_smart_alert_system()
    alerts = system.scan()
    return {
        "triggered": len(alerts),
        "alerts": [
            {
                "rule_name": a.rule_name,
                "priority": a.priority,
                "title": a.title,
                "content": a.content[:200],
                "source": a.source,
                "time": a.time,
            }
            for a in alerts
        ]
    }


@router.post("/smart-alerts/add-keyword-rule")
async def add_keyword_rule(
    name: str,
    keywords: List[str],
    priority: str = "normal",
):
    """添加关键词告警规则"""
    system = get_smart_alert_system()
    system.add_keyword_rule(name=name, keywords=keywords, priority=priority)
    return {"status": "ok", "message": f"Added rule: {name}"}


@router.post("/smart-alerts/add-stock-rule")
async def add_stock_rule(
    name: str,
    stock_codes: List[str],
    priority: str = "high",
):
    """添加自选股告警规则"""
    system = get_smart_alert_system()
    system.add_stock_rule(name=name, stock_codes=stock_codes, priority=priority)
    return {"status": "ok", "message": f"Added stock rule: {name}"}
