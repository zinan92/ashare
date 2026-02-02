#!/usr/bin/env python3
"""
市场简报生成器 v2
整合A股指数、异动、快讯、Crypto数据生成统一简报
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import json
import requests
from datetime import datetime
from typing import Dict, Any, List, Optional

BASE_URL = "http://127.0.0.1:8000"


def api_get(path: str, params: Optional[Dict] = None, timeout: int = 10) -> Any:
    """Call ashare API with error handling"""
    try:
        resp = requests.get(f"{BASE_URL}{path}", params=params, timeout=timeout)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        print(f"  ⚠ API调用失败 {path}: {e}", file=sys.stderr)
        return None


def get_index_data() -> Dict[str, Any]:
    """获取A股主要指数 via ashare API"""
    indices = {}
    index_map = {
        '000001.SH': '上证指数',
        '399001.SZ': '深证成指',
        '399006.SZ': '创业板指',
    }
    for ts_code, name in index_map.items():
        data = api_get(f"/api/index/realtime/{ts_code}")
        if data and 'price' in data:
            indices[name] = {
                'price': data['price'],
                'change': data.get('change', 0),
                'change_pct': data.get('change_pct', 0),
            }
    return indices


def get_news_summary() -> List[Dict[str, Any]]:
    """获取快讯摘要 via ashare API"""
    data = api_get("/api/news/latest", params={"limit": 10})
    if not data or 'news' not in data:
        return []
    return [
        {
            'source': n.get('source_name', ''),
            'title': (n.get('title', '') or '')[:60],
            'time': n.get('time', ''),
        }
        for n in data['news']
    ]


def get_alerts_summary() -> Dict[str, Any]:
    """获取异动摘要 via ashare API"""
    data = api_get("/api/news/market-alerts")
    if not data:
        return {}
    
    result = {}
    for alert_type, info in data.items():
        if isinstance(info, dict) and 'count' in info:
            result[alert_type] = {
                'count': info['count'],
                'top': [
                    f"{a.get('code', '')} {a.get('name', '')}"
                    for a in info.get('top', [])[:3]
                ]
            }
    return result


def get_crypto_data() -> Dict[str, Any]:
    """获取加密货币数据"""
    crypto = {}
    
    # 主要币种价格
    prices_data = api_get("/api/crypto/prices")
    if prices_data and 'prices' in prices_data:
        crypto['prices'] = prices_data['prices'][:8]  # Top 8
    
    # 市场概览
    overview = api_get("/api/crypto/market-overview")
    if overview:
        crypto['overview'] = overview
    
    # 资金费率
    funding = api_get("/api/crypto/funding-rates")
    if funding and 'funding_rates' in funding:
        crypto['funding_rates'] = funding['funding_rates']
    
    return crypto


def get_us_stock_data() -> Dict[str, Any]:
    """获取美股数据"""
    us = {}
    
    indexes = api_get("/api/us-stock/indexes")
    if indexes:
        us['indexes'] = indexes
    
    china_adr = api_get("/api/us-stock/china-adr")
    if china_adr:
        us['china_adr'] = china_adr
    
    return us


def format_change(pct: float) -> str:
    """Format change percentage with emoji"""
    emoji = '🔴' if pct < 0 else '🟢' if pct > 0 else '⚪'
    return f"{emoji} {pct:+.2f}%"


def format_briefing(
    indices: Dict,
    news: List,
    alerts: Dict,
    crypto: Dict,
    us_stocks: Dict,
    include_crypto: bool = True,
    include_us: bool = False,
) -> str:
    """格式化简报"""
    now = datetime.now().strftime('%Y-%m-%d %H:%M')
    lines = [f"📊 **市场简报** ({now})", ""]
    
    # === A股指数 ===
    if indices:
        lines.append("**📈 A股指数**")
        for name, data in indices.items():
            price = data.get('price', 0)
            pct = data.get('change_pct', 0)
            emoji = '🔴' if pct < 0 else '🟢' if pct > 0 else '⚪'
            lines.append(f"{emoji} {name}: {price:.2f} ({pct:+.2f}%)")
        lines.append("")
    
    # === 异动 ===
    if alerts:
        lines.append("**⚡ 异动提醒**")
        for alert_type, data in alerts.items():
            count = data.get('count', 0)
            if count > 0:
                top = ', '.join(data.get('top', []))
                lines.append(f"• {alert_type}: {count}只 ({top})")
        lines.append("")
    
    # === Crypto ===
    if include_crypto and crypto:
        prices = crypto.get('prices', [])
        overview = crypto.get('overview', {})
        funding = crypto.get('funding_rates', [])
        
        if prices:
            lines.append("**₿ 加密货币**")
            for coin in prices[:6]:
                sym = coin.get('symbol', '')
                price = coin.get('price', 0)
                chg = coin.get('change_24h', 0)
                emoji = '🔴' if chg < 0 else '🟢'
                
                # Format price nicely
                if price >= 1000:
                    price_str = f"${price:,.0f}"
                elif price >= 1:
                    price_str = f"${price:.2f}"
                else:
                    price_str = f"${price:.4f}"
                
                lines.append(f"{emoji} {sym}: {price_str} ({chg:+.1f}%)")
            
            if overview:
                total_cap = overview.get('total_market_cap_usd', 0)
                btc_dom = overview.get('bitcoin_dominance', 0)
                cap_chg = overview.get('market_cap_change_24h', 0)
                lines.append(f"💰 总市值: ${total_cap/1e12:.2f}T ({cap_chg:+.1f}%) | BTC主导率: {btc_dom:.1f}%")
            
            # Funding rates - highlight extreme values
            extreme_funding = [
                f for f in funding
                if f.get('funding_rate', 0) and abs(f['funding_rate']) > 0.005
            ]
            if extreme_funding:
                lines.append("📊 资金费率异常:")
                for f in extreme_funding:
                    rate = f['funding_rate']
                    sym = f['symbol']
                    direction = "空头付费" if rate > 0 else "多头付费"
                    lines.append(f"  • {sym}: {rate*100:.3f}% ({direction})")
            
            lines.append("")
    
    # === 美股 ===
    if include_us and us_stocks:
        indexes = us_stocks.get('indexes', {})
        adr = us_stocks.get('china_adr', {})
        
        if indexes:
            lines.append("**🇺🇸 美股**")
            for idx_name, idx_data in indexes.items():
                if isinstance(idx_data, dict):
                    price = idx_data.get('price', 0)
                    pct = idx_data.get('change_pct', 0)
                    emoji = '🔴' if pct < 0 else '🟢'
                    lines.append(f"{emoji} {idx_name}: {price:,.2f} ({pct:+.2f}%)")
        
        if adr:
            lines.append("**🇨🇳 中概股**")
            adr_list = adr if isinstance(adr, list) else adr.get('stocks', [])
            for stock in (adr_list[:5] if isinstance(adr_list, list) else []):
                name = stock.get('name', '')
                price = stock.get('price', 0)
                pct = stock.get('change_pct', 0)
                emoji = '🔴' if pct < 0 else '🟢'
                lines.append(f"{emoji} {name}: ${price:.2f} ({pct:+.2f}%)")
        
        lines.append("")
    
    # === 快讯 ===
    if news:
        lines.append("**📰 最新快讯**")
        for n in news[:5]:
            source = n.get('source', '')
            title = n.get('title', '')
            lines.append(f"• [{source}] {title}")
    
    return '\n'.join(lines)


def main():
    """生成并输出市场简报"""
    print("正在生成市场简报...\n")
    
    now = datetime.now()
    hour = now.hour
    is_trading_hours = 9 <= hour < 16
    is_after_hours = hour >= 16 or hour < 9
    
    # 获取数据
    indices = get_index_data()
    news = get_news_summary()
    alerts = get_alerts_summary()
    crypto = get_crypto_data()
    us_stocks = get_us_stock_data() if is_after_hours else {}
    
    # 格式化
    briefing = format_briefing(
        indices=indices,
        news=news,
        alerts=alerts,
        crypto=crypto,
        us_stocks=us_stocks,
        include_crypto=True,
        include_us=is_after_hours,
    )
    
    print(briefing)
    
    # JSON output
    print("\n--- JSON ---")
    print(json.dumps({
        'timestamp': now.isoformat(),
        'indices': indices,
        'news_count': len(news),
        'alerts': alerts,
        'crypto': {
            'prices_count': len(crypto.get('prices', [])),
            'overview': crypto.get('overview', {}),
        },
    }, ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()
