#!/usr/bin/env python3
"""
美股简报 v2 — 完整版
数据源: ashare API http://127.0.0.1:8000
"""

import requests
import json
import sys
from datetime import datetime

API = "http://127.0.0.1:8000"


def fetch(endpoint):
    try:
        r = requests.get(f"{API}{endpoint}", timeout=10)
        return r.json() if r.ok else {}
    except:
        return {}


def format_briefing():
    now = datetime.now().strftime("%H:%M")
    lines = [f"🇺🇸 美股简报 ({now})", ""]

    # 1. 三大指数
    idx = fetch("/api/us-stock/indexes")
    if idx.get("quotes"):
        lines.append("📈 三大指数")
        for q in idx["quotes"]:
            icon = "🟢" if q["change_pct"] >= 0 else "🔴"
            name = q.get("cn_name") or q["name"]
            if "恐慌" in name or "VIX" in name:
                lines.append(f"⚠️ {name}: {q['price']:.2f} ({q['change_pct']:+.2f}%)")
            else:
                lines.append(f"{icon} {name}: {q['price']:,.2f} ({q['change_pct']:+.2f}%)")
        lines.append("")

    # 2. 板块表现 — 按涨跌排序
    sec = fetch("/api/us-stock/sectors")
    if sec.get("sectors"):
        etf_sectors = [(s["name_cn"], s["etf"]) for s in sec["sectors"] if s.get("etf")]
        etf_sectors.sort(key=lambda x: x[1]["change_pct"], reverse=True)
        if etf_sectors:
            lines.append("🏛️ 板块表现")
            top3 = etf_sectors[:3]
            bot3 = etf_sectors[-3:]
            lines.append("领涨: " + " | ".join(
                f"{n}({e['symbol']}) {e['change_pct']:+.2f}%" for n, e in top3
            ))
            lines.append("领跌: " + " | ".join(
                f"{n}({e['symbol']}) {e['change_pct']:+.2f}%" for n, e in bot3
            ))
            lines.append("")

    # 3. Mag7
    mag = fetch("/api/us-stock/mag7")
    if mag.get("quotes"):
        lines.append("💎 Mag7")
        sorted_mag = sorted(mag["quotes"], key=lambda q: q["change_pct"], reverse=True)
        parts = []
        for q in sorted_mag:
            icon = "🟢" if q["change_pct"] >= 0 else "🔴"
            name = q.get("cn_name") or q["symbol"]
            parts.append(f"{icon}{name} {q['change_pct']:+.2f}%")
        lines.append(" | ".join(parts))
        lines.append("")

    # 4. 中概股
    adr = fetch("/api/us-stock/china-adr")
    if adr.get("quotes"):
        lines.append("🇨🇳 中概股")
        sorted_adr = sorted(adr["quotes"], key=lambda q: q["change_pct"], reverse=True)
        for q in sorted_adr:
            icon = "🟢" if q["change_pct"] >= 0 else "🔴"
            lines.append(f"{icon} {q['cn_name']} ${q['price']:.2f} ({q['change_pct']:+.2f}%)")
        lines.append("")

    # 5. 商品/债券/外汇
    commod = fetch("/api/us-stock/commodities")
    bonds = fetch("/api/us-stock/bonds")
    forex = fetch("/api/us-stock/forex")
    macro_items = []
    for d in [commod, bonds, forex]:
        for q in d.get("quotes", d.get("data", [])):
            if q.get("price"):
                icon = "🟢" if q.get("change_pct", 0) >= 0 else "🔴"
                name = q.get("cn_name") or q.get("name", "?")
                macro_items.append(f"{icon}{name} {q['price']:.2f} ({q.get('change_pct',0):+.2f}%)")
    if macro_items:
        lines.append("📦 宏观资产")
        lines.append(" | ".join(macro_items))
        lines.append("")

    # 6. RSS 英文快讯
    news = fetch("/api/us-stock/news")
    news_list = news if isinstance(news, list) else news.get("news", news.get("data", news.get("articles", [])))
    if news_list:
        lines.append("📰 快讯")
        for n in (news_list or [])[:4]:
            src = n.get("source", "")
            title = n.get("title", "")
            if title:
                lines.append(f"• [{src}] {title[:60]}")
        lines.append("")

    # 7. 近期经济日历
    cal = fetch("/api/us-stock/calendar")
    events = cal.get("events", cal.get("data", []))
    if events:
        upcoming = [e for e in events[:3]]
        if upcoming:
            lines.append("📅 经济日历")
            for e in upcoming:
                date = e.get("date", "")
                event = e.get("event", e.get("name", ""))
                lines.append(f"• {date} {event}")
            lines.append("")

    return "\n".join(lines)


if __name__ == "__main__":
    print(format_briefing())
