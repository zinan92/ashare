#!/usr/bin/env python3
"""
盘中市场监控 - 同花顺概念板块版（精简版）
直接用浏览器抓取同花顺数据，避免 akshare 的加载问题
"""

import json
import sys
from datetime import datetime
from pathlib import Path

import httpx

# 输出目录
OUTPUT_DIR = Path(__file__).parent.parent / "docs" / "monitor"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def get_index_realtime():
    """获取主要指数实时数据（东方财富）"""
    indices = {}
    
    try:
        url = "https://push2.eastmoney.com/api/qt/ulist.np/get"
        params = {
            "secids": "1.000001,0.399001,0.399006",  # 上证/深成指/创业板
            "fields": "f2,f3,f4,f12,f14",
            "fltt": 2,
        }
        resp = httpx.get(url, params=params, timeout=10)
        data = resp.json()
        
        if data.get("data") and data["data"].get("diff"):
            name_map = {"000001": "上证指数", "399001": "深证成指", "399006": "创业板指"}
            for item in data["data"]["diff"]:
                code = item.get("f12", "")
                name = name_map.get(code, item.get("f14", ""))
                indices[name] = {
                    "price": item.get("f2", 0),
                    "change_pct": item.get("f3", 0),
                }
    except Exception as e:
        print(f"获取指数失败: {e}")
    
    return indices


def get_btc_price():
    """获取BTC价格"""
    try:
        resp = httpx.get(
            "https://api.coingecko.com/api/v3/simple/price",
            params={"ids": "bitcoin", "vs_currencies": "usd", "include_24hr_change": "true"},
            timeout=10
        )
        data = resp.json()
        return {
            "price": data["bitcoin"]["usd"],
            "change_pct": data["bitcoin"].get("usd_24h_change", 0),
        }
    except Exception as e:
        print(f"获取BTC价格失败: {e}")
        return None


def get_ths_concept_ranking():
    """获取同花顺概念板块涨跌榜（通过浏览器抓取的数据页面）"""
    results = []
    
    try:
        url = "https://data.10jqka.com.cn/funds/gnzjl/field/tradezdf/order/desc/ajax/1/free/1/"
        headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
            "Referer": "https://data.10jqka.com.cn/",
        }
        resp = httpx.get(url, headers=headers, timeout=15)
        
        # 解析HTML表格
        import re
        html = resp.text
        
        # 提取表格行数据
        pattern = r'<tr[^>]*>.*?<td[^>]*>(\d+)</td>.*?<a[^>]*>([^<]+)</a>.*?<td[^>]*>([\d.]+)</td>.*?<td[^>]*class="[^"]*"[^>]*>([-\d.]+)%</td>.*?<td[^>]*>([-\d.]+)</td>.*?<td[^>]*>([-\d.]+)</td>.*?<td[^>]*>([-\d.]+)</td>'
        
        matches = re.findall(pattern, html, re.DOTALL)
        
        for match in matches[:50]:
            rank, name, index_val, change_pct, inflow, outflow, net = match
            results.append({
                "rank": int(rank),
                "name": name.strip(),
                "change_pct": float(change_pct),
                "money_inflow": float(net),
            })
            
    except Exception as e:
        print(f"获取同花顺概念榜失败: {e}")
    
    return results


def get_key_concepts_akshare(concepts: list[str]):
    """用akshare获取关键概念数据"""
    import akshare as ak
    
    results = []
    for name in concepts:
        try:
            df = ak.stock_board_concept_info_ths(symbol=name)
            data = dict(zip(df['项目'], df['值']))
            
            change_str = data.get('板块涨幅', '0%').replace('%', '')
            inflow_str = data.get('资金净流入(亿)', '0')
            
            results.append({
                "name": name,
                "change_pct": float(change_str),
                "money_inflow": float(inflow_str),
            })
        except Exception as e:
            print(f"   ✗ {name}: {e}")
    
    return sorted(results, key=lambda x: x['change_pct'], reverse=True)


def get_ths_concept_from_em(retries: int = 3):
    """从东方财富获取概念板块数据（备选）"""
    results = []
    
    for attempt in range(retries):
        try:
            url = "https://push2.eastmoney.com/api/qt/clist/get"
            params = {
                "fs": "m:90+t:3",
                "fields": "f3,f12,f14,f62",
                "pn": 1, "pz": 50, "po": 1, "fid": "f3", "np": 1, "fltt": 2,
            }
            
            with httpx.Client(timeout=15) as client:
                resp = client.get(url, params=params)
                data = resp.json()
            
            if data.get("data") and data["data"].get("diff"):
                for i, item in enumerate(data["data"]["diff"]):
                    results.append({
                        "rank": i + 1,
                        "name": item.get("f14", ""),
                        "change_pct": item.get("f3", 0),
                        "money_inflow": item.get("f62", 0) / 100000000,
                    })
                return results
                
        except Exception as e:
            if attempt == retries - 1:
                print(f"   东方财富API失败，切换到akshare...")
            else:
                import time
                time.sleep(0.5)
    
    return results


def detect_resistance(index_change: float, concepts: list[dict], threshold: float = 0.5) -> list[dict]:
    """扛指数检测"""
    if index_change >= 0:
        return []
    
    resistant = []
    for c in concepts:
        diff = c['change_pct'] - index_change
        if diff > threshold and c['change_pct'] > -0.5:
            resistant.append({**c, 'resistance_score': diff})
    
    return sorted(resistant, key=lambda x: x['resistance_score'], reverse=True)


def run_monitor():
    """运行监控"""
    print(f"\n{'='*60}")
    print(f"  盘中市场监控 - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*60}\n")
    
    result = {
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "source": "同花顺/东方财富",
        "indices": {},
        "btc": None,
        "top_concepts": [],
        "resistant_sectors": [],
    }
    
    # 1. 获取指数数据
    print("📊 获取指数数据...")
    result["indices"] = get_index_realtime()
    for name, data in result["indices"].items():
        emoji = "📈" if data['change_pct'] > 0 else "📉" if data['change_pct'] < 0 else ""
        print(f"   {name}: {data['price']:.2f} ({data['change_pct']:+.2f}%) {emoji}")
    
    # 2. 获取BTC
    print("\n₿ 获取BTC价格...")
    result["btc"] = get_btc_price()
    if result["btc"]:
        print(f"   BTC: ${result['btc']['price']:,.0f} ({result['btc']['change_pct']:+.2f}%)")
    
    # 3. 获取概念涨幅榜
    print(f"\n🔥 获取概念涨幅榜...")
    result["top_concepts"] = get_ths_concept_from_em()
    
    # 如果东方财富失败，用akshare获取关键概念
    if not result["top_concepts"]:
        print("   使用akshare获取关键概念...")
        key_concepts = [
            "黄金概念", "芯片概念", "军工", "人形机器人", "DeepSeek概念",
            "商业航天", "储能", "光伏概念", "人工智能", "新能源汽车",
            "小金属概念", "稀土永磁", "半导体", "低空经济", "机器人概念",
        ]
        result["top_concepts"] = get_key_concepts_akshare(key_concepts)
    
    for i, c in enumerate(result["top_concepts"][:15]):
        emoji = ""
        if any(k in c['name'] for k in ['黄金', '金属', '稀土', '稀缺']):
            emoji = "⭐资源"
        elif any(k in c['name'] for k in ['芯片', 'AI', '半导体', '算力', 'DeepSeek']):
            emoji = "⭐科技"
        elif any(k in c['name'] for k in ['军工', '航天']):
            emoji = "⭐军工"
        print(f"   {i+1:2d}. {c['name']}: {c['change_pct']:+.2f}% (净流入:{c['money_inflow']:+.1f}亿) {emoji}")
    
    # 4. 扛指数检测
    gem_change = result["indices"].get("创业板指", {}).get("change_pct", 0)
    if gem_change < -0.3:
        print(f"\n🛡️ 扛指数检测 (创业板 {gem_change:+.2f}%)...")
        result["resistant_sectors"] = detect_resistance(gem_change, result["top_concepts"])
        if result["resistant_sectors"]:
            for c in result["resistant_sectors"][:5]:
                print(f"   {c['name']}: {c['change_pct']:+.2f}% (抗跌分:{c['resistance_score']:.2f})")
        else:
            print("   无明显扛指数板块")
    
    # 5. 保存结果
    output_file = OUTPUT_DIR / "latest_ths.json"
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(result, f, ensure_ascii=False, indent=2)
    print(f"\n💾 结果已保存到: {output_file}")
    
    return result


def format_report(result: dict) -> str:
    """格式化报告"""
    lines = []
    ts = result["timestamp"].split()[1][:5]
    
    lines.append(f"## 📊 {ts} 盘中更新\n")
    
    # 指数
    lines.append("### 主要指数")
    lines.append("| 指数 | 价格 | 涨跌 |")
    lines.append("|-----|------|------|")
    for name, data in result["indices"].items():
        emoji = "📈" if data["change_pct"] > 0 else "📉" if data["change_pct"] < 0 else ""
        lines.append(f"| {name} | {data['price']:.2f} | {data['change_pct']:+.2f}% {emoji} |")
    
    if result["btc"]:
        lines.append(f"| BTC | ${result['btc']['price']:,.0f} | {result['btc']['change_pct']:+.2f}% |")
    
    # 涨幅榜
    if result["top_concepts"]:
        lines.append("\n### 🔥 概念涨幅榜 (同花顺)")
        lines.append("| 排名 | 概念 | 涨幅 | 净流入 |")
        lines.append("|-----|-----|------|--------|")
        for i, c in enumerate(result["top_concepts"][:10]):
            rank = c.get('rank', i + 1)
            lines.append(f"| {rank} | {c['name']} | {c['change_pct']:+.2f}% | {c['money_inflow']:+.1f}亿 |")
    
    # 扛指数
    if result["resistant_sectors"]:
        gem_change = result["indices"].get("创业板指", {}).get("change_pct", 0)
        lines.append(f"\n### 🛡️ 扛指数板块 (创业板 {gem_change:+.2f}%)")
        for c in result["resistant_sectors"][:5]:
            lines.append(f"- **{c['name']}**: {c['change_pct']:+.2f}%")
    
    return "\n".join(lines)


if __name__ == "__main__":
    result = run_monitor()
    print("\n" + "="*60)
    print(format_report(result))
