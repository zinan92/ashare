#!/usr/bin/env python3
"""
市场简报生成器 v3 — 供 Wendy 盘中/盘后简报使用
输出格式化文本，直接推送给Park

数据源:
- Sina: A股指数实时行情
- AKShare: 涨停池/跌停池/异动数据
- Sina News: 财经快讯
- Local API: 自选股列表 + 实时价格
"""
import sys
import time
import requests
from datetime import datetime

SINA_HEADERS = {'Referer': 'https://finance.sina.com.cn', 'User-Agent': 'Mozilla/5.0'}
API_BASE = 'http://127.0.0.1:8000'


def get_indices():
    """获取A股主要指数实时数据"""
    try:
        r = requests.get(
            'http://hq.sinajs.cn/list=s_sh000001,s_sz399001,s_sz399006,s_sh000688',
            headers=SINA_HEADERS, timeout=5
        )
        indices = {}
        for line in r.text.strip().split('\n'):
            if '"' in line:
                parts = line.split('"')[1].split(',')
                if len(parts) >= 5:
                    indices[parts[0]] = {
                        'price': parts[1],
                        'change': parts[2],
                        'pct': float(parts[3]),
                        'vol': float(parts[4]) / 1e4  # 亿
                    }
        return indices
    except Exception as e:
        print(f"⚠️ 指数获取失败: {e}", file=sys.stderr)
        return {}


def get_limit_stats():
    """获取涨停/跌停统计 + 大笔买卖"""
    import akshare as ak
    today = datetime.now().strftime('%Y%m%d')
    stats = {'limit_up': 0, 'limit_down': 0, 'up_names': [], 'down_names': [],
             'big_buy': 0, 'big_sell': 0}

    try:
        df = ak.stock_zt_pool_em(date=today)
        stats['limit_up'] = len(df)
        if len(df) > 0:
            stats['up_names'] = df['名称'].head(5).tolist()
    except:
        pass

    try:
        df2 = ak.stock_zt_pool_dtgc_em(date=today)
        stats['limit_down'] = len(df2)
        if len(df2) > 0:
            stats['down_names'] = df2['名称'].head(5).tolist()
    except:
        pass

    try:
        df3 = ak.stock_changes_em(symbol="大笔买入")
        stats['big_buy'] = len(df3)
    except:
        pass

    try:
        df4 = ak.stock_changes_em(symbol="大笔卖出")
        stats['big_sell'] = len(df4)
    except:
        pass

    return stats


def get_watchlist_movers():
    """获取自选股涨跌幅排名"""
    try:
        r = requests.get(f'{API_BASE}/api/watchlist', timeout=10)
        if r.status_code != 200:
            return [], []
        watchlist = r.json()
        tickers = [w['ticker'] for w in watchlist]
        name_map = {w['ticker']: w['name'] for w in watchlist}

        results = []
        for i in range(0, len(tickers), 50):
            batch = tickers[i:i + 50]
            codes = ','.join([f"sh{t}" if t.startswith('6') else f"sz{t}" for t in batch])
            pr = requests.get(
                f'http://hq.sinajs.cn/list={codes}',
                headers=SINA_HEADERS, timeout=10
            )
            for line in pr.text.strip().split('\n'):
                if 'hq_str_' in line and '"' in line:
                    code_part = line.split('hq_str_')[1].split('=')[0]
                    ticker = code_part[2:]
                    data = line.split('"')[1].split(',')
                    if len(data) > 4 and data[3] and data[2]:
                        try:
                            cur = float(data[3])
                            prev = float(data[2])
                            if prev > 0:
                                pct = (cur - prev) / prev * 100
                                results.append((name_map.get(ticker, ticker), ticker, pct))
                        except:
                            pass
            time.sleep(0.2)

        results.sort(key=lambda x: x[2], reverse=True)
        top5 = results[:5]
        bot5 = results[-5:] if len(results) >= 5 else results[::-1][:5]
        return top5, bot5
    except Exception as e:
        print(f"⚠️ 自选股获取失败: {e}", file=sys.stderr)
        return [], []


def get_news():
    """获取财经快讯 (Sina Feed)"""
    try:
        headers = {'User-Agent': 'Mozilla/5.0', 'Referer': 'https://finance.sina.com.cn'}
        r = requests.get(
            'https://feed.mix.sina.com.cn/api/roll/get?pageid=153&lid=2516&k=&num=8&page=1',
            headers=headers, timeout=8
        )
        if r.status_code == 200:
            data = r.json()
            items = data.get('result', {}).get('data', [])
            return [item.get('title', '')[:70] for item in items[:6] if item.get('title', '')]
    except:
        pass
    return []


def format_briefing(time_label=""):
    """生成完整简报"""
    now = datetime.now()
    if not time_label:
        time_label = now.strftime('%H:%M')

    lines = [f"📊 市场简报 ({time_label})", ""]

    # 1. 指数
    indices = get_indices()
    lines.append("📈 A股指数")
    for name in ['上证指数', '深证成指', '创业板指', '科创50']:
        if name in indices:
            d = indices[name]
            e = '🟢' if d['pct'] >= 0 else '🔴'
            sign = '+' if d['pct'] >= 0 else ''
            lines.append(
                f"  {e} {name}: {d['price']} ({sign}{d['pct']:.2f}%) 成交:{d['vol']:.0f}亿")

    # 2. 涨跌停统计
    stats = get_limit_stats()
    lines.append("")
    lines.append("⚡ 异动统计")
    up_str = '、'.join(stats['up_names'][:3]) + '…' if stats['up_names'] else ''
    down_str = '、'.join(stats['down_names'][:3]) + '…' if stats['down_names'] else ''
    lines.append(f"  🟢 涨停: {stats['limit_up']}只 | {up_str}")
    lines.append(f"  🔴 跌停: {stats['limit_down']}只 | {down_str}")
    net = stats['big_buy'] - stats['big_sell']
    lines.append(
        f"  💰 大笔买入: {stats['big_buy']}只 | 🔻 大笔卖出: {stats['big_sell']}只"
        f"（净{'买' if net >= 0 else '卖'}入{abs(net)}只差额）")

    # 3. 自选股异动
    top5, bot5 = get_watchlist_movers()
    if top5:
        lines.append("")
        lines.append("⭐ 自选股异动")
        top_str = ' | '.join([f"{n}{p:+.1f}%" for n, _, p in top5])
        bot_str = ' | '.join([f"{n}{p:+.1f}%" for n, _, p in bot5])
        lines.append(f"  📈 涨幅前5: {top_str}")
        lines.append(f"  📉 跌幅前5: {bot_str}")

    # 4. 快讯
    news = get_news()
    if news:
        lines.append("")
        lines.append("📰 财经快讯")
        for title in news[:5]:
            lines.append(f"  • {title}")

    return '\n'.join(lines)


if __name__ == '__main__':
    label = sys.argv[1] if len(sys.argv) > 1 else ""
    print(format_briefing(label))
