#!/usr/bin/env python3
"""
动量信号通知检查器
- 读取 momentum_signals.json
- 与上次通知状态对比
- 输出新信号（供 Clawdbot cron 推送）
"""

import json
import sys
from pathlib import Path
from datetime import datetime

SIGNALS_FILE = Path(__file__).parent.parent / 'data' / 'monitor' / 'momentum_signals.json'
STATE_FILE = Path(__file__).parent.parent / 'data' / 'monitor' / 'notified_signals_state.json'


def load_json(path: Path) -> dict:
    if not path.exists():
        return {}
    with open(path, 'r', encoding='utf-8') as f:
        return json.load(f)


def save_json(path: Path, data: dict):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def signal_key(sig: dict) -> str:
    """Generate a unique key for deduplication."""
    return f"{sig['concept_name']}|{sig['signal_type']}|{sig.get('timestamp', '')}"


def format_signal(sig: dict) -> str:
    """Format one signal for Telegram notification."""
    stype = sig.get('signal_type', '')
    name = sig.get('concept_name', '???')
    details = sig.get('details', '')
    total = sig.get('total_stocks', 0)

    if stype == 'surge':
        prev = sig.get('prev_up_count', '?')
        curr = sig.get('current_up_count', '?')
        delta = sig.get('delta_up_count', '?')
        return (
            f"🚀 上涨激增 | {name}\n"
            f"   上涨家数: {prev} → {curr} (+{delta})\n"
            f"   板块总数: {total} | {details}"
        )
    elif stype == 'kline_pattern':
        pct = sig.get('current_change_pct', 0)
        return (
            f"📊 K线形态 | {name} ({pct:+.2f}%)\n"
            f"   板块总数: {total} | {details}"
        )
    else:
        return f"⚡ {stype} | {name}: {details}"


def main():
    signals_data = load_json(SIGNALS_FILE)
    state = load_json(STATE_FILE)

    if not signals_data or signals_data.get('total_signals', 0) == 0:
        # No signals at all
        print("NO_SIGNALS")
        return

    signals = signals_data.get('signals', [])
    notified_keys = set(state.get('notified_keys', []))

    new_signals = []
    for sig in signals:
        key = signal_key(sig)
        if key not in notified_keys:
            new_signals.append(sig)
            notified_keys.add(key)

    if not new_signals:
        print("NO_NEW_SIGNALS")
        return

    # Build notification text
    header = f"🔔 动量信号 ({len(new_signals)}个) — {datetime.now().strftime('%H:%M')}"
    lines = [header, ""]
    for sig in new_signals:
        lines.append(format_signal(sig))
        lines.append("")

    print("\n".join(lines))

    # Prune old keys (keep last 200 to avoid unbounded growth)
    notified_list = list(notified_keys)
    if len(notified_list) > 200:
        notified_list = notified_list[-200:]

    save_json(STATE_FILE, {
        'last_check': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'notified_keys': notified_list
    })


if __name__ == '__main__':
    main()
