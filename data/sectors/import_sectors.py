#!/usr/bin/env python3
"""
导入赛道分类数据

使用方法:
    python data/sectors/import_sectors.py
"""

import sys
import sqlite3
from pathlib import Path
import json
from datetime import datetime

# 项目根目录
PROJECT_ROOT = Path(__file__).parent.parent.parent
DB_FILE = PROJECT_ROOT / "data" / "market.db"
SECTORS_DIR = Path(__file__).parent

def import_sectors():
    """导入赛道分类数据到数据库"""
    print("开始导入赛道分类数据...")
    print("")

    # 读取汇总JSON
    summary_file = SECTORS_DIR / "sectors_summary.json"
    if not summary_file.exists():
        print(f"错误: 找不到 {summary_file}")
        return

    with open(summary_file, 'r', encoding='utf-8') as f:
        data = json.load(f)

    # 连接数据库
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    # 1. 导入赛道定义
    print("1. 导入赛道定义...")
    cursor.execute("DELETE FROM available_sectors")  # 清空旧数据

    for sector_def in data['sectors']:
        cursor.execute(
            """INSERT INTO available_sectors (id, name, display_order, created_at)
               VALUES (?, ?, ?, ?)""",
            (sector_def['id'], sector_def['name'], sector_def['display_order'],
             sector_def.get('created_at', datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
        )

    conn.commit()
    print(f"   ✓ 已导入 {len(data['sectors'])} 个赛道定义")

    # 2. 导入成分股分类
    print("\n2. 导入成分股分类...")
    cursor.execute("DELETE FROM stock_sectors")  # 清空旧数据

    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    total_imported = 0

    for sector, info in data['stocks'].items():
        tickers = info['tickers']
        for ticker in tickers:
            cursor.execute(
                """INSERT INTO stock_sectors (ticker, sector, created_at, updated_at)
                   VALUES (?, ?, ?, ?)""",
                (ticker, sector, now, now)
            )
        total_imported += len(tickers)
        print(f"   ✓ {sector}: {len(tickers)} 只股票")

    conn.commit()
    conn.close()

    print("")
    print(f"导入完成！")
    print(f"  - 赛道数量: {len(data['sectors'])}")
    print(f"  - 股票数量: {total_imported}")

if __name__ == "__main__":
    import_sectors()
