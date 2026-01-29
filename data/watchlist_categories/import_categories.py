#!/usr/bin/env python3
"""
导入自选股分类数据

使用方法:
    python data/watchlist_categories/import_categories.py
"""

import sys
import sqlite3
from pathlib import Path
import json

# 项目根目录
PROJECT_ROOT = Path(__file__).parent.parent.parent
DB_FILE = PROJECT_ROOT / "data" / "market.db"
CATEGORIES_DIR = Path(__file__).parent

def import_categories():
    """导入分类数据到数据库"""
    print("开始导入自选股分类数据...")
    print("")

    # 读取汇总JSON
    summary_file = CATEGORIES_DIR / "categories_summary.json"
    if not summary_file.exists():
        print(f"错误: 找不到 {summary_file}")
        return

    with open(summary_file, 'r', encoding='utf-8') as f:
        categories = json.load(f)

    # 连接数据库
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    total_updated = 0
    for category, data in categories.items():
        tickers = data['tickers']

        # 更新每个股票的分类
        for ticker in tickers:
            cursor.execute(
                "UPDATE watchlist SET category = ? WHERE ticker = ?",
                (category, ticker)
            )

        conn.commit()
        updated = len(tickers)
        total_updated += updated
        print(f"✓ {category}: 更新了 {updated} 只股票")

    conn.close()

    print("")
    print(f"导入完成！共更新 {total_updated} 只股票的分类")

if __name__ == "__main__":
    import_categories()
