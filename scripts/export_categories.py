#!/usr/bin/env python3
"""
导出自选股分类数据

使用方法:
    python scripts/export_categories.py
"""

import sys
import sqlite3
import json
from pathlib import Path
from collections import defaultdict

# 项目根目录
PROJECT_ROOT = Path(__file__).parent.parent
DB_FILE = PROJECT_ROOT / "data" / "market.db"
OUTPUT_DIR = PROJECT_ROOT / "data" / "watchlist_categories"

def export_categories():
    """导出分类数据"""
    print("==================================")
    print("  导出自选股分类数据")
    print("==================================")
    print("")

    # 创建输出目录
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # 连接数据库
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    # 查询所有自选股
    cursor.execute("""
        SELECT ticker, category
        FROM watchlist
        ORDER BY category, ticker
    """)

    # 按分类分组
    categories = defaultdict(list)
    for row in cursor.fetchall():
        ticker = row['ticker']
        category = row['category'] or '未分类'
        categories[category].append(ticker)

    # 导出每个分类
    for category, tickers in sorted(categories.items()):
        # CSV 文件
        csv_file = OUTPUT_DIR / f"{category}.csv"
        with open(csv_file, 'w', encoding='utf-8') as f:
            f.write("ticker\n")
            for ticker in tickers:
                f.write(f"{ticker}\n")

        print(f"  ✓ {category}: {len(tickers)} 只股票 -> {csv_file.name}")

    # 导出汇总JSON
    summary_file = OUTPUT_DIR / "categories_summary.json"
    summary = {
        category: {
            "count": len(tickers),
            "tickers": tickers
        }
        for category, tickers in categories.items()
    }

    with open(summary_file, 'w', encoding='utf-8') as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)

    print(f"\n  ✓ 汇总: {summary_file.name}")

    # 生成导入脚本
    import_script = OUTPUT_DIR / "import_categories.py"
    with open(import_script, 'w', encoding='utf-8') as f:
        f.write('''#!/usr/bin/env python3
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
''')

    import_script.chmod(0o755)
    print(f"  ✓ 导入脚本: {import_script.name}")

    conn.close()

    print("")
    print("==================================")
    print("  ✓ 导出完成！")
    print("==================================")
    print("")
    print(f"导出目录: {OUTPUT_DIR}")
    print("")
    print("在新环境中恢复分类:")
    print("  1. 确保已部署项目并初始化数据库")
    print("  2. 将 data/watchlist_categories/ 目录添加到 git")
    print("     (或通过备份脚本迁移)")
    print("  3. 运行: python data/watchlist_categories/import_categories.py")
    print("")

if __name__ == "__main__":
    export_categories()
