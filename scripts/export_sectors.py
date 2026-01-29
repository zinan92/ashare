#!/usr/bin/env python3
"""
导出赛道分类数据

包括:
1. 16个赛道定义 (available_sectors)
2. 每个赛道的成分股 (stock_sectors)
"""

import sys
import sqlite3
import json
import csv
from pathlib import Path
from collections import defaultdict

# 项目根目录
PROJECT_ROOT = Path(__file__).parent.parent
DB_FILE = PROJECT_ROOT / "data" / "market.db"
OUTPUT_DIR = PROJECT_ROOT / "data" / "sectors"

def export_sectors():
    """导出赛道分类数据"""
    print("==================================")
    print("  导出赛道分类数据")
    print("==================================")
    print("")

    # 创建输出目录
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # 连接数据库
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    # 1. 导出赛道定义
    cursor.execute("""
        SELECT id, name, display_order, created_at
        FROM available_sectors
        ORDER BY display_order
    """)

    sectors_list = []
    for row in cursor.fetchall():
        sectors_list.append({
            "id": row['id'],
            "name": row['name'],
            "display_order": row['display_order'],
            "created_at": row['created_at']
        })

    # 保存赛道定义
    sectors_def_file = OUTPUT_DIR / "available_sectors.json"
    with open(sectors_def_file, 'w', encoding='utf-8') as f:
        json.dump(sectors_list, f, ensure_ascii=False, indent=2)

    print(f"✓ 赛道定义: {len(sectors_list)} 个赛道")
    for sector in sectors_list:
        print(f"  {sector['display_order'] + 1}. {sector['name']}")

    # 2. 导出成分股分类
    cursor.execute("""
        SELECT ticker, sector
        FROM stock_sectors
        ORDER BY sector, ticker
    """)

    # 按赛道分组
    sectors_stocks = defaultdict(list)
    total_stocks = 0
    for row in cursor.fetchall():
        ticker = row['ticker']
        sector = row['sector']
        sectors_stocks[sector].append(ticker)
        total_stocks += 1

    # 保存每个赛道的成分股
    print(f"\n✓ 成分股分类: {total_stocks} 只股票")
    for sector, tickers in sorted(sectors_stocks.items()):
        # CSV 文件
        csv_file = OUTPUT_DIR / f"{sector}.csv"
        with open(csv_file, 'w', encoding='utf-8', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(["ticker"])
            for ticker in tickers:
                writer.writerow([ticker])

        print(f"  ✓ {sector}: {len(tickers)} 只 -> {csv_file.name}")

    # 3. 保存汇总JSON
    summary = {
        "sectors": sectors_list,
        "stocks": {
            sector: {
                "count": len(tickers),
                "tickers": tickers
            }
            for sector, tickers in sectors_stocks.items()
        },
        "total_sectors": len(sectors_list),
        "total_stocks": total_stocks
    }

    summary_file = OUTPUT_DIR / "sectors_summary.json"
    with open(summary_file, 'w', encoding='utf-8') as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)

    print(f"\n✓ 汇总文件: {summary_file.name}")

    # 4. 生成导入脚本
    import_script = OUTPUT_DIR / "import_sectors.py"
    with open(import_script, 'w', encoding='utf-8') as f:
        f.write('''#!/usr/bin/env python3
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
    print("\\n2. 导入成分股分类...")
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
''')

    import_script.chmod(0o755)
    print(f"✓ 导入脚本: {import_script.name}")

    conn.close()

    print("")
    print("==================================")
    print("  ✓ 导出完成！")
    print("==================================")
    print("")
    print(f"导出目录: {OUTPUT_DIR}")
    print("")
    print("在新环境中恢复赛道分类:")
    print("  1. 确保已部署项目并初始化数据库")
    print("  2. 将 data/sectors/ 目录添加到 git")
    print("  3. 运行: python data/sectors/import_sectors.py")
    print("")

if __name__ == "__main__":
    export_sectors()
