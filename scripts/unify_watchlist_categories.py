#!/usr/bin/env python3
"""
统一自选股分类和赛道分类

将 watchlist 表中的 category 字段更新为 stock_sectors 表中的赛道分类
"""

import sqlite3
from pathlib import Path


def unify_categories(db_path: str):
    """统一分类"""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # 1. 分析当前状态
    print("=" * 60)
    print("当前状态分析")
    print("=" * 60)

    cursor.execute("""
        SELECT
            w.category as watchlist_category,
            s.sector as sector_category,
            COUNT(*) as count
        FROM watchlist w
        LEFT JOIN stock_sectors s ON w.ticker = s.ticker
        GROUP BY w.category, s.sector
        ORDER BY w.category, count DESC
    """)

    results = cursor.fetchall()
    for row in results:
        wcat, scat, count = row
        print(f"  自选股分类: {wcat or '(空)'} -> 赛道分类: {scat or '(无赛道)'} | {count}只")

    # 2. 更新 watchlist.category 为 stock_sectors.sector
    print("\n" + "=" * 60)
    print("开始统一分类...")
    print("=" * 60)

    cursor.execute("""
        UPDATE watchlist
        SET category = (
            SELECT sector
            FROM stock_sectors
            WHERE stock_sectors.ticker = watchlist.ticker
        )
        WHERE EXISTS (
            SELECT 1
            FROM stock_sectors
            WHERE stock_sectors.ticker = watchlist.ticker
        )
    """)

    updated_count = cursor.rowcount
    print(f"✅ 已更新 {updated_count} 只股票的分类")

    # 3. 检查未匹配的股票（在自选股中但没有赛道分类）
    cursor.execute("""
        SELECT ticker, category
        FROM watchlist
        WHERE ticker NOT IN (SELECT ticker FROM stock_sectors)
        ORDER BY category, ticker
    """)

    unmatched = cursor.fetchall()
    if unmatched:
        print(f"\n⚠️  以下 {len(unmatched)} 只自选股没有赛道分类:")
        for ticker, category in unmatched:
            print(f"  - {ticker} (当前分类: {category or '未分类'})")

    # 4. 显示统一后的分类统计
    print("\n" + "=" * 60)
    print("统一后的分类统计")
    print("=" * 60)

    cursor.execute("""
        SELECT
            COALESCE(category, '未分类') as category,
            COUNT(*) as count
        FROM watchlist
        GROUP BY category
        ORDER BY count DESC
    """)

    categories = cursor.fetchall()
    total = sum(count for _, count in categories)

    for category, count in categories:
        percentage = (count / total * 100) if total > 0 else 0
        print(f"  {category}: {count}只 ({percentage:.1f}%)")

    print(f"\n总计: {total}只自选股")

    # 5. 提交更改
    conn.commit()
    print("\n✅ 分类统一完成！")

    conn.close()


def main():
    """主函数"""
    db_path = Path(__file__).parent.parent / "data" / "market.db"

    if not db_path.exists():
        print(f"❌ 数据库文件不存在: {db_path}")
        return

    print(f"数据库: {db_path}\n")
    unify_categories(str(db_path))


if __name__ == "__main__":
    main()
