#!/usr/bin/env python3
"""
将自选股中缺失赛道分类的股票添加到 stock_sectors 表
"""

import sqlite3
from pathlib import Path
from datetime import datetime


def add_missing_sectors(db_path: str):
    """添加缺失的赛道分类"""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # 手动分类规则
    sector_mapping = {
        # 金属类
        "000426": "金属",  # 兴业银锡
        "000657": "金属",  # 中钨高新
        "000831": "金属",  # 中国稀土
        "000962": "金属",  # 东方钽业
        "002149": "金属",  # 西部材料
        "002167": "金属",  # 东方锆业
        "002182": "金属",  # 宝武镁业
        "002378": "金属",  # 章源钨业
        "002428": "金属",  # 云南锗业
        "002738": "金属",  # 中矿资源
        "002842": "金属",  # 翔鹭钨业
        "300328": "金属",  # 宜安科技
        "301026": "金属",  # 浩通科技
        "600281": "金属",  # 华阳新材
        "600301": "金属",  # 华锡有色
        "600392": "金属",  # 盛和资源
        "600456": "金属",  # 宝钛股份
        "600549": "金属",  # 厦门钨业
        "601958": "金属",  # 金钼股份
        "688750": "金属",  # 金天钛业

        # 其他类
        "300674": "其他",  # 宇信科技 (IT服务)
        "301188": "其他",  # 力诺药包 (医药包装)
        "301590": "其他",  # 优优绿能 (绿色能源)
        "600529": "其他",  # 山东药玻 (医药包装)
        "605136": "其他",  # 丽人丽妆 (美妆)
        "920068": "其他",  # 未知股票
    }

    # 插入到 stock_sectors 表
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    print("=" * 60)
    print("添加缺失的赛道分类")
    print("=" * 60)

    for ticker, sector in sector_mapping.items():
        # 检查是否已存在
        cursor.execute("SELECT sector FROM stock_sectors WHERE ticker = ?", (ticker,))
        existing = cursor.fetchone()

        if existing:
            print(f"  ⏭️  {ticker} 已有分类: {existing[0]}")
            continue

        # 获取股票名称
        cursor.execute("SELECT name FROM symbol_metadata WHERE ticker = ?", (ticker,))
        result = cursor.fetchone()
        name = result[0] if result else "未知"

        # 插入新分类
        cursor.execute(
            "INSERT INTO stock_sectors (ticker, sector, created_at, updated_at) VALUES (?, ?, ?, ?)",
            (ticker, sector, now, now)
        )
        print(f"  ✅ {ticker} ({name}) -> {sector}")

    # 统计每个赛道的股票数量
    print("\n" + "=" * 60)
    print("赛道分类统计")
    print("=" * 60)

    cursor.execute("""
        SELECT sector, COUNT(*) as count
        FROM stock_sectors
        GROUP BY sector
        ORDER BY count DESC
    """)

    sectors = cursor.fetchall()
    total = sum(count for _, count in sectors)

    for sector, count in sectors:
        percentage = (count / total * 100) if total > 0 else 0
        print(f"  {sector}: {count}只 ({percentage:.1f}%)")

    print(f"\n总计: {total}只股票")

    # 提交更改
    conn.commit()
    print("\n✅ 赛道分类添加完成！")

    conn.close()


def main():
    """主函数"""
    db_path = Path(__file__).parent.parent / "data" / "market.db"

    if not db_path.exists():
        print(f"❌ 数据库文件不存在: {db_path}")
        return

    print(f"数据库: {db_path}\n")
    add_missing_sectors(str(db_path))


if __name__ == "__main__":
    main()
