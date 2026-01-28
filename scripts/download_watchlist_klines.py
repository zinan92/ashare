#!/usr/bin/env python3
"""
下载自选股K线数据
从自选股列表批量下载K线数据（可配置周期和根数）
"""

import sys
from pathlib import Path
from datetime import datetime, timedelta
import asyncio

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.database import SessionLocal
from sqlalchemy import text


async def download_stock_klines(ticker, name, periods=200, timeframes=['1d', '30m']):
    """
    下载单只股票的K线数据

    Args:
        ticker: 股票代码
        name: 股票名称
        periods: K线根数 (默认200根)
        timeframes: 时间周期列表 (默认日线和30分钟)
    """
    print(f"📊 下载 {name} ({ticker}) - {periods}根K线")

    # 这里应该调用你现有的K线下载逻辑
    # 示例: 调用 tushare 或其他数据源
    # 实际实现需要根据你现有的下载脚本调整

    try:
        # 示例代码结构（需要根据实际情况修改）
        for timeframe in timeframes:
            print(f"  下载 {timeframe} 数据...")
            # TODO: 实现实际的下载逻辑
            # await download_kline_data(ticker, timeframe, periods)

        print(f"  ✅ {name} 完成")
        return True

    except Exception as e:
        print(f"  ❌ {name} 失败: {e}")
        return False


async def main():
    """主函数"""

    import argparse

    parser = argparse.ArgumentParser(description='下载自选股K线数据')
    parser.add_argument('--periods', type=int, default=200,
                       help='K线根数 (默认200)')
    parser.add_argument('--timeframes', nargs='+',
                       default=['1d', '30m'],
                       help='时间周期 (默认: 1d 30m)')
    parser.add_argument('--batch-size', type=int, default=10,
                       help='批次大小 (默认10只并发)')

    args = parser.parse_args()

    print("="*60)
    print("📥 下载自选股K线数据")
    print("="*60)
    print(f"K线根数: {args.periods}")
    print(f"时间周期: {args.timeframes}")
    print(f"批次大小: {args.batch_size}")

    db = SessionLocal()

    try:
        # 获取自选股列表
        result = db.execute(text("""
            SELECT w.ticker, s.name
            FROM watchlist w
            LEFT JOIN symbol_metadata s ON w.ticker = s.ticker
            ORDER BY w.added_at
        """)).fetchall()

        total = len(result)
        print(f"\n自选股总数: {total} 只")

        if total == 0:
            print("⚠️  自选股列表为空")
            return 1

        # 询问确认
        print(f"\n将下载 {total} 只股票的K线数据")
        print(f"预计时间: ~{total * 2 / 60:.1f} 分钟 (每只约2秒)")

        response = input("\n确认下载? (yes/no): ")
        if response.lower() != 'yes':
            print("取消下载")
            return 0

        # 批量下载
        print("\n" + "="*60)
        print("开始下载...")
        print("="*60)

        success_count = 0
        failed_count = 0

        # 分批处理
        for i in range(0, total, args.batch_size):
            batch = result[i:i + args.batch_size]
            print(f"\n批次 {i//args.batch_size + 1}/{(total + args.batch_size - 1)//args.batch_size}")
            print("-"*60)

            # 并发下载当前批次
            tasks = [
                download_stock_klines(
                    ticker=row[0],
                    name=row[1] if row[1] else row[0],
                    periods=args.periods,
                    timeframes=args.timeframes
                )
                for row in batch
            ]

            results = await asyncio.gather(*tasks, return_exceptions=True)

            for result in results:
                if result is True:
                    success_count += 1
                else:
                    failed_count += 1

        # 完成统计
        print("\n" + "="*60)
        print("✅ 下载完成")
        print("="*60)
        print(f"成功: {success_count} 只")
        print(f"失败: {failed_count} 只")
        print(f"总计: {total} 只")

        return 0

    except Exception as e:
        print(f"\n❌ 错误: {e}")
        import traceback
        traceback.print_exc()
        return 1

    finally:
        db.close()


if __name__ == "__main__":
    exit(asyncio.run(main()))
