#!/usr/bin/env python
"""
下载示例数据 - 用于快速测试
只下载前50只股票的数据
"""

import sys
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.config import get_settings
from src.services.data_pipeline import MarketDataService
from src.services.tushare_data_provider import TushareDataProvider
from src.services.tushare_client import TushareClient
from src.models import Timeframe
from src.database import init_db

def main():
    print("=" * 60)
    print("  下载示例数据（前50只股票）")
    print("=" * 60)

    # 1. 初始化数据库
    print("\n1. 初始化数据库...")
    init_db()
    print("✓ 数据库初始化完成")

    # 2. 获取股票列表
    print("\n2. 获取股票列表...")
    provider = TushareDataProvider()
    all_tickers = provider.fetch_all_tickers()
    print(f"✓ 获取到 {len(all_tickers)} 只股票")

    # 只取前50只作为示例
    sample_tickers = all_tickers[:50]
    print(f"✓ 将下载前 {len(sample_tickers)} 只股票的数据")

    # 3. 下载 K 线和元数据
    print("\n3. 开始下载 K 线数据...")
    print(f"   时间框架: 日线、周线、月线")
    print(f"   每只股票: 200 根 K 线")
    print(f"   预计时间: 约 3-5 分钟")

    service = MarketDataService(provider=provider)

    try:
        service.refresh_universe(
            tickers=sample_tickers,
            timeframes=[Timeframe.DAY, Timeframe.WEEK, Timeframe.MONTH]
        )
        print("\n✓ K 线数据下载完成")

    except Exception as e:
        print(f"\n❌ 下载失败: {e}")
        import traceback
        traceback.print_exc()
        return 1

    # 4. 同步概念板块（只同步前10个作为示例）
    print("\n4. 同步概念板块...")
    settings = get_settings()
    client = TushareClient(
        token=settings.tushare_token,
        points=settings.tushare_points,
        delay=settings.tushare_delay,
        max_retries=settings.tushare_max_retries,
    )

    try:
        # 获取概念板块列表
        concept_boards = client.fetch_ths_index(type='N')

        # 只同步前10个
        sample_boards = concept_boards.head(10)

        print(f"   共有 {len(concept_boards)} 个概念板块")
        print(f"   将同步前 {len(sample_boards)} 个板块作为示例")

        # 手动同步（简化版）
        from src.database import SessionLocal
        from src.models import BoardMapping
        from datetime import datetime, timezone

        session = SessionLocal()
        synced = 0

        try:
            for _, row in sample_boards.iterrows():
                board_code = row['ts_code']
                board_name = row['name']

                members_df = client.fetch_ths_member(ts_code=board_code)

                if not members_df.empty:
                    code_field = 'con_code' if 'con_code' in members_df.columns else 'code'
                    constituents = [
                        client.denormalize_ts_code(code)
                        for code in members_df[code_field].dropna().tolist()
                    ]

                    board_mapping = BoardMapping(
                        board_name=board_name,
                        board_type='concept',
                        board_code=board_code,
                        constituents=constituents,
                        last_updated=datetime.now(timezone.utc)
                    )

                    session.add(board_mapping)
                    synced += 1
                    print(f"   ✓ {board_name} ({len(constituents)} 只股票)")

            session.commit()
            print(f"\n✓ 同步了 {synced} 个概念板块")

        finally:
            session.close()

    except Exception as e:
        print(f"\n❌ 同步板块失败: {e}")
        import traceback
        traceback.print_exc()

    print("\n" + "=" * 60)
    print("  ✅ 示例数据下载完成！")
    print("=" * 60)
    print("\n下一步:")
    print("1. 启动应用: uvicorn src.main:app --reload")
    print("2. 访问: http://localhost:8000/api/candles/000001?timeframe=day&limit=200")
    print("3. 如需下载全部数据，运行: python scripts/download_all_data.py")
    print()

    return 0


if __name__ == "__main__":
    sys.exit(main())
