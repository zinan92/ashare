#!/usr/bin/env python
"""
测试 Tushare 迁移
验证所有核心功能是否正常工作
"""

import sys
from pathlib import Path

# 添加项目根目录到 Python 路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.config import get_settings
from src.services.tushare_client import TushareClient
from src.services.tushare_data_provider import TushareDataProvider
from src.services.tushare_client import TushareClient
from src.models import Timeframe


def print_section(title: str):
    """打印章节标题"""
    print(f"\n{'='*60}")
    print(f"  {title}")
    print(f"{'='*60}\n")


def test_configuration():
    """测试配置加载"""
    print_section("1. 测试配置加载")

    settings = get_settings()

    print(f"✓ Tushare Token: {settings.tushare_token[:20]}...")
    print(f"✓ 积分等级: {settings.tushare_points}")
    print(f"✓ 基础延迟: {settings.tushare_delay} 秒")
    print(f"✓ 最大重试: {settings.tushare_max_retries}")
    print(f"✓ 数据库URL: {settings.database_url}")
    print(f"✓ K线回溯: {settings.candle_lookback}")

    if not settings.tushare_token or settings.tushare_token == "your_tushare_token_here":
        print("\n❌ 错误：请在 .env 文件中配置 TUSHARE_TOKEN")
        return False

    print("\n✅ 配置加载成功")
    return True


def test_tushare_client():
    """测试 Tushare 客户端"""
    print_section("2. 测试 Tushare 客户端")

    settings = get_settings()

    try:
        client = TushareClient(
            token=settings.tushare_token,
            points=settings.tushare_points
        )
        print("✓ Tushare 客户端初始化成功")

        # 测试获取股票列表
        print("\n测试: 获取股票列表...")
        stock_list = client.fetch_stock_list(list_status='L')
        print(f"✓ 获取到 {len(stock_list)} 只上市股票")

        # 测试获取日线数据（贵州茅台）
        print("\n测试: 获取日线数据 (600519)...")
        daily_df = client.fetch_daily(
            ts_code='600519.SH',
            start_date='20241001',
            end_date='20241114'
        )
        print(f"✓ 获取到 {len(daily_df)} 根日K线")
        if not daily_df.empty:
            print(f"  最新日期: {daily_df.iloc[0]['trade_date']}")
            print(f"  收盘价: {daily_df.iloc[0]['close']}")

        # 测试获取每日指标
        print("\n测试: 获取每日指标 (600519)...")
        latest_date = client.get_latest_trade_date()
        print(f"  最新交易日: {latest_date}")

        basic_df = client.fetch_daily_basic(
            ts_code='600519.SH',
            trade_date=latest_date
        )

        if not basic_df.empty:
            row = basic_df.iloc[0]
            print(f"✓ PE(TTM): {row.get('pe_ttm', 'N/A')}")
            print(f"✓ PB: {row.get('pb', 'N/A')}")
            print(f"✓ 总市值: {row.get('total_mv', 'N/A')} 万元")
            print(f"✓ 流通市值: {row.get('circ_mv', 'N/A')} 万元")

        print("\n✅ Tushare 客户端测试通过")
        return True

    except Exception as e:
        print(f"\n❌ Tushare 客户端测试失败: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_data_provider():
    """测试数据提供者"""
    print_section("3. 测试 Tushare 数据提供者")

    try:
        provider = TushareDataProvider()
        print("✓ TushareDataProvider 初始化成功")

        # 测试获取 K 线数据
        print("\n测试: 获取日K线 (000001)...")
        candles_df = provider.fetch_candles(
            ticker='000001',
            timeframe=Timeframe.DAY,
            limit=10
        )

        print(f"✓ 获取到 {len(candles_df)} 根K线")
        print(f"  列: {list(candles_df.columns)}")

        if not candles_df.empty:
            latest = candles_df.iloc[-1]
            print(f"  最新日期: {latest['timestamp']}")
            print(f"  收盘价: {latest['close']}")
            print(f"  MA5: {latest['ma5']:.2f}")

        # 测试获取元数据
        print("\n测试: 获取元数据 (000001, 600519)...")
        metadata_df = provider.fetch_symbol_metadata(['000001', '600519'])

        print(f"✓ 获取到 {len(metadata_df)} 条元数据")

        for _, row in metadata_df.iterrows():
            print(f"\n  股票: {row['ticker']} - {row['name']}")
            print(f"    总市值: {row.get('total_mv', 'N/A')} 万")
            print(f"    PE(TTM): {row.get('pe_ttm', 'N/A')}")
            print(f"    PB: {row.get('pb', 'N/A')}")

        print("\n✅ 数据提供者测试通过")
        return True

    except Exception as e:
        print(f"\n❌ 数据提供者测试失败: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_board_service():
    """测试板块服务"""
    print_section("4. 测试板块服务")

    try:
        settings = get_settings()
        client = TushareClient(
            token=settings.tushare_token,
            points=settings.tushare_points,
            delay=settings.tushare_delay,
            max_retries=settings.tushare_max_retries,
        )
        print("✓ TushareClient (board queries) 初始化成功")

        # 测试获取概念板块（从 Tushare API）
        print("\n测试: 获取同花顺概念板块列表...")

        concept_boards = client.fetch_ths_index(type='N')

        print(f"✓ 获取到 {len(concept_boards)} 个概念板块")

        # 显示前5个
        print("\n前5个概念板块:")
        for _, row in concept_boards.head(5).iterrows():
            print(f"  - {row['name']} (代码: {row['ts_code']}, 成分股: {row.get('count', 0)})")

        # 测试获取成分股
        if not concept_boards.empty:
            first_board = concept_boards.iloc[0]
            board_code = first_board['ts_code']
            board_name = first_board['name']

            print(f"\n测试: 获取 '{board_name}' 的成分股...")

            members = client.fetch_ths_member(ts_code=board_code)

            print(f"✓ 成分股数量: {len(members)}")

            if not members.empty:
                # Tushare ths_member 返回的字段名是 'con_code' 而不是 'code'
                code_field = 'con_code' if 'con_code' in members.columns else 'code'
                print(f"  前5只: {list(members[code_field].head(5))}")

        print("\n✅ 板块服务测试通过")
        return True

    except Exception as e:
        print(f"\n❌ 板块服务测试失败: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    """主测试函数"""
    print("=" * 60)
    print("  Tushare 迁移测试")
    print("=" * 60)

    results = []

    # 运行所有测试
    results.append(("配置加载", test_configuration()))
    results.append(("Tushare 客户端", test_tushare_client()))
    results.append(("数据提供者", test_data_provider()))
    results.append(("板块服务", test_board_service()))

    # 打印测试摘要
    print_section("测试摘要")

    passed = sum(1 for _, result in results if result)
    total = len(results)

    for name, result in results:
        status = "✅ 通过" if result else "❌ 失败"
        print(f"{name}: {status}")

    print(f"\n总计: {passed}/{total} 通过")

    if passed == total:
        print("\n🎉 所有测试通过！迁移成功！")
        return 0
    else:
        print("\n⚠️  部分测试失败，请检查错误信息")
        return 1


if __name__ == "__main__":
    sys.exit(main())
