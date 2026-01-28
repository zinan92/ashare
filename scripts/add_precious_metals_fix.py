#!/usr/bin/env python3
"""
添加贵金属股票到贵金属板块

添加:
- 紫金矿业 (601899) - 目前在工业金属,需要添加到贵金属
"""

import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.database import SessionLocal
from src.models.board import BoardMapping
from sqlalchemy.orm import Session


def add_stock_to_board(
    session: Session,
    board_code: str,
    board_name: str,
    ticker: str,
    stock_name: str
) -> bool:
    """
    Add a stock to a board's constituents.

    Args:
        session: Database session
        board_code: Board code (e.g., '881169.TI')
        board_name: Board name for display
        ticker: Stock ticker (e.g., '601899')
        stock_name: Stock name for display

    Returns:
        True if added, False if already exists
    """
    board = session.query(BoardMapping).filter(
        BoardMapping.board_code == board_code
    ).first()

    if not board:
        print(f"✗ 板块不存在: {board_name} ({board_code})")
        return False

    # Check if already exists
    if board.constituents and ticker in board.constituents:
        print(f"  {stock_name} ({ticker}) - 已存在")
        return False

    # Add to constituents
    if board.constituents is None:
        board.constituents = []

    board.constituents = list(set(board.constituents + [ticker]))
    session.commit()

    print(f"✓ {stock_name} ({ticker}) - 已添加到 {board_name}")
    return True


def main():
    """Main function."""
    db = SessionLocal()

    try:
        print("="*60)
        print("添加贵金属股票到贵金属板块")
        print("="*60)

        # 贵金属板块代码
        precious_metal_code = '881169.TI'
        precious_metal_name = '贵金属'

        # 需要添加的股票
        stocks_to_add = [
            ('601899', '紫金矿业'),
        ]

        print(f"\n目标板块: {precious_metal_name} ({precious_metal_code})")
        print(f"需要添加: {len(stocks_to_add)} 只股票\n")

        added_count = 0
        existing_count = 0

        for ticker, name in stocks_to_add:
            if add_stock_to_board(
                db,
                precious_metal_code,
                precious_metal_name,
                ticker,
                name
            ):
                added_count += 1
            else:
                existing_count += 1

        # 显示最终统计
        print("\n" + "="*60)
        print(f"完成统计:")
        print(f"  新增: {added_count} 只")
        print(f"  已存在: {existing_count} 只")

        # 显示更新后的成分股列表
        board = db.query(BoardMapping).filter(
            BoardMapping.board_code == precious_metal_code
        ).first()

        if board:
            print(f"\n{precious_metal_name}板块当前成分股: {len(board.constituents)} 只")
            print(f"成分股列表: {sorted(board.constituents)}")

        print("="*60)

    except Exception as e:
        print(f"\n✗ 错误: {e}")
        import traceback
        traceback.print_exc()
        return 1

    finally:
        db.close()

    return 0


if __name__ == "__main__":
    exit(main())
