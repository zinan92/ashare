#!/usr/bin/env python3
"""
新机器初始化脚本
用于在新机器上快速搭建环境和恢复数据

使用场景:
1. 在新机器上 clone 代码
2. 运行此脚本初始化数据库和恢复自选股
3. 下载K线数据
"""

import sys
import subprocess
from pathlib import Path
from datetime import datetime

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))


def run_command(cmd, description):
    """运行shell命令"""
    print(f"\n{'='*60}")
    print(f"🔧 {description}")
    print(f"{'='*60}")
    print(f"命令: {cmd}")

    result = subprocess.run(cmd, shell=True, cwd=project_root)

    if result.returncode != 0:
        print(f"❌ 失败")
        return False

    print(f"✅ 完成")
    return True


def main():
    """主流程"""

    print("="*60)
    print("🚀 新机器初始化脚本")
    print("="*60)
    print(f"项目目录: {project_root}")
    print(f"当前时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # 检查必要文件
    print("\n" + "="*60)
    print("📋 检查必要文件")
    print("="*60)

    required_files = [
        'requirements.txt',
        '.env',
        'backups/watchlist_latest.json',
        'src/database.py',
        'alembic.ini'
    ]

    missing_files = []
    for file in required_files:
        file_path = project_root / file
        if file_path.exists():
            print(f"✅ {file}")
        else:
            print(f"❌ {file} (缺失)")
            missing_files.append(file)

    if missing_files:
        print(f"\n⚠️  缺失文件: {missing_files}")
        if '.env' in missing_files:
            print("\n💡 提示: 需要创建 .env 文件并配置 TUSHARE_TOKEN")
        return 1

    # 步骤1: 安装Python依赖
    if not run_command(
        "pip install -r requirements.txt",
        "步骤1: 安装Python依赖"
    ):
        return 1

    # 步骤2: 创建数据目录
    print("\n" + "="*60)
    print("🔧 步骤2: 创建数据目录")
    print("="*60)

    data_dir = project_root / "data"
    data_dir.mkdir(exist_ok=True)
    print(f"✅ 数据目录: {data_dir}")

    # 步骤3: 初始化数据库
    if not run_command(
        "alembic upgrade head",
        "步骤3: 初始化数据库Schema"
    ):
        print("\n💡 提示: 如果失败，可能需要手动运行:")
        print("   python -c 'from src.database import Base, engine; Base.metadata.create_all(engine)'")
        return 1

    # 步骤4: 恢复自选股
    backup_file = project_root / "backups/watchlist_latest.json"
    if backup_file.exists():
        print("\n" + "="*60)
        print("🔧 步骤4: 恢复自选股")
        print("="*60)

        # 询问是否恢复
        print(f"\n找到备份文件: {backup_file}")
        response = input("是否恢复自选股? (yes/no): ")

        if response.lower() == 'yes':
            if not run_command(
                f"python scripts/backup_watchlist.py restore {backup_file}",
                "恢复自选股数据"
            ):
                print("⚠️  自选股恢复失败，可稍后手动执行")
        else:
            print("⏭️  跳过自选股恢复")
    else:
        print("\n⚠️  未找到自选股备份文件")

    # 步骤5: 下载基础数据
    print("\n" + "="*60)
    print("🔧 步骤5: 下载基础数据")
    print("="*60)

    print("\n可选的数据下载任务:")
    print("  1. 股票基本信息 (必需)")
    print("  2. 历史K线数据 (可选，根据需要)")
    print("  3. 板块数据 (可选)")

    response = input("\n是否现在下载股票基本信息? (yes/no): ")

    if response.lower() == 'yes':
        # 下载股票列表和基本信息
        if not run_command(
            "python scripts/update_stock_list.py",
            "下载股票列表和基本信息"
        ):
            print("⚠️  股票列表下载失败")

    # 完成
    print("\n" + "="*60)
    print("✅ 初始化完成！")
    print("="*60)

    print("\n📝 后续步骤:")
    print("\n1. 下载自选股的K线数据:")
    print("   python scripts/download_watchlist_klines.py")

    print("\n2. 启动后端服务:")
    print("   uvicorn src.main:app --reload")

    print("\n3. 启动前端:")
    print("   cd frontend && npm install && npm run dev")

    print("\n4. 定期更新数据:")
    print("   python scripts/update_daily_klines.py")

    print("\n" + "="*60)

    return 0


if __name__ == "__main__":
    exit(main())
