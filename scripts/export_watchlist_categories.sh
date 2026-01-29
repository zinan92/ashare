#!/bin/bash

# 导出自选股分类数据脚本
# 用于在 fork 后恢复自定义分类

set -e

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$( cd "$SCRIPT_DIR/.." && pwd )"
DB_FILE="$PROJECT_ROOT/data/market.db"
OUTPUT_DIR="$PROJECT_ROOT/data/watchlist_categories"

echo "=================================="
echo "  导出自选股分类数据"
echo "=================================="
echo ""

# 创建输出目录
mkdir -p "$OUTPUT_DIR"

# 导出每个分类的股票列表
echo "正在导出分类数据..."

sqlite3 "$DB_FILE" << 'EOF' | while IFS='|' read -r category ticker name; do
    if [ ! -z "$category" ] && [ "$category" != "category" ]; then
        SAFE_CATEGORY=$(echo "$category" | tr '/' '_')
        echo "$ticker,$name" >> "$OUTPUT_DIR/${SAFE_CATEGORY}.csv"
    fi
done
SELECT category, ticker, name FROM watchlist ORDER BY category, ticker;
EOF

# 为每个分类文件添加header
for file in "$OUTPUT_DIR"/*.csv; do
    if [ -f "$file" ]; then
        # 添加header到临时文件
        echo "ticker,name" > "$file.tmp"
        cat "$file" >> "$file.tmp"
        mv "$file.tmp" "$file"

        CATEGORY_NAME=$(basename "$file" .csv)
        COUNT=$(wc -l < "$file" | tr -d ' ')
        COUNT=$((COUNT - 1))  # 减去header行
        echo "  ✓ ${CATEGORY_NAME}: ${COUNT} 只股票"
    fi
done

# 生成导入脚本
cat > "$OUTPUT_DIR/import_categories.py" << 'PYTHON_SCRIPT'
#!/usr/bin/env python3
"""
导入自选股分类数据

使用方法:
    python data/watchlist_categories/import_categories.py
"""

import sys
from pathlib import Path
import pandas as pd

# 添加项目根目录到path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.database import session_scope
from src.models import Watchlist

def import_categories():
    """导入分类数据到数据库"""
    data_dir = Path(__file__).parent

    # 获取所有分类文件
    csv_files = list(data_dir.glob("*.csv"))

    if not csv_files:
        print("没有找到分类数据文件")
        return

    with session_scope() as session:
        for csv_file in csv_files:
            category_name = csv_file.stem  # 文件名即分类名

            # 读取CSV
            df = pd.read_csv(csv_file)

            updated_count = 0
            for _, row in df.iterrows():
                ticker = row['ticker']

                # 查找现有记录
                watchlist_item = session.query(Watchlist).filter(
                    Watchlist.ticker == ticker
                ).first()

                if watchlist_item:
                    # 更新分类
                    watchlist_item.category = category_name
                    updated_count += 1

            session.commit()
            print(f"✓ {category_name}: 更新了 {updated_count} 只股票")

if __name__ == "__main__":
    print("开始导入自选股分类数据...")
    print("")
    import_categories()
    print("")
    print("导入完成！")
PYTHON_SCRIPT

chmod +x "$OUTPUT_DIR/import_categories.py"

echo ""
echo "=================================="
echo "  ✓ 导出完成！"
echo "=================================="
echo ""
echo "导出目录: $OUTPUT_DIR"
echo ""
echo "在新环境中恢复分类:"
echo "  1. 复制整个 watchlist_categories/ 目录到新项目"
echo "  2. 运行: python data/watchlist_categories/import_categories.py"
echo ""
