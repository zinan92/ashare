#!/bin/bash

# A-Share-Data 备份脚本
# 用于备份数据库、配置文件和关键数据

set -e

# 颜色定义
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

# 获取项目根目录
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$( cd "$SCRIPT_DIR/.." && pwd )"

# 备份目录
BACKUP_DIR="${BACKUP_DIR:-$PROJECT_ROOT/backups}"
DATE=$(date +%Y%m%d_%H%M%S)
BACKUP_NAME="ashare-backup-$DATE"
BACKUP_PATH="$BACKUP_DIR/$BACKUP_NAME"

echo "=================================="
echo "  A-Share-Data 备份工具"
echo "=================================="
echo ""

# 创建备份目录
mkdir -p "$BACKUP_DIR"
mkdir -p "$BACKUP_PATH"

echo -e "${GREEN}备份目录:${NC} $BACKUP_PATH"
echo ""

# 1. 备份环境变量
echo "1. 备份环境变量..."
if [ -f "$PROJECT_ROOT/.env" ]; then
    cp "$PROJECT_ROOT/.env" "$BACKUP_PATH/.env"
    echo "   ✓ .env"
else
    echo "   ⚠ .env 不存在"
fi

# 2. 备份数据库
echo ""
echo "2. 备份数据库..."
if [ -d "$PROJECT_ROOT/data" ]; then
    mkdir -p "$BACKUP_PATH/data"

    # 备份所有 .db 文件
    find "$PROJECT_ROOT/data" -name "*.db" -type f | while read -r db_file; do
        filename=$(basename "$db_file")
        cp "$db_file" "$BACKUP_PATH/data/$filename"
        echo "   ✓ $filename"
    done
else
    echo "   ⚠ data/ 目录不存在"
fi

# 3. 备份关键数据文件
echo ""
echo "3. 备份关键数据文件..."
if [ -d "$PROJECT_ROOT/data" ]; then
    # 备份重要的 CSV 文件
    CSV_FILES=(
        "concept_to_tickers.csv"
        "hot_concept_categories.csv"
        "concept_board_constituents.csv"
    )

    for csv in "${CSV_FILES[@]}"; do
        if [ -f "$PROJECT_ROOT/data/$csv" ]; then
            cp "$PROJECT_ROOT/data/$csv" "$BACKUP_PATH/data/$csv"
            echo "   ✓ $csv"
        fi
    done
fi

# 4. 备份日志（可选）
echo ""
read -p "是否备份日志文件? (y/N) " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    echo "4. 备份日志..."
    if [ -d "$PROJECT_ROOT/logs" ]; then
        mkdir -p "$BACKUP_PATH/logs"
        cp -r "$PROJECT_ROOT/logs"/* "$BACKUP_PATH/logs/" 2>/dev/null || echo "   ⚠ 没有日志文件"
        echo "   ✓ 日志已备份"
    fi
else
    echo "4. 跳过日志备份"
fi

# 5. 压缩备份
echo ""
echo "5. 压缩备份..."
cd "$BACKUP_DIR"
tar -czf "$BACKUP_NAME.tar.gz" "$BACKUP_NAME"
rm -rf "$BACKUP_NAME"

# 显示备份信息
BACKUP_SIZE=$(du -h "$BACKUP_NAME.tar.gz" | cut -f1)
echo ""
echo "=================================="
echo "  ✓ 备份完成！"
echo "=================================="
echo ""
echo "备份文件: $BACKUP_DIR/$BACKUP_NAME.tar.gz"
echo "文件大小: $BACKUP_SIZE"
echo ""
echo "恢复备份:"
echo "  cd $PROJECT_ROOT"
echo "  tar -xzf $BACKUP_DIR/$BACKUP_NAME.tar.gz"
echo "  cp -r $BACKUP_NAME/* ."
echo ""

# 清理旧备份（保留最近5个）
echo "清理旧备份（保留最近5个）..."
cd "$BACKUP_DIR"
ls -t ashare-backup-*.tar.gz 2>/dev/null | tail -n +6 | xargs rm -f 2>/dev/null || true
echo "✓ 清理完成"
echo ""
