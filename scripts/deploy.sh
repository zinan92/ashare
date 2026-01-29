#!/bin/bash
set -e

# A-Share-Data 自动化部署脚本
# 用于快速在新环境中部署项目

echo "=================================="
echo "  A-Share-Data 自动化部署脚本"
echo "=================================="
echo ""

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# 检查命令是否存在
command_exists() {
    command -v "$1" >/dev/null 2>&1
}

# 打印成功消息
print_success() {
    echo -e "${GREEN}✓${NC} $1"
}

# 打印错误消息
print_error() {
    echo -e "${RED}✗${NC} $1"
}

# 打印警告消息
print_warning() {
    echo -e "${YELLOW}!${NC} $1"
}

# 步骤1: 检查环境依赖
echo "步骤 1/7: 检查环境依赖..."
echo ""

# 检查 Python
if command_exists python3; then
    PYTHON_VERSION=$(python3 --version | cut -d' ' -f2)
    print_success "Python 3 已安装 (版本: $PYTHON_VERSION)"
else
    print_error "Python 3 未安装，请先安装 Python 3.9+"
    exit 1
fi

# 检查 pip
if command_exists pip3; then
    print_success "pip3 已安装"
else
    print_error "pip3 未安装，请先安装 pip"
    exit 1
fi

# 检查 Node.js
if command_exists node; then
    NODE_VERSION=$(node --version)
    print_success "Node.js 已安装 (版本: $NODE_VERSION)"
else
    print_warning "Node.js 未安装，前端功能将不可用"
    SKIP_FRONTEND=1
fi

# 检查 npm
if command_exists npm; then
    NPM_VERSION=$(npm --version)
    print_success "npm 已安装 (版本: $NPM_VERSION)"
else
    if [ -z "$SKIP_FRONTEND" ]; then
        print_warning "npm 未安装，前端功能将不可用"
        SKIP_FRONTEND=1
    fi
fi

echo ""

# 步骤2: 创建必要目录
echo "步骤 2/7: 创建必要目录..."
mkdir -p data logs
print_success "已创建 data/ 和 logs/ 目录"
echo ""

# 步骤3: 配置环境变量
echo "步骤 3/7: 配置环境变量..."
if [ ! -f .env ]; then
    if [ -f .env.example ]; then
        cp .env.example .env
        print_success "已从 .env.example 创建 .env 文件"
        print_warning "请编辑 .env 文件，填入你的 TUSHARE_TOKEN"
        echo ""
        echo "  编辑命令: nano .env"
        echo "  或: code .env"
        echo ""
        read -p "是否现在编辑 .env 文件? (y/N) " -n 1 -r
        echo
        if [[ $REPLY =~ ^[Yy]$ ]]; then
            ${EDITOR:-nano} .env
        fi
    else
        print_error ".env.example 文件不存在"
        exit 1
    fi
else
    print_success ".env 文件已存在"
fi
echo ""

# 步骤4: 创建 Python 虚拟环境
echo "步骤 4/7: 创建 Python 虚拟环境..."
if [ ! -d "venv" ]; then
    python3 -m venv venv
    print_success "已创建虚拟环境 venv/"
else
    print_success "虚拟环境已存在"
fi

# 激活虚拟环境
source venv/bin/activate || . venv/Scripts/activate 2>/dev/null
print_success "已激活虚拟环境"
echo ""

# 步骤5: 安装 Python 依赖
echo "步骤 5/7: 安装 Python 依赖..."
if [ -f requirements.txt ]; then
    pip install --upgrade pip -q
    pip install -r requirements.txt -q
    print_success "已安装 Python 依赖"
else
    print_error "requirements.txt 不存在"
    exit 1
fi
echo ""

# 步骤6: 初始化数据库
echo "步骤 6/7: 初始化数据库..."
if [ -f scripts/init_db.py ]; then
    if [ -f data/market.db ]; then
        print_warning "数据库已存在，跳过初始化"
        read -p "是否要重新初始化数据库? (y/N) " -n 1 -r
        echo
        if [[ $REPLY =~ ^[Yy]$ ]]; then
            python scripts/init_db.py
            print_success "数据库初始化完成"
        fi
    else
        python scripts/init_db.py
        print_success "数据库初始化完成"
    fi
else
    print_warning "初始化脚本不存在，请手动初始化数据库"
fi
echo ""

# 步骤7: 安装前端依赖（如果可用）
if [ -z "$SKIP_FRONTEND" ]; then
    echo "步骤 7/7: 安装前端依赖..."
    if [ -d frontend ]; then
        cd frontend
        if [ -f package.json ]; then
            npm install -q
            print_success "已安装前端依赖"
        else
            print_error "frontend/package.json 不存在"
        fi
        cd ..
    else
        print_warning "frontend/ 目录不存在"
    fi
else
    echo "步骤 7/7: 跳过前端安装（Node.js/npm 未安装）"
fi
echo ""

# 部署完成
echo "=================================="
echo "  ✓ 部署完成！"
echo "=================================="
echo ""
echo "接下来的步骤："
echo ""
echo "1. 确认环境变量配置："
echo "   编辑 .env 文件，填入你的 TUSHARE_TOKEN"
echo ""
echo "2. 启动后端服务："
echo "   source venv/bin/activate  # 激活虚拟环境"
echo "   uvicorn src.main:app --reload"
echo ""

if [ -z "$SKIP_FRONTEND" ]; then
    echo "3. 启动前端服务（新终端）："
    echo "   cd frontend"
    echo "   npm run dev"
    echo ""
fi

echo "4. 访问应用："
echo "   后端: http://localhost:8000"
if [ -z "$SKIP_FRONTEND" ]; then
    echo "   前端: http://localhost:5173"
fi
echo "   API文档: http://localhost:8000/docs"
echo ""

echo "需要帮助？查看文档:"
echo "   docs/DEPLOYMENT_GUIDE.md"
echo ""

# 检查是否需要配置 TUSHARE_TOKEN
if grep -q "your_token_here" .env 2>/dev/null || grep -q "TUSHARE_TOKEN=$" .env 2>/dev/null; then
    print_warning "警告: 请配置 .env 中的 TUSHARE_TOKEN 后再启动服务"
fi
