# 新机器快速部署指南

在新机器上快速搭建 A-Share-Data 项目环境

## 📋 前置要求

- Python 3.9+
- Git
- Node.js 16+ (前端)
- Tushare Token (在 tushare.pro 注册获取)

## 🚀 快速开始

### 1. Clone 代码

```bash
git clone https://github.com/zinan92/ashare.git
cd a-share-data
```

### 2. 配置环境

创建 `.env` 文件:

```bash
cp .env.example .env
# 编辑 .env 文件，填入你的 TUSHARE_TOKEN
```

```env
TUSHARE_TOKEN=your_token_here
TUSHARE_POINTS=15000
TUSHARE_DELAY=0.3
```

### 3. 运行初始化脚本

```bash
python scripts/init_new_machine.py
```

这个脚本会自动:
- ✅ 安装Python依赖
- ✅ 创建数据目录
- ✅ 初始化数据库Schema
- ✅ 恢复自选股列表 (348只)
- ✅ 下载股票基本信息

### 4. 下载K线数据

#### 方式A: 只下载自选股数据 (推荐，快速)

```bash
# 下载自选股的K线数据 (200根日线 + 30分钟线)
python scripts/download_watchlist_klines.py --periods 200
```

**预计时间**: 约12分钟 (348只 × 2秒)

#### 方式B: 下载全市场数据 (完整，耗时)

```bash
# 下载所有股票的日线数据
python scripts/download_all_klines.py --timeframe 1d --periods 200
```

**预计时间**: 约2-3小时 (5000+只股票)

### 5. 启动服务

#### 后端

```bash
uvicorn src.main:app --reload --port 8000
```

#### 前端

```bash
cd frontend
npm install
npm run dev
```

访问: http://localhost:5173

## 📊 数据说明

### 什么数据在 Git 中？

✅ **在 Git 中 (会同步)**:
- 代码和脚本
- 数据库Schema (Alembic迁移)
- **自选股列表** (348只股票的ticker)
- 配置文件模板

❌ **不在 Git 中 (需重新下载)**:
- `data/market.db` (2.5GB数据库)
- K线数据 (每只股票200根 × 多个周期)
- 实时行情缓存

### 为什么不上传数据库？

1. **太大**: 2.5GB，会让仓库变得臃肿
2. **频繁变化**: 每日更新会产生大量git历史
3. **可重建**: K线数据可以从Tushare重新下载
4. **更灵活**: 不同机器可以有不同的数据范围

### 自选股如何同步？

✅ **自选股列表已备份到 Git**:
- `backups/watchlist_latest.json` - 最新备份
- `backups/watchlist_tickers.txt` - Ticker列表

在新机器上运行初始化脚本时会自动恢复。

## 🔄 日常使用

### 更新自选股备份

在主力机器上定期备份:

```bash
python scripts/backup_watchlist.py
git add backups/watchlist_latest.json
git commit -m "update: 更新自选股备份"
git push
```

在新机器上同步:

```bash
git pull
python scripts/backup_watchlist.py restore backups/watchlist_latest.json
```

### 更新K线数据

```bash
# 更新自选股的最新K线
python scripts/update_watchlist_klines.py

# 或使用现有的更新脚本
python scripts/update_daily_klines.py
```

## 📁 目录结构

```
a-share-data/
├── data/                    # 数据目录 (不在git中)
│   └── market.db           # SQLite数据库
├── backups/                 # 备份目录 (在git中)
│   ├── watchlist_latest.json
│   └── watchlist_tickers.txt
├── scripts/                 # 脚本
│   ├── init_new_machine.py        # 新机器初始化
│   ├── backup_watchlist.py        # 自选股备份
│   └── download_watchlist_klines.py  # 下载K线
├── src/                     # 源代码
│   ├── database.py         # 数据库配置
│   ├── models/             # 数据模型
│   └── api/                # API路由
└── alembic/                # 数据库迁移
```

## 🛠️ 故障排查

### 问题1: 数据库初始化失败

```bash
# 手动创建数据库表
python -c "from src.database import Base, engine; Base.metadata.create_all(engine)"
```

### 问题2: Tushare API调用失败

检查:
- `.env` 文件中的 `TUSHARE_TOKEN` 是否正确
- Tushare积分是否足够
- 网络连接是否正常

### 问题3: 自选股恢复失败

手动恢复:

```bash
python scripts/backup_watchlist.py restore backups/watchlist_latest.json
```

### 问题4: K线下载太慢

优化策略:
- 只下载自选股 (348只)
- 减少K线根数 (--periods 100)
- 只下载日线 (--timeframes 1d)

## 📈 K线数据策略

### 最小化方案 (快速启动)

只下载自选股的日线:

```bash
python scripts/download_watchlist_klines.py --periods 100 --timeframes 1d
```

**数据量**: ~350MB
**下载时间**: ~6分钟

### 标准方案 (推荐)

下载自选股的日线和30分钟线:

```bash
python scripts/download_watchlist_klines.py --periods 200 --timeframes 1d 30m
```

**数据量**: ~700MB
**下载时间**: ~12分钟

### 完整方案 (数据分析)

下载全市场数据:

```bash
python scripts/download_all_klines.py --periods 200
```

**数据量**: ~2GB+
**下载时间**: ~2-3小时

## 🔐 安全提示

⚠️ **不要提交到 Git**:
- `.env` 文件 (包含Token)
- `data/` 目录 (数据库)
- `*.db` 文件

✅ **已在 .gitignore**:
```gitignore
.env
data/
*.db
```

## 📞 支持

遇到问题？
- 查看项目 Issues: https://github.com/zinan92/ashare/issues
- 查看现有脚本: `scripts/` 目录
- 查看文档: `docs/` 目录

---

**更新日期**: 2026-01-28
**维护者**: @zinan92
