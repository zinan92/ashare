# A-Share-Data 部署指南

## 快速概览

当你 fork 这个项目到新环境时，以下内容**不会**被 git 同步（在 `.gitignore` 中）：

### ❌ 不会同步的内容

1. **环境变量** (`.env`)
2. **数据库文件** (`data/*.db`)
3. **数据文件** (`data/*.csv`)
4. **日志文件** (`logs/`)
5. **Python虚拟环境** (`venv/`, `.venv/`)
6. **前端依赖** (`frontend/node_modules/`)
7. **IDE配置** (`.vscode/`, `.idea/`)

### ✅ 会同步的内容

1. **所有源代码** (`src/`, `frontend/src/`)
2. **配置模板** (`.env.example`)
3. **依赖清单** (`requirements.txt`, `package.json`)
4. **文档** (`docs/`, `README.md`)

---

## 完整部署流程

### 方案一：手动部署（推荐新手）

#### 1. Fork 并克隆项目

```bash
# Fork 项目到你的 GitHub 账户，然后克隆
git clone https://github.com/YOUR_USERNAME/a-share-data.git
cd a-share-data
```

#### 2. 配置环境变量

```bash
# 复制模板文件
cp .env.example .env

# 编辑 .env 文件，填入你的配置
nano .env  # 或使用你喜欢的编辑器
```

**必须配置的变量**：

```bash
# Tushare API Token (必须)
TUSHARE_TOKEN=your_token_here

# 数据库路径 (默认即可)
DATABASE_URL=sqlite:///data/market.db

# 前端CORS (根据你的前端端口调整)
ALLOW_ORIGINS=http://localhost:5173
```

**可选配置**：

```bash
# 自选股列表（逗号分隔）
DEFAULT_SYMBOLS=600519,601318,000001

# 定时更新时间（Cron表达式）
DAILY_REFRESH_CRON=30 16 * * 1-5

# Tushare积分等级
TUSHARE_POINTS=15000  # 根据你的实际积分调整
```

#### 3. 创建必要的目录

```bash
mkdir -p data logs
```

#### 4. 安装后端依赖

```bash
# 创建虚拟环境
python3 -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate

# 安装依赖
pip install -r requirements.txt
```

#### 5. 初始化数据库

```bash
# 运行数据库初始化脚本
python scripts/init_db.py
```

#### 6. 安装前端依赖

```bash
cd frontend
npm install
cd ..
```

#### 7. 启动服务

**后端**：
```bash
# 方式1: 开发模式
uvicorn src.main:app --reload --host 0.0.0.0 --port 8000

# 方式2: 生产模式
python -m uvicorn src.main:app --host 0.0.0.0 --port 8000
```

**前端**：
```bash
cd frontend
npm run dev
```

---

### 方案二：自动化部署（推荐有经验的用户）

我们提供了自动化部署脚本：

```bash
# 1. 克隆项目
git clone https://github.com/YOUR_USERNAME/a-share-data.git
cd a-share-data

# 2. 运行部署脚本
./scripts/deploy.sh
```

部署脚本会自动：
- ✅ 检查环境依赖
- ✅ 创建必要目录
- ✅ 复制环境变量模板
- ✅ 安装Python和Node.js依赖
- ✅ 初始化数据库
- ✅ 提示你配置 `.env`

---

## 敏感信息管理

### 🔐 需要手动迁移的敏感信息

#### 1. Tushare API Token

**获取方式**：
1. 注册 [Tushare](https://tushare.pro/)
2. 查看你的 Token: https://tushare.pro/user/token
3. 复制到 `.env` 文件的 `TUSHARE_TOKEN`

**重要**: Token 是私密的，**永远不要提交到 git**！

#### 2. 数据库文件

**选项A**: 从旧环境导出数据
```bash
# 在旧机器上
cd /path/to/old-project
tar -czf data-backup.tar.gz data/*.db data/*.csv

# 传输到新机器（通过 scp、rsync 等）
scp data-backup.tar.gz new-machine:/path/to/new-project/

# 在新机器上解压
cd /path/to/new-project
tar -xzf data-backup.tar.gz
```

**选项B**: 重新初始化数据库
```bash
# 直接运行初始化脚本（会下载最新数据）
python scripts/init_db.py
python scripts/fetch_initial_data.py
```

#### 3. 自选股和自定义配置

如果你在旧环境有自定义配置：

```bash
# 从旧 .env 复制特定配置
DEFAULT_SYMBOLS=600519,601318,000001,300750  # 你的自选股
DAILY_REFRESH_CRON=30 16 * * 1-5  # 你的定时任务

# 或直接复制整个 .env（记得删除敏感信息后再分享）
```

---

## 跨平台注意事项

### macOS → Linux

```bash
# 路径分隔符相同，无需修改
# 注意文件权限
chmod +x scripts/*.sh
```

### Windows → Linux/macOS

```bash
# 转换行结尾符（如果出现问题）
dos2unix scripts/*.sh

# Windows路径使用反斜杠，Linux/macOS使用正斜杠
# 项目已使用 pathlib 处理，无需手动修改
```

### 不同Python版本

```bash
# 检查Python版本（需要 3.9+）
python3 --version

# 如果版本不对，使用 pyenv 或 conda
pyenv install 3.11
pyenv local 3.11
```

---

## 生产环境部署建议

### 1. 使用环境变量而非 .env 文件

**在服务器上设置环境变量**：

```bash
# systemd service 文件
[Service]
Environment="TUSHARE_TOKEN=your_token"
Environment="DATABASE_URL=sqlite:///data/market.db"
```

或使用 Docker：

```yaml
# docker-compose.yml
services:
  backend:
    environment:
      - TUSHARE_TOKEN=${TUSHARE_TOKEN}
      - DATABASE_URL=${DATABASE_URL}
```

### 2. 数据持久化

```bash
# 将 data/ 目录挂载到持久卷
# Docker
volumes:
  - ./data:/app/data

# 或使用专用数据库（PostgreSQL/MySQL）
DATABASE_URL=postgresql://user:pass@localhost/ashare
```

### 3. 定时任务

```bash
# 使用 crontab
30 16 * * 1-5 cd /path/to/project && ./scripts/update_daily.sh

# 或使用 systemd timer
# /etc/systemd/system/ashare-update.timer
```

### 4. 反向代理

```nginx
# nginx 配置
server {
    listen 80;
    server_name your-domain.com;

    location /api {
        proxy_pass http://localhost:8000;
    }

    location / {
        proxy_pass http://localhost:5173;
    }
}
```

---

## 常见问题

### Q: Fork 后如何同步原项目的更新？

```bash
# 添加上游仓库
git remote add upstream https://github.com/zinan92/ashare.git

# 拉取上游更新
git fetch upstream
git merge upstream/main

# 或使用 rebase
git rebase upstream/main
```

### Q: 如何备份我的数据和配置？

```bash
# 创建备份脚本（建议加入 crontab）
#!/bin/bash
DATE=$(date +%Y%m%d)
tar -czf backup-$DATE.tar.gz data/ .env logs/
```

### Q: 数据库太大，如何优化？

```bash
# 清理旧数据（保留最近90天）
python scripts/cleanup_old_data.py --days 90

# 压缩数据库
sqlite3 data/market.db "VACUUM;"
```

### Q: 如何在多台机器间同步配置？

**推荐方案**：使用私有配置仓库

```bash
# 创建私有仓库存储配置
git init private-config
cd private-config

# 添加配置文件
cp /path/to/project/.env .
cp /path/to/project/data/*.csv .

# 推送到私有仓库
git add .
git commit -m "Add configs"
git remote add origin git@github.com:YOUR_USERNAME/ashare-private-config.git
git push -u origin main
```

在新机器上：

```bash
# 克隆主项目
git clone https://github.com/YOUR_USERNAME/a-share-data.git

# 克隆私有配置
git clone git@github.com:YOUR_USERNAME/ashare-private-config.git

# 复制配置文件
cp ashare-private-config/.env a-share-data/
cp ashare-private-config/*.csv a-share-data/data/
```

---

## 安全检查清单

部署前请确认：

- [ ] `.env` 文件不在 git 仓库中
- [ ] `TUSHARE_TOKEN` 已替换为你的 Token
- [ ] 数据库文件不被公开访问
- [ ] 生产环境使用了 HTTPS
- [ ] 前端 CORS 配置正确
- [ ] 日志文件定期清理
- [ ] 定期备份数据库和配置

---

## 获取帮助

如果遇到问题：

1. 查看 [README.md](../README.md)
2. 检查 [Issues](https://github.com/zinan92/ashare/issues)
3. 运行诊断脚本: `python scripts/diagnose.sh`
4. 提交新 Issue 并附上日志

---

**最后更新**: 2026-01-29
