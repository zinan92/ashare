# 分类数据完整指南

## 📊 统一分类体系

A-Share-Data 项目使用 **统一的赛道分类系统**，简化了分类管理：

```
赛道分类 (16个) - 投资主题分类
├── 数据存储: stock_sectors + available_sectors 表
├── 数据文件: data/sectors/*.csv
├── 应用范围: 所有股票 + 自选股
└── Fork后: ✅ 自动同步（已加入 git）
```

---

## 🎯 赛道分类 (Sectors)

### 基本信息
- **数量**: 16个赛道
- **总股票数**: 369只
- **数据来源**:
  - 数据库: `stock_sectors` + `available_sectors` 表
  - CSV文件: `data/sectors/*.csv`
- **API端点**: `GET /api/sectors/list/available`
- **Fork后**: ✅ 会自动同步（现已加入 git）

### 当前赛道分类统计

| 赛道名称 | 股票数量 | 占比 | 数据文件 |
|---------|---------|------|---------|
| AI应用 | 67只 | 18.2% | AI应用.csv |
| 金属 | 58只 | 15.7% | 金属.csv |
| 军工 | 46只 | 12.5% | 军工.csv |
| 机器人 | 38只 | 10.3% | 机器人.csv |
| 芯片 | 33只 | 8.9% | 芯片.csv |
| 创新药 | 27只 | 7.3% | 创新药.csv |
| 光伏 | 20只 | 5.4% | 光伏.csv |
| 发电 | 19只 | 5.1% | 发电.csv |
| 新能源汽车 | 14只 | 3.8% | 新能源汽车.csv |
| 贵金属 | 12只 | 3.3% | 贵金属.csv |
| 其他 | 12只 | 3.3% | 其他.csv |
| 半导体 | 7只 | 1.9% | 半导体.csv |
| PCB | 7只 | 1.9% | PCB.csv |
| 消费 | 4只 | 1.1% | 消费.csv |
| 可控核聚变 | 3只 | 0.8% | 可控核聚变.csv |
| 脑机接口 | 2只 | 0.5% | 脑机接口.csv |

### 与自选股的关系

✅ **统一分类**: 自选股现在直接使用赛道分类

- 自选股表 (`watchlist`) 中的 `category` 字段引用 `stock_sectors.sector`
- 348只自选股全部使用16个赛道分类
- 不再有独立的自选股分类系统

**示例**:
```sql
-- 某只股票在两个表中的分类一致
stock_sectors:  600519 -> "消费"
watchlist:      600519 -> "消费"  (统一)
```

---

## 🔄 Fork 后恢复分类

### 方式1: 自动导入（推荐）

赛道分类数据已导出到 `data/sectors/`，fork 后会随 git 同步。

**步骤**:
```bash
# 1. Fork 并克隆项目
git clone https://github.com/YOUR_USERNAME/a-share-data.git
cd a-share-data

# 2. 部署项目
./scripts/deploy.sh

# 3. 导入赛道分类数据
python data/sectors/import_sectors.py
```

输出示例:
```
开始导入赛道分类数据...

✓ AI应用: 插入了 67 只股票
✓ 芯片: 插入了 33 只股票
✓ 金属: 插入了 58 只股票
...

导入完成！共导入 369 只股票
```

### 方式2: 从备份恢复

```bash
# 在旧环境备份
./scripts/backup.sh

# 传输备份到新环境
scp backups/ashare-backup-*.tar.gz new-machine:/path/

# 在新环境恢复
cd /path/to/new-project
tar -xzf ashare-backup-*.tar.gz
cp -r ashare-backup-*/data/sectors data/
python data/sectors/import_sectors.py
```

---

## 🛠️ 管理赛道分类

### 添加新股票到赛道

#### 方式1: 通过Python脚本
```python
import sqlite3
from datetime import datetime

conn = sqlite3.connect('data/market.db')
cursor = conn.cursor()

# 添加股票到赛道
now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
cursor.execute(
    "INSERT INTO stock_sectors (ticker, sector, created_at, updated_at) VALUES (?, ?, ?, ?)",
    ("600519", "消费", now, now)
)

conn.commit()
conn.close()
```

#### 方式2: 直接SQL
```sql
INSERT INTO stock_sectors (ticker, sector, created_at, updated_at)
VALUES ('600519', '消费', datetime('now'), datetime('now'));
```

### 批量添加股票

编辑对应的CSV文件，比如 `data/sectors/消费.csv`:
```csv
ticker
600519
000858
600887
```

然后导入:
```bash
python data/sectors/import_sectors.py
```

### 修改股票分类

```sql
UPDATE stock_sectors
SET sector = '新能源汽车', updated_at = datetime('now')
WHERE ticker = '600519';
```

### 添加新赛道

```sql
-- 1. 添加赛道定义
INSERT INTO available_sectors (name, display_order, created_at)
VALUES ('量子科技', 16, datetime('now'));

-- 2. 添加成分股
INSERT INTO stock_sectors (ticker, sector, created_at, updated_at)
VALUES ('688027', '量子科技', datetime('now'), datetime('now'));
```

### 导出最新分类

当你修改了分类后，重新导出以同步到 git:

```bash
# 导出当前数据库中的分类
python scripts/export_sectors.py

# 提交到 git
git add data/sectors/
git commit -m "更新赛道分类"
git push
```

---

## 📝 API 端点汇总

| 功能 | 端点 | 说明 |
|------|------|------|
| 获取所有赛道定义 | `GET /api/sectors/list/available` | 16个赛道列表 |
| 获取赛道成分股 | `GET /api/sectors/{sector_name}/stocks` | 某个赛道的所有股票 |
| 获取赛道成交额统计 | `GET /api/sectors/turnover` | 各赛道成交额排名 |
| 批量查询赛道成交额 | `POST /api/sectors/batch` | 批量获取多个赛道数据 |
| 获取自选股列表 | `GET /api/watchlist` | 348只自选股（含赛道分类） |
| 添加自选股 | `POST /api/watchlist` | 添加股票到自选股 |
| 更新自选股分类 | `PUT /api/watchlist/{ticker}` | 修改自选股的赛道分类 |
| 删除自选股 | `DELETE /api/watchlist/{ticker}` | 从自选股移除 |

### API 使用示例

**获取所有赛道**:
```bash
curl http://localhost:8000/api/sectors/list/available
```

**获取某个赛道的成分股**:
```bash
curl http://localhost:8000/api/sectors/AI应用/stocks
```

**获取赛道成交额统计**:
```bash
curl http://localhost:8000/api/sectors/turnover
```

**添加自选股并指定赛道**:
```bash
curl -X POST http://localhost:8000/api/watchlist \
  -H "Content-Type: application/json" \
  -d '{"ticker": "600519", "category": "消费"}'
```

---

## 🔄 数据流向

```
┌──────────────────────────────────────────┐
│  赛道分类定义 (16个)                      │
│  available_sectors 表                    │
│  data/sectors/available_sectors.json     │
├──────────────────────────────────────────┤
│  1. AI应用     (display_order: 0)        │
│  2. 芯片       (display_order: 1)        │
│  3. PCB        (display_order: 2)        │
│  ...                                     │
│  16. 贵金属    (display_order: 15)       │
└──────────────────────────────────────────┘
              ↓
┌──────────────────────────────────────────┐
│  股票赛道映射 (369只)                     │
│  stock_sectors 表                        │
│  data/sectors/*.csv                      │
├──────────────────────────────────────────┤
│  600519 -> 消费                          │
│  000858 -> 新能源汽车                     │
│  601012 -> 金属                          │
│  ...                                     │
└──────────────────────────────────────────┘
              ↓
┌──────────────────────────────────────────┐
│  自选股 (348只)                          │
│  watchlist 表                            │
│  category 字段引用 stock_sectors.sector  │
├──────────────────────────────────────────┤
│  600519: category = "消费" ✅            │
│  000858: category = "新能源汽车" ✅      │
│  601012: category = "金属" ✅            │
└──────────────────────────────────────────┘
```

---

## 🛠️ 工具脚本

### 导出赛道分类数据
```bash
python scripts/export_sectors.py
```
- 从数据库导出到 `data/sectors/`
- 生成每个赛道的CSV文件
- 生成汇总JSON文件
- 生成导入脚本

### 导入赛道分类数据
```bash
python data/sectors/import_sectors.py
```
- 从 `data/sectors/` 导入到数据库
- 先清空表再导入（完整替换）
- 适用于fork后的初始化

### 统一分类
```bash
python scripts/unify_watchlist_categories.py
```
- 将自选股分类统一为赛道分类
- 已执行过，无需再次运行

### 添加缺失的赛道分类
```bash
python scripts/add_missing_sectors.py
```
- 为自选股中未分类的股票添加赛道
- 已执行过，无需再次运行

### 备份所有数据
```bash
./scripts/backup.sh
```

### 部署到新环境
```bash
./scripts/deploy.sh
```

---

## ❓ 常见问题

### Q1: 为什么统一分类？

**A**: 之前有两套分类系统很混乱：
- ❌ 自选股分类 (5个): AI应用、创新药、金属、贵金属、未分类
- ✅ 赛道分类 (16个): 更完整的投资主题分类

现在统一使用赛道分类，分类更细致，管理更方便。

### Q2: 自选股的分类数据存在哪里？

**A**: `watchlist` 表的 `category` 字段，值来自 `stock_sectors.sector`
- 不再有独立的自选股分类CSV文件
- 直接使用16个赛道分类

### Q3: 如何添加新赛道？

**步骤**:
1. 添加赛道定义到 `available_sectors` 表
2. 添加成分股到 `stock_sectors` 表
3. 导出数据: `python scripts/export_sectors.py`
4. 提交到git

### Q4: Fork后只有部分股票有赛道分类怎么办？

**A**: 运行导入脚本:
```bash
python data/sectors/import_sectors.py
```

这会从git同步的CSV文件恢复所有369只股票的赛道分类。

### Q5: 数据库中的分类和CSV中的分类不一致怎么办？

**A**: CSV是导出的快照，数据库是实时数据。

**更新流程**:
1. 修改数据库中的分类（通过API或直接SQL）
2. 运行 `python scripts/export_sectors.py` 导出
3. 提交到 git

---

## 📚 相关文档

- [部署指南](DEPLOYMENT_GUIDE.md) - 完整部署流程
- [README.md](../README.md) - 项目概览
- [API文档](http://localhost:8000/docs) - 完整API参考

---

**最后更新**: 2026-01-29
**版本**: v2.0 - 统一分类系统
