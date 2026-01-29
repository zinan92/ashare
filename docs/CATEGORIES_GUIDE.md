# 分类数据完整指南

## 📊 分类体系概览

A-Share-Data 项目有 **三层分类体系**，每层有不同的用途和数据来源：

```
├── 1. 超级行业组 (14个) - 宏观行业分类
├── 2. 自定义赛道 (4个) - 投资主题跟踪
└── 3. 自选股分类 (5个) - 个人分类管理
```

---

## 1️⃣ 超级行业组 (Super Categories)

### 基本信息
- **数量**: 14个
- **数据来源**: `data/super_category_mapping.csv`
- **API端点**: `GET /api/boards/super-categories`
- **Fork后**: ✅ 会自动同步（现已加入 git）

### 分类列表

| 序号 | 分类名称 | 进攻性评分 | 说明 |
|------|---------|----------|------|
| 1 | 半导体与硬件 | 95 | 芯片制造、电子元器件 |
| 2 | 软件与互联网 | 90 | 软件、IT服务、互联网平台 |
| 3 | 通信与5G | 75 | 通信设备、电信运营 |
| 4 | 消费电子 | 80 | 手机、可穿戴设备 |
| 5 | 新能源产业链 | 85 | 光伏、电池、风电 |
| 6 | 汽车产业链 | 70 | 整车、零部件、汽车服务 |
| 7 | 智能制造 | 65 | 工程机械、自动化设备 |
| 8 | 军工航天 | 55 | 军工电子、国防装备 |
| 9 | 医药健康 | 35 | 化学制药、生物制品、医疗器械 |
| 10 | 大消费 | 40 | 白酒、食品、零售、家电 |
| 11 | 资源能源 | 50 | 煤炭、油气、钢铁、化工 |
| 12 | 基建地产链 | 45 | 建筑、建材、房地产、物流 |
| 13 | 公用事业 | 10 | 电力、燃气、环保 |
| 14 | 金融地产 | 25 | 银行、保险、证券 |

### 使用方式

**前端调用**:
```typescript
const response = await fetch('/api/boards/super-categories');
const data = await response.json();
```

**查看数据文件**:
```bash
cat data/super_category_mapping.csv
```

---

## 2️⃣ 自定义赛道 (Custom Tracks)

### 基本信息
- **数量**: 4个
- **数据来源**: `src/api/routes_tracks.py` (硬编码)
- **API端点**: `GET /api/tracks`
- **Fork后**: ✅ 会自动同步（在代码中）

### 分类列表

| 赛道名称 | 对应概念板块 | 说明 |
|---------|------------|------|
| 人形机器人 | 人形机器人 | 人形机器人产业链 |
| PCB | PCB概念 | 印刷电路板 |
| 液冷 | 液冷服务器 | 数据中心液冷技术 |
| 储能 | 储能 | 储能设备和系统 |

### 添加新赛道

编辑 `src/api/routes_tracks.py`:

```python
CUSTOM_TRACKS: Dict[str, List[str]] = {
    "人形机器人": ["人形机器人"],
    "PCB": ["PCB概念"],
    "液冷": ["液冷服务器"],
    "储能": ["储能"],
    # 添加新赛道
    "AI芯片": ["AI芯片", "GPU"],
}

TRACK_ORDER: List[str] = ["人形机器人", "PCB", "液冷", "储能", "AI芯片"]
```

### 使用方式

**获取所有赛道**:
```bash
curl http://localhost:8000/api/tracks
```

**获取赛道成分股**:
```bash
curl http://localhost:8000/api/tracks/人形机器人/symbols
```

---

## 3️⃣ 自选股分类 (Watchlist Categories)

### 基本信息
- **数量**: 5个（你的当前配置）
- **总股票数**: 348只
- **数据来源**: 数据库 `watchlist` 表 + `data/watchlist_categories/`
- **API端点**: `GET /api/watchlist`
- **Fork后**: ✅ 现在会自动同步（已导出到 git）

### 当前分类统计

| 分类名称 | 股票数量 | 数据文件 |
|---------|---------|---------|
| AI应用 | 19 | AI应用.csv |
| 创新药 | 20 | 创新药.csv |
| 金属 | 26 | 金属.csv |
| 贵金属 | 11 | 贵金属.csv |
| 未分类 | 272 | 未分类.csv |

### Fork 后恢复分类

#### 方式1: 自动导入（推荐）

分类数据已导出到 `data/watchlist_categories/`，fork 后会随 git 同步。

**步骤**:
```bash
# 1. Fork 并克隆项目
git clone https://github.com/YOUR_USERNAME/a-share-data.git
cd a-share-data

# 2. 部署项目
./scripts/deploy.sh

# 3. 导入分类数据
python data/watchlist_categories/import_categories.py
```

输出示例:
```
开始导入自选股分类数据...

✓ AI应用: 更新了 19 只股票
✓ 创新药: 更新了 20 只股票
✓ 金属: 更新了 26 只股票
✓ 贵金属: 更新了 11 只股票
✓ 未分类: 更新了 272 只股票

导入完成！共更新 348 只股票的分类
```

#### 方式2: 从备份恢复

```bash
# 在旧环境备份
./scripts/backup.sh

# 传输备份到新环境
scp backups/ashare-backup-*.tar.gz new-machine:/path/

# 在新环境恢复
cd /path/to/new-project
tar -xzf ashare-backup-*.tar.gz
cp -r ashare-backup-*/data/watchlist_categories data/
python data/watchlist_categories/import_categories.py
```

### 修改分类

#### 通过API修改单个股票分类:

```bash
curl -X PUT http://localhost:8000/api/watchlist/600519 \
  -H "Content-Type: application/json" \
  -d '{"category": "大消费"}'
```

#### 批量修改分类:

编辑 `data/watchlist_categories/AI应用.csv`，添加新股票:
```csv
ticker
688111
002230
600519  # 新增
```

然后导入:
```bash
python data/watchlist_categories/import_categories.py
```

### 导出最新分类

当你修改了分类后，重新导出以同步到 git:

```bash
# 导出当前数据库中的分类
python scripts/export_categories.py

# 提交到 git
git add data/watchlist_categories/
git commit -m "更新自选股分类"
git push
```

---

## 🔄 数据流向

```
┌─────────────────────────────────────┐
│  1. 超级行业组 (14个)                │
│  data/super_category_mapping.csv    │
│  ├─ 半导体与硬件                    │
│  ├─ 软件与互联网                    │
│  └─ ...                             │
└─────────────────────────────────────┘
              ↓ 用于行业分析
┌─────────────────────────────────────┐
│  2. 自定义赛道 (4个)                 │
│  src/api/routes_tracks.py           │
│  ├─ 人形机器人 → 概念板块映射        │
│  ├─ PCB                             │
│  └─ ...                             │
└─────────────────────────────────────┘
              ↓ 投资主题跟踪
┌─────────────────────────────────────┐
│  3. 自选股分类 (5个)                 │
│  data/watchlist_categories/         │
│  ├─ AI应用 (19只)                   │
│  ├─ 创新药 (20只)                   │
│  └─ ...                             │
└─────────────────────────────────────┘
              ↓ 个人持仓管理
         ┌─────────┐
         │ 数据库  │
         └─────────┘
```

---

## 📝 API 端点汇总

| 功能 | 端点 | 分类层级 |
|------|------|---------|
| 获取超级行业组列表 | `GET /api/boards/super-categories` | 1 |
| 获取超级行业组日线数据 | `GET /api/boards/super-categories/daily` | 1 |
| 获取所有赛道 | `GET /api/tracks` | 2 |
| 获取赛道详情 | `GET /api/tracks/{track_name}` | 2 |
| 获取赛道成分股 | `GET /api/tracks/{track_name}/symbols` | 2 |
| 获取自选股列表 | `GET /api/watchlist` | 3 |
| 添加自选股 | `POST /api/watchlist` | 3 |
| 更新自选股分类 | `PUT /api/watchlist/{ticker}` | 3 |
| 删除自选股 | `DELETE /api/watchlist/{ticker}` | 3 |

---

## 🛠️ 工具脚本

### 导出分类数据
```bash
python scripts/export_categories.py
```

### 导入分类数据
```bash
python data/watchlist_categories/import_categories.py
```

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

### Q1: Fork 后为什么只看到5个分类？

**A**: 你可能在看**自选股分类**，而不是**超级行业组**。

- 超级行业组: 14个，用于行业分析
- 自选股分类: 5个，用于个人持仓管理

两者是不同的分类体系。

### Q2: 如何添加新的自选股分类？

**方式1**: 通过API添加股票时指定分类
```bash
curl -X POST http://localhost:8000/api/watchlist \
  -H "Content-Type: application/json" \
  -d '{"ticker": "600519", "category": "白酒"}'
```

**方式2**: 编辑CSV文件
```bash
# 创建新分类文件
echo "ticker" > data/watchlist_categories/白酒.csv
echo "600519" >> data/watchlist_categories/白酒.csv

# 导入
python data/watchlist_categories/import_categories.py
```

### Q3: 如何修改超级行业组？

编辑 `data/super_category_mapping.csv`:
```csv
超级行业组,进攻性评分,行业名称,备注
新分类,85,细分行业1,说明
新分类,85,细分行业2,说明
```

提交后会自动同步到所有 fork。

### Q4: 数据库中的分类和CSV中的分类不一致怎么办？

CSV是导出的快照，数据库是实时数据。

**更新流程**:
1. 修改数据库中的分类（通过API或直接SQL）
2. 运行 `python scripts/export_categories.py` 导出
3. 提交到 git

---

## 📚 相关文档

- [部署指南](DEPLOYMENT_GUIDE.md) - 完整部署流程
- [README.md](../README.md) - 项目概览
- [API文档](http://localhost:8000/docs) - 完整API参考

---

**最后更新**: 2026-01-29
