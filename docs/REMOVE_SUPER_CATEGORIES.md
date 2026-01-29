# 删除超级行业组分类计划

## 概述

删除14个超级行业组分类系统，保留以下分类：
- ✅ 赛道分类 (16个) - 主要投资主题分类
- ✅ 自选股分类 (5个) - 个人持仓管理

---

## 要删除的文件和代码

### 1. 数据文件
- `data/super_category_mapping.csv` (已在git中)

### 2. 后端API相关

**routes_boards.py** 中的超级行业组端点：
- `GET /api/boards/super-categories` - 获取超级行业组列表
- `GET /api/boards/super-categories/daily` - 获取日线数据

需要删除的方法：
- `get_super_categories()`
- `get_super_category_daily()`

### 3. 前端组件

需要删除或修改的文件：
- `frontend/src/components/SuperCategoryView.tsx` - 整个组件删除
- `frontend/src/components/MarketHeatMap.tsx` - 修改为使用赛道分类
- `frontend/src/components/BoardsView.tsx` - 移除super-categories相关代码
- `frontend/src/components/StockDetail.tsx` - 移除superCategory显示
- `frontend/src/components/CandleCard.tsx` - 移除superCategory显示

### 4. 类型定义

**frontend/src/types/symbol.ts**:
- 移除 `superCategory: string | null;` 字段

### 5. 数据库表

- `super_category_daily` 表（可选择保留或删除）

### 6. 模型和Schema

**src/models/symbol.py**:
- 移除 `super_category` 字段

**src/schemas/base.py**:
- 移除 `super_category` 字段

---

## 影响分析

### 可能受影响的功能

1. **市场热力图** (MarketHeatMap)
   - 当前使用超级行业组
   - **解决方案**: 改为使用16个赛道分类

2. **板块视图** (BoardsView)
   - 显示超级行业组
   - **解决方案**: 只显示赛道分类

3. **股票详情** (StockDetail)
   - 显示超级行业组标签
   - **解决方案**: 改为显示赛道分类

4. **K线卡片** (CandleCard)
   - 显示超级行业组
   - **解决方案**: 改为显示赛道分类

### 不受影响的功能

- ✅ 赛道成交额统计 (已使用 stock_sectors)
- ✅ 概念板块监控
- ✅ 自选股管理
- ✅ K线图表

---

## 删除步骤

### Step 1: 删除数据文件
```bash
git rm data/super_category_mapping.csv
```

### Step 2: 删除后端代码

编辑 `src/api/routes_boards.py`，删除：
- `get_super_categories()` 方法（约50行）
- `get_super_category_daily()` 方法（约80行）

### Step 3: 删除/修改前端组件

```bash
# 删除整个组件
git rm frontend/src/components/SuperCategoryView.tsx

# 需要手动修改的文件
# - MarketHeatMap.tsx: 改用 /api/sectors/
# - BoardsView.tsx: 移除 super-categories
# - StockDetail.tsx: 移除 superCategory 显示
# - CandleCard.tsx: 移除 superCategory 显示
```

### Step 4: 清理类型定义

```typescript
// frontend/src/types/symbol.ts
// 删除这一行:
superCategory: string | null;
```

### Step 5: 清理后端模型（可选）

如果想彻底清理：
```python
# src/models/symbol.py
# 删除:
super_category: Mapped[str | None] = mapped_column(...)

# src/schemas/base.py
# 删除:
super_category: Optional[str] = Field(...)
```

### Step 6: 删除数据库表（可选）

```bash
sqlite3 data/market.db "DROP TABLE IF EXISTS super_category_daily;"
```

---

## 替代方案：用赛道分类替代

### 修改 MarketHeatMap 使用赛道分类

```typescript
// 原来:
const response = await apiFetch("/api/boards/super-categories");

// 改为:
const response = await apiFetch("/api/sectors/list/available");
// 然后获取每个赛道的统计数据
```

### 修改 StockDetail 显示赛道

```typescript
// 原来:
{symbol.superCategory && (
  <span>{symbol.superCategory}</span>
)}

// 改为:
{symbol.sector && (
  <span>{symbol.sector}</span>
)}
```

---

## 测试清单

删除后需要测试：

- [ ] 前端启动无错误
- [ ] 后端API启动无错误
- [ ] 市场热力图正常显示（使用赛道分类）
- [ ] 板块视图正常显示
- [ ] 股票详情页正常显示
- [ ] K线卡片正常显示
- [ ] 赛道成交额统计正常工作

---

## 回滚方案

如果删除后出现问题，可以通过git回滚：

```bash
git checkout HEAD~1 data/super_category_mapping.csv
git checkout HEAD~1 src/api/routes_boards.py
git checkout HEAD~1 frontend/src/components/SuperCategoryView.tsx
```
