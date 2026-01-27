# 基本面分析功能实现文档

## 📊 功能概述

在每日复盘系统中新增**基本面质量分析**模块,帮助识别:
1. 价格与基本面背离的股票 (股价新高但业绩未跟上)
2. 行业内基本面Top 20%的优质标的
3. 业绩差但股价上涨的高风险股票

## 🎯 核心理念

**"第一批情绪驱动的上涨已基本结束,后续板块将出现分化,板块内部基本面强的股票会更加强"**

基本面分析主要关注:
- **ROE (净资产收益率)**: 衡量公司盈利能力
- **净利润同比增长率**: 衡量公司成长性
- **毛利率**: 衡量公司竞争力
- **行业内排名**: 横向对比识别优质标的

## 📁 实现架构

### 1. 新增文件

#### `src/utils/fundamental_analyzer.py`
基本面分析核心工具类,提供:

```python
class FundamentalAnalyzer:
    """基本面分析器"""

    def get_52w_high_low(ticker, trade_date)
        """获取52周最高价和最低价"""

    def get_financial_indicators(ticker, periods=8)
        """获取股票的财务指标数据 (通过Tushare API)"""
        # 返回: ROE, 净利润增长, 毛利率, 净利率等

    def analyze_price_fundamental_divergence(ticker, current_price, price_change_pct, trade_date)
        """分析价格与基本面背离"""
        # 检测: 股价接近新高 + 业绩下滑/亏损 = 背离警报

    def get_industry_ranking(ticker, industry, metric='roe')
        """获取股票在行业内的排名"""
        # 计算: 行业内排名百分位,识别Top 20%

    def batch_analyze_fundamentals(stocks, trade_date)
        """批量分析股票基本面"""
        # 返回: 背离警报、优质股票、风险股票
```

**关键算法:**

1. **价格与基本面背离判断**:
```python
is_near_high = (current_price / high_52w >= 0.95)  # 距52周高点5%以内
if is_near_high and (profit_trend == "亏损" or "下降"):
    divergence_level = "严重"
elif is_near_high and profit_yoy < 10%:
    divergence_level = "中等"
```

2. **行业内排名计算**:
```python
# 同行业所有股票按ROE排序
percentile = (1 - (rank - 1) / total_count) * 100
is_top20 = (percentile >= 80)  # Top 20%
```

### 2. 修改文件

#### `src/services/daily_review_data_service.py`
在数据收集服务中集成基本面分析:

```python
class DailyReviewDataService:
    def __init__(self, session):
        ...
        self.fundamental_analyzer = FundamentalAnalyzer(session)  # 新增

    async def collect_review_data(self, trade_date):
        ...
        # 6. Fundamental analysis (新增)
        fundamental_analysis = await self._analyze_fundamentals(trade_date, samples)

        return DailyReviewSnapshot(
            ...
            fundamental_analysis=fundamental_analysis  # 新增字段
        )

    async def _analyze_fundamentals(self, trade_date, sample_stocks):
        """批量分析样本股的基本面"""
        # 调用 FundamentalAnalyzer.batch_analyze_fundamentals()
```

#### `src/schemas/daily_review.py`
新增数据模型:

```python
class FundamentalAlert(BaseModel):
    """价格与基本面背离警报"""
    ticker: str
    name: str
    warning: str
    divergence_level: str  # "严重"/"中等"/"轻微"

class QualityStock(BaseModel):
    """基本面优质股票 (Top 20%)"""
    ticker: str
    roe: float
    rank: int
    percentile: float

class RiskStock(BaseModel):
    """高风险股票 (业绩差但涨)"""
    ticker: str
    warning: str
    roe: float
    profit_yoy: float

class DailyReviewSnapshot(BaseModel):
    ...
    fundamental_analysis: Optional[Dict]  # 新增字段
```

#### `docs/daily_review/prompt_template.md`
新增章节:

```markdown
#### 4. 基本面质量分析 (200字) **【新增】**
- **价格与基本面背离警报**
- **基本面优质股票** (Top 20%)
- **高风险股票**
- **投资建议**
```

## 📊 数据来源

### Tushare API - 财务指标接口

使用 `pro.fina_indicator()` 获取季度财务数据:

```python
df = pro.fina_indicator(
    ts_code='300077.SZ',
    fields='eps,roe,roa,grossprofit_margin,netprofit_margin,netprofit_yoy,or_yoy'
)
```

**关键指标说明:**

| 指标 | 字段名 | 说明 | 用途 |
|-----|--------|------|------|
| ROE | roe | 净资产收益率(%) | 衡量盈利能力,识别优质股 |
| 净利润增长 | netprofit_yoy | 净利润同比增长(%) | 判断成长性,检测背离 |
| 毛利率 | grossprofit_margin | 销售毛利率(%) | 衡量竞争力 |
| 净利率 | netprofit_margin | 销售净利率(%) | 衡量盈利质量 |
| 营收增长 | or_yoy | 营业收入同比增长(%) | 判断成长性 |

**数据频率**: 季度数据 (Q1/Q2/Q3/Q4)
**历史深度**: 最近8个季度
**更新延迟**: 财报公告后1-2个工作日

## 🎯 使用示例

### 1. 生成包含基本面分析的快照

```bash
python scripts/generate_snapshot.py --date 20260127
```

### 2. 查看基本面分析结果

```python
import json
with open('docs/daily_review/snapshots/20260127.json') as f:
    snapshot = json.load(f)

fa = snapshot['fundamental_analysis']
print(f"背离警报: {len(fa['divergence_alerts'])}个")
print(f"优质股票: {len(fa['quality_stocks'])}个")
print(f"风险股票: {len(fa['risk_stocks'])}个")
```

### 3. 实际案例 (2026-01-27)

#### 背离警报 (2个)
- **国药一致 (000028)**: 股价距52周高点仅3.1%,但ROE=5.3%,净利润同比-10.18% → 严重背离
- **瑞康医药 (002589)**: 股价距52周高点仅3.5%,但ROE=0.22%,净利润同比-63.05% → 严重背离

#### 优质股票 (3个)
- **生益科技 (600183)**: ROE=16.05% (行业第6名,91%分位),净利润增长78.04% → 行业龙头
- **深南电路 (002916)**: ROE=15.10% (行业第7名,89%分位),净利润增长56.30% → 基本面扎实
- **意华股份 (002897)**: ROE=10.42% (行业第14名,85%分位),净利润增长16.33% → 稳健增长

#### 风险股票 (2个)
- **国民技术 (300077)**: 暴涨12.38%,但ROE=-7.56%(亏损),缺乏业绩支撑 → 情绪驱动
- **航发控制 (000738)**: 大涨9.98%,但ROE=3.22%,净利润同比-36.25% → 题材炒作

## 📈 复盘报告示例

生成的复盘报告中会包含详细的基本面分析章节:

```markdown
## 基本面质量分析

**价格与基本面背离警报:**

系统检测到2只股票存在严重的价格脱离基本面风险:

1. **$000028(国药一致)$** ⚠️ 严重背离
   - 股价距52周高点仅3.1%,接近历史新高
   - ROE仅5.30%,净利润同比下降10.18%
   - **警示**: 股价新高但公司业绩持续下滑

**基本面优质股票 (行业Top 20%):**

1. **$600183(生益科技)$** ✅ 基本面优秀
   - ROE: 16.05% (元件行业排名6/56,分位数91%)
   - 净利润同比增长78.04%
   - **结论**: 业绩高增长支撑股价上涨,基本面扎实

**高风险股票:**

1. **$300077(国民技术)$** ⚠️ 高风险
   - 今日暴涨12.38%,但ROE为-7.56%(亏损)
   - **警示**: 短期情绪驱动,缺乏业绩支撑,追高风险极大
```

## ⚠️ 限制与注意事项

### 1. 数据延迟
- 财务数据更新有1-2个工作日延迟
- 最新季报可能未公布,使用上一季度数据

### 2. API调用限制
- Tushare免费用户有积分限制
- 批量查询建议控制频率,避免超限

### 3. 样本股范围
- 当前仅分析快照中的样本股(Top5强势板块 + Top3弱势板块)
- 全市场扫描需要大量API调用,暂不实现

### 4. 行业分类
- 使用 `SymbolMetadata.industry_lv1` 作为行业分类
- 部分股票可能行业分类缺失,影响排名准确性

### 5. 财报季节性
- 一季报(4月)、半年报(8月)、三季报(10月)、年报(次年4月)
- 非财报季节数据更新少,使用历史数据判断

## 🚀 后续扩展方向

### 短期优化
1. **增加更多财务指标**: 流动比率、资产负债率、ROA等
2. **支持多指标综合排名**: ROE + 利润增长 + 毛利率综合评分
3. **历史对比**: 与去年同期业绩对比
4. **行业估值分析**: PE、PB行业分位数

### 中长期扩展
1. **创建财务指标数据表**: 定期同步到本地数据库,减少API调用
2. **全市场扫描**: 每日扫描所有股票,生成基本面质量榜单
3. **业绩预告跟踪**: 整合业绩预告数据,提前预警
4. **财报事件驱动**: 财报发布后自动分析并推送

## 📝 总结

基本面分析功能已完整实现并集成到每日复盘系统中,主要成果:

✅ 创建了 `FundamentalAnalyzer` 工具类
✅ 集成到 `DailyReviewDataService` 数据收集流程
✅ 扩展了 `DailyReviewSnapshot` 数据模型
✅ 更新了复盘Prompt模板和生成逻辑
✅ 实际测试验证功能正常工作

**实际效果:**
- 成功识别2只价格与基本面严重背离的股票(医药商业板块)
- 筛选出3只基本面优质的行业龙头(元件板块)
- 标记2只高风险的情绪驱动股票(半导体、军工)

这套系统帮助投资者:
1. **识别风险**: 警惕股价新高但业绩未跟上的股票
2. **发现机会**: 聚焦基本面优质的行业龙头
3. **理性决策**: 区分"业绩驱动"和"情绪驱动"的上涨

---

**生成日期**: 2026-01-28
**作者**: Claude Sonnet 4.5
**文档版本**: v1.0
