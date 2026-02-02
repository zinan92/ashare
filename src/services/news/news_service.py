"""
新闻快讯服务
获取财联社、同花顺等实时快讯
"""
import logging
from typing import List, Optional, Dict, Any
from datetime import datetime
import akshare as ak
import pandas as pd

logger = logging.getLogger(__name__)


class NewsService:
    """新闻快讯服务"""
    
    # 数据源配置
    SOURCES = {
        'cls': {
            'name': '财联社',
            'func': 'stock_info_global_cls',
            'columns': ['标题', '内容', '发布日期', '发布时间'],
        },
        'ths': {
            'name': '同花顺',
            'func': 'stock_info_global_ths', 
            'columns': ['标题', '内容', '发布时间', '链接'],
        },
        'futu': {
            'name': '富途牛牛',
            'func': 'stock_info_global_futu',
            'columns': ['标题', '内容', '发布时间'],
        },
    }
    
    def __init__(self):
        self._cache: Dict[str, Dict[str, Any]] = {}
        self._cache_ttl = 30  # 缓存30秒
    
    def _is_cache_valid(self, source: str) -> bool:
        """检查缓存是否有效"""
        if source not in self._cache:
            return False
        cached = self._cache[source]
        elapsed = (datetime.now() - cached['timestamp']).total_seconds()
        return elapsed < self._cache_ttl
    
    def fetch_news(
        self, 
        source: str = 'cls',
        limit: int = 20,
        use_cache: bool = True
    ) -> List[Dict[str, Any]]:
        """
        获取快讯
        
        Args:
            source: 数据源 (cls/ths/futu)
            limit: 返回条数
            use_cache: 是否使用缓存
            
        Returns:
            新闻列表
        """
        if source not in self.SOURCES:
            raise ValueError(f"Unknown source: {source}. Available: {list(self.SOURCES.keys())}")
        
        # 检查缓存
        if use_cache and self._is_cache_valid(source):
            logger.debug(f"Using cached {source} news")
            return self._cache[source]['data'][:limit]
        
        source_config = self.SOURCES[source]
        func_name = source_config['func']
        
        try:
            logger.info(f"Fetching news from {source_config['name']} ({func_name})")
            func = getattr(ak, func_name)
            df = func()
            
            if df is None or df.empty:
                logger.warning(f"No data returned from {source}")
                return []
            
            # 标准化数据
            news_list = self._normalize_news(df, source)
            
            # 更新缓存
            self._cache[source] = {
                'data': news_list,
                'timestamp': datetime.now()
            }
            
            logger.info(f"Fetched {len(news_list)} news from {source_config['name']}")
            return news_list[:limit]
            
        except Exception as e:
            logger.error(f"Error fetching news from {source}: {e}")
            return []
    
    def _normalize_news(self, df: pd.DataFrame, source: str) -> List[Dict[str, Any]]:
        """
        标准化新闻数据格式
        
        Returns:
            [
                {
                    'source': 'cls',
                    'source_name': '财联社',
                    'title': '...',
                    'content': '...',
                    'time': '2026-01-28 16:00:00',
                    'url': '...',  # 可选
                }
            ]
        """
        news_list = []
        source_config = self.SOURCES[source]
        
        for _, row in df.iterrows():
            item = {
                'source': source,
                'source_name': source_config['name'],
            }
            
            # 标题
            if '标题' in row:
                item['title'] = str(row['标题']) if pd.notna(row['标题']) else ''
            else:
                item['title'] = ''
            
            # 内容
            if '内容' in row:
                item['content'] = str(row['内容']) if pd.notna(row['内容']) else ''
            else:
                item['content'] = ''
            
            # 如果没有标题，用内容前50字作为标题
            if not item['title'] and item['content']:
                item['title'] = item['content'][:50] + ('...' if len(item['content']) > 50 else '')
            
            # 时间处理
            if '发布时间' in row:
                time_str = str(row['发布时间'])
                item['time'] = time_str
            elif '发布日期' in row and '发布时间' in row.index:
                date_str = str(row.get('发布日期', ''))
                time_str = str(row.get('发布时间', ''))
                if date_str and time_str:
                    item['time'] = f"{date_str} {time_str}"
                else:
                    item['time'] = date_str or time_str
            else:
                item['time'] = ''
            
            # URL（如果有）
            if '链接' in row:
                item['url'] = str(row['链接']) if pd.notna(row['链接']) else ''
            else:
                item['url'] = ''
            
            news_list.append(item)
        
        return news_list
    
    def fetch_all_sources(self, limit_per_source: int = 10) -> List[Dict[str, Any]]:
        """
        获取所有数据源的快讯
        
        Args:
            limit_per_source: 每个数据源的条数限制
            
        Returns:
            合并后的新闻列表（按时间排序）
        """
        all_news = []
        
        for source in ['cls', 'ths']:  # 只用最可靠的两个源
            try:
                news = self.fetch_news(source=source, limit=limit_per_source)
                all_news.extend(news)
            except Exception as e:
                logger.error(f"Error fetching from {source}: {e}")
        
        # 按时间排序（最新的在前）
        all_news.sort(key=lambda x: x.get('time', ''), reverse=True)
        
        return all_news
    
    def fetch_stock_news(self, symbol: str, limit: int = 20) -> List[Dict[str, Any]]:
        """
        获取个股新闻（东方财富）
        
        Args:
            symbol: 股票代码 (如 000001)
            limit: 返回条数
        """
        try:
            logger.info(f"Fetching stock news for {symbol}")
            df = ak.stock_news_em(symbol=symbol)
            
            if df is None or df.empty:
                return []
            
            news_list = []
            for _, row in df.head(limit).iterrows():
                item = {
                    'source': 'eastmoney',
                    'source_name': '东方财富',
                    'title': str(row.get('新闻标题', '')) if pd.notna(row.get('新闻标题')) else '',
                    'content': str(row.get('新闻内容', '')) if pd.notna(row.get('新闻内容')) else '',
                    'time': str(row.get('发布时间', '')) if pd.notna(row.get('发布时间')) else '',
                    'url': str(row.get('新闻链接', '')) if pd.notna(row.get('新闻链接')) else '',
                    'symbol': symbol,
                }
                news_list.append(item)
            
            return news_list
            
        except Exception as e:
            logger.error(f"Error fetching stock news for {symbol}: {e}")
            return []


# 单例
_news_service: Optional[NewsService] = None

def get_news_service() -> NewsService:
    """获取新闻服务单例"""
    global _news_service
    if _news_service is None:
        _news_service = NewsService()
    return _news_service
