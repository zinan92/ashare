"""
板块轮动分析服务
追踪资金在板块间的流动，识别轮动趋势
"""
from typing import List, Dict, Optional
from datetime import datetime, timedelta
import pandas as pd
from sqlalchemy import text
from src.database import SessionLocal


class SectorRotationService:
    """板块轮动分析"""
    
    def __init__(self):
        self.session = SessionLocal()
    
    def close(self):
        self.session.close()
    
    def get_concept_data(self) -> pd.DataFrame:
        """获取概念板块数据"""
        result = self.session.execute(text("""
            SELECT trade_date, name, code, pct_change, net_inflow, up_count, down_count, rank
            FROM concept_daily
            WHERE net_inflow IS NOT NULL
            ORDER BY trade_date DESC, net_inflow DESC
        """)).fetchall()
        
        if not result:
            return pd.DataFrame()
        
        df = pd.DataFrame(result, columns=['date', 'name', 'code', 'pct_change', 'net_inflow', 
                                           'up_count', 'down_count', 'rank'])
        return df
    
    def get_industry_data(self) -> pd.DataFrame:
        """获取行业板块数据"""
        result = self.session.execute(text("""
            SELECT trade_date, industry as name, ts_code as code, pct_change, 
                   COALESCE(net_amount, 0) as net_inflow, up_count, down_count
            FROM industry_daily
            ORDER BY trade_date DESC
        """)).fetchall()
        
        if not result:
            return pd.DataFrame()
        
        df = pd.DataFrame(result, columns=['date', 'name', 'code', 'pct_change', 'net_inflow',
                                           'up_count', 'down_count'])
        return df
    
    def analyze_single_day(self, df: pd.DataFrame) -> List[Dict]:
        """分析单日数据"""
        if df.empty:
            return []
        
        today = df['date'].max()
        today_df = df[df['date'] == today].copy()
        
        results = []
        for _, row in today_df.iterrows():
            # 计算涨跌比
            up = row.get('up_count', 0) or 0
            down = row.get('down_count', 0) or 0
            ratio = up / (up + down) if (up + down) > 0 else 0.5
            
            # 根据资金流入判断信号
            inflow = row.get('net_inflow', 0) or 0
            pct = row.get('pct_change', 0) or 0
            
            if inflow > 30 and pct > 1:
                signal = 'strong_inflow'
                strength = min(100, inflow / 2 + pct * 10)
            elif inflow > 10:
                signal = 'inflow'
                strength = min(80, inflow / 2 + pct * 5)
            elif inflow < -30 and pct < -1:
                signal = 'strong_outflow'
                strength = min(100, abs(inflow) / 2 + abs(pct) * 10)
            elif inflow < -10:
                signal = 'outflow'
                strength = min(80, abs(inflow) / 2 + abs(pct) * 5)
            else:
                signal = 'neutral'
                strength = 0
            
            results.append({
                'name': row['name'],
                'date': today,
                'pct_change': pct,
                'net_inflow': inflow,
                'up_count': up,
                'down_count': down,
                'up_down_ratio': ratio,
                'rank': row.get('rank'),
                'rotation_signal': signal,
                'signal_strength': strength
            })
        
        return results
    
    def get_rotation_signals(self) -> Dict[str, List[Dict]]:
        """获取轮动信号"""
        df = self.get_concept_data()
        results = self.analyze_single_day(df)
        
        # 分类
        strong_inflow = [r for r in results if r['rotation_signal'] == 'strong_inflow']
        inflow = [r for r in results if r['rotation_signal'] == 'inflow']
        strong_outflow = [r for r in results if r['rotation_signal'] == 'strong_outflow']
        outflow = [r for r in results if r['rotation_signal'] == 'outflow']
        
        # 按强度排序
        strong_inflow.sort(key=lambda x: x['signal_strength'], reverse=True)
        inflow.sort(key=lambda x: x['signal_strength'], reverse=True)
        strong_outflow.sort(key=lambda x: x['signal_strength'], reverse=True)
        outflow.sort(key=lambda x: x['signal_strength'], reverse=True)
        
        return {
            'inflow_accelerating': strong_inflow[:20] + inflow[:10],
            'outflow_accelerating': strong_outflow[:20] + outflow[:10],
            'summary': {
                'total_analyzed': len(results),
                'inflow_count': len(strong_inflow) + len(inflow),
                'outflow_count': len(strong_outflow) + len(outflow)
            }
        }
    
    def get_top_inflow_sectors(self, limit: int = 20) -> List[Dict]:
        """获取资金流入TOP板块"""
        df = self.get_concept_data()
        results = self.analyze_single_day(df)
        
        # 按资金流入排序
        sorted_results = sorted(results, key=lambda x: x.get('net_inflow', 0) or 0, reverse=True)
        return sorted_results[:limit]
    
    def get_top_outflow_sectors(self, limit: int = 20) -> List[Dict]:
        """获取资金流出TOP板块"""
        df = self.get_concept_data()
        results = self.analyze_single_day(df)
        
        # 按资金流出排序
        sorted_results = sorted(results, key=lambda x: x.get('net_inflow', 0) or 0)
        return sorted_results[:limit]


def get_rotation_analysis() -> Dict:
    """获取轮动分析结果"""
    service = SectorRotationService()
    try:
        return service.get_rotation_signals()
    finally:
        service.close()


def get_top_inflow(limit: int = 20) -> List[Dict]:
    """获取资金流入TOP"""
    service = SectorRotationService()
    try:
        return service.get_top_inflow_sectors(limit)
    finally:
        service.close()
