#!/usr/bin/env python3
"""创建板块轮动分析表"""
import sys
from pathlib import Path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.database import engine
from sqlalchemy import text

def create_table():
    with engine.connect() as conn:
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS sector_rotation (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            trade_date VARCHAR(8) NOT NULL,
            sector_type VARCHAR(16) NOT NULL,  -- 'industry' or 'concept'
            sector_name VARCHAR(64) NOT NULL,
            sector_code VARCHAR(16),
            
            -- 基础数据
            pct_change FLOAT,          -- 当日涨跌幅
            net_inflow FLOAT,          -- 当日资金净流入(亿)
            volume FLOAT,              -- 成交量
            
            -- 相对强弱指标
            rs_5d FLOAT,               -- 5日相对强弱
            rs_10d FLOAT,              -- 10日相对强弱
            rs_20d FLOAT,              -- 20日相对强弱
            
            -- 资金流入趋势
            inflow_ma5 FLOAT,          -- 5日资金流入均值
            inflow_change FLOAT,       -- 资金流入变化率
            inflow_accel FLOAT,        -- 资金流入加速度
            
            -- 轮动信号
            rotation_signal VARCHAR(32), -- 'inflow_accel' / 'outflow_accel' / 'neutral'
            signal_strength FLOAT,       -- 信号强度 0-100
            
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            
            UNIQUE(trade_date, sector_type, sector_name)
        )
        """))
        
        conn.execute(text("CREATE INDEX IF NOT EXISTS ix_sr_date ON sector_rotation(trade_date)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS ix_sr_type ON sector_rotation(sector_type)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS ix_sr_signal ON sector_rotation(rotation_signal)"))
        conn.commit()
        
    print("✅ sector_rotation 表创建成功")

if __name__ == '__main__':
    create_table()
