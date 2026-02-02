"""
加密货币数据服务
整合 CoinGecko 和 Binance 公共API，提供加密货币价格、K线、市场概览等数据
"""
import asyncio
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
import httpx
from urllib.parse import urlencode

logger = logging.getLogger(__name__)


class CryptoService:
    """加密货币数据服务"""
    
    def __init__(self):
        self.timeout = httpx.Timeout(10.0)
        
        # 主要加密货币列表
        self.MAJOR_CRYPTOS = {
            'bitcoin': 'BTC',
            'ethereum': 'ETH', 
            'solana': 'SOL',
            'binancecoin': 'BNB',
            'dogecoin': 'DOGE',
            'cardano': 'ADA',
            'sui': 'SUI',
            'chainlink': 'LINK',
            'polkadot': 'DOT',
            'avalanche-2': 'AVAX',
            'polygon': 'MATIC',
            'uniswap': 'UNI',
            'litecoin': 'LTC',
            'bitcoin-cash': 'BCH',
            'stellar': 'XLM'
        }
        
        # Binance 交易对映射 (用于K线和资金费率)
        self.BINANCE_PAIRS = {
            'BTC': 'BTCUSDT',
            'ETH': 'ETHUSDT',
            'SOL': 'SOLUSDT', 
            'BNB': 'BNBUSDT',
            'DOGE': 'DOGEUSDT',
            'ADA': 'ADAUSDT',
            'SUI': 'SUIUSDT',
            'LINK': 'LINKUSDT',
            'DOT': 'DOTUSDT',
            'AVAX': 'AVAXUSDT',
            'MATIC': 'MATICUSDT',
            'UNI': 'UNIUSDT',
            'LTC': 'LTCUSDT',
            'BCH': 'BCHUSDT',
            'XLM': 'XLMUSDT'
        }
        
        # 时间间隔映射 (前端 -> Binance API)
        self.INTERVAL_MAPPING = {
            '1m': '1m',
            '5m': '5m', 
            '15m': '15m',
            '1h': '1h',
            '4h': '4h',
            '1d': '1d'
        }

    async def get_prices(self) -> List[Dict[str, Any]]:
        """获取主要加密货币实时价格 (CoinGecko API)"""
        try:
            crypto_ids = ','.join(self.MAJOR_CRYPTOS.keys())
            url = f"https://api.coingecko.com/api/v3/simple/price?ids={crypto_ids}&vs_currencies=usd&include_24hr_change=true&include_24hr_vol=true&include_market_cap=true"
            
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                response = await client.get(url)
                response.raise_for_status()
                data = response.json()
                
                result = []
                for crypto_id, symbol in self.MAJOR_CRYPTOS.items():
                    if crypto_id in data:
                        crypto_data = data[crypto_id]
                        result.append({
                            'symbol': symbol,
                            'name': crypto_id.replace('-', ' ').title(),
                            'price': crypto_data.get('usd', 0),
                            'change_24h': crypto_data.get('usd_24h_change', 0),
                            'volume_24h': crypto_data.get('usd_24h_vol', 0),
                            'market_cap': crypto_data.get('usd_market_cap', 0),
                            'last_update': datetime.now().isoformat()
                        })
                
                return result
                
        except Exception as e:
            logger.error(f"获取加密货币价格失败: {e}")
            return []

    async def get_quote(self, symbol: str) -> Optional[Dict[str, Any]]:
        """获取单个加密货币详细报价"""
        try:
            # 找到对应的 CoinGecko ID
            crypto_id = None
            for cid, sym in self.MAJOR_CRYPTOS.items():
                if sym.upper() == symbol.upper():
                    crypto_id = cid
                    break
            
            if not crypto_id:
                return None
                
            url = f"https://api.coingecko.com/api/v3/coins/{crypto_id}?localization=false&tickers=false&market_data=true&community_data=false&developer_data=false"
            
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                response = await client.get(url)
                response.raise_for_status()
                data = response.json()
                
                market_data = data.get('market_data', {})
                
                return {
                    'symbol': symbol.upper(),
                    'name': data.get('name', ''),
                    'price': market_data.get('current_price', {}).get('usd', 0),
                    'change_24h': market_data.get('price_change_percentage_24h', 0),
                    'change_7d': market_data.get('price_change_percentage_7d', 0), 
                    'volume_24h': market_data.get('total_volume', {}).get('usd', 0),
                    'market_cap': market_data.get('market_cap', {}).get('usd', 0),
                    'market_cap_rank': data.get('market_cap_rank', 0),
                    'circulating_supply': market_data.get('circulating_supply', 0),
                    'total_supply': market_data.get('total_supply', 0),
                    'ath': market_data.get('ath', {}).get('usd', 0),
                    'ath_change_percentage': market_data.get('ath_change_percentage', {}).get('usd', 0),
                    'atl': market_data.get('atl', {}).get('usd', 0),
                    'atl_change_percentage': market_data.get('atl_change_percentage', {}).get('usd', 0),
                    'last_update': datetime.now().isoformat()
                }
                
        except Exception as e:
            logger.error(f"获取 {symbol} 报价失败: {e}")
            return None

    async def get_klines(self, symbol: str, interval: str = '1h', limit: int = 100) -> List[Dict[str, Any]]:
        """获取K线数据 (Binance API)"""
        try:
            # 验证交易对
            if symbol.upper() not in self.BINANCE_PAIRS:
                return []
                
            binance_symbol = self.BINANCE_PAIRS[symbol.upper()]
            
            # 验证时间间隔
            if interval not in self.INTERVAL_MAPPING:
                interval = '1h'
                
            binance_interval = self.INTERVAL_MAPPING[interval]
            
            url = f"https://api.binance.com/api/v3/klines"
            params = {
                'symbol': binance_symbol,
                'interval': binance_interval,
                'limit': min(limit, 1000)  # Binance 限制
            }
            
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                response = await client.get(url, params=params)
                response.raise_for_status()
                data = response.json()
                
                result = []
                for kline in data:
                    timestamp = int(kline[0])
                    dt = datetime.fromtimestamp(timestamp / 1000)
                    
                    result.append({
                        'time': dt.isoformat(),
                        'timestamp': timestamp,
                        'open': float(kline[1]),
                        'high': float(kline[2]),
                        'low': float(kline[3]),
                        'close': float(kline[4]),
                        'volume': float(kline[5])
                    })
                
                return result
                
        except Exception as e:
            logger.error(f"获取 {symbol} K线数据失败: {e}")
            return []

    async def get_funding_rates(self) -> List[Dict[str, Any]]:
        """获取永续合约资金费率 (Binance API)"""
        try:
            # 获取主要币种的资金费率
            major_symbols = ['BTCUSDT', 'ETHUSDT', 'SOLUSDT', 'BNBUSDT', 'DOGEUSDT', 'ADAUSDT']
            
            url = "https://fapi.binance.com/fapi/v1/fundingRate"
            
            tasks = []
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                for binance_symbol in major_symbols:
                    params = {'symbol': binance_symbol, 'limit': 1}
                    tasks.append(self._get_funding_rate(client, binance_symbol, params))
                
                results = await asyncio.gather(*tasks, return_exceptions=True)
                
                funding_rates = []
                for i, result in enumerate(results):
                    if isinstance(result, Exception):
                        logger.warning(f"获取 {major_symbols[i]} 资金费率失败: {result}")
                        continue
                    if result:
                        funding_rates.extend(result)
                
                return funding_rates
                
        except Exception as e:
            logger.error(f"获取资金费率失败: {e}")
            return []

    async def _get_funding_rate(self, client: httpx.AsyncClient, symbol: str, params: Dict) -> List[Dict[str, Any]]:
        """获取单个币种资金费率"""
        try:
            response = await client.get("https://fapi.binance.com/fapi/v1/fundingRate", params=params)
            response.raise_for_status()
            data = response.json()
            
            if not data:
                return []
                
            rate_data = data[0]  # 最新的资金费率
            
            # 转换symbol为我们的格式
            crypto_symbol = symbol.replace('USDT', '')
            
            return [{
                'symbol': crypto_symbol,
                'funding_rate': float(rate_data['fundingRate']) * 100,  # 转换为百分比
                'funding_time': datetime.fromtimestamp(int(rate_data['fundingTime']) / 1000).isoformat(),
                'mark_price': 0,  # Binance 资金费率接口不返回标记价格
                'last_update': datetime.now().isoformat()
            }]
            
        except Exception as e:
            logger.error(f"获取 {symbol} 资金费率失败: {e}")
            return []

    async def get_market_overview(self) -> Dict[str, Any]:
        """获取市场概览 (CoinGecko API)"""
        try:
            url = "https://api.coingecko.com/api/v3/global"
            
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                response = await client.get(url)
                response.raise_for_status()
                data = response.json()
                
                global_data = data.get('data', {})
                
                return {
                    'total_market_cap_usd': global_data.get('total_market_cap', {}).get('usd', 0),
                    'total_volume_24h_usd': global_data.get('total_volume', {}).get('usd', 0),
                    'bitcoin_dominance': global_data.get('market_cap_percentage', {}).get('btc', 0),
                    'ethereum_dominance': global_data.get('market_cap_percentage', {}).get('eth', 0),
                    'active_cryptocurrencies': global_data.get('active_cryptocurrencies', 0),
                    'markets': global_data.get('markets', 0),
                    'market_cap_change_24h': global_data.get('market_cap_change_percentage_24h_usd', 0),
                    'last_update': datetime.now().isoformat()
                }
                
        except Exception as e:
            logger.error(f"获取市场概览失败: {e}")
            return {}


# 服务实例
_crypto_service = None

def get_crypto_service() -> CryptoService:
    """获取加密货币服务实例"""
    global _crypto_service
    if _crypto_service is None:
        _crypto_service = CryptoService()
    return _crypto_service