"""
加密货币服务测试
"""
import pytest
from unittest.mock import Mock, patch, AsyncMock
import json
from datetime import datetime

from src.services.crypto_service import CryptoService, get_crypto_service


class TestCryptoService:
    """加密货币服务单元测试"""
    
    def test_init(self):
        """测试初始化"""
        service = CryptoService()
        assert service is not None
        assert service.timeout is not None
        assert hasattr(service, 'MAJOR_CRYPTOS')
        assert hasattr(service, 'BINANCE_PAIRS')
        assert hasattr(service, 'INTERVAL_MAPPING')
    
    def test_crypto_config(self):
        """测试加密货币配置"""
        service = CryptoService()
        
        # 检查主要加密货币
        assert 'bitcoin' in service.MAJOR_CRYPTOS
        assert 'ethereum' in service.MAJOR_CRYPTOS
        assert 'solana' in service.MAJOR_CRYPTOS
        assert service.MAJOR_CRYPTOS['bitcoin'] == 'BTC'
        assert service.MAJOR_CRYPTOS['ethereum'] == 'ETH'
        
        # 检查 Binance 交易对
        assert 'BTC' in service.BINANCE_PAIRS
        assert 'ETH' in service.BINANCE_PAIRS
        assert service.BINANCE_PAIRS['BTC'] == 'BTCUSDT'
        assert service.BINANCE_PAIRS['ETH'] == 'ETHUSDT'
        
        # 检查时间间隔映射
        assert '1h' in service.INTERVAL_MAPPING
        assert '1d' in service.INTERVAL_MAPPING
        assert service.INTERVAL_MAPPING['1h'] == '1h'
        assert service.INTERVAL_MAPPING['1d'] == '1d'
    
    def test_get_crypto_service_singleton(self):
        """测试单例模式"""
        service1 = get_crypto_service()
        service2 = get_crypto_service()
        assert service1 is service2

    @pytest.mark.asyncio
    @patch('httpx.AsyncClient.get')
    async def test_get_prices_success(self, mock_get):
        """测试获取价格成功"""
        # 模拟 CoinGecko API 响应
        mock_response = Mock()
        mock_response.json.return_value = {
            'bitcoin': {
                'usd': 50000,
                'usd_24h_change': 2.5,
                'usd_24h_vol': 1000000000,
                'usd_market_cap': 1000000000000
            },
            'ethereum': {
                'usd': 3000,
                'usd_24h_change': -1.2,
                'usd_24h_vol': 500000000,
                'usd_market_cap': 300000000000
            }
        }
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response
        
        service = CryptoService()
        prices = await service.get_prices()
        
        assert isinstance(prices, list)
        assert len(prices) > 0
        
        # 找到 BTC 数据
        btc_data = next((p for p in prices if p['symbol'] == 'BTC'), None)
        assert btc_data is not None
        assert btc_data['price'] == 50000
        assert btc_data['change_24h'] == 2.5
        assert 'last_update' in btc_data
    
    @pytest.mark.asyncio
    @patch('httpx.AsyncClient.get')
    async def test_get_prices_api_error(self, mock_get):
        """测试获取价格API错误"""
        # 模拟网络错误
        mock_get.side_effect = Exception("API Error")
        
        service = CryptoService()
        prices = await service.get_prices()
        
        assert prices == []

    @pytest.mark.asyncio
    @patch('httpx.AsyncClient.get')
    async def test_get_quote_success(self, mock_get):
        """测试获取单币报价成功"""
        # 模拟 CoinGecko 币种详情 API 响应
        mock_response = Mock()
        mock_response.json.return_value = {
            'name': 'Bitcoin',
            'market_cap_rank': 1,
            'market_data': {
                'current_price': {'usd': 50000},
                'price_change_percentage_24h': 2.5,
                'price_change_percentage_7d': 5.0,
                'total_volume': {'usd': 1000000000},
                'market_cap': {'usd': 1000000000000},
                'circulating_supply': 19000000,
                'total_supply': 21000000,
                'ath': {'usd': 69000},
                'ath_change_percentage': {'usd': -27.5},
                'atl': {'usd': 67.81},
                'atl_change_percentage': {'usd': 73645.2}
            }
        }
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response
        
        service = CryptoService()
        quote = await service.get_quote('BTC')
        
        assert quote is not None
        assert quote['symbol'] == 'BTC'
        assert quote['name'] == 'Bitcoin'
        assert quote['price'] == 50000
        assert quote['change_24h'] == 2.5
        assert quote['market_cap_rank'] == 1
        assert 'last_update' in quote

    @pytest.mark.asyncio
    async def test_get_quote_unsupported_symbol(self):
        """测试不支持的币种"""
        service = CryptoService()
        quote = await service.get_quote('UNSUPPORTED')
        
        assert quote is None

    @pytest.mark.asyncio
    @patch('httpx.AsyncClient.get')
    async def test_get_klines_success(self, mock_get):
        """测试获取K线成功"""
        # 模拟 Binance K线 API 响应
        mock_response = Mock()
        mock_response.json.return_value = [
            [1640995200000, "50000.0", "50500.0", "49500.0", "50200.0", "100.0"],
            [1640998800000, "50200.0", "50800.0", "50000.0", "50600.0", "120.0"]
        ]
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response
        
        service = CryptoService()
        klines = await service.get_klines('BTC', '1h', 100)
        
        assert isinstance(klines, list)
        assert len(klines) == 2
        
        kline = klines[0]
        assert 'time' in kline
        assert 'timestamp' in kline
        assert kline['open'] == 50000.0
        assert kline['high'] == 50500.0
        assert kline['low'] == 49500.0
        assert kline['close'] == 50200.0
        assert kline['volume'] == 100.0

    @pytest.mark.asyncio
    async def test_get_klines_unsupported_symbol(self):
        """测试不支持的K线交易对"""
        service = CryptoService()
        klines = await service.get_klines('UNSUPPORTED', '1h', 100)
        
        assert klines == []

    @pytest.mark.asyncio
    @patch('httpx.AsyncClient.get')
    async def test_get_funding_rates_success(self, mock_get):
        """测试获取资金费率成功"""
        # 模拟 Binance 资金费率 API 响应
        mock_response = Mock()
        mock_response.json.return_value = [{
            'symbol': 'BTCUSDT',
            'fundingRate': '0.0001',
            'fundingTime': 1640995200000
        }]
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response
        
        service = CryptoService()
        rates = await service.get_funding_rates()
        
        assert isinstance(rates, list)
        # 至少应该有一些数据（如果网络正常）
        if rates:
            rate = rates[0]
            assert 'symbol' in rate
            assert 'funding_rate' in rate
            assert 'funding_time' in rate

    @pytest.mark.asyncio
    @patch('httpx.AsyncClient.get')
    async def test_get_market_overview_success(self, mock_get):
        """测试获取市场概览成功"""
        # 模拟 CoinGecko 全球数据 API 响应
        mock_response = Mock()
        mock_response.json.return_value = {
            'data': {
                'total_market_cap': {'usd': 2000000000000},
                'total_volume': {'usd': 100000000000},
                'market_cap_percentage': {'btc': 50.0, 'eth': 18.0},
                'active_cryptocurrencies': 10000,
                'markets': 500,
                'market_cap_change_percentage_24h_usd': 2.5
            }
        }
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response
        
        service = CryptoService()
        overview = await service.get_market_overview()
        
        assert isinstance(overview, dict)
        assert overview['total_market_cap_usd'] == 2000000000000
        assert overview['bitcoin_dominance'] == 50.0
        assert overview['ethereum_dominance'] == 18.0
        assert 'last_update' in overview


class TestCryptoServiceIntegration:
    """加密货币服务集成测试（需要网络）"""
    
    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_get_prices_real(self):
        """测试获取真实价格"""
        service = CryptoService()
        prices = await service.get_prices()
        
        if prices:  # 可能因网络问题返回空列表
            assert isinstance(prices, list)
            assert len(prices) > 0
            
            # 检查 BTC 数据
            btc_data = next((p for p in prices if p['symbol'] == 'BTC'), None)
            if btc_data:
                assert btc_data['price'] > 0
                assert 'change_24h' in btc_data
                assert 'volume_24h' in btc_data
                assert 'market_cap' in btc_data

    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_get_quote_real(self):
        """测试获取真实报价"""
        service = CryptoService()
        quote = await service.get_quote('BTC')
        
        if quote:  # 可能因网络问题返回 None
            assert quote['symbol'] == 'BTC'
            assert quote['price'] > 0
            assert 'change_24h' in quote
            assert 'market_cap_rank' in quote

    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_get_klines_real(self):
        """测试获取真实K线"""
        service = CryptoService()
        klines = await service.get_klines('BTC', '1h', 10)
        
        if klines:
            assert isinstance(klines, list)
            assert len(klines) > 0
            
            kline = klines[0]
            assert 'open' in kline
            assert 'close' in kline
            assert kline['open'] > 0
            assert kline['close'] > 0

    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_get_funding_rates_real(self):
        """测试获取真实资金费率"""
        service = CryptoService()
        rates = await service.get_funding_rates()
        
        if rates:
            assert isinstance(rates, list)
            # 应该有 BTC 的数据
            btc_rate = next((r for r in rates if r['symbol'] == 'BTC'), None)
            if btc_rate:
                assert 'funding_rate' in btc_rate
                assert 'funding_time' in btc_rate

    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_get_market_overview_real(self):
        """测试获取真实市场概览"""
        service = CryptoService()
        overview = await service.get_market_overview()
        
        if overview:
            assert 'total_market_cap_usd' in overview
            assert 'bitcoin_dominance' in overview
            assert overview.get('total_market_cap_usd', 0) > 0

    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_service_performance(self):
        """测试服务性能"""
        import time
        
        service = CryptoService()
        
        # 测试所有主要方法的响应时间
        start_time = time.time()
        
        tasks = [
            service.get_prices(),
            service.get_quote('BTC'),
            service.get_klines('BTC', '1h', 10),
            service.get_funding_rates(),
            service.get_market_overview()
        ]
        
        # 并发执行所有任务
        import asyncio
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        elapsed = time.time() - start_time
        
        # 所有请求应该在15秒内完成
        assert elapsed < 15.0
        
        # 检查结果
        prices, quote, klines, rates, overview = results
        
        # 至少应该有一些成功的响应
        success_count = sum(1 for r in results if not isinstance(r, Exception) and r)
        assert success_count > 0


if __name__ == '__main__':
    pytest.main([__file__, '-v'])