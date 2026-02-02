"""
加密货币API路由测试
"""
import pytest
from unittest.mock import Mock, patch, AsyncMock
from fastapi.testclient import TestClient
from fastapi import FastAPI

from src.api.routes_crypto import router as crypto_router


# 创建测试应用
app = FastAPI()
app.include_router(crypto_router, prefix="/crypto")

client = TestClient(app)


class TestCryptoAPIRoutes:
    """加密货币API路由单元测试"""
    
    @patch('src.api.routes_crypto.get_crypto_service')
    def test_get_prices_success(self, mock_get_service):
        """测试获取价格接口成功"""
        # 模拟服务响应
        mock_service = Mock()
        mock_service.get_prices = AsyncMock(return_value=[
            {
                'symbol': 'BTC',
                'name': 'Bitcoin',
                'price': 50000.0,
                'change_24h': 2.5,
                'volume_24h': 1000000000.0,
                'market_cap': 1000000000000.0,
                'last_update': '2024-01-01T00:00:00'
            },
            {
                'symbol': 'ETH',
                'name': 'Ethereum',
                'price': 3000.0,
                'change_24h': -1.2,
                'volume_24h': 500000000.0,
                'market_cap': 300000000000.0,
                'last_update': '2024-01-01T00:00:00'
            }
        ])
        mock_get_service.return_value = mock_service
        
        response = client.get("/crypto/prices")
        
        assert response.status_code == 200
        data = response.json()
        assert data['count'] == 2
        assert len(data['prices']) == 2
        assert data['prices'][0]['symbol'] == 'BTC'
        assert data['prices'][0]['price'] == 50000.0

    @patch('src.api.routes_crypto.get_crypto_service')
    def test_get_quote_success(self, mock_get_service):
        """测试获取单币报价成功"""
        mock_service = Mock()
        mock_service.get_quote = AsyncMock(return_value={
            'symbol': 'BTC',
            'name': 'Bitcoin',
            'price': 50000.0,
            'change_24h': 2.5,
            'change_7d': 5.0,
            'volume_24h': 1000000000.0,
            'market_cap': 1000000000000.0,
            'market_cap_rank': 1,
            'circulating_supply': 19000000.0,
            'total_supply': 21000000.0,
            'ath': 69000.0,
            'ath_change_percentage': -27.5,
            'atl': 67.81,
            'atl_change_percentage': 73645.2,
            'last_update': '2024-01-01T00:00:00'
        })
        mock_get_service.return_value = mock_service
        
        response = client.get("/crypto/quote/BTC")
        
        assert response.status_code == 200
        data = response.json()
        assert data['symbol'] == 'BTC'
        assert data['name'] == 'Bitcoin'
        assert data['price'] == 50000.0
        assert data['market_cap_rank'] == 1

    @patch('src.api.routes_crypto.get_crypto_service')
    def test_get_quote_not_found(self, mock_get_service):
        """测试获取不存在币种报价"""
        mock_service = Mock()
        mock_service.get_quote = AsyncMock(return_value=None)
        mock_service.MAJOR_CRYPTOS = {'bitcoin': 'BTC', 'ethereum': 'ETH'}
        mock_get_service.return_value = mock_service
        
        response = client.get("/crypto/quote/INVALID")
        
        assert response.status_code == 404
        assert "not found" in response.json()['detail'].lower()

    @patch('src.api.routes_crypto.get_crypto_service')
    def test_get_klines_success(self, mock_get_service):
        """测试获取K线成功"""
        mock_service = Mock()
        mock_service.BINANCE_PAIRS = {'BTC': 'BTCUSDT', 'ETH': 'ETHUSDT'}
        mock_service.get_klines = AsyncMock(return_value=[
            {
                'time': '2024-01-01T00:00:00',
                'timestamp': 1640995200000,
                'open': 50000.0,
                'high': 50500.0,
                'low': 49500.0,
                'close': 50200.0,
                'volume': 100.0
            },
            {
                'time': '2024-01-01T01:00:00',
                'timestamp': 1640998800000,
                'open': 50200.0,
                'high': 50800.0,
                'low': 50000.0,
                'close': 50600.0,
                'volume': 120.0
            }
        ])
        mock_get_service.return_value = mock_service
        
        response = client.get("/crypto/kline/BTC?interval=1h&limit=100")
        
        assert response.status_code == 200
        data = response.json()
        assert data['symbol'] == 'BTC'
        assert data['interval'] == '1h'
        assert data['count'] == 2
        assert len(data['klines']) == 2
        assert data['klines'][0]['open'] == 50000.0
        assert data['klines'][0]['close'] == 50200.0

    @patch('src.api.routes_crypto.get_crypto_service')
    def test_get_klines_unsupported_symbol(self, mock_get_service):
        """测试获取不支持币种的K线"""
        mock_service = Mock()
        mock_service.BINANCE_PAIRS = {'BTC': 'BTCUSDT'}
        mock_get_service.return_value = mock_service
        
        response = client.get("/crypto/kline/INVALID")
        
        assert response.status_code == 404
        assert "not supported" in response.json()['detail']

    @patch('src.api.routes_crypto.get_crypto_service')
    def test_get_klines_no_data(self, mock_get_service):
        """测试K线无数据"""
        mock_service = Mock()
        mock_service.BINANCE_PAIRS = {'BTC': 'BTCUSDT'}
        mock_service.get_klines = AsyncMock(return_value=[])
        mock_get_service.return_value = mock_service
        
        response = client.get("/crypto/kline/BTC")
        
        assert response.status_code == 404
        assert "not found" in response.json()['detail'].lower()

    @patch('src.api.routes_crypto.get_crypto_service')
    def test_get_funding_rates_success(self, mock_get_service):
        """测试获取资金费率成功"""
        mock_service = Mock()
        mock_service.get_funding_rates = AsyncMock(return_value=[
            {
                'symbol': 'BTC',
                'funding_rate': 0.01,
                'funding_time': '2024-01-01T08:00:00',
                'mark_price': 0.0,
                'last_update': '2024-01-01T00:00:00'
            },
            {
                'symbol': 'ETH',
                'funding_rate': -0.005,
                'funding_time': '2024-01-01T08:00:00',
                'mark_price': 0.0,
                'last_update': '2024-01-01T00:00:00'
            }
        ])
        mock_get_service.return_value = mock_service
        
        response = client.get("/crypto/funding-rates")
        
        assert response.status_code == 200
        data = response.json()
        assert data['count'] == 2
        assert len(data['funding_rates']) == 2
        assert data['funding_rates'][0]['symbol'] == 'BTC'
        assert data['funding_rates'][0]['funding_rate'] == 0.01

    @patch('src.api.routes_crypto.get_crypto_service')
    def test_get_market_overview_success(self, mock_get_service):
        """测试获取市场概览成功"""
        mock_service = Mock()
        mock_service.get_market_overview = AsyncMock(return_value={
            'total_market_cap_usd': 2000000000000.0,
            'total_volume_24h_usd': 100000000000.0,
            'bitcoin_dominance': 50.0,
            'ethereum_dominance': 18.0,
            'active_cryptocurrencies': 10000,
            'markets': 500,
            'market_cap_change_24h': 2.5,
            'last_update': '2024-01-01T00:00:00'
        })
        mock_get_service.return_value = mock_service
        
        response = client.get("/crypto/market-overview")
        
        assert response.status_code == 200
        data = response.json()
        assert data['total_market_cap_usd'] == 2000000000000.0
        assert data['bitcoin_dominance'] == 50.0
        assert data['active_cryptocurrencies'] == 10000

    @patch('src.api.routes_crypto.get_crypto_service')
    def test_get_market_overview_no_data(self, mock_get_service):
        """测试市场概览无数据"""
        mock_service = Mock()
        mock_service.get_market_overview = AsyncMock(return_value={})
        mock_get_service.return_value = mock_service
        
        response = client.get("/crypto/market-overview")
        
        assert response.status_code == 503
        assert "temporarily unavailable" in response.json()['detail']

    @patch('src.api.routes_crypto.get_crypto_service')
    def test_get_supported_symbols(self, mock_get_service):
        """测试获取支持的币种列表"""
        mock_service = Mock()
        mock_service.MAJOR_CRYPTOS = {
            'bitcoin': 'BTC',
            'ethereum': 'ETH'
        }
        mock_service.BINANCE_PAIRS = {
            'BTC': 'BTCUSDT',
            'ETH': 'ETHUSDT'
        }
        mock_get_service.return_value = mock_service
        
        response = client.get("/crypto/supported-symbols")
        
        assert response.status_code == 200
        data = response.json()
        assert data['count'] == 2
        assert len(data['symbols']) == 2
        
        btc_symbol = next((s for s in data['symbols'] if s['symbol'] == 'BTC'), None)
        assert btc_symbol is not None
        assert btc_symbol['name'] == 'Bitcoin'
        assert btc_symbol['binance_pair'] == 'BTCUSDT'
        assert btc_symbol['supports_klines'] == True
        assert btc_symbol['supports_funding_rates'] == True

    @patch('src.api.routes_crypto.get_crypto_service')
    def test_health_check_healthy(self, mock_get_service):
        """测试健康检查 - 健康状态"""
        mock_service = Mock()
        mock_service.MAJOR_CRYPTOS = {'bitcoin': 'BTC'}
        mock_service.get_quote = AsyncMock(return_value={
            'symbol': 'BTC',
            'price': 50000.0,
            'last_update': '2024-01-01T00:00:00'
        })
        mock_get_service.return_value = mock_service
        
        response = client.get("/crypto/health")
        
        assert response.status_code == 200
        data = response.json()
        assert data['status'] == 'healthy'
        assert 'operational' in data['message']
        assert data['timestamp'] == '2024-01-01T00:00:00'

    @patch('src.api.routes_crypto.get_crypto_service')
    def test_health_check_degraded(self, mock_get_service):
        """测试健康检查 - 降级状态"""
        mock_service = Mock()
        mock_service.MAJOR_CRYPTOS = {'bitcoin': 'BTC'}
        mock_service.get_quote = AsyncMock(return_value=None)
        mock_get_service.return_value = mock_service
        
        response = client.get("/crypto/health")
        
        assert response.status_code == 200
        data = response.json()
        assert data['status'] == 'degraded'
        assert 'slow' in data['message']
        assert data['timestamp'] is None

    @patch('src.api.routes_crypto.get_crypto_service')
    def test_health_check_unhealthy(self, mock_get_service):
        """测试健康检查 - 不健康状态"""
        mock_service = Mock()
        mock_service.MAJOR_CRYPTOS = {'bitcoin': 'BTC'}
        mock_service.get_quote = AsyncMock(side_effect=Exception("API Error"))
        mock_get_service.return_value = mock_service
        
        response = client.get("/crypto/health")
        
        assert response.status_code == 200
        data = response.json()
        assert data['status'] == 'unhealthy'
        assert 'failed' in data['message']

    def test_kline_validation_limit_range(self):
        """测试K线参数验证 - limit范围"""
        response = client.get("/crypto/kline/BTC?limit=2000")
        # 应该被限制为最大值
        assert response.status_code in [404, 422]  # 取决于是否找到symbol

    def test_kline_validation_invalid_symbol_format(self):
        """测试K线参数验证 - 无效symbol格式"""
        # 测试空symbol
        response = client.get("/crypto/kline/")
        assert response.status_code == 404  # FastAPI path not found


class TestCryptoAPIIntegration:
    """加密货币API集成测试（需要网络）"""
    
    @pytest.mark.integration
    def test_prices_endpoint_real(self):
        """测试价格接口真实数据"""
        response = client.get("/crypto/prices")
        
        if response.status_code == 200:
            data = response.json()
            assert 'count' in data
            assert 'prices' in data
            assert isinstance(data['prices'], list)
            
            if data['prices']:
                price_item = data['prices'][0]
                assert 'symbol' in price_item
                assert 'price' in price_item
                assert price_item['price'] > 0

    @pytest.mark.integration
    def test_quote_endpoint_real(self):
        """测试报价接口真实数据"""
        response = client.get("/crypto/quote/BTC")
        
        if response.status_code == 200:
            data = response.json()
            assert data['symbol'] == 'BTC'
            assert data['price'] > 0
            assert 'change_24h' in data

    @pytest.mark.integration
    def test_kline_endpoint_real(self):
        """测试K线接口真实数据"""
        response = client.get("/crypto/kline/BTC?interval=1h&limit=10")
        
        if response.status_code == 200:
            data = response.json()
            assert data['symbol'] == 'BTC'
            assert data['interval'] == '1h'
            assert isinstance(data['klines'], list)
            
            if data['klines']:
                kline = data['klines'][0]
                assert 'open' in kline
                assert 'close' in kline
                assert kline['open'] > 0

    @pytest.mark.integration
    def test_funding_rates_endpoint_real(self):
        """测试资金费率接口真实数据"""
        response = client.get("/crypto/funding-rates")
        
        if response.status_code == 200:
            data = response.json()
            assert 'count' in data
            assert 'funding_rates' in data

    @pytest.mark.integration
    def test_market_overview_endpoint_real(self):
        """测试市场概览接口真实数据"""
        response = client.get("/crypto/market-overview")
        
        if response.status_code == 200:
            data = response.json()
            assert 'total_market_cap_usd' in data
            assert 'bitcoin_dominance' in data
            assert data.get('total_market_cap_usd', 0) > 0

    @pytest.mark.integration
    def test_all_endpoints_response_time(self):
        """测试所有接口响应时间"""
        import time
        
        endpoints = [
            "/crypto/prices",
            "/crypto/quote/BTC", 
            "/crypto/kline/BTC?interval=1h&limit=5",
            "/crypto/funding-rates",
            "/crypto/market-overview",
            "/crypto/supported-symbols",
            "/crypto/health"
        ]
        
        start_time = time.time()
        
        for endpoint in endpoints:
            response = client.get(endpoint)
            # 接受各种状态码，主要测试响应时间
            assert response.status_code in [200, 404, 503]
        
        elapsed = time.time() - start_time
        # 所有接口应该在30秒内完成
        assert elapsed < 30.0


if __name__ == '__main__':
    pytest.main([__file__, '-v'])