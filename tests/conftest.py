"""
Pytest configuration and shared fixtures for all tests
"""

import sys
from unittest.mock import MagicMock

import pytest
import tempfile
import shutil
import os
from pathlib import Path

# Mock external dependencies that may not be installed or aren't needed for unit tests
# These mocks must be set up BEFORE any imports of modules that use them
boto3_mock = MagicMock()
boto3_mock.client = MagicMock(return_value=MagicMock())
boto3_mock.Session = MagicMock()
sys.modules['boto3'] = boto3_mock

botocore_mock = MagicMock()
botocore_exceptions_mock = MagicMock()
botocore_exceptions_mock.NoCredentialsError = Exception
botocore_exceptions_mock.ClientError = Exception
sys.modules['botocore'] = botocore_mock
sys.modules['botocore.exceptions'] = botocore_exceptions_mock

sys.modules['ccxt'] = MagicMock()
sys.modules['aiohttp'] = MagicMock()
sys.modules['pandas_ta'] = MagicMock()


@pytest.fixture
def temp_dir():
    """Create and cleanup a temporary directory for tests"""
    temp_path = tempfile.mkdtemp()
    yield temp_path
    # Cleanup
    if os.path.exists(temp_path):
        shutil.rmtree(temp_path)


@pytest.fixture
def temp_log_file(temp_dir):
    """Create a temporary log file"""
    log_file = os.path.join(temp_dir, "test_log.txt")
    yield log_file
    # Cleanup
    if os.path.exists(log_file):
        os.remove(log_file)


@pytest.fixture
def temp_csv_file(temp_dir):
    """Create a temporary CSV file"""
    csv_file = os.path.join(temp_dir, "test_data.csv")
    yield csv_file
    # Cleanup
    if os.path.exists(csv_file):
        os.remove(csv_file)


@pytest.fixture
def mock_coin_data(temp_csv_file):
    """Create mock coin data CSV"""
    import pandas as pd
    
    data = {
        'coin': ['BTC', 'ETH', 'BNB', 'ADA', 'SOL'],
        'market_cap_category': ['mega', 'mega', 'large', 'mid', 'mid'],
        'market_cap_value': [1000000000000, 500000000000, 100000000000, 50000000000, 30000000000]
    }
    
    df = pd.DataFrame(data)
    df.to_csv(temp_csv_file, index=False)
    
    yield temp_csv_file


@pytest.fixture
def mock_price_data(temp_csv_file):
    """Create mock price data CSV"""
    import pandas as pd
    from datetime import datetime, timedelta
    
    base_date = datetime.now()
    dates = [base_date - timedelta(hours=i) for i in range(10)]
    
    data = {
        'symbol': ['BTCUSDT'] * 10,
        'timestamp': dates,
        'open': [40000 + i*100 for i in range(10)],
        'high': [40500 + i*100 for i in range(10)],
        'low': [39500 + i*100 for i in range(10)],
        'close': [40200 + i*100 for i in range(10)],
        'volume': [1000000 + i*10000 for i in range(10)]
    }
    
    df = pd.DataFrame(data)
    df.to_csv(temp_csv_file, index=False)
    
    yield temp_csv_file


@pytest.fixture
def mock_market_cap_response():
    """Mock CoinMarketCap API response"""
    return {
        "data": {
            "BTC": [{
                "quote": {
                    "USD": {"market_cap": 1000000000000}
                }
            }],
            "ETH": [{
                "quote": {
                    "USD": {"market_cap": 500000000000}
                }
            }],
            "BNB": [{
                "quote": {
                    "USD": {"market_cap": 100000000000}
                }
            }]
        }
    }


def pytest_configure(config):
    """Configure pytest with custom markers"""
    config.addinivalue_line(
        "markers", "unit: mark test as a unit test"
    )
    config.addinivalue_line(
        "markers", "integration: mark test as an integration test"
    )
    config.addinivalue_line(
        "markers", "slow: mark test as slow running"
    )
    config.addinivalue_line(
        "markers", "external_api: mark test as requiring external API calls"
    )


@pytest.fixture(autouse=True)
def reset_modules():
    """Reset module imports between tests to avoid state pollution"""
    yield
    # Cleanup after test


class MockConfig:
    """Mock configuration for testing"""
    LOG_PATH = "/tmp/test_logs/"
    OUTPUT_PATH = "/tmp/test_output/"
    MAIN_CRON_SCHED = "*/5 * * * *"
    LOGS_CLEANER_CRON_SCHED = "0 15 * * *"
    COIN_DATA_COLLECTOR_CRON_SCHED = "0 12 * * *"


@pytest.fixture
def mock_config(monkeypatch):
    """Mock config module settings"""
    import config
    monkeypatch.setattr(config, 'LOG_PATH', '/tmp/test_logs/')
    monkeypatch.setattr(config, 'OUTPUT_PATH', '/tmp/test_output/')
    return config


# Session-wide fixtures for heavy setup
@pytest.fixture(scope="session")
def session_temp_dir():
    """Session-wide temporary directory"""
    temp_path = tempfile.mkdtemp()
    yield temp_path
    # Cleanup
    if os.path.exists(temp_path):
        shutil.rmtree(temp_path)


# ============================================================================
# Validation-specific Fixtures
# ============================================================================

@pytest.fixture
def sample_ohlcv_data():
    """Create sample OHLCV data for validation tests"""
    return [
        [1704067200000, "45000", "45500", "44500", "45200", "1000000", 1704070800000, "45200000000", 100, "500000", "225000000", "0"],
        [1704070800000, "45200", "45700", "45000", "45500", "1100000", 1704074400000, "50050000000", 110, "550000", "247750000", "0"],
        [1704074400000, "45500", "46000", "45400", "45800", "1200000", 1704078000000, "54960000000", 120, "600000", "271200000", "0"]
    ]


@pytest.fixture
def sample_price_change_records():
    """Create sample price change records for validation tests"""
    from datetime import datetime
    now = datetime.now()
    
    return [
        {
            'symbol': 'BTCUSDT',
            'timestamp': now,
            'close': 45200.0,
            'previous_close': 44000.0,
            'price_change': 2.73,
            'trend_category': 'uptrend',
            'market_cap_category': 'Large Cap'
        },
        {
            'symbol': 'ETHUSDT',
            'timestamp': now,
            'close': 2500.0,
            'previous_close': 2400.0,
            'price_change': 4.17,
            'trend_category': 'uptrend',
            'market_cap_category': 'Large Cap'
        },
        {
            'symbol': 'BNBUSDT',
            'timestamp': now,
            'close': 600.0,
            'previous_close': 620.0,
            'price_change': -3.23,
            'trend_category': 'downtrend',
            'market_cap_category': 'Large Cap'
        }
    ]


@pytest.fixture
def sample_indicator_records():
    """Create sample indicator records for validation tests"""
    from datetime import datetime
    now = datetime.now()
    
    return [
        {
            'symbol': 'BTCUSDT',
            'timestamp': now,
            'open': 45000.0,
            'high': 45500.0,
            'low': 44500.0,
            'close': 45200.0,
            'volume': 1000000.0,
            'sma20': 44800.0,
            'sma50': 44000.0,
            'sma100': 43000.0,
            'rsi14': 65.5,
            'volume_sma20': 950000.0
        },
        {
            'symbol': 'ETHUSDT',
            'timestamp': now,
            'open': 2400.0,
            'high': 2500.0,
            'low': 2380.0,
            'close': 2500.0,
            'volume': 500000.0,
            'sma20': 2450.0,
            'sma50': 2400.0,
            'sma100': 2350.0,
            'rsi14': 72.3,
            'volume_sma20': 480000.0
        }
    ]


@pytest.fixture
def sample_oi_change_records():
    """Create sample OI change records for validation tests"""
    from datetime import datetime
    now = datetime.now()
    
    return [
        {
            'symbol': 'BTCUSDT',
            'timestamp': now,
            'current_oi': 1000000000.0,
            'previous_oi': 900000000.0,
            'oi_change': 11.11,
            'oi_change_abs': 100000000.0,
            'market_cap_category': 'Large Cap'
        },
        {
            'symbol': 'ETHUSDT',
            'timestamp': now,
            'current_oi': 500000000.0,
            'previous_oi': 550000000.0,
            'oi_change': -9.09,
            'oi_change_abs': -50000000.0,
            'market_cap_category': 'Large Cap'
        }
    ]


@pytest.fixture
def sample_market_breadth_data():
    """Create sample market breadth data for validation tests"""
    from datetime import datetime
    
    return {
        'timestamp': datetime.now(),
        'total_coins': 100,
        'positive_coins': 65,
        'negative_coins': 35,
        'uptrend_count': 45,
        'downtrend_count': 25,
        'pullback_count': 20,
        'reversal_up_count': 7,
        'reversal_down_count': 3,
        'btc_change': 2.5,
        'btc_dominance_change': 1.2
    }


@pytest.fixture
def sample_coin_data_records():
    """Create sample coin data records for validation tests"""
    return [
        {
            'coin': 'BTC',
            'market_cap': 1000000000000,
            'category': 'Large Cap'
        },
        {
            'coin': 'ETH',
            'market_cap': 500000000000,
            'category': 'Large Cap'
        },
        {
            'coin': 'BNB',
            'market_cap': 100000000000,
            'category': 'Mid Cap'
        },
        {
            'coin': 'ADA',
            'market_cap': 20000000000,
            'category': 'Small Cap'
        }
    ]


@pytest.fixture
def invalid_ohlcv_data():
    """Create invalid OHLCV data for negative validation tests"""
    return [
        # High < Low
        [1704067200000, "45000", "44500", "45500", "45200", "1000000", 1704070800000, "45200000000", 100, "500000", "225000000", "0"],
        # Close > High
        [1704070800000, "45200", "45500", "45000", "46000", "1100000", 1704074400000, "50050000000", 110, "550000", "247750000", "0"],
        # Negative price
        [-1000, "45500", "46000", "45400", "45800", "1200000", 1704078000000, "54960000000", 120, "600000", "271200000", "0"]
    ]
