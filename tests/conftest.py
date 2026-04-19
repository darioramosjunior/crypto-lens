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
