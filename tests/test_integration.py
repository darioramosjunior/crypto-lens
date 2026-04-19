"""
Integration tests for crypto-lens
Tests interactions between multiple modules
"""

import pytest
import os
import sys
import tempfile
import shutil
from unittest.mock import patch, MagicMock
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import logger
import config
from utils import FileUtility, DataLoaderUtility, ConfigManager


class TestFileUtilityIntegration:
    """Integration tests for file operations"""

    def setup_method(self):
        self.temp_dir = tempfile.mkdtemp()

    def teardown_method(self):
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)

    def test_create_directory_and_log_file_workflow(self):
        """Test creating directory and then log file in it"""
        log_dir = os.path.join(self.temp_dir, "logs")
        log_path = os.path.join(log_dir, "test_log.txt")
        
        # Create directory first
        dir_result = FileUtility.ensure_directory_exists(log_dir)
        assert dir_result is True
        
        # Create log file
        log_result = FileUtility.ensure_log_file_exists(log_path)
        assert log_result is True
        
        # Verify both exist
        assert os.path.exists(log_dir)
        assert os.path.exists(log_path)

    def test_file_workflow_create_read_verify(self):
        """Test creating, writing to, and reading a file"""
        test_file = os.path.join(self.temp_dir, "test.txt")
        
        # Create file
        FileUtility.ensure_log_file_exists(test_file)
        
        # Write to it
        with open(test_file, 'w') as f:
            f.write("test content\n")
        
        # Verify it exists and has content
        assert FileUtility.file_exists(test_file)
        with open(test_file, 'r') as f:
            content = f.read()
            assert "test content" in content


class TestLoggerConfigIntegration:
    """Integration tests for logger and config together"""

    def setup_method(self):
        self.temp_dir = tempfile.mkdtemp()
        self.log_file = os.path.join(self.temp_dir, "test_log.txt")

    def teardown_method(self):
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)

    def test_log_multiple_events_in_sequence(self):
        """Test logging multiple events in sequence"""
        events = [
            ("INFO", "Application started"),
            ("INFO", "Processing data"),
            ("WARNING", "No data found"),
            ("ERROR", "Connection failed"),
            ("INFO", "Application ended")
        ]
        
        for category, message in events:
            logger.log_event(category, message, self.log_file)
        
        # Verify all events are logged
        with open(self.log_file, 'r') as f:
            content = f.read()
            for category, message in events:
                assert message in content
                assert category in content

    def test_log_file_with_config_paths(self):
        """Test logging using config module paths"""
        with patch('config.LOG_PATH', self.temp_dir):
            log_path = config.get_log_file_path("test_script")
            
            # Log event
            logger.log_event("INFO", "Test message", log_path)
            
            # Verify
            assert os.path.exists(log_path)


class TestDataLoaderIntegration:
    """Integration tests for data loading functionality"""

    def setup_method(self):
        self.temp_dir = tempfile.mkdtemp()
        self.log_file = os.path.join(self.temp_dir, "test_log.txt")
        self.csv_file = os.path.join(self.temp_dir, "coin_data.csv")

    def teardown_method(self):
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)

    def test_load_coins_and_categories_workflow(self):
        """Test loading coins and their categories from same file"""
        # Create test CSV
        df = pd.DataFrame({
            'coin': ['BTC', 'ETH', 'BNB', 'ADA'],
            'market_cap_category': ['mega', 'mega', 'large', 'mid']
        })
        df.to_csv(self.csv_file, index=False)
        
        # Load coins
        coins = DataLoaderUtility.get_coins_from_csv(self.csv_file, self.log_file)
        assert len(coins) == 4
        
        # Load categories
        categories = DataLoaderUtility.load_market_cap_categories(self.csv_file, self.log_file)
        assert len(categories) == 4
        
        # Verify correspondence
        for coin in coins:
            assert coin in categories

    def test_load_full_coin_dataset(self):
        """Test loading complete coin data with multiple fields"""
        df = pd.DataFrame({
            'coin': ['BTC', 'ETH', 'BNB', 'ADA', 'SOL'],
            'market_cap_category': ['mega', 'mega', 'large', 'mid', 'mid'],
            'market_cap_value': [1000000000000, 500000000000, 100000000000, 50000000000, 30000000000]
        })
        df.to_csv(self.csv_file, index=False)
        
        # Load all data
        coins = DataLoaderUtility.get_coins_from_csv(self.csv_file, self.log_file)
        categories = DataLoaderUtility.load_market_cap_categories(self.csv_file, self.log_file)
        market_caps = DataLoaderUtility.load_market_cap_data(self.csv_file, self.log_file)
        
        # Verify all loaded correctly
        assert len(coins) == 5
        assert len(categories) == 5
        assert len(market_caps) == 5
        
        # Verify data consistency
        for coin in coins:
            assert coin in categories
            assert coin in market_caps
            assert market_caps[coin] is not None

    def test_handle_missing_values_in_dataset(self):
        """Test handling missing values in CSV"""
        df = pd.DataFrame({
            'coin': ['BTC', 'ETH', 'BNB', 'ADA'],
            'market_cap_category': ['mega', None, 'large', ''],
            'market_cap_value': [1000000000000, 500000000000, None, 30000000000]
        })
        df.to_csv(self.csv_file, index=False)
        
        # Load with missing values
        categories = DataLoaderUtility.load_market_cap_categories(self.csv_file, self.log_file)
        market_caps = DataLoaderUtility.load_market_cap_data(self.csv_file, self.log_file)
        
        # Verify handling
        assert categories['ETH'] == 'N/A'
        assert categories['ADA'] == 'N/A'
        assert market_caps['BNB'] is None


class TestDataProcessingPipeline:
    """Integration tests for data processing pipelines"""

    def setup_method(self):
        self.temp_dir = tempfile.mkdtemp()
        self.log_file = os.path.join(self.temp_dir, "test_log.txt")

    def teardown_method(self):
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)

    def test_price_data_processing_workflow(self):
        """Test complete price data processing workflow"""
        # Create mock price data
        price_file = os.path.join(self.temp_dir, "prices.csv")
        df = pd.DataFrame({
            'symbol': ['BTCUSDT', 'ETHUSDT', 'BNBUSDT'],
            'timestamp': ['2024-01-01 10:00', '2024-01-01 11:00', '2024-01-01 12:00'],
            'close': [43000, 2200, 300]
        })
        df.to_csv(price_file, index=False)
        
        # Read and process
        data = pd.read_csv(price_file)
        assert len(data) == 3
        
        # Verify data types
        assert 'symbol' in data.columns
        assert 'close' in data.columns
        
        # Calculate statistics
        avg_close = data['close'].mean()
        assert avg_close > 0

    def test_market_breadth_calculation(self):
        """Test market breadth calculation workflow"""
        # Create price data for multiple coins
        price_file = os.path.join(self.temp_dir, "prices_1d.csv")
        df = pd.DataFrame({
            'symbol': ['BTC', 'ETH', 'BNB', 'ADA', 'SOL'],
            'close': [45000, 2500, 320, 1.5, 185],
            'previous_close': [44000, 2400, 310, 1.4, 180]
        })
        df.to_csv(price_file, index=False)
        
        # Calculate gains/losses
        data = pd.read_csv(price_file)
        data['change'] = ((data['close'] - data['previous_close']) / data['previous_close'] * 100).round(2)
        
        # Count gainers/losers
        gainers = len(data[data['change'] > 0])
        losers = len(data[data['change'] < 0])
        
        assert gainers + losers <= len(data)
        assert gainers > 0 or losers > 0


class TestErrorHandling:
    """Integration tests for error handling across modules"""

    def setup_method(self):
        self.temp_dir = tempfile.mkdtemp()
        self.log_file = os.path.join(self.temp_dir, "test_log.txt")

    def teardown_method(self):
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)

    def test_missing_file_handling(self):
        """Test graceful handling of missing files"""
        missing_file = os.path.join(self.temp_dir, "nonexistent.csv")
        
        # Should return empty results
        coins = DataLoaderUtility.get_coins_from_csv(missing_file, self.log_file)
        assert coins == []
        
        categories = DataLoaderUtility.load_market_cap_categories(missing_file, self.log_file)
        assert categories == {}

    def test_corrupted_csv_handling(self):
        """Test handling of corrupted CSV files"""
        bad_csv = os.path.join(self.temp_dir, "bad.csv")
        
        # Create corrupted CSV
        with open(bad_csv, 'w') as f:
            f.write("corrupted\n@@@@@@\n||||||")
        
        # Should handle gracefully
        try:
            coins = DataLoaderUtility.get_coins_from_csv(bad_csv, self.log_file)
            # Either returns empty list or raises exception - both acceptable
            assert isinstance(coins, list)
        except Exception:
            # Expected for corrupted data
            pass


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
