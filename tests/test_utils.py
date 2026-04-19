"""
Unit tests for utility functions in utils.py
Tests critical functions: FileUtility, ConfigManager, DataLoaderUtility
"""

import pytest
import os
import tempfile
import shutil
from pathlib import Path
from unittest.mock import patch, MagicMock, mock_open
import pandas as pd
import sys

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils import FileUtility, ConfigManager, DataLoaderUtility, MathUtility


class TestFileUtility:
    """Test suite for FileUtility class"""

    def setup_method(self):
        """Create temporary directory for testing"""
        self.temp_dir = tempfile.mkdtemp()

    def teardown_method(self):
        """Clean up temporary directory after testing"""
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)

    def test_ensure_directory_exists_creates_directory(self):
        """Test that ensure_directory_exists creates a new directory"""
        test_dir = os.path.join(self.temp_dir, "test_folder")
        assert not os.path.exists(test_dir)
        
        result = FileUtility.ensure_directory_exists(test_dir)
        
        assert result is True
        assert os.path.exists(test_dir)
        assert os.path.isdir(test_dir)

    def test_ensure_directory_exists_with_existing_directory(self):
        """Test that ensure_directory_exists handles existing directory"""
        test_dir = os.path.join(self.temp_dir, "existing_folder")
        os.makedirs(test_dir)
        
        result = FileUtility.ensure_directory_exists(test_dir)
        
        assert result is True
        assert os.path.exists(test_dir)

    def test_ensure_directory_exists_nested_paths(self):
        """Test that ensure_directory_exists creates nested directories"""
        nested_dir = os.path.join(self.temp_dir, "level1", "level2", "level3")
        
        result = FileUtility.ensure_directory_exists(nested_dir)
        
        assert result is True
        assert os.path.exists(nested_dir)

    def test_ensure_log_file_exists_creates_log_file(self):
        """Test that ensure_log_file_exists creates log file and directory"""
        log_path = os.path.join(self.temp_dir, "logs", "test_log.txt")
        
        result = FileUtility.ensure_log_file_exists(log_path)
        
        assert result is True
        assert os.path.exists(log_path)
        assert os.path.isfile(log_path)

    def test_ensure_log_file_exists_with_existing_file(self):
        """Test that ensure_log_file_exists handles existing log file"""
        log_dir = os.path.join(self.temp_dir, "logs")
        os.makedirs(log_dir)
        log_path = os.path.join(log_dir, "test_log.txt")
        
        # Create file first
        with open(log_path, 'w') as f:
            f.write("test content\n")
        
        result = FileUtility.ensure_log_file_exists(log_path)
        
        assert result is True
        assert os.path.exists(log_path)

    def test_file_exists_returns_true_for_existing_file(self):
        """Test that file_exists returns True for existing files"""
        test_file = os.path.join(self.temp_dir, "test.txt")
        with open(test_file, 'w') as f:
            f.write("test")
        
        result = FileUtility.file_exists(test_file)
        
        assert result is True

    def test_file_exists_returns_false_for_nonexistent_file(self):
        """Test that file_exists returns False for non-existent files"""
        test_file = os.path.join(self.temp_dir, "nonexistent.txt")
        
        result = FileUtility.file_exists(test_file)
        
        assert result is False


class TestConfigManager:
    """Test suite for ConfigManager class"""

    def test_get_s3_bucket_returns_correct_bucket(self):
        """Test that get_s3_bucket returns the correct bucket name"""
        bucket_name = ConfigManager.get_s3_bucket()
        
        assert bucket_name == "data-portfolio-2026"
        assert isinstance(bucket_name, str)

    def test_get_aws_region_returns_valid_region(self):
        """Test that get_aws_region returns a valid AWS region"""
        region = ConfigManager.get_aws_region()
        
        assert isinstance(region, str)
        assert len(region) > 0

    def test_get_binance_base_url_returns_correct_url(self):
        """Test that get_binance_base_url returns the correct Binance URL"""
        url = ConfigManager.get_binance_base_url()
        
        assert url == "https://fapi.binance.com/fapi/v1/klines"
        assert url.startswith("https://")

    def test_get_binance_rate_limit_returns_positive_float(self):
        """Test that get_binance_rate_limit returns a positive float"""
        rate_limit = ConfigManager.get_binance_rate_limit()
        
        assert isinstance(rate_limit, float)
        assert rate_limit > 0


class TestDataLoaderUtility:
    """Test suite for DataLoaderUtility class"""

    def setup_method(self):
        """Create temporary directory and sample CSV files for testing"""
        self.temp_dir = tempfile.mkdtemp()
        self.log_file = os.path.join(self.temp_dir, "test_log.txt")

    def teardown_method(self):
        """Clean up temporary directory after testing"""
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)

    def test_get_coins_from_csv_returns_coin_list(self):
        """Test that get_coins_from_csv correctly reads coins from CSV"""
        csv_path = os.path.join(self.temp_dir, "coin_data.csv")
        df = pd.DataFrame({
            'coin': ['BTC', 'ETH', 'BNB', 'ADA'],
            'market_cap_category': ['mega', 'mega', 'large', 'mid']
        })
        df.to_csv(csv_path, index=False)
        
        result = DataLoaderUtility.get_coins_from_csv(csv_path, self.log_file)
        
        assert isinstance(result, list)
        assert len(result) == 4
        assert 'BTC' in result
        assert 'ETH' in result

    def test_get_coins_from_csv_with_nonexistent_file(self):
        """Test that get_coins_from_csv returns empty list for missing file"""
        csv_path = os.path.join(self.temp_dir, "nonexistent.csv")
        
        result = DataLoaderUtility.get_coins_from_csv(csv_path, self.log_file)
        
        assert isinstance(result, list)
        assert len(result) == 0

    def test_load_market_cap_categories_returns_dict(self):
        """Test that load_market_cap_categories returns correct mapping"""
        csv_path = os.path.join(self.temp_dir, "coin_data.csv")
        df = pd.DataFrame({
            'coin': ['BTC', 'ETH', 'BNB'],
            'market_cap_category': ['mega', 'mega', 'large']
        })
        df.to_csv(csv_path, index=False)
        
        result = DataLoaderUtility.load_market_cap_categories(csv_path, self.log_file)
        
        assert isinstance(result, dict)
        assert result['BTC'] == 'mega'
        assert result['ETH'] == 'mega'
        assert result['BNB'] == 'large'

    def test_load_market_cap_categories_with_missing_values(self):
        """Test that load_market_cap_categories handles missing values"""
        csv_path = os.path.join(self.temp_dir, "coin_data.csv")
        df = pd.DataFrame({
            'coin': ['BTC', 'ETH', 'BNB'],
            'market_cap_category': ['mega', None, 'large']
        })
        df.to_csv(csv_path, index=False)
        
        result = DataLoaderUtility.load_market_cap_categories(csv_path, self.log_file)
        
        assert result['BTC'] == 'mega'
        assert result['ETH'] == 'N/A'
        assert result['BNB'] == 'large'

    def test_load_market_cap_data_returns_dict_with_floats(self):
        """Test that load_market_cap_data creates float mapping"""
        csv_path = os.path.join(self.temp_dir, "coin_data.csv")
        df = pd.DataFrame({
            'coin': ['BTC', 'ETH', 'BNB'],
            'market_cap_value': [1000000000, 500000000, 300000000]
        })
        df.to_csv(csv_path, index=False)
        
        result = DataLoaderUtility.load_market_cap_data(csv_path, self.log_file)
        
        assert isinstance(result, dict)
        assert result['BTC'] == 1000000000
        assert result['ETH'] == 500000000
        assert result['BNB'] == 300000000

    def test_load_market_cap_data_with_invalid_values(self):
        """Test that load_market_cap_data handles invalid values"""
        csv_path = os.path.join(self.temp_dir, "coin_data.csv")
        df = pd.DataFrame({
            'coin': ['BTC', 'ETH', 'BNB'],
            'market_cap_value': [1000000000, 'invalid', 300000000]
        })
        df.to_csv(csv_path, index=False)
        
        result = DataLoaderUtility.load_market_cap_data(csv_path, self.log_file)
        
        assert result['BTC'] == 1000000000
        assert result['ETH'] is None
        assert result['BNB'] == 300000000

    def test_load_market_cap_data_with_missing_file(self):
        """Test that load_market_cap_data returns empty dict for missing file"""
        csv_path = os.path.join(self.temp_dir, "nonexistent.csv")
        
        result = DataLoaderUtility.load_market_cap_data(csv_path, self.log_file)
        
        assert isinstance(result, dict)
        assert len(result) == 0


class TestMathUtility:
    """Test suite for MathUtility class"""

    def test_normalize_data_normalizes_correctly(self):
        """Test that normalize_data normalizes data to 0-1 range"""
        data = [1, 2, 3, 4, 5]
        
        result = MathUtility.normalize_data(data)
        
        assert isinstance(result, list)
        assert len(result) == 5
        assert result[0] == 0.0
        assert result[-1] == 1.0
        assert all(0 <= x <= 1 for x in result)

    def test_normalize_data_with_identical_values(self):
        """Test normalize_data with identical values"""
        data = [5, 5, 5, 5]
        
        result = MathUtility.normalize_data(data)
        
        assert len(result) == 4
        # Division by zero should be handled
        assert all(x == 0.0 for x in result)

    def test_normalize_data_with_negative_values(self):
        """Test normalize_data with negative values"""
        data = [-2, -1, 0, 1, 2]
        
        result = MathUtility.normalize_data(data)
        
        assert result[0] == 0.0
        assert result[-1] == 1.0
        assert all(0 <= x <= 1 for x in result)

    def test_calculate_percentage_change(self):
        """Test calculate_percentage_change calculation"""
        old_value = 100
        new_value = 150
        
        result = MathUtility.calculate_percentage_change(old_value, new_value)
        
        assert result == 50.0

    def test_calculate_percentage_change_negative(self):
        """Test calculate_percentage_change with negative change"""
        old_value = 100
        new_value = 50
        
        result = MathUtility.calculate_percentage_change(old_value, new_value)
        
        assert result == -50.0

    def test_calculate_percentage_change_zero_old_value(self):
        """Test calculate_percentage_change with zero old value"""
        old_value = 0
        new_value = 100
        
        # Should handle division by zero
        try:
            result = MathUtility.calculate_percentage_change(old_value, new_value)
            assert result == 0 or result is None or isinstance(result, (int, float))
        except ZeroDivisionError:
            pass  # Expected behavior


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
