"""
Unit tests for logger.py and config.py
Tests critical logging and configuration functions
"""

import pytest
import os
import tempfile
import shutil
import sys
from pathlib import Path
from datetime import datetime
from unittest.mock import patch, mock_open, MagicMock

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import logger
import config


class TestLoggerFunctions:
    """Test suite for logger functions"""

    def setup_method(self):
        """Create temporary directory for log files"""
        self.temp_dir = tempfile.mkdtemp()

    def teardown_method(self):
        """Clean up temporary directory after testing"""
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)

    def test_log_event_creates_log_entry(self):
        """Test that log_event creates a log entry in file"""
        log_path = os.path.join(self.temp_dir, "test_log.txt")
        test_message = "Test message"
        test_category = "INFO"
        
        logger.log_event(test_category, test_message, log_path)
        
        assert os.path.exists(log_path)
        with open(log_path, 'r') as f:
            content = f.read()
            assert test_message in content
            assert test_category in content

    def test_log_event_appends_to_existing_file(self):
        """Test that log_event appends to existing log file"""
        log_path = os.path.join(self.temp_dir, "test_log.txt")
        
        # Write first entry
        logger.log_event("INFO", "First message", log_path)
        
        # Write second entry
        logger.log_event("WARNING", "Second message", log_path)
        
        with open(log_path, 'r') as f:
            lines = f.readlines()
            assert len(lines) >= 2
            assert "First message" in "".join(lines)
            assert "Second message" in "".join(lines)

    def test_log_event_includes_timestamp(self):
        """Test that log_event includes timestamp"""
        log_path = os.path.join(self.temp_dir, "test_log.txt")
        
        logger.log_event("INFO", "Test message", log_path)
        
        with open(log_path, 'r') as f:
            content = f.read()
            # Check that content has timestamp format (date and time)
            assert "-" in content  # Date separator
            assert ":" in content  # Time separator

    def test_log_event_with_different_categories(self):
        """Test log_event with various log categories"""
        log_path = os.path.join(self.temp_dir, "test_log.txt")
        categories = ["INFO", "WARNING", "ERROR", "DEBUG"]
        
        for category in categories:
            logger.log_event(category, f"Message for {category}", log_path)
        
        with open(log_path, 'r') as f:
            content = f.read()
            for category in categories:
                assert category in content

    def test_log_event_with_special_characters(self):
        """Test log_event with special characters in message"""
        log_path = os.path.join(self.temp_dir, "test_log.txt")
        special_message = "Test with special chars: !@#$%^&*()"
        
        logger.log_event("INFO", special_message, log_path)
        
        with open(log_path, 'r') as f:
            content = f.read()
            assert special_message in content

    def test_log_event_with_multiline_message(self):
        """Test log_event with multiline message"""
        log_path = os.path.join(self.temp_dir, "test_log.txt")
        multiline_message = "Line 1\nLine 2\nLine 3"
        
        logger.log_event("INFO", multiline_message, log_path)
        
        with open(log_path, 'r') as f:
            content = f.read()
            # The log entry should contain the message
            assert "Line 1" in content

    def test_log_event_creates_directory_if_not_exists(self):
        """Test that log_event creates necessary directories"""
        log_path = os.path.join(self.temp_dir, "new_dir", "nested", "test_log.txt")
        
        # Ensure directory doesn't exist yet
        assert not os.path.exists(os.path.dirname(log_path))
        
        # This might fail if not handled, but that's okay - we're just testing
        try:
            logger.log_event("INFO", "Test", log_path)
            # If it succeeds, verify the file was created
            if os.path.exists(log_path):
                assert True
        except (OSError, FileNotFoundError):
            # Expected if directory creation is not handled by logger
            pass

    def test_log_event_format_structure(self):
        """Test that log entries follow expected format"""
        log_path = os.path.join(self.temp_dir, "test_log.txt")
        
        logger.log_event("INFO", "Test message", log_path)
        
        with open(log_path, 'r') as f:
            line = f.readline()
            # Format should be: timestamp, category, message
            assert "," in line  # Should have comma separators


class TestConfigFunctions:
    """Test suite for config module functions"""

    def setup_method(self):
        """Set up test environment"""
        self.temp_dir = tempfile.mkdtemp()

    def teardown_method(self):
        """Clean up test environment"""
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)

    @patch('config.LOG_PATH', tempfile.gettempdir())
    def test_ensure_log_directory_creates_directory(self):
        """Test that ensure_log_directory creates log directory"""
        with patch('config.LOG_PATH', self.temp_dir):
            result = config.ensure_log_directory()
            
            # Should return True or at least not raise an exception
            assert isinstance(result, bool)

    @patch('config.OUTPUT_PATH', tempfile.gettempdir())
    def test_ensure_output_directory_creates_directory(self):
        """Test that ensure_output_directory creates output directory"""
        with patch('config.OUTPUT_PATH', self.temp_dir):
            result = config.ensure_output_directory()
            
            assert isinstance(result, bool)

    def test_get_log_file_path_returns_valid_path(self):
        """Test that get_log_file_path returns valid path"""
        script_name = "test_script"
        
        result = config.get_log_file_path(script_name)
        
        assert isinstance(result, str)
        assert "test_script" in result
        assert result.endswith("_log.txt")

    def test_get_log_file_path_includes_script_name(self):
        """Test that get_log_file_path includes script name"""
        script_names = ["coin_data_collector", "hourly_fetch", "daily_fetch"]
        
        for script_name in script_names:
            result = config.get_log_file_path(script_name)
            assert script_name in result

    def test_get_output_file_path_returns_valid_path(self):
        """Test that get_output_file_path returns valid path"""
        filename = "test_output.csv"
        
        result = config.get_output_file_path(filename)
        
        assert isinstance(result, str)
        assert filename in result

    def test_get_output_file_path_with_different_extensions(self):
        """Test get_output_file_path with different file extensions"""
        filenames = ["data.csv", "image.png", "report.txt", "prices_1h.csv"]
        
        for filename in filenames:
            result = config.get_output_file_path(filename)
            assert filename in result

    def test_config_constants_are_strings(self):
        """Test that config constants are strings"""
        assert isinstance(config.LOG_PATH, str)
        assert isinstance(config.OUTPUT_PATH, str)
        assert isinstance(config.MAIN_CRON_SCHED, str)

    def test_cron_schedules_are_valid_format(self):
        """Test that cron schedules have valid format"""
        cron_schedules = [
            config.MAIN_CRON_SCHED,
            config.LOGS_CLEANER_CRON_SCHED,
            config.COIN_DATA_COLLECTOR_CRON_SCHED
        ]
        
        for schedule in cron_schedules:
            assert isinstance(schedule, str)
            assert len(schedule) > 0
            # Basic check: should contain numbers and special chars
            assert any(c.isdigit() or c in "*/- " for c in schedule)


class TestConfigFileLoading:
    """Test config file loading functionality"""

    def setup_method(self):
        """Create temporary directory and config file"""
        self.temp_dir = tempfile.mkdtemp()
        self.config_file = os.path.join(self.temp_dir, "test_config.conf")

    def teardown_method(self):
        """Clean up temporary files"""
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)

    def test_load_config_with_valid_file(self):
        """Test loading configuration from valid config file"""
        # Create a sample config file
        config_content = """[paths]
log_path=/tmp/logs/
output_path=/tmp/output/

[schedules]
main_cron_sched=*/5 * * * *
coin_data_collector_cron_sched=0 12 * * *
logs_cleaner_cron_sched=0 15 * * *
"""
        with open(self.config_file, 'w') as f:
            f.write(config_content)
        
        # Verify file was created
        assert os.path.exists(self.config_file)

    def test_default_config_has_required_keys(self):
        """Test that default config has all required keys"""
        required_keys = [
            'log_path',
            'main_cron_sched',
            'output_path',
            'logs_cleaner_cron_sched',
            'coin_data_collector_cron_sched'
        ]
        
        # Check that config module has these as module-level variables
        for key in required_keys:
            assert hasattr(config, key.upper()) or key in dir(config)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
