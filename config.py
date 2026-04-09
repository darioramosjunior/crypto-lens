"""
Configuration file for crypto-lens project
Stores default paths, cron schedules, and other global settings
"""

import os
import json

# Configuration defaults
_DEFAULT_CONFIG = {
    "log_path": "/var/log/crypto-lens/",
    "main_cron_sched": "*/5 * * * *"
}

# Load configuration from config.conf file
def _load_config():
    """
    Load configuration from config.conf file
    :return: Dictionary with configuration values
    """
    config_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), "config.conf")
    
    try:
        if os.path.exists(config_file):
            with open(config_file, 'r') as f:
                config = json.load(f)
                return config
        else:
            print(f"[WARNING] Configuration file {config_file} not found. Using default settings.")
            return _DEFAULT_CONFIG
    except Exception as e:
        print(f"[WARNING] Failed to load configuration from {config_file}: {e}. Using default settings.")
        return _DEFAULT_CONFIG

_config = _load_config()

# Log path configuration
LOG_PATH = _config.get("log_path", _DEFAULT_CONFIG["log_path"])

# Cron schedule configuration
MAIN_CRON_SCHED = _config.get("main_cron_sched", _DEFAULT_CONFIG["main_cron_sched"])

# Ensure log directory exists
def ensure_log_directory():
    """Create log directory if it doesn't exist"""
    try:
        os.makedirs(LOG_PATH, exist_ok=True)
        return True
    except Exception as e:
        print(f"[WARNING] Failed to create log directory {LOG_PATH}: {e}")
        # Fallback to local logs directory
        return False

def get_log_file_path(script_name):
    """
    Get the full path for a log file based on script name
    :param script_name: Name of the script (e.g., 'coin_data_collector', 'hourly_fetch_and_pulse')
    :return: Full path to the log file
    """
    log_filename = f"{script_name}_log.txt"
    return os.path.join(LOG_PATH, log_filename)
