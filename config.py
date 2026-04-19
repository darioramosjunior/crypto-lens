"""
Configuration file for crypto-lens project
Stores default paths, cron schedules, and other global settings
"""

import os
import configparser
from typing import Dict, Any

# Configuration defaults
_DEFAULT_CONFIG: Dict[str, str] = {
    "log_path": "/var/log/crypto-lens/",
    "main_cron_sched": "*/5 * * * *",
    "output_path": "/var/run/crypto-lens/",
    "logs_cleaner_cron_sched": "0 15 * * *",
    "coin_data_collector_cron_sched": "0 12 * * *"
}

# Load configuration from config.conf file
def _load_config() -> Dict[str, str]:
    """
    Load configuration from config.conf file (INI format)
    :return: Dictionary with configuration values
    """
    config_file: str = os.path.join(os.path.dirname(os.path.abspath(__file__)), "config.conf")
    
    try:
        if os.path.exists(config_file):
            config = configparser.ConfigParser()
            config.read(config_file)
            
            # Extract values from sections
            config_dict: Dict[str, str] = {}
            
            # Read paths section
            if config.has_section('paths'):
                if config.has_option('paths', 'log_path'):
                    config_dict['log_path'] = config.get('paths', 'log_path')
                if config.has_option('paths', 'output_path'):
                    config_dict['output_path'] = config.get('paths', 'output_path')
            
            # Read schedules section
            if config.has_section('schedules'):
                if config.has_option('schedules', 'main_cron_sched'):
                    config_dict['main_cron_sched'] = config.get('schedules', 'main_cron_sched')
                if config.has_option('schedules', 'coin_data_collector_cron_sched'):
                    config_dict['coin_data_collector_cron_sched'] = config.get('schedules', 'coin_data_collector_cron_sched')
                if config.has_option('schedules', 'logs_cleaner_cron_sched'):
                    config_dict['logs_cleaner_cron_sched'] = config.get('schedules', 'logs_cleaner_cron_sched')
            
            # Merge with defaults (defaults are fallback values)
            final_config: Dict[str, str] = _DEFAULT_CONFIG.copy()
            final_config.update(config_dict)
            return final_config
        else:
            print(f"[WARNING] Configuration file {config_file} not found. Using default settings.")
            return _DEFAULT_CONFIG
    except Exception as e:
        print(f"[WARNING] Failed to load configuration from {config_file}: {e}. Using default settings.")
        return _DEFAULT_CONFIG

_config: Dict[str, str] = _load_config()

# Log path configuration
LOG_PATH: str = _config.get("log_path", _DEFAULT_CONFIG["log_path"])

# Cron schedule configuration
MAIN_CRON_SCHED: str = _config.get("main_cron_sched", _DEFAULT_CONFIG["main_cron_sched"])

# Output path configuration
OUTPUT_PATH: str = _config.get("output_path", _DEFAULT_CONFIG["output_path"])

# Cron schedule configurations
LOGS_CLEANER_CRON_SCHED: str = _config.get("logs_cleaner_cron_sched", _DEFAULT_CONFIG["logs_cleaner_cron_sched"])

COIN_DATA_COLLECTOR_CRON_SCHED: str = _config.get("coin_data_collector_cron_sched", _DEFAULT_CONFIG["coin_data_collector_cron_sched"])

COIN_DATA_COLLECTOR_CRON_SCHED = _config.get("coin_data_collector_cron_sched", _DEFAULT_CONFIG["coin_data_collector_cron_sched"])

# Ensure log directory exists
def ensure_log_directory() -> bool:
    """Create log directory if it doesn't exist"""
    try:
        os.makedirs(LOG_PATH, exist_ok=True)
        return True
    except Exception as e:
        print(f"[WARNING] Failed to create log directory {LOG_PATH}: {e}")
        # Fallback to local logs directory
        return False

def get_log_file_path(script_name: str) -> str:
    """
    Get the full path for a log file based on script name
    :param script_name: Name of the script (e.g., 'coin_data_collector', 'hourly_fetch_and_pulse')
    :return: Full path to the log file
    """
    log_filename: str = f"{script_name}_log.txt"
    return os.path.join(LOG_PATH, log_filename)

def ensure_output_directory() -> bool:
    """Create output directory if it doesn't exist"""
    try:
        os.makedirs(OUTPUT_PATH, exist_ok=True)
        return True
    except Exception as e:
        print(f"[WARNING] Failed to create output directory {OUTPUT_PATH}: {e}")
        return False

def get_output_file_path(filename: str) -> str:
    """
    Get the full path for an output file
    :param filename: Name of the file (e.g., 'prices_1h.csv', 'market_pulse.png')
    :return: Full path to the output file
    """
    return os.path.join(OUTPUT_PATH, filename)
