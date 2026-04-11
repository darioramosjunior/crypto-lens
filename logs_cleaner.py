import os
from glob import glob
import config

script_path = os.path.dirname(os.path.abspath(__file__))
logs_path = config.LOG_PATH


def delete_content():
    """Delete content of all log files in the configured log path"""
    if not os.path.exists(logs_path):
        print(f"[WARNING] Log path {logs_path} does not exist. No logs to clean.")
        return
    
    # Get both .txt and .log files
    log_files = glob(os.path.join(logs_path, "*.txt")) + glob(os.path.join(logs_path, "*.log"))
    if not log_files:
        print(f"[INFO] No log files found in {logs_path}")
        return
    
    for path in log_files:
        try:
            print(f"Cleaning: {path}")
            with open(path, 'w') as file:
                file.write("")
        except Exception as e:
            print(f"[ERROR] Failed to clean {path}: {e}")


if __name__ == "__main__":
    print(f"Running {__file__}")
    print(f"Script path: {script_path}")
    print(f"Logs path: {logs_path}")
    try:
        delete_content()
        print("Successfully cleaned log files...")
    except Exception as e:
        print(f"Failed to clean log files: {e}")