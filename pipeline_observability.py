"""
Pipeline Observability Script
Monitors the latest run of each data pipeline script by analyzing their log files.
Generates a summary of errors/warnings and sends it to Discord.
"""

import os
import re
from datetime import datetime
from collections import defaultdict
from typing import Optional, Dict, List, Any

try:
    import requests
except ImportError:
    requests = None

try:
    from dotenv import load_dotenv
except Exception:
    def load_dotenv() -> None:
        return None

import config
import logger

load_dotenv()
os.umask(0o022)

# Ensure log directory exists
config.ensure_log_directory()

script_dir: str = os.path.dirname(os.path.abspath(__file__))
log_path: str = config.get_log_file_path("pipeline_observability")

# Create log file if it doesn't exist
try:
    os.makedirs(os.path.dirname(log_path), exist_ok=True)
    if not os.path.exists(log_path):
        open(log_path, 'a').close()
except Exception as e:
    print(f"[WARNING] Failed to create log file {log_path}: {e}")

# Pipeline scripts to monitor
PIPELINE_SCRIPTS: List[str] = [
    "coin_data_collector",
    "daily_fetch_and_pulse",
    "hourly_fetch_and_pulse",
    "oi_change_screener",
    "market_breadth"
]

# Discord webhook from environment
WEBHOOK_URL: Optional[str] = os.getenv("PIPELINE_ERRORS")

if not WEBHOOK_URL:
    logger.log_event(
        log_category="WARNING",
        message="PIPELINE_ERRORS webhook not set. Summary will not be sent to Discord.",
        path=log_path
    )


def parse_log_entry(log_line: str) -> Optional[Dict[str, Any]]:
    """
    Parse a single log entry into its components
    Format: YYYY-MM-DD HH:MM:SS.MMMMMM, CATEGORY, MESSAGE
    :param log_line: str - a single log line
    :return: dict with keys: timestamp, category, message (or None if parse fails)
    """
    try:
        # Split by first two commas to extract timestamp and category
        parts: List[str] = log_line.split(", ", 2)
        if len(parts) < 3:
            return None

        timestamp_str: str = parts[0].strip()
        category: str = parts[1].strip()
        message: str = parts[2].strip()

        # Parse the timestamp
        try:
            timestamp: datetime = datetime.fromisoformat(timestamp_str)
        except ValueError:
            # Try alternative format in case of milliseconds
            timestamp = datetime.strptime(timestamp_str.split('.')[0], "%Y-%m-%d %H:%M:%S")

        return {
            "timestamp": timestamp,
            "category": category,
            "message": message
        }
    except Exception:
        return None


def read_log_file(script_name: str) -> List[Dict[str, Any]]:
    """
    Read and parse log file for a specific script
    :param script_name: str - name of the script (e.g., "coin_data_collector")
    :return: list of parsed log entries
    """
    log_file_path: str = config.get_log_file_path(script_name)

    if not os.path.exists(log_file_path):
        return []

    try:
        with open(log_file_path, 'r') as f:
            lines: List[str] = f.readlines()

        parsed_logs: List[Dict[str, Any]] = []
        for line in lines:
            if line.strip():
                parsed_entry: Optional[Dict[str, Any]] = parse_log_entry(line.strip())
                if parsed_entry:
                    parsed_logs.append(parsed_entry)

        return parsed_logs
    except Exception as e:
        logger.log_event(
            log_category="ERROR",
            message=f"Failed to read log file for {script_name}: {e}",
            path=log_path
        )
        return []


def get_latest_run_logs(script_name: str, parsed_logs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Extract logs from the latest run of a script
    A new run is indicated by an INFO log message containing "Running"
    :param script_name: str - name of the script
    :param parsed_logs: list - all parsed log entries
    :return: list of log entries from the latest run
    """
    if not parsed_logs:
        return []

    # Find the index of the latest "Running" message
    latest_run_start_idx: int = -1
    for idx, log_entry in enumerate(reversed(parsed_logs)):
        if log_entry["category"] == "INFO" and "Running" in log_entry["message"]:
            latest_run_start_idx = len(parsed_logs) - 1 - idx
            break

    # If no running message found, use the most recent entries (last 1 minute worth)
    if latest_run_start_idx == -1:
        if parsed_logs:
            # Return all logs from the last timestamp
            latest_time: datetime = parsed_logs[-1]["timestamp"]
            # Get all logs from the same timestamp
            latest_run_logs = [
                log for log in parsed_logs
                if log["timestamp"] == latest_time
            ]
            # If no logs from same timestamp, return last entry
            if not latest_run_logs:
                latest_run_logs = [parsed_logs[-1]]
            return latest_run_logs
        return []

    # Return logs from latest run start to end of file
    return parsed_logs[latest_run_start_idx:]


def analyze_latest_run(script_name):
    """
    Analyze the latest run of a script
    :param script_name: str - name of the script
    :return: dict with analysis results
    """
    parsed_logs = read_log_file(script_name)
    latest_logs = get_latest_run_logs(script_name, parsed_logs)

    if not latest_logs:
        return {
            "script": script_name,
            "ran": False,
            "error_count": 0,
            "warning_count": 0,
            "errors": [],
            "warnings": [],
            "status": "NO LOGS FOUND"
        }

    # Extract errors and warnings
    errors = []
    warnings = []
    has_completion = False

    for log_entry in latest_logs:
        category = log_entry["category"]
        message = log_entry["message"]

        if category == "ERROR":
            errors.append(message)
        elif category == "WARNING":
            warnings.append(message)
        elif category == "INFO" and "completed successfully" in message.lower():
            has_completion = True

    # Deduplicate errors and warnings
    unique_errors = list(set(errors))
    unique_warnings = list(set(warnings))

    status = "SUCCESS" if (has_completion and not errors) else "COMPLETED WITH ISSUES"
    if errors:
        status = "FAILED"

    return {
        "script": script_name,
        "ran": True,
        "error_count": len(errors),
        "warning_count": len(warnings),
        "unique_error_count": len(unique_errors),
        "unique_warning_count": len(unique_warnings),
        "errors": unique_errors,
        "warnings": unique_warnings,
        "status": status
    }


def format_discord_message(analysis_results):
    """
    Format the analysis results into a Discord message
    :param analysis_results: list of dicts - analysis results for each script
    :return: str - formatted Discord message
    """
    message = "🔍 **Pipeline Observability Report** 🔍\n"
    message += f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}\n\n"

    for result in analysis_results:
        script_name = result["script"]
        status = result["status"]

        # Color code based on status
        if "SUCCESS" in status:
            status_emoji = "✅"
        elif "FAILED" in status:
            status_emoji = "❌"
        else:
            status_emoji = "⚠️"

        message += f"{status_emoji} **{script_name}**\n"
        message += f"Status: {status}\n"

        if not result["ran"]:
            message += "  No logs found for this script\n"
        else:
            error_count = result["error_count"]
            warning_count = result["warning_count"]

            if error_count == 0 and warning_count == 0:
                message += "  Errors/Warnings: None\n"
            else:
                if error_count > 0:
                    message += f"  ❌ Errors: {error_count} (unique: {result['unique_error_count']})\n"
                    # Show first 3 unique errors
                    for i, error in enumerate(result["errors"][:3], 1):
                        # Truncate if too long
                        error_display = error[:80] + "..." if len(error) > 80 else error
                        message += f"     {i}. {error_display}\n"
                    if len(result["errors"]) > 3:
                        message += f"     ... and {len(result['errors']) - 3} more\n"

                if warning_count > 0:
                    message += f"  ⚠️ Warnings: {warning_count} (unique: {result['unique_warning_count']})\n"
                    # Show first 2 unique warnings
                    for i, warning in enumerate(result["warnings"][:2], 1):
                        warning_display = warning[:70] + "..." if len(warning) > 70 else warning
                        message += f"     {i}. {warning_display}\n"
                    if len(result["warnings"]) > 2:
                        message += f"     ... and {len(result['warnings']) - 2} more\n"

        message += "\n"

    return message


def send_to_discord(webhook_url, message):
    """
    Send the summary message to Discord
    :param webhook_url: str - Discord webhook URL
    :param message: str - message to send
    :return: bool - True if successful, False otherwise
    """
    if requests is None:
        logger.log_event(
            log_category="ERROR",
            message="requests library not installed. Cannot send to Discord.",
            path=log_path
        )
        return False

    try:
        response = requests.post(
            webhook_url,
            data={"content": message}
        )

        if response.status_code == 200:
            logger.log_event(
                log_category="INFO",
                message="Successfully sent pipeline observability report to Discord",
                path=log_path
            )
            return True
        else:
            logger.log_event(
                log_category="ERROR",
                message=f"Failed to send to Discord. Status: {response.status_code}",
                path=log_path
            )
            return False
    except Exception as e:
        logger.log_event(
            log_category="ERROR",
            message=f"Exception while sending to Discord: {e}",
            path=log_path
        )
        return False


def main():
    """
    Main function - analyze all pipeline scripts and send report to Discord
    """
    logger.log_event(
        log_category="INFO",
        message="Starting pipeline observability analysis",
        path=log_path
    )

    # Analyze each pipeline script
    analysis_results = []
    for script_name in PIPELINE_SCRIPTS:
        print(f"Analyzing {script_name}...")
        result = analyze_latest_run(script_name)
        analysis_results.append(result)

    # Format message
    discord_message = format_discord_message(analysis_results)

    # Print report locally
    print("\n" + "=" * 80)
    print(discord_message)
    print("=" * 80)

    # Send to Discord if webhook is configured
    if WEBHOOK_URL:
        print("\nSending report to Discord...")
        send_to_discord(WEBHOOK_URL, discord_message)
    else:
        print("\nWARNING: PIPELINE_ERRORS webhook not configured. Skipping Discord notification.")

    # Log summary
    total_errors = sum(r["error_count"] for r in analysis_results)
    total_warnings = sum(r["warning_count"] for r in analysis_results)
    failed_scripts = [r["script"] for r in analysis_results if r["status"] == "FAILED"]

    logger.log_event(
        log_category="INFO",
        message=f"Pipeline observability analysis complete. Total errors: {total_errors}, Total warnings: {total_warnings}, Failed scripts: {len(failed_scripts)}",
        path=log_path
    )

    print(f"\n[OK] Analysis complete!")
    print(f"  Total Errors: {total_errors}")
    print(f"  Total Warnings: {total_warnings}")
    print(f"  Failed Scripts: {len(failed_scripts)}")


if __name__ == "__main__":
    print(f"Running {__file__}...")
    main()
