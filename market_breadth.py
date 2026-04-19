import os
from datetime import datetime
import csv
from io import BytesIO
from typing import Optional, Any

try:
    import pandas as pd
except Exception:
    pd = None

try:
    from dotenv import load_dotenv
except Exception:
    def load_dotenv() -> None:
        return None

try:
    import boto3
except Exception:
    boto3 = None

import config
import logger
from discord_integrator import send_to_discord
from utils import FileUtility, ConfigManager, S3Manager
from validations import MarketBreadthData


load_dotenv()
os.umask(0o022)

# Ensure log and output directories exist
config.ensure_log_directory()
config.ensure_output_directory()

script_dir: str = os.path.dirname(os.path.abspath(__file__))
log_path: str = config.get_log_file_path("market_breadth")
output_dir: str = config.OUTPUT_PATH

# Create log file
FileUtility.ensure_log_file_exists(log_path)


def upload_dataframe_to_s3(dataframe: Any, s3_key: str) -> bool:
    """Wrapper for S3Manager - upload DataFrame to S3"""
    return S3Manager.upload_dataframe_to_s3(dataframe, s3_key, log_path)


def main() -> None:
    """Main function to calculate and report market breadth"""
    # Get file paths
    prices_1d_path: str = config.get_output_file_path("prices_1d.csv")
    market_breadth_csv: str = config.get_output_file_path("market_breadth.csv")

    webhook_url: Optional[str] = os.getenv("MARKET_BREADTH_WEBHOOK") or os.getenv("DAY_CHANGE_WEBHOOK")
    if not webhook_url:
        logger.log_event(log_category="WARNING", message="MARKET_BREADTH_WEBHOOK and DAY_CHANGE_WEBHOOK are not set; message will not be sent to Discord.", path=log_path)

    # Check if prices_1d.csv exists
    if not os.path.exists(prices_1d_path):
        logger.log_event(log_category="ERROR", message=f"{prices_1d_path} not found. Run daily_fetch_and_pulse.py first.", path=log_path)
        print(f"[ERROR] {prices_1d_path} not found. Run daily_fetch_and_pulse.py first.")
        return

    # Check if pandas is available
    if pd is None:
        logger.log_event(log_category="ERROR", message="pandas is required but not installed.", path=log_path)
        print("[ERROR] pandas is required but not installed.")
        return

    try:
        # Read prices_1d.csv
        df: Any = pd.read_csv(prices_1d_path)
        df.columns = df.columns.str.strip()
    except Exception as e:
        logger.log_event(log_category="ERROR", message=f"Failed to read {prices_1d_path}: {e}", path=log_path)
        print(f"[ERROR] Failed to read {prices_1d_path}: {e}")
        return

    if len(df) == 0:
        logger.log_event(log_category="ERROR", message="prices_1d.csv is empty.", path=log_path)
        print("[ERROR] prices_1d.csv is empty.")
        return

    # Get the latest timestamp (all rows should have the same timestamp as they're current data)
    latest_timestamp: Any = df['timestamp'].iloc[0]

    # Extract BTC and BTCDOM data
    btc_row: Any = df[df['symbol'] == 'BTCUSDT']
    btcd_row: Any = df[df['symbol'] == 'BTCDOMUSDT']

    btc_pct: Optional[float] = round(float(btc_row['price_change'].iloc[0]), 2) if len(btc_row) > 0 and pd.notna(btc_row['price_change'].iloc[0]) else None
    btcd_pct: Optional[float] = round(float(btcd_row['price_change'].iloc[0]), 2) if len(btcd_row) > 0 and pd.notna(btcd_row['price_change'].iloc[0]) else None

    # Exclude BTC and BTCDOM from market breadth calculation
    excluded: set = {'BTCUSDT', 'BTCDOMUSDT'}
    filtered_df: Any = df[~df['symbol'].isin(excluded)].copy()

    total: int = len(filtered_df)
    positive: int = (filtered_df['price_change'] > 0).sum()

    market_breadth: float = round((positive / total) * 100.0, 2) if total > 0 else 0.0

    now: str = datetime.utcnow().isoformat()

    # Validate market breadth data
    try:
        MarketBreadthData(
            timestamp=datetime.fromisoformat(now),
            total_coins=total,
            positive_coins=positive,
            negative_coins=total - positive,
            btc_change=btc_pct,
            btc_dominance_change=btcd_pct
        )
        logger.log_event(log_category="INFO", message="Market breadth data validation passed", path=log_path)
    except Exception as e:
        logger.log_event(log_category="WARNING", message=f"Market breadth validation warning: {e}", path=log_path)

    # Create result DataFrame
    result_df: Any = pd.DataFrame([{
        'timestamp': now,
        'market_breadth_pct': market_breadth,
        'positive_count': int(positive),
        'total_count': total,
        'btc_pct': btc_pct if btc_pct is not None else '',
        'btcd_pct': btcd_pct if btcd_pct is not None else ''
    }])

    # Save locally
    try:
        os.makedirs(output_dir, exist_ok=True)
        result_df.to_csv(market_breadth_csv, index=False)
        logger.log_event(log_category="INFO", message=f"Successfully saved market breadth data to {market_breadth_csv}", path=log_path)
        print(f"[OK] Saved market_breadth.csv locally to {market_breadth_csv}")
    except Exception as e:
        logger.log_event(log_category="ERROR", message=f"Failed to save market_breadth.csv locally: {e}", path=log_path)
        print(f"[ERROR] Failed to save {market_breadth_csv}: {e}")
        return

    # Upload to S3
    upload_dataframe_to_s3(result_df, "market_breadth/market_breadth.csv")

    # Prepare message for Discord
    lines: list[str] = []
    lines.append(f"Market Breadth: {market_breadth}% ({positive}/{total} coins positive)")
    if btc_pct is not None:
        lines.append(f"BTC Day Change: {btc_pct}%")
    else:
        lines.append("BTC Day Change: N/A")
    if btcd_pct is not None:
        lines.append(f"BTCDOM Day Change: {btcd_pct}%")
    else:
        lines.append("BTCDOM Day Change: N/A")

    message: str = "\n".join(lines)

    print(message)
    logger.log_event(log_category="INFO", message=f"Market breadth summary: {message}", path=log_path)

    if webhook_url:
        send_to_discord(webhook_url, message=message)


if __name__ == '__main__':
    print(f"Running {__file__}...")
    main()
