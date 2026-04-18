import os
from datetime import datetime
import csv
from io import BytesIO

try:
    import pandas as pd
except Exception:
    pd = None

try:
    from dotenv import load_dotenv
except Exception:
    def load_dotenv():
        return None

try:
    import boto3
except Exception:
    boto3 = None

import config
import logger
from discord_integrator import send_to_discord


load_dotenv()
os.umask(0o022)

# Ensure log and output directories exist
config.ensure_log_directory()
config.ensure_output_directory()

script_dir = os.path.dirname(os.path.abspath(__file__))
log_path = config.get_log_file_path("market_breadth")
output_dir = config.OUTPUT_PATH

# Create log file if it doesn't exist
try:
    os.makedirs(os.path.dirname(log_path), exist_ok=True)
    if not os.path.exists(log_path):
        open(log_path, 'a').close()
except Exception as e:
    print(f"[WARNING] Failed to create log file {log_path}: {e}")

# AWS S3 configuration
S3_BUCKET_NAME = "data-portfolio-2026"
AWS_REGION = os.getenv("AWS_REGION", "ap-southeast-2")


def upload_dataframe_to_s3(dataframe, s3_key):
    """
    Upload DataFrame directly to S3 as CSV
    :param dataframe: pandas DataFrame to upload
    :param s3_key: S3 key path
    """
    if boto3 is None:
        logger.log_event(log_category="WARNING", message=f"boto3 not installed. Skipping S3 upload for {s3_key}", path=log_path)
        return False

    try:
        s3_client = boto3.client('s3', region_name=AWS_REGION)
        csv_buffer = BytesIO()
        dataframe.to_csv(csv_buffer, index=False)
        csv_buffer.seek(0)
        s3_client.upload_fileobj(csv_buffer, S3_BUCKET_NAME, s3_key)
        logger.log_event(log_category="INFO", message=f"Successfully uploaded {s3_key} to S3 bucket {S3_BUCKET_NAME}", path=log_path)
        print(f"[OK] Uploaded {s3_key} to S3 bucket {S3_BUCKET_NAME}")
        return True
    except Exception as e:
        logger.log_event(log_category="ERROR", message=f"Failed to upload {s3_key} to S3: {e}", path=log_path)
        print(f"[ERROR] Failed to upload {s3_key} to S3: {e}")
        return False


def main():
    # Get file paths
    prices_1d_path = config.get_output_file_path("prices_1d.csv")
    market_breadth_csv = config.get_output_file_path("market_breadth.csv")

    webhook_url = os.getenv("MARKET_BREADTH_WEBHOOK") or os.getenv("DAY_CHANGE_WEBHOOK")
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
        df = pd.read_csv(prices_1d_path)
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
    latest_timestamp = df['timestamp'].iloc[0]

    # Extract BTC and BTCDOM data
    btc_row = df[df['symbol'] == 'BTCUSDT']
    btcd_row = df[df['symbol'] == 'BTCDOMUSDT']

    btc_pct = round(float(btc_row['price_change'].iloc[0]), 2) if len(btc_row) > 0 and pd.notna(btc_row['price_change'].iloc[0]) else None
    btcd_pct = round(float(btcd_row['price_change'].iloc[0]), 2) if len(btcd_row) > 0 and pd.notna(btcd_row['price_change'].iloc[0]) else None

    # Exclude BTC and BTCDOM from market breadth calculation
    excluded = {'BTCUSDT', 'BTCDOMUSDT'}
    filtered_df = df[~df['symbol'].isin(excluded)].copy()

    total = len(filtered_df)
    positive = (filtered_df['price_change'] > 0).sum()

    market_breadth = round((positive / total) * 100.0, 2) if total > 0 else 0.0

    now = datetime.utcnow().isoformat()

    # Create result DataFrame
    result_df = pd.DataFrame([{
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
    lines = []
    lines.append(f"Market Breadth: {market_breadth}% ({positive}/{total} coins positive)")
    if btc_pct is not None:
        lines.append(f"BTC Day Change: {btc_pct}%")
    else:
        lines.append("BTC Day Change: N/A")
    if btcd_pct is not None:
        lines.append(f"BTCDOM Day Change: {btcd_pct}%")
    else:
        lines.append("BTCDOM Day Change: N/A")

    message = "\n".join(lines)

    print(message)
    logger.log_event(log_category="INFO", message=f"Market breadth summary: {message}", path=log_path)

    if webhook_url:
        send_to_discord(webhook_url, message=message)


if __name__ == '__main__':
    print(f"Running {__file__}...")
    main()
