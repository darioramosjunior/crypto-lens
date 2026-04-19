import os
import pandas as pd
import numpy as np
import logger
import sys
import asyncio
from collections import defaultdict
import matplotlib.pyplot as plt
from scipy.stats import skew
from io import BytesIO
from datetime import datetime
from itertools import islice
import config
from typing import List, Dict, Any, Optional, Tuple
import aiohttp
from utils import FileUtility, ConfigManager, DataLoaderUtility, BinanceDataFetcher, IndicatorCalculator, MathUtility, S3Manager
from validations import PriceChangeData, TrendCounts, MarketBreadthData

if sys.platform.startswith('win'):
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

import time
import pandas_ta as ta
from discord_integrator import send_to_discord, upload_to_discord
from dotenv import load_dotenv
import boto3

load_dotenv()
os.umask(0o022)

# Ensure log and output directories exist
config.ensure_log_directory()
config.ensure_output_directory()

script_dir: str = os.path.dirname(os.path.abspath(__file__))
log_path: str = config.get_log_file_path("daily_fetch_and_pulse")
coin_data_path: str = config.get_output_file_path("coin_data.csv")
output_dir: str = config.OUTPUT_PATH
prices_1d_path: str = config.get_output_file_path("prices_1d.csv")
trend_1d_path: str = config.get_output_file_path("coin_trend_1d.csv")
market_pulse_image_path: str = config.get_output_file_path("market_pulse_daily.png")

# Create log file
FileUtility.ensure_log_file_exists(log_path)

# Discord webhook
discord_webhook_url: Optional[str] = os.getenv("DAY_CHANGE_WEBHOOK")
if not discord_webhook_url:
    logger.log_event(log_category="WARNING", message="DAY_CHANGE_WEBHOOK not set; using fallback hard-coded webhook. Consider setting DAY_CHANGE_WEBHOOK in .env or CI secrets.", path=log_path)

# Configuration (centralized in ConfigManager)
INTERVAL: str = "1d"
LIMIT: int = 200


# Utility functions removed - now using centralized utilities from utils module
# Functions replaced: get_coins, load_market_cap_categories, fetch_ohlcv, get_coin_data, parse_raw_data_to_dataframe


# Functions removed - now using centralized utilities from utils module
# Functions: calculate_indicators_in_memory, determine_trend, calculate_trend_counts 


def calculate_price_changes_with_trend(in_memory_data, indicators_data, market_cap_categories):
    """
    Calculate daily price changes for all symbols and save directly to S3
    :param in_memory_data: Dictionary {symbol: DataFrame}
    :param indicators_data: Dictionary {symbol: DataFrame} with indicators
    :param market_cap_categories: dict mapping coin -> market_cap_category
    :return: DataFrame with symbol, timestamp, close, previous_close, price_change, trend_category, market_cap_category
    """
    price_changes = []
    day_change_dict = {}
    validation_issues = []
    
    try:
        for symbol, df in in_memory_data.items():
            df_sorted = df.sort_values('timestamp').copy()
            
            if len(df_sorted) < 2:
                continue
            
            # Get latest data
            latest_row = df_sorted.iloc[-1]
            previous_row = df_sorted.iloc[-2]
            
            latest_close = latest_row['close']
            previous_close = previous_row['close']
            latest_timestamp = latest_row['timestamp']
            
            # Calculate price change
            if pd.notna(latest_close) and pd.notna(previous_close) and previous_close != 0:
                price_change = ((latest_close - previous_close) / previous_close * 100)
                day_change_dict[symbol] = round(float(price_change), 2)
            else:
                price_change = 0.0
                day_change_dict[symbol] = 0.0
            
            # Get latest trend category
            trend_category = 'N/A'
            if symbol in indicators_data:
                indicator_df = indicators_data[symbol]
                if len(indicator_df) > 0:
                    latest_indicator = indicator_df.iloc[-1]
                    trend_category = determine_trend(latest_indicator)
            
            # Get market cap category
            market_cap_category = market_cap_categories.get(symbol, 'N/A')
            
            price_change_record = {
                'symbol': symbol,
                'timestamp': latest_timestamp,
                'close': latest_close,
                'previous_close': previous_close,
                'price_change': price_change,
                'trend_category': trend_category,
                'market_cap_category': market_cap_category
            }
            
            # Validate price change data
            try:
                PriceChangeData(
                    symbol=symbol,
                    timestamp=pd.Timestamp(latest_timestamp) if not isinstance(latest_timestamp, pd.Timestamp) else latest_timestamp,
                    close=float(latest_close),
                    previous_close=float(previous_close),
                    price_change=float(price_change),
                    trend_category=trend_category,
                    market_cap_category=market_cap_category
                )
            except Exception as e:
                validation_issues.append(f"{symbol}: {str(e)[:40]}")
            
            price_changes.append(price_change_record)
        
        price_changes_df = pd.DataFrame(price_changes)
        
        # Ensure numeric columns are float type
        for col in ['close', 'previous_close', 'price_change']:
            if col in price_changes_df.columns:
                price_changes_df[col] = pd.to_numeric(price_changes_df[col], errors='coerce')
        
        if validation_issues:
            logger.log_event(
                log_category="INFO",
                message=f"Price change validation completed ({len(validation_issues)} items checked)",
                path=log_path
            )
        
        # Save locally
        os.makedirs(output_dir, exist_ok=True)
        price_changes_df.to_csv(prices_1d_path, index=False)
        logger.log_event(log_category="INFO", message=f"Successfully saved daily price changes locally to {prices_1d_path}", path=log_path)
        print(f"[OK] Saved prices_1d.csv locally to {prices_1d_path}")
        
        # Save directly to S3
        S3Manager.upload_dataframe_to_s3(price_changes_df, "price-change/prices_1d.csv", log_path)
        logger.log_event(log_category="INFO", message=f"Successfully saved daily price changes to S3", path=log_path)
        
        return price_changes_df, day_change_dict
    except Exception as e:
        logger.log_event(log_category="ERROR", message=f"Failed to calculate price changes. Error: {e}", path=log_path)
        return None, {}


# upload_dataframe_to_s3 removed - use S3Manager.upload_dataframe_to_s3() instead


def sort_gainers_losers(day_change_dict):
    """
    Sort coins by price change to get gainers and losers
    :param day_change_dict: dict mapping coin -> price_change_percent
    :return: tuple of (sorted_gainers, sorted_losers)
    """
    sorted_gainers = dict(sorted(day_change_dict.items(), key=lambda item: item[1], reverse=True))
    sorted_losers = dict(sorted(day_change_dict.items(), key=lambda item: item[1], reverse=False))
    return sorted_gainers, sorted_losers


def calculate_percentage(numerator, denominator):
    """Calculate percentage between two numbers - local use only"""
    return MathUtility.calculate_percentage(numerator, denominator)

def determine_trend(row):
    """
    Classify trend based on moving averages - wrapper for utility
    """
    return IndicatorCalculator.determine_trend(row)

def generate_market_pulse_chart(trend_df):
    """
    Generate daily market pulse visualization chart
    :param trend_df: DataFrame with trend counts
    """
    try:
        # Get last 100 rows for plotting (same as hourly)
        df_plot = trend_df.sort_index().tail(100).reset_index()
        
        if len(df_plot) == 0:
            logger.log_event(log_category="WARNING", message="No data available to plot daily market pulse", path=log_path)
            return False
        
        latest = df_plot.iloc[-1]
        total = int(latest['uptrend'] + latest['pullback'] + latest['downtrend'] + latest['reversal-down'] + latest['reversal-up'] + latest['uncategorized'])
        
        # Create output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)
        
        # Plot using matplotlib (same formatting as hourly)
        plt.figure(figsize=(12, 6))
        plt.plot(df_plot['timestamp'], df_plot['uptrend'], label=f"Uptrend - {int(latest['uptrend'])} ({MathUtility.calculate_percentage(latest['uptrend'], total)})", color='green')
        plt.plot(df_plot['timestamp'], df_plot['pullback'], label=f"Pullback - {int(latest['pullback'])} ({MathUtility.calculate_percentage(latest['pullback'], total)})", color='yellow')
        plt.plot(df_plot['timestamp'], df_plot['downtrend'], label=f"Downtrend - {int(latest['downtrend'])} ({MathUtility.calculate_percentage(latest['downtrend'], total)})", color='red')
        plt.plot(df_plot['timestamp'], df_plot['reversal-down'], label=f"Reversing down - {int(latest['reversal-down'])} ({MathUtility.calculate_percentage(latest['reversal-down'], total)})", color='orange')
        plt.plot(df_plot['timestamp'], df_plot['reversal-up'], label=f"Reversing up - {int(latest['reversal-up'])} ({MathUtility.calculate_percentage(latest['reversal-up'], total)})", color='blue')
        plt.plot(df_plot['timestamp'], df_plot['uncategorized'], label=f"Uncategorized - {int(latest['uncategorized'])} ({MathUtility.calculate_percentage(latest['uncategorized'], total)})", color='gray')
        
        plt.title('Daily Market Pulse')
        plt.xlabel('Timestamp')
        plt.ylabel('Number of Symbols')
        plt.xticks(rotation=45)
        plt.legend(loc='upper left')
        plt.tight_layout()
        
        # Save to PNG
        plt.savefig(market_pulse_image_path)
        plt.close()
        
        logger.log_event(log_category="INFO", message=f"Successfully saved daily market pulse chart to {market_pulse_image_path}", path=log_path)
        print(f"[OK] Saved market_pulse.png to {market_pulse_image_path}")
        return True
    except Exception as e:
        logger.log_event(log_category="ERROR", message=f"Failed to generate daily market pulse chart. Error: {e}", path=log_path)
        return False


def format_message(input_dict, market_cap_categories, gainers=True):
    """
    Format message for Discord with top gainers or losers
    """
    title = "===== TOP GAINERS =====" if gainers else "===== TOP LOSERS ====="
    now = datetime.now()
    
    message = f"{now}\n{title}\n"
    for coin, percent_change in input_dict.items():
        category = market_cap_categories.get(coin, 'N/A')
        message += f"{coin} - {percent_change}% [{category}]\n"
    
    return message


if __name__ == "__main__":
    print(f"Running {__file__}...")
    
    # Step 1: Get coin list
    coins = DataLoaderUtility.get_coins_from_csv(coin_data_path, log_path)
    if not coins:
        print("No coins to process. Exiting.")
        sys.exit(1)
    
    print(f"\n[OK] Retrieved {len(coins)} coins")
    
    # Step 2: Load market cap categories
    market_cap_categories = DataLoaderUtility.load_market_cap_categories(coin_data_path, log_path)
    
    # Step 3: Fetch daily data asynchronously
    print(f"\nFetching daily OHLCV data for {len(coins)} symbols...")
    start = time.time()
    raw_results = asyncio.run(BinanceDataFetcher.get_coin_data(coins, INTERVAL, LIMIT, 20, log_path))
    end = time.time()
    print(f"Fetched {len(raw_results)} symbols in {end - start:.2f} seconds.")
    
    # Step 4: Parse raw data into DataFrames
    print("Parsing data into DataFrames...")
    in_memory_data = {}
    for symbol, raw_data in raw_results.items():
        df = BinanceDataFetcher.parse_raw_data_to_dataframe(symbol, raw_data, log_path)
        if df is not None:
            in_memory_data[symbol] = df
    
    print(f"[OK] Successfully parsed {len(in_memory_data)} symbols.")
    
    # Step 5: Calculate indicators in memory
    print("Calculating indicators...")
    indicators_data = IndicatorCalculator.calculate_indicators_in_memory(in_memory_data, log_path)
    
    # Step 6: Calculate trend counts and save to S3
    print("Calculating trend counts and uploading to S3...")
    trend_df = IndicatorCalculator.calculate_trend_counts(indicators_data, log_path)
    if trend_df is not None:
        os.makedirs(output_dir, exist_ok=True)
        trend_df.to_csv(trend_1d_path)
        logger.log_event(log_category="INFO", message=f"Successfully saved trend counts locally to {trend_1d_path}", path=log_path)
        print(f"[OK] Saved coin_trend_1d.csv locally to {trend_1d_path}")
        S3Manager.upload_dataframe_to_s3(trend_df, "market-pulse/coin_trend_1d.csv", log_path)
    
    # Step 7: Calculate price changes with trend and market cap categories
    print("Calculating price changes with trend categories...")
    prices_df, day_change_dict = calculate_price_changes_with_trend(in_memory_data, indicators_data, market_cap_categories)
    
    # Step 8: Sort gainers and losers
    print("Sorting top gainers and losers...")
    top_gainers, top_losers = sort_gainers_losers(day_change_dict)
    
    # Step 9: Get top 30 gainers and losers
    top_30_gainers = dict(islice(top_gainers.items(), 30))
    top_30_losers = dict(islice(top_losers.items(), 30))
    
    # Step 10: Format messages
    top_30_gainers_formatted = format_message(top_30_gainers, market_cap_categories, gainers=True)
    top_30_losers_formatted = format_message(top_30_losers, market_cap_categories, gainers=False)
    
    full_message = top_30_gainers_formatted + "\n\n" + top_30_losers_formatted
    
    # Step 11: Generate market pulse chart
    print("Generating daily market pulse chart...")
    if generate_market_pulse_chart(trend_df):
        print("[OK] Daily market pulse chart generated successfully")
    
    # Step 12: Send to Discord
    print("Sending results to Discord...")
    try:
        send_to_discord(discord_webhook_url, message=full_message)
        logger.log_event(log_category="INFO", message="Successfully sent top gainers/losers to Discord", path=log_path)
        print("[OK] Top gainers/losers sent to Discord")
    except Exception as e:
        logger.log_event(log_category="ERROR", message=f"Failed to send to Discord. Error: {e}", path=log_path)
        print(f"[ERROR] Failed to send to Discord: {e}")
    
    print("\n[OK] Process completed successfully!")
    print(f"  - Prices saved locally: {prices_1d_path}")
    print(f"  - Prices uploaded to S3: s3://{ConfigManager.get_s3_bucket()}/price-change/prices_1d.csv")
    print(f"  - Trend counts saved locally: {trend_1d_path}")
    print(f"  - Trend counts uploaded to S3: s3://{ConfigManager.get_s3_bucket()}/market-pulse/coin_trend_1d.csv")
    print(f"  - Market pulse chart saved to: {market_pulse_image_path}")
    print(f"  - Top gainers/losers sent to Discord")
