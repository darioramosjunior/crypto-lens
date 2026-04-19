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
import config
from typing import List, Dict, Any, Optional, Tuple
import aiohttp
from utils import FileUtility, ConfigManager, DataLoaderUtility, BinanceDataFetcher, IndicatorCalculator, MathUtility, S3Manager
from validations import PriceChangeData, TrendCounts, MarketBreadthData

if sys.platform.startswith('win'):
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

import time
import pandas_ta as ta
from discord_integrator import upload_to_discord
from dotenv import load_dotenv
import boto3

load_dotenv()
os.umask(0o022)

# Ensure log and output directories exist
config.ensure_log_directory()
config.ensure_output_directory()

script_dir: str = os.path.dirname(os.path.abspath(__file__))
log_path: str = config.get_log_file_path("hourly_fetch_and_pulse")
coin_data_path: str = config.get_output_file_path("coin_data.csv")
output_dir: str = config.OUTPUT_PATH
market_pulse_image_path: str = config.get_output_file_path("market_pulse.png")
rsi_sentiment_image_path: str = config.get_output_file_path("rsi_sentiment.png")
prices_1h_path: str = config.get_output_file_path("prices_1h.csv")
trend_1h_path: str = config.get_output_file_path("coin_trend_1h.csv")

# Create log file
FileUtility.ensure_log_file_exists(log_path)

# Read webhook from environment
discord_webhook_url: Optional[str] = os.getenv("MARKET_PULSE_WEBHOOK")
if not discord_webhook_url:
    logger.log_event(log_category="WARNING", message="MARKET_PULSE_WEBHOOK not set; using fallback hard-coded webhook. Consider setting MARKET_PULSE_WEBHOOK in .env or CI secrets.", path=log_path)

# Configuration (centralized in ConfigManager)
INTERVAL: str = "1h"
LIMIT: int = 200


# Utility functions removed - now using centralized utilities from utils module
# Functions replaced: get_coins, load_market_cap_categories, fetch_ohlcv, get_coin_data, parse_raw_data_to_dataframe


def calculate_price_changes_with_trend(in_memory_data, indicators_data, market_cap_categories):
    """
    Calculate hourly price changes with trend and market cap categories
    :param in_memory_data: Dictionary {symbol: DataFrame}
    :param indicators_data: Dictionary {symbol: DataFrame} with indicators
    :param market_cap_categories: dict mapping coin -> market_cap_category
    :return: DataFrame with symbol, timestamp, close, previous_close, price_change, trend_category, market_cap_category
    """
    price_changes = []
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
            else:
                price_change = 0.0
            
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
        price_changes_df.to_csv(prices_1h_path, index=False)
        logger.log_event(log_category="INFO", message=f"Successfully saved latest price changes locally to {prices_1h_path}", path=log_path)
        print(f"[OK] Saved prices_1h.csv locally to {prices_1h_path}")
        
        # Save directly to S3
        S3Manager.upload_dataframe_to_s3(price_changes_df, "price-change/prices_1h.csv", log_path)
        logger.log_event(log_category="INFO", message=f"Successfully saved latest price changes to S3", path=log_path)
        return price_changes_df
    except Exception as e:
        logger.log_event(log_category="ERROR", message=f"Failed to calculate price changes. Error: {e}", path=log_path)
        return None


# Utility functions removed - now using centralized utilities from utils module
# Functions replaced: calculate_indicators_in_memory, determine_trend, calculate_trend_counts, calculate_percentage


def calculate_percentage(numerator, denominator):
    """Calculate percentage between two numbers - wrapper for utility"""
    return MathUtility.calculate_percentage(numerator, denominator)


def determine_trend(row):
    """
    Classify trend based on moving averages - wrapper for utility
    """
    return IndicatorCalculator.determine_trend(row)


def calculate_indicators_in_memory(in_memory_data):
    """
    Calculate indicators for all symbols and store in memory
    Extended version with hourly-specific calculations (day_change_percent)
    :param in_memory_data: Dictionary {symbol: DataFrame}
    :return: Dictionary {symbol: DataFrame} with indicators added
    """
    # Get base indicators from utility
    indicators_data = IndicatorCalculator.calculate_indicators_in_memory(in_memory_data, log_path)
    
    # Add hourly-specific calculations
    try:
        for symbol, df in indicators_data.items():
            try:
                df_sorted = df.sort_values('timestamp').copy()
                df_sorted['timestamp'] = pd.to_datetime(df_sorted['timestamp'])
                df_sorted['date_only'] = df_sorted['timestamp'].dt.date
                
                # Calculate day change percent (hourly specific)
                day_open = df_sorted.groupby('date_only')['open'].transform('first')
                df_sorted['day_change_percent'] = ((df_sorted['close'] - day_open) / day_open) * 100
                
                indicators_data[symbol] = df_sorted
                logger.log_event(log_category="INFO", message=f"Successfully calculated hourly-specific indicators for symbol {symbol}", path=log_path)
            except Exception as e:
                logger.log_event(log_category="ERROR", message=f"Failed to add hourly indicators for {symbol}: {e}", path=log_path)
        
        return indicators_data
    except Exception as e:
        logger.log_event(log_category="ERROR", message=f"Failed to process hourly indicators. Error: {e}", path=log_path)
        return {}


def generate_market_pulse_chart(trend_df):
    """
    Generate market pulse visualization chart
    :param trend_df: DataFrame with trend counts
    """
    try:
        # Get last 100 rows for plotting
        df_plot = trend_df.sort_index().tail(100).reset_index()
        
        if len(df_plot) == 0:
            logger.log_event(log_category="WARNING", message="No data available to plot market pulse", path=log_path)
            return False
        
        latest = df_plot.iloc[-1]
        total = int(latest['uptrend'] + latest['pullback'] + latest['downtrend'] + latest['reversal-down'] + latest['reversal-up'] + latest['uncategorized'])
        
        # Create output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)
        
        # Plot using matplotlib
        plt.figure(figsize=(12, 6))
        plt.plot(df_plot['timestamp'], df_plot['uptrend'], label=f"Uptrend - {int(latest['uptrend'])} ({MathUtility.calculate_percentage(latest['uptrend'], total)})", color='green')
        plt.plot(df_plot['timestamp'], df_plot['pullback'], label=f"Pullback - {int(latest['pullback'])} ({MathUtility.calculate_percentage(latest['pullback'], total)})", color='yellow')
        plt.plot(df_plot['timestamp'], df_plot['downtrend'], label=f"Downtrend - {int(latest['downtrend'])} ({MathUtility.calculate_percentage(latest['downtrend'], total)})", color='red')
        plt.plot(df_plot['timestamp'], df_plot['reversal-down'], label=f"Reversing down - {int(latest['reversal-down'])} ({MathUtility.calculate_percentage(latest['reversal-down'], total)})", color='orange')
        plt.plot(df_plot['timestamp'], df_plot['reversal-up'], label=f"Reversing up - {int(latest['reversal-up'])} ({MathUtility.calculate_percentage(latest['reversal-up'], total)})", color='blue')
        plt.plot(df_plot['timestamp'], df_plot['uncategorized'], label=f"Uncategorized - {int(latest['uncategorized'])} ({MathUtility.calculate_percentage(latest['uncategorized'], total)})", color='gray')
        
        plt.title('Hourly Market Pulse')
        plt.xlabel('Timestamp')
        plt.ylabel('Number of Symbols')
        plt.xticks(rotation=45)
        plt.legend(loc='upper left')
        plt.tight_layout()
        
        # Save to PNG
        plt.savefig(market_pulse_image_path)
        plt.close()
        
        logger.log_event(log_category="INFO", message=f"Successfully saved market pulse chart to {market_pulse_image_path}", path=log_path)
        return True
    except Exception as e:
        logger.log_event(log_category="ERROR", message=f"Failed to generate market pulse chart. Error: {e}", path=log_path)
        return False


def generate_rsi_sentiment_chart(indicators_data):
    """
    Generate RSI sentiment visualization chart
    :param indicators_data: Dictionary {symbol: DataFrame}
    """
    rsi_values = []
    
    try:
        # Extract latest RSI values for each symbol
        for symbol, df in indicators_data.items():
            if len(df) > 0 and 'rsi14' in df.columns:
                last_row = df.iloc[-1]
                if pd.notna(last_row['rsi14']):
                    rsi_values.append(last_row['rsi14'])
        
        if len(rsi_values) == 0:
            logger.log_event(log_category="WARNING", message="No RSI values available for sentiment chart", path=log_path)
            return False
        
        # Create output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)
        
        # Calculate statistics
        mean_rsi = np.mean(rsi_values)
        median_rsi = np.median(rsi_values)
        rsi_arr = np.array(rsi_values)
        rsi_skew = skew(rsi_arr)
        
        if rsi_skew < -0.5:
            sentiment = "Bullish"
        elif rsi_skew > 0.5:
            sentiment = "Bearish"
        else:
            sentiment = "Neutral"
        
        # Create histogram
        plt.figure(figsize=(12, 6))
        plt.hist(rsi_values, bins=30, color="green", edgecolor="white", alpha=0.7)
        
        plt.axvline(70, color="red", linestyle="--", linewidth=0.5, label="Overbought (70)")
        plt.axvline(30, color="green", linestyle="--", linewidth=0.5, label="Oversold (30)")
        plt.axvline(50, color="gray", linestyle="--", linewidth=0.5, label="Neutral (50)")
        plt.axvline(mean_rsi, color="blue", linestyle="-.", linewidth=1, label=f"Mean RSI = {mean_rsi:.2f}")
        plt.axvline(median_rsi, color="purple", linestyle="-.", linewidth=1, label=f"Median RSI = {median_rsi:.2f}")
        
        plt.title(f"Hourly Market Sentiment: {sentiment} (Skew = {rsi_skew:.2f})")
        plt.xlabel("RSI(14)")
        plt.ylabel("Number of Coins")
        plt.legend()
        
        plt.savefig(rsi_sentiment_image_path)
        plt.close()
        
        logger.log_event(log_category="INFO", message=f"Successfully saved RSI sentiment chart to {rsi_sentiment_image_path}", path=log_path)
        return True
    except Exception as e:
        logger.log_event(log_category="ERROR", message=f"Failed to generate RSI sentiment chart. Error: {e}", path=log_path)
        return False


# upload_dataframe_to_s3 removed - use S3Manager.upload_dataframe_to_s3() instead


if __name__ == "__main__":
    print(f"Running {__file__}...")
    
    # Step 1: Get coin list
    coins = DataLoaderUtility.get_coins_from_csv(coin_data_path, log_path)
    if not coins:
        print("No coins to process. Exiting.")
        sys.exit(1)
    
    print(f"[OK] Retrieved {len(coins)} coins")
    
    # Step 2: Load market cap categories
    market_cap_categories = DataLoaderUtility.load_market_cap_categories(coin_data_path, log_path)
    
    # Step 3: Fetch OHLCV data asynchronously and store in memory
    print(f"\nFetching OHLCV data for {len(coins)} symbols...")
    start = time.time()
    raw_results = asyncio.run(BinanceDataFetcher.get_coin_data(coins, INTERVAL, LIMIT, 20, log_path))
    end = time.time()
    print(f"Fetched {len(raw_results)} symbols in {end - start:.2f} seconds.")
    
    # Step 4: Parse raw data into DataFrames and keep in memory
    print("Parsing data into DataFrames...")
    in_memory_data = {}
    for symbol, raw_data in raw_results.items():
        df = BinanceDataFetcher.parse_raw_data_to_dataframe(symbol, raw_data, log_path)
        if df is not None:
            in_memory_data[symbol] = df
    
    print(f"Successfully parsed {len(in_memory_data)} symbols.")
    
    # Step 5: Calculate indicators in memory
    print("Calculating indicators...")
    indicators_data = IndicatorCalculator.calculate_indicators_in_memory(in_memory_data, log_path)
    
    # Step 6: Calculate price changes with trend and save directly to S3
    print("Calculating price changes with trend categories...")
    calculate_price_changes_with_trend(in_memory_data, indicators_data, market_cap_categories)
    
    # Step 7: Calculate trend counts and save directly to S3
    print("Calculating trend counts and uploading to S3...")
    trend_df = IndicatorCalculator.calculate_trend_counts(indicators_data, log_path)
    if trend_df is not None:
        os.makedirs(output_dir, exist_ok=True)
        trend_df.to_csv(trend_1h_path)
        logger.log_event(log_category="INFO", message=f"Successfully saved trend counts locally to {trend_1h_path}", path=log_path)
        print(f"[OK] Saved coin_trend_1h.csv locally to {trend_1h_path}")
        S3Manager.upload_dataframe_to_s3(trend_df, "market-pulse/coin_trend_1h.csv", log_path)
    
    # Step 8: Generate visualizations
    print("Generating market pulse chart...")
    if generate_market_pulse_chart(trend_df):
        # Step 9: Upload market pulse to Discord
        try:
            upload_to_discord(discord_webhook_url, image_path=market_pulse_image_path)
            logger.log_event(log_category="INFO", message="Successfully uploaded market pulse chart to Discord", path=log_path)
            print("[OK] Market pulse chart uploaded to Discord")
        except Exception as e:
            logger.log_event(log_category="ERROR", message=f"Failed to upload market pulse chart to Discord. Error: {e}", path=log_path)
            print(f"[ERROR] Failed to upload market pulse chart: {e}")
    
    print("Generating RSI sentiment chart...")
    if generate_rsi_sentiment_chart(indicators_data):
        # Step 10: Upload RSI sentiment to Discord
        try:
            upload_to_discord(discord_webhook_url, image_path=rsi_sentiment_image_path)
            logger.log_event(log_category="INFO", message="Successfully uploaded RSI sentiment chart to Discord", path=log_path)
            print("[OK] RSI sentiment chart uploaded to Discord")
        except Exception as e:
            logger.log_event(log_category="ERROR", message=f"Failed to upload RSI sentiment chart to Discord. Error: {e}", path=log_path)
            print(f"[ERROR] Failed to upload RSI sentiment chart: {e}")
    
    print("\n[OK] Process completed successfully!")
    print(f"  - Price changes uploaded to S3: s3://{ConfigManager.get_s3_bucket()}/price-change/prices_1h.csv")
    print(f"  - Trend counts uploaded to S3: s3://{ConfigManager.get_s3_bucket()}/market-pulse/coin_trend_1h.csv")
    print(f"  - Market pulse chart saved to: {market_pulse_image_path}")
    print(f"  - RSI sentiment chart saved to: {rsi_sentiment_image_path}")
