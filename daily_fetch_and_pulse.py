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

if sys.platform.startswith('win'):
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

import aiohttp
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

script_dir = os.path.dirname(os.path.abspath(__file__))
log_path = config.get_log_file_path("daily_fetch_and_pulse")
coin_data_path = config.get_output_file_path("coin_data.csv")
output_dir = config.OUTPUT_PATH
prices_1d_path = config.get_output_file_path("prices_1d.csv")
trend_1d_path = config.get_output_file_path("coin_trend_1d.csv")
market_pulse_image_path = config.get_output_file_path("market_pulse_daily.png")

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

# Discord webhook
discord_webhook_url = os.getenv("DAY_CHANGE_WEBHOOK")
if not discord_webhook_url:
    logger.log_event(log_category="WARNING", message="DAY_CHANGE_WEBHOOK not set; using fallback hard-coded webhook. Consider setting DAY_CHANGE_WEBHOOK in .env or CI secrets.", path=log_path)

BASE_URL = "https://fapi.binance.com/fapi/v1/klines"
INTERVAL = "1d"
LIMIT = 200
RATE_LIMIT = 1000 / 60


def get_coins():
    """
    Get the list of active coins from coin_data.csv
    :return: list[]
    """
    try:
        df = pd.read_csv(coin_data_path)
        coin_list = df['coin'].tolist()
        logger.log_event(log_category="INFO", message="Successfully retrieved coin list from coin_data.csv", path=log_path)
        return coin_list
    except Exception as e:
        logger.log_event(log_category="ERROR", message=f"Failed to retrieve coin list. Error={e}", path=log_path)
        return []


def load_market_cap_categories():
    """
    Load market cap categories from coin_data.csv
    :return: dict mapping coin -> category
    """
    try:
        df = pd.read_csv(coin_data_path)
        df.columns = df.columns.str.strip()
        category_map = {}
        for idx, row in df.iterrows():
            coin = row.get('coin', '')
            category = row.get('market_cap_category', '')
            category_map[coin] = category if (pd.notna(category) and str(category).strip()) else 'N/A'
        logger.log_event(log_category="INFO", message=f"Loaded market cap categories for {len(category_map)} coins", path=log_path)
        return category_map
    except Exception as e:
        logger.log_event(log_category="ERROR", message=f"Failed to load market cap categories. Error: {e}", path=log_path)
        return {}


async def fetch_ohlcv(session, symbol):
    """
    Fetch OHLCV data for a single symbol
    """
    params = {
        "symbol": symbol,
        "interval": INTERVAL,
        "limit": LIMIT
    }
    try:
        async with session.get(BASE_URL, params=params, timeout=10) as response:
            if response.status == 200:
                data = await response.json()
                logger.log_event(log_category="INFO", message=f"Successfully fetched OHLCV for symbol {symbol}", path=log_path)
                return symbol, data
            else:
                logger.log_event(log_category="ERROR", message=f"Failed to fetch OHLCV for symbol {symbol} with status {response.status}", path=log_path)
                return symbol, None
    except Exception as e:
        logger.log_event(log_category="ERROR", message=f"Failed to fetch OHLCV for symbol {symbol}. Error: {e}", path=log_path)
        return symbol, None


async def get_coin_data(symbols, max_concurrent=20):
    """
    Async fetch OHLCV data for all symbols
    """
    connector = aiohttp.TCPConnector(limit=max_concurrent)
    timeout = aiohttp.ClientTimeout(total=30)
    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        sem = asyncio.Semaphore(max_concurrent)

        async def sem_task(symbol):
            async with sem:
                return await fetch_ohlcv(session, symbol)

        tasks = [sem_task(symbol) for symbol in symbols]
        results = await asyncio.gather(*tasks)
        return dict(results)


def parse_raw_data_to_dataframe(symbol, raw_data):
    """
    Convert raw API response to DataFrame with proper formatting
    """
    if not raw_data:
        return None
    try:
        df = pd.DataFrame(raw_data, columns=[
            "timestamp", "open", "high", "low", "close", "volume",
            "close_time", "quote_asset_volume", "number_of_trades",
            "taker_buy_base_volume", "taker_buy_quote_volume", "ignore"
        ])
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")
        df['timestamp'] = df['timestamp'].dt.tz_localize('UTC')
        df['timestamp'] = df['timestamp'].dt.tz_localize(None)
        
        # Convert numeric columns
        df[['open', 'high', 'low', 'close', 'volume']] = df[['open', 'high', 'low', 'close', 'volume']].astype(float)
        
        return df
    except Exception as e:
        logger.log_event(log_category="ERROR", message=f"Failed to parse data for {symbol}. Error: {e}", path=log_path)
        return None


def calculate_indicators_in_memory(in_memory_data):
    """
    Calculate indicators for all symbols and store in memory
    :param in_memory_data: Dictionary {symbol: DataFrame}
    :return: Dictionary {symbol: DataFrame} with indicators added
    """
    indicators_data = {}
    
    try:
        for symbol, df in in_memory_data.items():
            try:
                df_sorted = df.sort_values('timestamp').copy()
                df_sorted['timestamp'] = pd.to_datetime(df_sorted['timestamp'])
                df_sorted.set_index('timestamp', inplace=True)
                
                close = df_sorted['close']
                df_sorted['sma20'] = close.rolling(window=20).mean()
                df_sorted['sma50'] = close.rolling(window=50).mean()
                df_sorted['sma100'] = close.rolling(window=100).mean()
                df_sorted['rsi14'] = ta.rsi(close, length=14)
                
                volume = df_sorted['volume']
                df_sorted['volume_sma20'] = volume.rolling(window=20).mean()
                
                df_sorted.reset_index(inplace=True)
                indicators_data[symbol] = df_sorted
                logger.log_event(log_category="INFO", message=f"Successfully calculated indicators for symbol {symbol}", path=log_path)
            except Exception as e:
                logger.log_event(log_category="ERROR", message=f"Failed to calculate indicators for symbol {symbol}. Error: {e}", path=log_path)
        
        return indicators_data
    except Exception as e:
        logger.log_event(log_category="ERROR", message=f"Failed to process indicators. Error: {e}", path=log_path)
        return {}


def determine_trend(row):
    """
    Classify trend based on moving averages
    """
    ma20, ma50, ma100 = row['sma20'], row['sma50'], row['sma100']
    
    # Skip rows with NaN values
    if pd.isna(ma20) or pd.isna(ma50) or pd.isna(ma100):
        return 'uncategorized'
    
    if ma20 > ma50 > ma100:
        return 'uptrend'
    elif ma20 < ma50 > ma100:
        return 'pullback'
    elif ma20 < ma50 < ma100:
        return 'downtrend'
    elif ma20 < ma100 < ma50:
        return 'reversal-down'
    elif ma20 > ma100 > ma50:
        return 'reversal-up'
    else:
        return 'uncategorized'


def calculate_trend_counts(indicators_data):
    """
    Calculate trend counts per timestamp across all symbols and save directly to S3
    :param indicators_data: Dictionary {symbol: DataFrame}
    :return: DataFrame with trend counts per timestamp
    """
    trend_counter = defaultdict(lambda: {
        'uptrend': 0,
        'pullback': 0,
        'downtrend': 0,
        'reversal-up': 0,
        'reversal-down': 0,
        'uncategorized': 0
    })
    
    try:
        for symbol, df in indicators_data.items():
            df['trend'] = df.apply(determine_trend, axis=1)
            
            for idx, row in df.iterrows():
                ts, trend = row['timestamp'], row['trend']
                trend_counter[ts][trend] += 1
        
        # Convert to DataFrame
        trend_df = pd.DataFrame.from_dict(trend_counter, orient='index')
        trend_df.index.name = 'timestamp'
        trend_df = trend_df.sort_index()
        
        # Ensure all trend count columns are int type
        for col in trend_df.columns:
            trend_df[col] = trend_df[col].astype('int64')
        
        # Save locally
        os.makedirs(output_dir, exist_ok=True)
        trend_df.to_csv(trend_1d_path)
        logger.log_event(log_category="INFO", message=f"Successfully saved trend counts locally to {trend_1d_path}", path=log_path)
        print(f"[OK] Saved coin_trend_1d.csv locally to {trend_1d_path}")
        
        # Save directly to S3
        upload_dataframe_to_s3(trend_df, "market-pulse/coin_trend_1d.csv")
        logger.log_event(log_category="INFO", message=f"Successfully saved trend counts to S3", path=log_path)
        return trend_df
    except Exception as e:
        logger.log_event(log_category="ERROR", message=f"Failed to calculate trend counts. Error: {e}", path=log_path)
        return None


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
            
            price_changes.append({
                'symbol': symbol,
                'timestamp': latest_timestamp,
                'close': latest_close,
                'previous_close': previous_close,
                'price_change': price_change,
                'trend_category': trend_category,
                'market_cap_category': market_cap_category
            })
        
        price_changes_df = pd.DataFrame(price_changes)
        
        # Ensure numeric columns are float type
        for col in ['close', 'previous_close', 'price_change']:
            if col in price_changes_df.columns:
                price_changes_df[col] = pd.to_numeric(price_changes_df[col], errors='coerce')
        
        # Save locally
        os.makedirs(output_dir, exist_ok=True)
        price_changes_df.to_csv(prices_1d_path, index=False)
        logger.log_event(log_category="INFO", message=f"Successfully saved daily price changes locally to {prices_1d_path}", path=log_path)
        print(f"[OK] Saved prices_1d.csv locally to {prices_1d_path}")
        
        # Save directly to S3
        upload_dataframe_to_s3(price_changes_df, "price-change/prices_1d.csv")
        logger.log_event(log_category="INFO", message=f"Successfully saved daily price changes to S3", path=log_path)
        
        return price_changes_df, day_change_dict
    except Exception as e:
        logger.log_event(log_category="ERROR", message=f"Failed to calculate price changes. Error: {e}", path=log_path)
        return None, {}


def upload_dataframe_to_s3(dataframe, s3_key):
    """
    Upload DataFrame directly to S3 as CSV without saving locally
    :param dataframe: pandas DataFrame to upload
    :param s3_key: S3 key path
    """
    try:
        # Initialize S3 client
        s3_client = boto3.client('s3', region_name=AWS_REGION)
        
        # Convert DataFrame to CSV in memory
        csv_buffer = BytesIO()
        dataframe.to_csv(csv_buffer, index=True)
        csv_buffer.seek(0)
        
        # Upload to S3
        s3_client.upload_fileobj(csv_buffer, S3_BUCKET_NAME, s3_key)
        logger.log_event(log_category="INFO", message=f"Successfully uploaded {s3_key} to S3 bucket {S3_BUCKET_NAME}", path=log_path)
        print(f"[OK] Uploaded {s3_key} to S3")
        return True
    
    except Exception as e:
        logger.log_event(log_category="ERROR", message=f"Failed to upload {s3_key} to S3. Error: {e}", path=log_path)
        print(f"[ERROR] Failed to upload {s3_key} to S3: {e}")
        return False


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
    """Calculate percentage between two numbers"""
    if denominator == 0:
        return "0.00 %"
    result = numerator / denominator
    percentage = result * 100
    return "{:.2f} %".format(percentage)


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
        plt.plot(df_plot['timestamp'], df_plot['uptrend'], label=f"Uptrend - {int(latest['uptrend'])} ({calculate_percentage(latest['uptrend'], total)})", color='green')
        plt.plot(df_plot['timestamp'], df_plot['pullback'], label=f"Pullback - {int(latest['pullback'])} ({calculate_percentage(latest['pullback'], total)})", color='yellow')
        plt.plot(df_plot['timestamp'], df_plot['downtrend'], label=f"Downtrend - {int(latest['downtrend'])} ({calculate_percentage(latest['downtrend'], total)})", color='red')
        plt.plot(df_plot['timestamp'], df_plot['reversal-down'], label=f"Reversing down - {int(latest['reversal-down'])} ({calculate_percentage(latest['reversal-down'], total)})", color='orange')
        plt.plot(df_plot['timestamp'], df_plot['reversal-up'], label=f"Reversing up - {int(latest['reversal-up'])} ({calculate_percentage(latest['reversal-up'], total)})", color='blue')
        plt.plot(df_plot['timestamp'], df_plot['uncategorized'], label=f"Uncategorized - {int(latest['uncategorized'])} ({calculate_percentage(latest['uncategorized'], total)})", color='gray')
        
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
    coins = get_coins()
    if not coins:
        print("No coins to process. Exiting.")
        sys.exit(1)
    
    print(f"\n[OK] Retrieved {len(coins)} coins")
    
    # Step 2: Load market cap categories
    market_cap_categories = load_market_cap_categories()
    
    # Step 3: Fetch daily data asynchronously
    print(f"\nFetching daily OHLCV data for {len(coins)} symbols...")
    start = time.time()
    raw_results = asyncio.run(get_coin_data(coins, max_concurrent=20))
    end = time.time()
    print(f"Fetched {len(raw_results)} symbols in {end - start:.2f} seconds.")
    
    # Step 4: Parse raw data into DataFrames
    print("Parsing data into DataFrames...")
    in_memory_data = {}
    for symbol, raw_data in raw_results.items():
        df = parse_raw_data_to_dataframe(symbol, raw_data)
        if df is not None:
            in_memory_data[symbol] = df
    
    print(f"[OK] Successfully parsed {len(in_memory_data)} symbols.")
    
    # Step 5: Calculate indicators in memory
    print("Calculating indicators...")
    indicators_data = calculate_indicators_in_memory(in_memory_data)
    
    # Step 6: Calculate trend counts and save to S3
    print("Calculating trend counts and uploading to S3...")
    trend_df = calculate_trend_counts(indicators_data)
    
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
    print(f"  - Prices uploaded to S3: s3://{S3_BUCKET_NAME}/price-change/prices_1d.csv")
    print(f"  - Trend counts saved locally: {trend_1d_path}")
    print(f"  - Trend counts uploaded to S3: s3://{S3_BUCKET_NAME}/market-pulse/coin_trend_1d.csv")
    print(f"  - Market pulse chart saved to: {market_pulse_image_path}")
    print(f"  - Top gainers/losers sent to Discord")
