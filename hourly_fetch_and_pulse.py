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

# Create log file if it doesn't exist
try:
    os.makedirs(os.path.dirname(log_path), exist_ok=True)
    if not os.path.exists(log_path):
        open(log_path, 'a').close()
except Exception as e:
    print(f"[WARNING] Failed to create log file {log_path}: {e}")

# Read webhook from environment
discord_webhook_url: Optional[str] = os.getenv("MARKET_PULSE_WEBHOOK")
if not discord_webhook_url:
    logger.log_event(log_category="WARNING", message="MARKET_PULSE_WEBHOOK not set; using fallback hard-coded webhook. Consider setting MARKET_PULSE_WEBHOOK in .env or CI secrets.", path=log_path)

# AWS S3 configuration
S3_BUCKET_NAME: str = "data-portfolio-2026"
AWS_REGION: str = os.getenv("AWS_REGION", "ap-southeast-2")

BASE_URL: str = "https://fapi.binance.com/fapi/v1/klines"
INTERVAL: str = "1h"
LIMIT: int = 200
RATE_LIMIT: float = 1000 / 60  # Binance Futures limit: 1200 reqs per minute => ~20 reqs/sec safe


def get_coins() -> List[str]:
    """
    Get the list of active coins from coin_data.csv
    :return: list[]
    """
    try:
        df: pd.DataFrame = pd.read_csv(coin_data_path)
        coin_list: List[str] = df['coin'].tolist()
        logger.log_event(log_category="INFO", message="Successfully retrieved coin list from coin_data.csv", path=log_path)
        return coin_list
    except Exception as e:
        logger.log_event(log_category="ERROR", message=f"Failed to retrieve coin list. Error={e}", path=log_path)
        return []


def load_market_cap_categories() -> Dict[str, str]:
    """
    Load market cap categories from coin_data.csv
    :return: dict mapping coin -> category
    """
    try:
        df: pd.DataFrame = pd.read_csv(coin_data_path)
        df.columns = df.columns.str.strip()
        category_map: Dict[str, str] = {}
        for idx, row in df.iterrows():
            coin: str = row.get('coin', '')
            category: str = row.get('market_cap_category', '')
            category_map[coin] = category if (pd.notna(category) and str(category).strip()) else 'N/A'
        logger.log_event(log_category="INFO", message=f"Loaded market cap categories for {len(category_map)} coins", path=log_path)
        return category_map
    except Exception as e:
        logger.log_event(log_category="ERROR", message=f"Failed to load market cap categories. Error: {e}", path=log_path)
        return {}


async def fetch_ohlcv(session: aiohttp.ClientSession, symbol: str) -> Tuple[str, Optional[List[List[Any]]]]:
    """
    Fetch OHLCV data for a single symbol
    :param session: aiohttp ClientSession
    :param symbol: str - Symbol to fetch
    :return: Tuple of (symbol, data or None)
    """
    params: Dict[str, Any] = {
        "symbol": symbol,
        "interval": INTERVAL,
        "limit": LIMIT
    }
    try:
        async with session.get(BASE_URL, params=params, timeout=10) as response:
            if response.status == 200:
                data: List[List[Any]] = await response.json()
                logger.log_event(log_category="INFO", message=f"Successfully fetched OHLCV for symbol {symbol}", path=log_path)
                return symbol, data
            else:
                logger.log_event(log_category="ERROR", message=f"Failed to fetch OHLCV for symbol {symbol} with status {response.status}", path=log_path)
                return symbol, None
    except Exception as e:
        logger.log_event(log_category="ERROR", message=f"Failed to fetch OHLCV for symbol {symbol}. Error: {e}", path=log_path)
        return symbol, None


async def get_coin_data(symbols: List[str], max_concurrent: int = 20) -> Dict[str, Optional[List[List[Any]]]]:
    """
    Async fetch OHLCV data for all symbols
    :param symbols: List of symbols
    :param max_concurrent: Maximum concurrent requests
    :return: Dictionary mapping symbol to data
    """
    connector: aiohttp.TCPConnector = aiohttp.TCPConnector(limit=max_concurrent)
    timeout: aiohttp.ClientTimeout = aiohttp.ClientTimeout(total=30)
    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        sem: asyncio.Semaphore = asyncio.Semaphore(max_concurrent)

        async def sem_task(symbol: str) -> Tuple[str, Optional[List[List[Any]]]]:
            async with sem:
                return await fetch_ohlcv(session, symbol)

        tasks: List[asyncio.Task] = [sem_task(symbol) for symbol in symbols]
        results: List[Tuple[str, Optional[List[List[Any]]]]] = await asyncio.gather(*tasks)
        return dict(results)


def parse_raw_data_to_dataframe(symbol: str, raw_data: Optional[List[List[Any]]]) -> Optional[pd.DataFrame]:
    """
    Convert raw API response to DataFrame with proper formatting
    :param symbol: str - Symbol name
    :param raw_data: List of raw data or None
    :return: DataFrame or None
    """
    if not raw_data:
        return None
    try:
        df: pd.DataFrame = pd.DataFrame(raw_data, columns=[
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


def calculate_price_changes_with_trend(in_memory_data, indicators_data, market_cap_categories):
    """
    Calculate hourly price changes with trend and market cap categories
    :param in_memory_data: Dictionary {symbol: DataFrame}
    :param indicators_data: Dictionary {symbol: DataFrame} with indicators
    :param market_cap_categories: dict mapping coin -> market_cap_category
    :return: DataFrame with symbol, timestamp, close, previous_close, price_change, trend_category, market_cap_category
    """
    price_changes = []
    
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
        price_changes_df.to_csv(prices_1h_path, index=False)
        logger.log_event(log_category="INFO", message=f"Successfully saved latest price changes locally to {prices_1h_path}", path=log_path)
        print(f"[OK] Saved prices_1h.csv locally to {prices_1h_path}")
        
        # Save directly to S3
        upload_dataframe_to_s3(price_changes_df, "price-change/prices_1h.csv")
        logger.log_event(log_category="INFO", message=f"Successfully saved latest price changes to S3", path=log_path)
        return price_changes_df
    except Exception as e:
        logger.log_event(log_category="ERROR", message=f"Failed to calculate price changes. Error: {e}", path=log_path)
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
                df_sorted['date_only'] = df_sorted['timestamp'].dt.date
                df_sorted.set_index('timestamp', inplace=True)
                
                close = df_sorted['close']
                df_sorted['sma20'] = close.rolling(window=20).mean()
                df_sorted['sma50'] = close.rolling(window=50).mean()
                df_sorted['sma100'] = close.rolling(window=100).mean()
                df_sorted['rsi14'] = ta.rsi(close, length=14)
                
                day_open = df_sorted.groupby('date_only')['open'].transform('first')
                df_sorted['day_change_percent'] = ((df_sorted['close'] - day_open) / day_open) * 100
                
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
        trend_df.to_csv(trend_1h_path)
        logger.log_event(log_category="INFO", message=f"Successfully saved trend counts locally to {trend_1h_path}", path=log_path)
        print(f"[OK] Saved coin_trend_1h.csv locally to {trend_1h_path}")
        
        # Save directly to S3
        upload_dataframe_to_s3(trend_df, "market-pulse/coin_trend_1h.csv")
        logger.log_event(log_category="INFO", message=f"Successfully saved trend counts to S3", path=log_path)
        return trend_df
    except Exception as e:
        logger.log_event(log_category="ERROR", message=f"Failed to calculate trend counts. Error: {e}", path=log_path)
        return None


def calculate_percentage(numerator, denominator):
    """Calculate percentage between two numbers"""
    if denominator == 0:
        return "0.00 %"
    result = numerator / denominator
    percentage = result * 100
    return "{:.2f} %".format(percentage)


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
        plt.plot(df_plot['timestamp'], df_plot['uptrend'], label=f"Uptrend - {int(latest['uptrend'])} ({calculate_percentage(latest['uptrend'], total)})", color='green')
        plt.plot(df_plot['timestamp'], df_plot['pullback'], label=f"Pullback - {int(latest['pullback'])} ({calculate_percentage(latest['pullback'], total)})", color='yellow')
        plt.plot(df_plot['timestamp'], df_plot['downtrend'], label=f"Downtrend - {int(latest['downtrend'])} ({calculate_percentage(latest['downtrend'], total)})", color='red')
        plt.plot(df_plot['timestamp'], df_plot['reversal-down'], label=f"Reversing down - {int(latest['reversal-down'])} ({calculate_percentage(latest['reversal-down'], total)})", color='orange')
        plt.plot(df_plot['timestamp'], df_plot['reversal-up'], label=f"Reversing up - {int(latest['reversal-up'])} ({calculate_percentage(latest['reversal-up'], total)})", color='blue')
        plt.plot(df_plot['timestamp'], df_plot['uncategorized'], label=f"Uncategorized - {int(latest['uncategorized'])} ({calculate_percentage(latest['uncategorized'], total)})", color='gray')
        
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


def upload_dataframe_to_s3(dataframe, s3_key):
    """
    Upload DataFrame directly to S3 as CSV without saving locally
    :param dataframe: pandas DataFrame to upload
    :param s3_key: S3 key path (e.g., "market-pulse/coin_trend_1h.csv" or "price-change/prices_1h.csv")
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


if __name__ == "__main__":
    print(f"Running {__file__}...")
    
    # Step 1: Get coin list
    coins = get_coins()
    if not coins:
        print("No coins to process. Exiting.")
        sys.exit(1)
    
    print(f"[OK] Retrieved {len(coins)} coins")
    
    # Step 2: Load market cap categories
    market_cap_categories = load_market_cap_categories()
    
    # Step 3: Fetch OHLCV data asynchronously and store in memory
    print(f"\nFetching OHLCV data for {len(coins)} symbols...")
    start = time.time()
    raw_results = asyncio.run(get_coin_data(coins, max_concurrent=20))
    end = time.time()
    print(f"Fetched {len(raw_results)} symbols in {end - start:.2f} seconds.")
    
    # Step 4: Parse raw data into DataFrames and keep in memory
    print("Parsing data into DataFrames...")
    in_memory_data = {}
    for symbol, raw_data in raw_results.items():
        df = parse_raw_data_to_dataframe(symbol, raw_data)
        if df is not None:
            in_memory_data[symbol] = df
    
    print(f"Successfully parsed {len(in_memory_data)} symbols.")
    
    # Step 5: Calculate indicators in memory
    print("Calculating indicators...")
    indicators_data = calculate_indicators_in_memory(in_memory_data)
    
    # Step 6: Calculate price changes with trend and save directly to S3
    print("Calculating price changes with trend categories...")
    calculate_price_changes_with_trend(in_memory_data, indicators_data, market_cap_categories)
    
    # Step 7: Calculate trend counts and save directly to S3
    print("Calculating trend counts and uploading to S3...")
    trend_df = calculate_trend_counts(indicators_data)
    
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
    print(f"  - Price changes uploaded to S3: s3://{S3_BUCKET_NAME}/price-change/prices_1h.csv")
    print(f"  - Trend counts uploaded to S3: s3://{S3_BUCKET_NAME}/market-pulse/coin_trend_1h.csv")
    print(f"  - Market pulse chart saved to: {market_pulse_image_path}")
    print(f"  - RSI sentiment chart saved to: {rsi_sentiment_image_path}")
