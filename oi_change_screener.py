import os
import logger
import sys
import asyncio
import time
import json
import pandas as pd
from datetime import datetime, timedelta
from io import BytesIO
import config
from typing import List, Dict, Any, Optional, Set

if sys.platform.startswith('win'):
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

import aiohttp
import boto3
from discord_integrator import send_to_discord
from dotenv import load_dotenv

load_dotenv()
os.umask(0o022)

# Ensure log and output directories exist
config.ensure_log_directory()
config.ensure_output_directory()

script_dir: str = os.path.dirname(os.path.abspath(__file__))
log_path: str = config.get_log_file_path("oi_change_screener")
coin_data_csv: str = config.get_output_file_path("coin_data.csv")
output_dir: str = config.OUTPUT_PATH
previous_top20_path: str = os.path.join(script_dir, "oi_change_top20_previous.json")
prices_csv: str = config.get_output_file_path("prices_1h.csv")
oi_changes_csv: str = config.get_output_file_path("oi_changes_1h.csv")

# Create log file if it doesn't exist
try:
    os.makedirs(os.path.dirname(log_path), exist_ok=True)
    if not os.path.exists(log_path):
        open(log_path, 'a').close()
except Exception as e:
    print(f"[WARNING] Failed to create log file {log_path}: {e}")

# AWS S3 configuration
S3_BUCKET_NAME: str = "data-portfolio-2026"
AWS_REGION: str = os.getenv("AWS_REGION", "ap-southeast-2")

BASE_URL: str = "https://fapi.binance.com/fapi/v1"
CURRENT_OI_ENDPOINT: str = f"{BASE_URL}/openInterest"
HISTORICAL_OI_ENDPOINT: str = f"https://fapi.binance.com/futures/data/openInterestHist"
RATE_LIMIT: float = 1000 / 60  # Binance Futures limit: 1200 reqs per minute => ~20 reqs/sec safe

# Read webhook from environment
webhook_url: Optional[str] = os.getenv("OI_CHANGE_WEBHOOK")

if not webhook_url:
    logger.log_event(
        log_category="WARNING",
        message="OI_CHANGE_WEBHOOK not set; using fallback hard-coded webhook. Consider setting OI_CHANGE_WEBHOOK in .env or CI secrets.",
        path=log_path
    )


def get_coins() -> List[str]:
    """
    Get the list of active coins from coin_data.csv
    :return: list[]
    """
    try:
        df: pd.DataFrame = pd.read_csv(coin_data_csv)
        df.columns = df.columns.str.strip()
        coin_list: List[str] = df['coin'].tolist()
        logger.log_event(log_category="INFO", message=f"Successfully retrieved {len(coin_list)} coins from coin_data.csv", path=log_path)
        return coin_list
    except Exception as e:
        logger.log_event(log_category="ERROR", message=f"Failed to retrieve coin list from coin_data.csv. Error={e}", path=log_path)
        return []


def get_previous_top20() -> Set[str]:
    """
    Get the previous top 20 results from file
    :return: set of symbol names, or empty set if file doesn't exist
    """
    try:
        if os.path.exists(previous_top20_path):
            with open(previous_top20_path, 'r') as file:
                data: Dict[str, Any] = json.load(file)
                previous_symbols: Set[str] = set(data.get("symbols", []))
                logger.log_event(log_category="INFO", message=f"Retrieved {len(previous_symbols)} previous top 20 symbols", path=log_path)
                return previous_symbols
        else:
            logger.log_event(log_category="INFO", message="No previous top 20 file found. Starting fresh.", path=log_path)
            return set()
    except Exception as e:
        logger.log_event(log_category="ERROR", message=f"Failed to retrieve previous top 20. Error={e}", path=log_path)
        return set()


def load_category_data(coin_data_csv_path: str) -> Dict[str, str]:
    """
    Load market cap category information from coin_data.csv
    Returns dict mapping coin -> market_cap_category (defaults to 'N/A' if empty)
    :param coin_data_csv_path: Path to coin data CSV
    :return: Dictionary mapping coin to category
    """
    try:
        if os.path.exists(coin_data_csv_path):
            df: pd.DataFrame = pd.read_csv(coin_data_csv_path)
            df.columns = df.columns.str.strip()
            category_map: Dict[str, str] = {}
            for idx, row in df.iterrows():
                coin: str = row.get('coin', '')
                category: str = row.get('market_cap_category', '')
                # Use 'N/A' if category is empty or NaN
                category_map[coin] = category if (pd.notna(category) and str(category).strip()) else 'N/A'
            logger.log_event(log_category="INFO", message=f"Loaded market cap categories for {len(category_map)} coins", path=log_path)
            return category_map
        else:
            logger.log_event(log_category="WARNING", message=f"Coin data CSV not found at {coin_data_csv_path}. Using N/A for all categories.", path=log_path)
            return {}
    except Exception as e:
        logger.log_event(log_category="ERROR", message=f"Error loading market cap category data: {e}", path=log_path)
        return {}


def load_market_cap_data(coin_data_csv_path: str) -> Dict[str, Optional[float]]:
    """
    Load market cap values from coin_data.csv
    Returns dict mapping coin -> market_cap_value (float or None if empty)
    :param coin_data_csv_path: Path to coin data CSV
    :return: Dictionary mapping coin to market cap value
    """
    try:
        if os.path.exists(coin_data_csv_path):
            df: pd.DataFrame = pd.read_csv(coin_data_csv_path)
            df.columns = df.columns.str.strip()
            market_cap_map: Dict[str, Optional[float]] = {}
            for idx, row in df.iterrows():
                coin: str = row.get('coin', '')
                market_cap: Any = row.get('market_cap_value', '')
                # Convert to float if not empty, otherwise None
                if pd.notna(market_cap) and str(market_cap).strip():
                    try:
                        market_cap_map[coin] = float(market_cap)
                    except (ValueError, TypeError):
                        market_cap_map[coin] = None
                else:
                    market_cap_map[coin] = None
            logger.log_event(log_category="INFO", message=f"Loaded market cap values for {len([m for m in market_cap_map.values() if m is not None])} coins", path=log_path)
            return market_cap_map
        else:
            logger.log_event(log_category="WARNING", message=f"Coin data CSV not found at {coin_data_csv_path}. Using None for all market cap values.", path=log_path)
            return {}
    except Exception as e:
        logger.log_event(log_category="ERROR", message=f"Error loading market cap value data: {e}", path=log_path)
        return {}


def format_market_cap(market_cap_value: Optional[float]) -> str:
    """
    Format market cap value to human-readable format (M, B, T, etc.)
    e.g., 1500000000 -> "1.50B", 50000000 -> "50.00M"
    Returns "N/A" if market_cap_value is None or 0
    :param market_cap_value: Market cap value in USD
    :return: Formatted string representation
    """
    if market_cap_value is None or market_cap_value == 0:
        return "N/A"
    
    abs_value: float = abs(market_cap_value)
    
    if abs_value >= 1e12:
        return f"${market_cap_value / 1e12:.2f}T"
    elif abs_value >= 1e9:
        return f"${market_cap_value / 1e9:.2f}B"
    elif abs_value >= 1e6:
        return f"${market_cap_value / 1e6:.2f}M"
    elif abs_value >= 1e3:
        return f"${market_cap_value / 1e3:.2f}K"
    else:
        return f"${market_cap_value:.2f}"


def save_current_top20(top_oi_changes: List[Dict[str, Any]]) -> None:
    """
    Save the current top 20 results to file for next run
    :param top_oi_changes: list of current top OI changes
    :return: None
    """
    try:
        symbols_data: List[Dict[str, Any]] = [
            {
                "symbol": item["symbol"],
                "category": item.get("category", "N/A"),
                "market_cap": item.get("market_cap")
            }
            for item in top_oi_changes[:20]
        ]
        data: Dict[str, Any] = {
            "symbols": [item["symbol"] for item in symbols_data],
            "symbols_with_category": symbols_data,
            "timestamp": datetime.now().isoformat()
        }
        with open(previous_top20_path, 'w') as file:
            json.dump(data, file, indent=2)
        logger.log_event(log_category="INFO", message=f"Saved current top 20 to {previous_top20_path}", path=log_path)
    except Exception as e:
        logger.log_event(log_category="ERROR", message=f"Failed to save current top 20. Error={e}", path=log_path)


def save_oi_changes_to_csv(oi_changes: List[Dict[str, Any]]) -> None:
    """
    Save all OI changes to oi_changes_1h.csv locally and upload to S3
    :param oi_changes: list of all OI change records
    :return: None
    """
    try:
        timestamp: str = datetime.now().isoformat()
        records: List[Dict[str, Any]] = []
        for item in oi_changes:
            market_cap: Optional[float] = item.get("market_cap")
            records.append({
                "symbol": item["symbol"],
                "timestamp": timestamp,
                "open_interest": item["current_oi"],
                "previous_open_interest": item["previous_oi"],
                "oi_change": item["change_percentage"],
                "market_cap_category": item.get("category", "N/A"),
                "market_cap": market_cap if market_cap is not None else "N/A"
            })
        
        df: pd.DataFrame = pd.DataFrame(records)
        
        # Ensure numeric columns are float type
        for col in ['open_interest', 'previous_open_interest', 'oi_change']:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # Save locally
        df.to_csv(oi_changes_csv, index=False)
        logger.log_event(log_category="INFO", message=f"Saved {len(records)} OI changes to {oi_changes_csv}", path=log_path)
        print(f"[OK] Saved oi_changes_1h.csv locally to {oi_changes_csv}")
        
        # Upload to S3
        upload_dataframe_to_s3(df, "oi-change/oi_changes_1h.csv")
    except Exception as e:
        logger.log_event(log_category="ERROR", message=f"Failed to save OI changes to CSV. Error={e}", path=log_path)


def get_hourly_price_data(symbols: List[str]) -> Dict[str, Optional[float]]:
    """
    Get price change data from prices_1h.csv
    :param symbols: list of symbols to fetch price data for
    :return: dict with symbol -> {price_change_percentage}
    """
    price_data: Dict[str, Optional[float]] = {}
    
    try:
        if not os.path.exists(prices_csv):
            logger.log_event(
                log_category="WARNING",
                message=f"Prices CSV not found at {prices_csv}",
                path=log_path
            )
            return {}
        
        df = pd.read_csv(prices_csv)
        df.columns = df.columns.str.strip()
        
        for symbol in symbols:
            try:
                # Find the row for this symbol
                symbol_data = df[df['symbol'] == symbol]
                
                if symbol_data.empty:
                    logger.log_event(
                        log_category="WARNING",
                        message=f"No price data found for {symbol} in prices_1h.csv",
                        path=log_path
                    )
                    continue
                
                price_change = float(symbol_data.iloc[0]['price_change'])
                
                price_data[symbol] = {
                    "price_change_percentage": price_change
                }
                
                logger.log_event(
                    log_category="INFO",
                    message=f"Successfully retrieved price change for {symbol}: {price_change:.2f}%",
                    path=log_path
                )
            except Exception as e:
                logger.log_event(
                    log_category="ERROR",
                    message=f"Failed to get price data for {symbol}. Error: {e}",
                    path=log_path
                )
                continue
    except Exception as e:
        logger.log_event(
            log_category="ERROR",
            message=f"Failed to read prices CSV. Error: {e}",
            path=log_path
        )
    
    return price_data


def upload_dataframe_to_s3(dataframe, s3_key):
    """
    Upload DataFrame directly to S3 as CSV
    :param dataframe: pandas DataFrame to upload
    :param s3_key: S3 key path (e.g., "oi-change/oi_changes_1h.csv")
    """
    try:
        # Initialize S3 client
        s3_client = boto3.client('s3', region_name=AWS_REGION)
        
        # Convert DataFrame to CSV in memory
        csv_buffer = BytesIO()
        dataframe.to_csv(csv_buffer, index=False)
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


async def fetch_current_oi(session, symbol):
    """
    Fetch current Open Interest for a symbol
    """
    params = {"symbol": symbol}
    try:
        async with session.get(CURRENT_OI_ENDPOINT, params=params, timeout=10) as response:
            if response.status == 200:
                data = await response.json()
                oi_value = float(data.get("openInterest", 0))
                logger.log_event(
                    log_category="INFO",
                    message=f"Successfully fetched current OI for {symbol}",
                    path=log_path
                )
                return symbol, oi_value
            else:
                logger.log_event(
                    log_category="ERROR",
                    message=f"Failed to fetch current OI for {symbol} with status {response.status}",
                    path=log_path
                )
                return symbol, None
    except Exception as e:
        logger.log_event(
            log_category="ERROR",
            message=f"Failed to fetch current OI for {symbol}. Error: {e}",
            path=log_path
        )
        return symbol, None


async def fetch_historical_oi(session, symbol, period="1h", limit=2):
    """
    Fetch historical Open Interest for a symbol
    period: e.g., "1h", "4h", "1d"
    limit: number of candles to fetch (2 = current and previous)
    """
    # Calculate startTime: 3 hours ago in milliseconds (to get data points)
    now_ms = int(time.time() * 1000)
    start_time_ms = now_ms - (3 * 60 * 60 * 1000)  # 3 hours ago
    
    params = {
        "symbol": symbol,
        "period": period,
        "limit": limit,
        "startTime": start_time_ms
    }
    try:
        async with session.get(HISTORICAL_OI_ENDPOINT, params=params, timeout=10) as response:
            if response.status == 200:
                data = await response.json()
                logger.log_event(
                    log_category="INFO",
                    message=f"Successfully fetched historical OI for {symbol}",
                    path=log_path
                )
                return symbol, data
            elif response.status == 404:
                # 404 means the symbol doesn't have historical OI data (common for new coins)
                logger.log_event(
                    log_category="WARNING",
                    message=f"No historical OI data available for {symbol} (symbol may be new or unsupported)",
                    path=log_path
                )
                return symbol, None
            else:
                logger.log_event(
                    log_category="ERROR",
                    message=f"Failed to fetch historical OI for {symbol} with status {response.status}",
                    path=log_path
                )
                return symbol, None
    except Exception as e:
        logger.log_event(
            log_category="ERROR",
            message=f"Failed to fetch historical OI for {symbol}. Error: {e}",
            path=log_path
        )
        return symbol, None


async def get_oi_data(symbols, max_concurrent=20):
    """
    Fetch OI data for all symbols concurrently
    """
    connector = aiohttp.TCPConnector(limit=max_concurrent)
    timeout = aiohttp.ClientTimeout(total=30)
    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        sem = asyncio.Semaphore(max_concurrent)

        async def sem_task_current(symbol):
            async with sem:
                return await fetch_current_oi(session, symbol)

        async def sem_task_historical(symbol):
            async with sem:
                return await fetch_historical_oi(session, symbol)

        # Fetch current and historical OI
        current_tasks = [sem_task_current(symbol) for symbol in symbols]
        historical_tasks = [sem_task_historical(symbol) for symbol in symbols]

        current_results = await asyncio.gather(*current_tasks)
        historical_results = await asyncio.gather(*historical_tasks)

        return dict(current_results), dict(historical_results)


def calculate_oi_change_percentage(current_results, historical_results, price_data, category_data=None, market_cap_data=None, trend_category_data=None):
    """
    Calculate OI change percentage from previous hour
    Historical results come in chronological order: [oldest, newest]
    We want the oldest value (1 hour ago)
    """
    if category_data is None:
        category_data = {}
    if market_cap_data is None:
        market_cap_data = {}
    if trend_category_data is None:
        trend_category_data = {}
    
    oi_changes = []

    for symbol in current_results.keys():
        current_oi = current_results.get(symbol)
        historical_oi_list = historical_results.get(symbol)

        # Skip if data is missing
        if current_oi is None or historical_oi_list is None or len(historical_oi_list) < 2:
            continue

        try:
            # Get the previous OI (oldest in the list for 1 hour ago)
            previous_oi = float(historical_oi_list[0].get("sumOpenInterest", 0))

            # Avoid division by zero
            if previous_oi == 0:
                continue

            # Calculate percentage change for OI
            change_percentage = ((current_oi - previous_oi) / previous_oi) * 100

            # Get price change from prices_1h.csv if available
            price_change_percentage = None
            if symbol in price_data:
                price_change_percentage = price_data[symbol].get("price_change_percentage")

            # Get category (default to N/A if not found)
            category = category_data.get(symbol, "N/A")
            
            # Get market cap (default to None if not found)
            market_cap = market_cap_data.get(symbol)

            oi_changes.append({
                "symbol": symbol,
                "current_oi": current_oi,
                "previous_oi": previous_oi,
                "change_percentage": change_percentage,
                "price_change_percentage": price_change_percentage,
                "category": category,
                "market_cap": market_cap
            })
        except (KeyError, ValueError, TypeError) as e:
            logger.log_event(
                log_category="WARNING",
                message=f"Failed to calculate OI change for {symbol}. Error: {e}",
                path=log_path
            )
            continue

    # Sort by change percentage (highest first)
    oi_changes.sort(key=lambda x: x["change_percentage"], reverse=True)

    return oi_changes


def format_discord_message(top_oi_changes, previous_top20_symbols, limit=20):
    """
    Format the top OI changes for Discord message, highlighting new coins
    """
    message = "🔥 **Top 20 Coins by Open Interest Change (Last Hour)** 🔥\n\n"
    message += "```\n"
    message += f"{'SYMBOL':<12} {'OI CHG %':<10} {'PRICE CHG %':<15} {'CATEGORY':<12} {'MARKET CAP':<13}\n"
    message += "-" * 80 + "\n"

    for i, item in enumerate(top_oi_changes[:limit], 1):
        symbol = item["symbol"]
        change_pct = item["change_percentage"]
        price_change = item["price_change_percentage"]
        category = item.get("category", "N/A")
        market_cap = item.get("market_cap")
        
        # Check if this symbol is new to the top 20
        is_new = symbol not in previous_top20_symbols

        # Format with appropriate precision
        if is_new:
            marker = "🆕"  # New marker
        else:
            marker = "  "  # Regular spacing

        # Format price change with emoji indicator (single space between emoji and value)
        if price_change is not None:
            if price_change > 0:
                price_str = f"📈 {price_change:.2f}%"
            elif price_change < 0:
                price_str = f"📉 {price_change:.2f}%"
            else:
                price_str = f"  {price_change:.2f}%"
        else:
            price_str = "N/A"
        
        # Right-align price string
        price_str = f"{price_str:>15}"

        # Truncate category if needed
        cat_str = category[:10] if len(category) > 10 else category
        
        # Format market cap
        market_cap_str = format_market_cap(market_cap)

        message += f"{marker} {symbol:<9} {change_pct:>8.2f}%  {price_str}  {cat_str:<12} {market_cap_str:>11}\n"

    message += "```\n"
    
    # Add legend for new coins
    if any(item["symbol"] not in previous_top20_symbols for item in top_oi_changes[:limit]):
        message += "\n🆕 = Newly entered top 20\n"
    
    message += f"\n📊 Updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}"

    return message


if __name__ == "__main__":
    print(f"Running {__file__}...")

    # Load market cap category and market cap values from coin_data.csv
    category_data = load_category_data(coin_data_csv)
    print(f"Loaded market cap categories for {len(category_data)} coins")
    
    market_cap_data = load_market_cap_data(coin_data_csv)
    print(f"Loaded market cap values for {len([m for m in market_cap_data.values() if m is not None])} coins")

    # Get coin list
    coins = get_coins()
    if not coins:
        print("No coins found. Exiting.")
        sys.exit(1)

    print(f"Found {len(coins)} coins. Fetching OI data...")

    # Fetch OI data
    start = time.time()
    current_oi, historical_oi = asyncio.run(get_oi_data(coins, max_concurrent=20))
    end = time.time()

    print(f"Fetched OI data for {len(current_oi)} symbols in {end - start:.2f} seconds.")

    # Get top 20 symbols first (before calculating price changes)
    oi_changes_initial = calculate_oi_change_percentage(current_oi, historical_oi, {}, category_data, market_cap_data, {})

    if not oi_changes_initial:
        print("No OI change data available.")
        logger.log_event(log_category="WARNING", message="No OI change data calculated", path=log_path)
        sys.exit(0)

    # Get top 20 symbols
    top_20_symbols = [item["symbol"] for item in oi_changes_initial[:20]]
    
    # Fetch price change data for top 20 symbols from prices_1h.csv
    print(f"Fetching price change data for top 20 symbols...")
    price_data = get_hourly_price_data(top_20_symbols)
    
    # Recalculate OI changes with price data
    oi_changes_with_prices = calculate_oi_change_percentage(current_oi, historical_oi, price_data, category_data, market_cap_data, {})
    top_20_oi_changes = oi_changes_with_prices[:20]

    print(f"\nTop 20 coins by OI change:")
    for i, item in enumerate(top_20_oi_changes, 1):
        if item['price_change_percentage'] is not None:
            if item['price_change_percentage'] > 0:
                price_str = f"📈 {item['price_change_percentage']:.2f}%"
            elif item['price_change_percentage'] < 0:
                price_str = f"📉 {item['price_change_percentage']:.2f}%"
            else:
                price_str = f"  {item['price_change_percentage']:.2f}%"
        else:
            price_str = "N/A"
        category = item.get("category", "N/A")
        market_cap_str = format_market_cap(item.get("market_cap"))
        print(f"{i:2}. {item['symbol']:<15} {item['change_percentage']:>8.2f}% | Price: {price_str} | Category: {category} | Market Cap: {market_cap_str}")

    # Save all OI changes to CSV
    save_oi_changes_to_csv(oi_changes_with_prices)

    # Get previous top 20 and identify new coins
    previous_top20_symbols = get_previous_top20()
    new_coins = [item["symbol"] for item in top_20_oi_changes if item["symbol"] not in previous_top20_symbols]
    
    if new_coins:
        print(f"\n🆕 NEW COINS in top 20: {', '.join(new_coins)}")

    # Format and send to Discord
    discord_message = format_discord_message(top_20_oi_changes, previous_top20_symbols, limit=20)
    print(f"\nSending to Discord...")
    send_to_discord(webhook_url, discord_message)

    # Save current top 20 for next run
    save_current_top20(top_20_oi_changes)

    logger.log_event(
        log_category="INFO",
        message=f"Successfully processed {len(oi_changes_with_prices)} coins and sent top 20 OI changes to Discord. New coins: {len(new_coins)}",
        path=log_path
    )
    print("Done!")
    print(f"  - OI changes uploaded to S3: s3://{S3_BUCKET_NAME}/oi-change/oi_changes_1h.csv")
    print(f"  - OI changes saved locally to: {oi_changes_csv}")
