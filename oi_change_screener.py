import os
import logger
import sys
import asyncio
import time
import json
import pandas as pd
from datetime import datetime, timedelta

if sys.platform.startswith('win'):
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

import aiohttp
from discord_integrator import send_to_discord
from dotenv import load_dotenv

load_dotenv()

script_dir = os.path.dirname(os.path.abspath(__file__))
log_path = os.path.join(script_dir, "logs", "oi_change_screener_log.txt")
coin_list_path = os.path.join(script_dir, "coin_list.txt")
previous_top20_path = os.path.join(script_dir, "oi_change_top20_previous.json")
hourly_data_dir = os.path.join(script_dir, "hourly_data")
market_cap_csv = os.path.join(script_dir, "market_cap_data.csv")

BASE_URL = "https://fapi.binance.com/fapi/v1"
CURRENT_OI_ENDPOINT = f"{BASE_URL}/openInterest"
HISTORICAL_OI_ENDPOINT = f"https://fapi.binance.com/futures/data/openInterestHist"
RATE_LIMIT = 1000 / 60  # Binance Futures limit: 1200 reqs per minute => ~20 reqs/sec safe

# Read webhook from environment
webhook_url = os.getenv(
    "OI_CHANGE_WEBHOOK",
    "https://discord.com/api/webhooks/1476932257615839434/8AnKIHpED8HLMP5jHz7xKXByHTpogM6ONhvJh3KDB_nhXHeIsRs5AXZYhu78Y7Wr7iXO"
)
if not os.getenv("OI_CHANGE_WEBHOOK"):
    logger.log_event(
        log_category="WARNING",
        message="OI_CHANGE_WEBHOOK not set; using fallback hard-coded webhook. Consider setting OI_CHANGE_WEBHOOK in .env or CI secrets.",
        path=log_path
    )


def get_coins():
    """
    Get the list of active coins before requesting data
    :return: list[]
    """
    try:
        with open(coin_list_path, 'r') as file:
            coin_list = [line.strip() for line in file]

        logger.log_event(log_category="INFO", message="Successfully retrieved coin list", path=log_path)
        return coin_list
    except Exception as e:
        logger.log_event(log_category="ERROR", message=f"Failed to retrieve coin list. Error={e}", path=log_path)
        return []


def get_previous_top20():
    """
    Get the previous top 20 results from file
    :return: set of symbol names, or empty set if file doesn't exist
    """
    try:
        if os.path.exists(previous_top20_path):
            with open(previous_top20_path, 'r') as file:
                data = json.load(file)
                previous_symbols = set(data.get("symbols", []))
                logger.log_event(log_category="INFO", message=f"Retrieved {len(previous_symbols)} previous top 20 symbols", path=log_path)
                return previous_symbols
        else:
            logger.log_event(log_category="INFO", message="No previous top 20 file found. Starting fresh.", path=log_path)
            return set()
    except Exception as e:
        logger.log_event(log_category="ERROR", message=f"Failed to retrieve previous top 20. Error={e}", path=log_path)
        return set()


def load_category_data(market_cap_csv_path):
    """
    Load category information from market_cap_data.csv
    Returns dict mapping coin -> category (defaults to 'N/A' if empty)
    """
    try:
        if os.path.exists(market_cap_csv_path):
            df = pd.read_csv(market_cap_csv_path)
            df.columns = df.columns.str.strip()
            category_map = {}
            for idx, row in df.iterrows():
                coin = row.get('coin', '')
                category = row.get('category', '')
                # Use 'N/A' if category is empty or NaN
                category_map[coin] = category if (pd.notna(category) and str(category).strip()) else 'N/A'
            logger.log_event(log_category="INFO", message=f"Loaded categories for {len(category_map)} coins", path=log_path)
            return category_map
        else:
            logger.log_event(log_category="WARNING", message=f"Market cap CSV not found at {market_cap_csv_path}. Using N/A for all categories.", path=log_path)
            return {}
    except Exception as e:
        logger.log_event(log_category="ERROR", message=f"Error loading category data: {e}", path=log_path)
        return {}


def load_market_cap_data(market_cap_csv_path):
    """
    Load market cap information from market_cap_data.csv
    Returns dict mapping coin -> market_cap (float or None if empty)
    """
    try:
        if os.path.exists(market_cap_csv_path):
            df = pd.read_csv(market_cap_csv_path)
            df.columns = df.columns.str.strip()
            market_cap_map = {}
            for idx, row in df.iterrows():
                coin = row.get('coin', '')
                market_cap = row.get('market_cap', '')
                # Convert to float if not empty, otherwise None
                if pd.notna(market_cap) and str(market_cap).strip():
                    try:
                        market_cap_map[coin] = float(market_cap)
                    except (ValueError, TypeError):
                        market_cap_map[coin] = None
                else:
                    market_cap_map[coin] = None
            logger.log_event(log_category="INFO", message=f"Loaded market caps for {len([m for m in market_cap_map.values() if m is not None])} coins", path=log_path)
            return market_cap_map
        else:
            logger.log_event(log_category="WARNING", message=f"Market cap CSV not found at {market_cap_csv_path}. Using None for all market caps.", path=log_path)
            return {}
    except Exception as e:
        logger.log_event(log_category="ERROR", message=f"Error loading market cap data: {e}", path=log_path)
        return {}


def format_market_cap(market_cap_value):
    """
    Format market cap value to human-readable format (M, B, T, etc.)
    e.g., 1500000000 -> "1.50B", 50000000 -> "50.00M"
    Returns "N/A" if market_cap_value is None or 0
    """
    if market_cap_value is None or market_cap_value == 0:
        return "N/A"
    
    abs_value = abs(market_cap_value)
    
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


def save_current_top20(top_oi_changes):
    """
    Save the current top 20 results to file for next run
    :param top_oi_changes: list of current top OI changes
    """
    try:
        symbols_data = [
            {
                "symbol": item["symbol"],
                "category": item.get("category", "N/A"),
                "market_cap": item.get("market_cap")
            }
            for item in top_oi_changes[:20]
        ]
        data = {
            "symbols": [item["symbol"] for item in symbols_data],
            "symbols_with_category": symbols_data,
            "timestamp": datetime.now().isoformat()
        }
        with open(previous_top20_path, 'w') as file:
            json.dump(data, file, indent=2)
        logger.log_event(log_category="INFO", message=f"Saved current top 20 to {previous_top20_path}", path=log_path)
    except Exception as e:
        logger.log_event(log_category="ERROR", message=f"Failed to save current top 20. Error={e}", path=log_path)


def get_hourly_price_data(symbols):
    """
    Get current and previous hour price data from hourly_data CSVs
    :param symbols: list of symbols to fetch price data for
    :return: dict with symbol -> {current_price, previous_close}
    """
    price_data = {}
    
    for symbol in symbols:
        try:
            csv_path = os.path.join(hourly_data_dir, f"{symbol}.csv")
            if not os.path.exists(csv_path):
                logger.log_event(
                    log_category="WARNING",
                    message=f"Hourly data CSV not found for {symbol}",
                    path=log_path
                )
                continue
            
            # Read CSV and get the last two rows
            df = pd.read_csv(csv_path)
            if len(df) < 2:
                logger.log_event(
                    log_category="WARNING",
                    message=f"Insufficient data in CSV for {symbol} (need at least 2 rows)",
                    path=log_path
                )
                continue
            
            # Get current price (last row close) and previous hour close (second-to-last row)
            current_price = float(df.iloc[-1]['close'])
            previous_close = float(df.iloc[-2]['close'])
            
            price_data[symbol] = {
                "current_price": current_price,
                "previous_close": previous_close
            }
            
            logger.log_event(
                log_category="INFO",
                message=f"Successfully retrieved price data for {symbol}: current={current_price}, previous={previous_close}",
                path=log_path
            )
        except Exception as e:
            logger.log_event(
                log_category="ERROR",
                message=f"Failed to get price data for {symbol}. Error: {e}",
                path=log_path
            )
            continue
    
    return price_data


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


def calculate_oi_change_percentage(current_results, historical_results, price_data, category_data=None, market_cap_data=None):
    """
    Calculate OI change percentage from previous hour
    Historical results come in chronological order: [oldest, newest]
    We want the oldest value (1 hour ago)
    """
    if category_data is None:
        category_data = {}
    if market_cap_data is None:
        market_cap_data = {}
    
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

            # Calculate price change if available from CSV data
            price_change_percentage = None
            if symbol in price_data:
                current_price = price_data[symbol]["current_price"]
                previous_close = price_data[symbol]["previous_close"]
                if previous_close > 0:
                    price_change_percentage = ((current_price - previous_close) / previous_close) * 100

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
    message += f"{'SYMBOL':<12} {'OI CHG %':<10} {'PRICE CHG %':<14} {'CATEGORY':<15} {'MARKET CAP':<15}\n"
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

        # Format price change with emoji indicator
        if price_change is not None:
            if price_change > 0:
                price_str = f"📈 {price_change:>9.2f}%"
            elif price_change < 0:
                price_str = f"📉 {price_change:>9.2f}%"
            else:
                price_str = f"  {price_change:>9.2f}%"
        else:
            price_str = "N/A        "

        # Truncate category if needed
        cat_str = category[:13] if len(category) > 13 else category
        
        # Format market cap
        market_cap_str = format_market_cap(market_cap)

        message += f"{marker} {symbol:<9} {change_pct:>8.2f}%  {price_str}  {cat_str:<15} {market_cap_str:>13}\n"

    message += "```\n"
    
    # Add legend for new coins
    if any(item["symbol"] not in previous_top20_symbols for item in top_oi_changes[:limit]):
        message += "\n🆕 = Newly entered top 20\n"
    
    message += f"\n📊 Updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}"

    return message


if __name__ == "__main__":
    print(f"Running {__file__}...")

    # Load category and market cap data from market_cap_data.csv
    category_data = load_category_data(market_cap_csv)
    print(f"Loaded categories for {len(category_data)} coins")
    
    market_cap_data = load_market_cap_data(market_cap_csv)
    print(f"Loaded market caps for {len([m for m in market_cap_data.values() if m is not None])} coins")

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

    # Calculate OI change percentage (without price data first to identify top 20)
    oi_changes = calculate_oi_change_percentage(current_oi, historical_oi, {}, category_data, market_cap_data)

    if not oi_changes:
        print("No OI change data available.")
        logger.log_event(log_category="WARNING", message="No OI change data calculated", path=log_path)
        sys.exit(0)

    # Get top 20 symbols
    top_20_symbols = [item["symbol"] for item in oi_changes[:20]]
    
    # Fetch hourly price data for top 20 symbols only (efficient)
    print(f"Fetching hourly price data for top 20 symbols...")
    price_data = get_hourly_price_data(top_20_symbols)
    
    # Recalculate OI changes with price data
    oi_changes_with_prices = calculate_oi_change_percentage(current_oi, historical_oi, price_data, category_data, market_cap_data)
    top_20_oi_changes = oi_changes_with_prices[:20]

    print(f"\nTop 20 coins by OI change:")
    for i, item in enumerate(top_20_oi_changes, 1):
        if item['price_change_percentage'] is not None:
            if item['price_change_percentage'] > 0:
                price_str = f"📈 {item['price_change_percentage']:>8.2f}%"
            elif item['price_change_percentage'] < 0:
                price_str = f"📉 {item['price_change_percentage']:>8.2f}%"
            else:
                price_str = f"  {item['price_change_percentage']:>8.2f}%"
        else:
            price_str = "N/A"
        category = item.get("category", "N/A")
        market_cap_str = format_market_cap(item.get("market_cap"))
        print(f"{i:2}. {item['symbol']:<15} {item['change_percentage']:>8.2f}% | Price: {price_str} | Category: {category} | Market Cap: {market_cap_str}")

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
