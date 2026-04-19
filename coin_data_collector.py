import ccxt
import json
import ssl
import urllib.parse
import urllib.request
import urllib.error
import csv
import os
import certifi
import logger
import time
from dotenv import load_dotenv
import boto3
from io import BytesIO
import config
from typing import List, Dict, Any, Optional
from utils import FileUtility, ConfigManager, S3Manager

load_dotenv()
os.umask(0o022)

# Ensure log and output directories exist
config.ensure_log_directory()
config.ensure_output_directory()

script_dir: str = os.path.dirname(os.path.abspath(__file__))
log_path: str = config.get_log_file_path("coin_data_collector")
output_dir: str = config.OUTPUT_PATH
coin_data_output_path: str = config.get_output_file_path("coin_data.csv")

# Create log file
FileUtility.ensure_log_file_exists(log_path)

CMC_API_KEY: Optional[str] = os.environ.get("cmc_api_key")
if not CMC_API_KEY:
    logger.log_event(log_category="WARNING", message="cmc_api_key environment variable not set. Market cap data will not be collected.", path=log_path)


def is_valid_symbol(coin: str) -> bool:
    """Check if coin symbol contains only ASCII alphanumeric characters"""
    # CMC API only accepts ASCII characters (A-Z, a-z, 0-9), not Unicode
    return all(c.isascii() and c.isalnum() for c in coin)


def get_coins_from_binance() -> List[str]:
    """
    Get all active futures coins from Binance
    :return: list[] of all active USDT futures coins
    """
    try:
        binance = ccxt.binance({
            'options': {
                'defaultType': 'future'
            }
        })

        markets: Dict[str, Any] = binance.load_markets()

        usdt_perps: List[str] = [
            symbol for symbol, market in markets.items()
            if market['contract'] and market['linear'] and market['quote'] == 'USDT' and market['active']
        ]

        # Clean up coin symbols and filter out unicode characters
        coins: List[str] = []
        invalid_coins: List[str] = []
        for symbol in usdt_perps:
            formatted_coin: str = symbol.replace("/USDT:", "")
            if "-" not in formatted_coin:
                # Only keep coins with valid ASCII alphanumeric characters
                clean_coin: str = formatted_coin.replace("USDT", "")
                if is_valid_symbol(clean_coin):
                    coins.append(formatted_coin)
                else:
                    invalid_coins.append(formatted_coin)

        if invalid_coins:
            logger.log_event(log_category="WARNING", message=f"Filtered out {len(invalid_coins)} coins with unicode characters: {invalid_coins[:10]}", path=log_path)

        coin_count: int = len(coins)
        logger.log_event(log_category="INFO", message=f"Successfully retrieved {coin_count} valid coins from Binance ({len(invalid_coins)} filtered out)", path=log_path)
        return coins

    except Exception as e:
        logger.log_event(log_category="ERROR", message=f"Failed to retrieve coins from Binance. Error: {e}", path=log_path)
        return []


def get_market_cap_data(coins: List[str]) -> Dict[str, Dict[str, Any]]:
    """
    Get market cap data from CoinMarketCap API for all coins
    :param coins: list of coin symbols
    :return: dict with coin -> {market_cap, category}
    """
    if not CMC_API_KEY:
        logger.log_event(log_category="WARNING", message="CMC API key not set, returning empty market cap data", path=log_path)
        return {coin: {"market_cap": "N/A", "category": "N/A"} for coin in coins}

    market_cap_data: Dict[str, Dict[str, Any]] = {}
    context: ssl.SSLContext = ssl.create_default_context(cafile=certifi.where())

    # All coins at this point are already validated to be ASCII alphanumeric
    # So we can proceed directly with batching
    batch_size: int = 50
    batches: List[List[str]] = [coins[i : i + batch_size] for i in range(0, len(coins), batch_size)]

    print(f"Fetching market cap data for {len(coins)} coins...")

    for batch_num, batch in enumerate(batches, 1):
        print(f"Processing batch {batch_num}/{len(batches)} ({len(batch)} coins)...")

        # Create comma-separated symbol list (remove USDT suffix for CMC API)
        # All symbols are already validated to be ASCII alphanumeric at this point
        symbols: str = ",".join([coin.replace("USDT", "") for coin in batch])

        # Retry logic with exponential backoff
        MAX_RETRIES: int = 5
        base_delay: int = 2  # Start with 2 seconds
        retry_count: int = 0
        
        while retry_count < MAX_RETRIES:
            try:
                # Use GET request with URL parameters (CMC API v2 supports this)
                # Smaller batch size (50) keeps URL length within limits
                params: str = urllib.parse.urlencode({
                    "symbol": symbols,
                    "convert": "USD",
                })

                url: str = f"https://pro-api.coinmarketcap.com/v2/cryptocurrency/quotes/latest?{params}"
                
                request: urllib.request.Request = urllib.request.Request(
                    url,
                    headers={
                        "Accept": "application/json",
                        "X-CMC_PRO_API_KEY": CMC_API_KEY,
                    },
                )

                # Add 30-second timeout to prevent hanging
                with urllib.request.urlopen(request, context=context, timeout=30) as response:
                    data: Dict[str, Any] = json.load(response)

                # Process response
                if "data" in data:
                    for symbol_key, coin_data_list in data["data"].items():
                        original_symbol = f"{symbol_key}USDT"

                        # Check if the list has data (API returns list of matches)
                        if coin_data_list and len(coin_data_list) > 0:
                            coin_data = coin_data_list[0]  # Get the first (best match)

                            try:
                                price = coin_data.get("quote", {}).get("USD", {}).get("price")
                                circulating_supply = coin_data.get("circulating_supply", 0)

                                if price and circulating_supply:
                                    market_cap = price * circulating_supply
                                else:
                                    market_cap = None
                            except (KeyError, TypeError):
                                market_cap = None

                            # Categorize market cap
                            if market_cap is None:
                                market_cap_data[original_symbol] = {
                                    "market_cap": "N/A",
                                    "category": "N/A"
                                }
                            else:
                                market_cap_str = f"{market_cap:.2f}"
                                if market_cap > 10_000_000_000:  # >10B
                                    category = "Large Cap"
                                elif market_cap >= 1_000_000_000:  # 1B-10B
                                    category = "Mid Cap"
                                else:  # <1B
                                    category = "Small Cap"

                                market_cap_data[original_symbol] = {
                                    "market_cap": market_cap_str,
                                    "category": category
                                }
                        else:
                            # Empty list means coin not found in API
                            market_cap_data[original_symbol] = {
                                "market_cap": "N/A",
                                "category": "N/A"
                            }
                
                # Success - break out of retry loop
                logger.log_event(log_category="INFO", message=f"Batch {batch_num} processed successfully", path=log_path)
                break

            except urllib.error.HTTPError as e:
                error_body = ""
                if e.code == 400:  # Bad Request - log response body for debugging
                    try:
                        error_body = e.read().decode('utf-8')
                        logger.log_event(log_category="DEBUG", message=f"API response body: {error_body}", path=log_path)
                    except:
                        pass
                
                if e.code == 429:  # Rate limit error
                    retry_count += 1
                    if retry_count < MAX_RETRIES:
                        wait_time = base_delay * (2 ** (retry_count - 1))  # Exponential backoff
                        logger.log_event(log_category="WARNING", message=f"Batch {batch_num}: Rate limited (429). Retry {retry_count}/{MAX_RETRIES} after {wait_time}s", path=log_path)
                        print(f"  [RATE LIMITED] Batch {batch_num}: Waiting {wait_time}s before retry ({retry_count}/{MAX_RETRIES})...")
                        time.sleep(wait_time)
                    else:
                        logger.log_event(log_category="ERROR", message=f"Batch {batch_num}: Failed after {MAX_RETRIES} retries due to rate limiting", path=log_path)
                        print(f"  [ERROR] Batch {batch_num}: Failed after {MAX_RETRIES} retries - marking as N/A")
                        for coin in batch:
                            market_cap_data[coin] = {"market_cap": "N/A", "category": "N/A"}
                        break
                else:
                    logger.log_event(log_category="ERROR", message=f"Batch {batch_num}: HTTP error {e.code}: {e.reason}. {error_body}", path=log_path)
                    print(f"  [ERROR] Batch {batch_num}: HTTP {e.code} - {e.reason}")
                    if error_body:
                        print(f"    Response: {error_body[:200]}")
                    for coin in batch:
                        market_cap_data[coin] = {"market_cap": "N/A", "category": "N/A"}
                    break

            except urllib.error.URLError as e:
                retry_count += 1
                if retry_count < MAX_RETRIES:
                    wait_time = base_delay * (2 ** (retry_count - 1))
                    logger.log_event(log_category="WARNING", message=f"Batch {batch_num}: Connection error. Retry {retry_count}/{MAX_RETRIES} after {wait_time}s: {e}", path=log_path)
                    print(f"  [RETRY] Batch {batch_num}: Connection error, retrying in {wait_time}s ({retry_count}/{MAX_RETRIES})...")
                    time.sleep(wait_time)
                else:
                    logger.log_event(log_category="ERROR", message=f"Batch {batch_num}: Failed after {MAX_RETRIES} retries - {e}", path=log_path)
                    print(f"  [ERROR] Batch {batch_num}: Failed after {MAX_RETRIES} retries")
                    for coin in batch:
                        market_cap_data[coin] = {"market_cap": "N/A", "category": "N/A"}
                    break

            except Exception as e:
                logger.log_event(log_category="ERROR", message=f"Batch {batch_num}: Unexpected error - {e}", path=log_path)
                print(f"  [ERROR] Batch {batch_num}: {type(e).__name__}: {e}")
                for coin in batch:
                    market_cap_data[coin] = {"market_cap": "N/A", "category": "N/A"}
                break

        # Add delay between batch requests to respect API rate limits
        if batch_num < len(batches):
            delay = 1.0  # 1 second delay between batches (reduced from 1.5s since batches are now half the size)
            print(f"  Waiting {delay}s before next batch...")
            time.sleep(delay)

    logger.log_event(log_category="INFO", message=f"Successfully retrieved market cap data for {len(market_cap_data)} coins", path=log_path)
    return market_cap_data


def save_coin_data(coins: List[str], market_cap_data: Dict[str, Dict[str, Any]]) -> None:
    """
    Save combined coin data to CSV locally and to S3
    :param coins: list of coins
    :param market_cap_data: dict with market cap and category info
    """
    try:
        os.makedirs(output_dir, exist_ok=True)

        # Create DataFrame with proper data types
        data_list: List[Dict[str, Any]] = []
        for coin in coins:
            data: Dict[str, Any] = market_cap_data.get(coin, {"market_cap": "N/A", "category": "N/A"})
            market_cap: Any = data["market_cap"]
            # Convert market cap to float if it's not "N/A"
            if market_cap != "N/A":
                try:
                    market_cap = float(market_cap)
                except (ValueError, TypeError):
                    market_cap = None
            else:
                market_cap = None
            data_list.append({
                "coin": coin,
                "market_cap_value": market_cap,
                "market_cap_category": data["category"]
            })
        
        df = pd.DataFrame(data_list)
        df.to_csv(coin_data_output_path, index=False)

        logger.log_event(log_category="INFO", message=f"Successfully saved coin data to {coin_data_output_path}", path=log_path)
        print(f"\n[OK] Results saved locally to {coin_data_output_path}")
        print(f"  Total coins: {len(coins)}")
    except Exception as e:
        logger.log_event(log_category="ERROR", message=f"Failed to save coin data locally. Error: {e}", path=log_path)
        print(f"Error saving to CSV: {e}")


def upload_dataframe_to_s3(dataframe: 'pd.DataFrame', s3_key: str) -> bool:
    """Wrapper for S3Manager - upload DataFrame to S3"""
    return S3Manager.upload_dataframe_to_s3(dataframe, s3_key, log_path)


if __name__ == "__main__":
    import pandas as pd
    print(f"Running {__file__}...")

    # Step 1: Get coin list from Binance
    print("\nStep 1: Fetching active futures coins from Binance...")
    coins = get_coins_from_binance()

    if not coins:
        print("No coins retrieved from Binance. Exiting.")
        exit(1)

    print(f"[OK] Retrieved {len(coins)} coins from Binance")

    # Step 2: Get market cap data
    print("\nStep 2: Fetching market cap data from CoinMarketCap...")
    market_cap_data = get_market_cap_data(coins)

    # Step 3: Save combined data locally
    print("\nStep 3: Saving combined coin data locally...")
    save_coin_data(coins, market_cap_data)

    # Step 4: Upload to S3
    print("\nStep 4: Uploading coin data to S3...")
    import pandas as pd
    df_for_s3 = pd.read_csv(coin_data_output_path)
    upload_dataframe_to_s3(df_for_s3, "coin-data/coin_data.csv")

    print("\n[OK] Process completed successfully!")
