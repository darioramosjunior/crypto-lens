import os
import pandas as pd
import logger
import sys
import asyncio
import glob
import pandas_ta as ta

if sys.platform.startswith('win'):
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

import aiohttp
import time
from indicator_calculator import calculate_and_save_indicators

script_dir = os.path.dirname(os.path.abspath(__file__))
log_path = os.path.join(script_dir, "logs", "4h_dc_log.txt")
data_path = os.path.join(script_dir, "four-hour_data")
coin_list_path = os.path.join(script_dir, "coin_list.txt")

BASE_URL = "https://fapi.binance.com/fapi/v1/klines"
INTERVAL = "4h"
LIMIT = 50
RATE_LIMIT = 1000 / 60  # Binance Futures limit: 1200 reqs per minute => ~20 reqs/sec safe


def get_coins():
    """
    get the list of active coins before requesting data
    :return: list[]
    """

    try:
        with open(coin_list_path, 'r') as file:
            coin_list = [line.strip() for line in file]

        logger.log_event(log_category="INFO", message="Successfully retrieved coin list", path=log_path)
        return coin_list
    except Exception as e:
        logger.log_event(log_category="ERROR", message=f"Failed to retrieve coin list. Error={e}", path=log_path)


async def fetch_ohlcv(session, symbol):
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
                text = await response.text()
                logger.log_event(log_category="ERROR", message=f"Failed to fetch OHLCV for symbol {symbol} with status {response.status} - {text}", path=log_path)
                return symbol, None
    except Exception as e:
        logger.log_event(log_category="ERROR", message=f"Failed to fetch OHLCV for symbol {symbol}. Error: {e}", path=log_path)
        return symbol, None


async def get_coin_data(symbols, max_concurrent=10):
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


def save_to_csv(symbol, data):
    if not data:
        logger.log_event(log_category="ERROR", message=f"Failed to save data for {symbol}. No data", path=log_path)
        return
    try:
        df = pd.DataFrame(data, columns=[
            "timestamp", "open", "high", "low", "close", "volume",
            "close_time", "quote_asset_volume", "number_of_trades",
            "taker_buy_base_volume", "taker_buy_quote_volume", "ignore"
        ])

        float_columns = ["open", "high", "low", "close", "volume",
                         "quote_asset_volume", "taker_buy_base_volume", "taker_buy_quote_volume"]
        df[float_columns] = df[float_columns].astype(float)

        df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")
        df['timestamp'] = df['timestamp'].dt.tz_localize('UTC')
        df['timestamp'] = df['timestamp'].dt.tz_localize(None)

        close = df['close']
        df['sma20'] = close.rolling(window=20).mean()
        df['sma50'] = close.rolling(window=50).mean()
        df['sma100'] = close.rolling(window=100).mean()
        df['rsi14'] = ta.rsi(close, length=14)

        csv_path = os.path.join(data_path, f"{symbol}.csv")
        df.to_csv(csv_path, index=False, mode='w')
        logger.log_event(log_category="INFO", message=f"Successfully saved data for {symbol}.", path=log_path)
    except Exception as e:
        logger.log_event(log_category="ERROR", message=f"Failed to save data for {symbol}. Error: {e}", path=log_path)


if __name__ == "__main__":
    print(f"Running {__file__}...")
    coins = get_coins()
    start = time.time()
    results = asyncio.run(get_coin_data(coins, max_concurrent=20))
    end = time.time()

    print(f"\nFetched {len(results)} symbols in {end - start:.2f} seconds.")

    print("Deleting existing csv files first...")
    csv_files = glob.glob(os.path.join(data_path, "*.csv"))
    for file in csv_files:
        os.remove(file)
        print(f"Deleted: {file}")

    for symbol, data in results.items():
        save_to_csv(symbol, data)