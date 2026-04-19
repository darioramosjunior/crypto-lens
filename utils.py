"""
Utility classes for shared helper functions across all modules.
Reduces code duplication by consolidating common operations.
"""

import os
import sys
import asyncio
import pandas as pd
import boto3
from typing import List, Dict, Any, Optional, Tuple
from io import BytesIO
from collections import defaultdict
import aiohttp
import logger
import config
import pandas_ta as ta
from validations import (
    OHLCVData, OHLCVCandle, IndicatorData, PriceChangeData, 
    TrendCounts, validate_dataframe_schema
)

if sys.platform.startswith('win'):
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())


class FileUtility:
    """Handles file and directory operations"""

    @staticmethod
    def ensure_directory_exists(directory_path: str) -> bool:
        """
        Ensure a directory exists, create if it doesn't
        :param directory_path: Path to directory
        :return: True if successful, False otherwise
        """
        try:
            os.makedirs(directory_path, exist_ok=True)
            return True
        except Exception as e:
            print(f"[WARNING] Failed to create directory {directory_path}: {e}")
            return False

    @staticmethod
    def ensure_log_file_exists(log_path: str) -> bool:
        """
        Ensure log file and its directory exist
        :param log_path: Path to log file
        :return: True if successful, False otherwise
        """
        try:
            os.makedirs(os.path.dirname(log_path), exist_ok=True)
            if not os.path.exists(log_path):
                open(log_path, 'a').close()
            return True
        except Exception as e:
            print(f"[WARNING] Failed to create log file {log_path}: {e}")
            return False

    @staticmethod
    def file_exists(file_path: str) -> bool:
        """Check if file exists"""
        return os.path.exists(file_path)


class ConfigManager:
    """Manages configuration and AWS S3 settings"""

    S3_BUCKET_NAME: str = "data-portfolio-2026"
    AWS_REGION: str = os.getenv("AWS_REGION", "ap-southeast-2")
    BINANCE_BASE_URL: str = "https://fapi.binance.com/fapi/v1/klines"
    BINANCE_RATE_LIMIT: float = 1000 / 60  # Binance Futures limit: 1200 reqs/min

    @staticmethod
    def get_s3_bucket() -> str:
        """Get S3 bucket name"""
        return ConfigManager.S3_BUCKET_NAME

    @staticmethod
    def get_aws_region() -> str:
        """Get AWS region"""
        return ConfigManager.AWS_REGION

    @staticmethod
    def get_binance_base_url() -> str:
        """Get Binance Futures API base URL"""
        return ConfigManager.BINANCE_BASE_URL

    @staticmethod
    def get_binance_rate_limit() -> float:
        """Get Binance rate limit (requests per minute)"""
        return ConfigManager.BINANCE_RATE_LIMIT


class DataLoaderUtility:
    """Handles loading and parsing data from CSV files"""

    @staticmethod
    def get_coins_from_csv(csv_path: str, log_path: str) -> List[str]:
        """
        Get the list of active coins from coin_data.csv
        :param csv_path: Path to coin_data.csv
        :param log_path: Path to log file
        :return: List of coins
        """
        try:
            df: pd.DataFrame = pd.read_csv(csv_path)
            coin_list: List[str] = df['coin'].tolist()
            logger.log_event(
                log_category="INFO",
                message="Successfully retrieved coin list from coin_data.csv",
                path=log_path
            )
            return coin_list
        except Exception as e:
            logger.log_event(
                log_category="ERROR",
                message=f"Failed to retrieve coin list. Error={e}",
                path=log_path
            )
            return []

    @staticmethod
    def load_market_cap_categories(csv_path: str, log_path: str) -> Dict[str, str]:
        """
        Load market cap categories from coin_data.csv
        :param csv_path: Path to coin_data.csv
        :param log_path: Path to log file
        :return: Dict mapping coin -> market_cap_category
        """
        try:
            df: pd.DataFrame = pd.read_csv(csv_path)
            df.columns = df.columns.str.strip()
            category_map: Dict[str, str] = {}
            for idx, row in df.iterrows():
                coin: str = row.get('coin', '')
                category: str = row.get('market_cap_category', '')
                category_map[coin] = (
                    category if (pd.notna(category) and str(category).strip()) else 'N/A'
                )
            logger.log_event(
                log_category="INFO",
                message=f"Loaded market cap categories for {len(category_map)} coins",
                path=log_path
            )
            return category_map
        except Exception as e:
            logger.log_event(
                log_category="ERROR",
                message=f"Failed to load market cap categories. Error: {e}",
                path=log_path
            )
            return {}

    @staticmethod
    def load_market_cap_data(csv_path: str, log_path: str) -> Dict[str, Optional[float]]:
        """
        Load market cap values from coin_data.csv
        :param csv_path: Path to coin_data.csv
        :param log_path: Path to log file
        :return: Dict mapping coin -> market_cap_value (float or None)
        """
        try:
            if not os.path.exists(csv_path):
                logger.log_event(
                    log_category="WARNING",
                    message=f"Coin data CSV not found at {csv_path}",
                    path=log_path
                )
                return {}

            df: pd.DataFrame = pd.read_csv(csv_path)
            df.columns = df.columns.str.strip()
            market_cap_map: Dict[str, Optional[float]] = {}

            for idx, row in df.iterrows():
                coin: str = row.get('coin', '')
                market_cap: Any = row.get('market_cap_value', '')

                if pd.notna(market_cap) and str(market_cap).strip():
                    try:
                        market_cap_map[coin] = float(market_cap)
                    except (ValueError, TypeError):
                        market_cap_map[coin] = None
                else:
                    market_cap_map[coin] = None

            valid_count = len([m for m in market_cap_map.values() if m is not None])
            logger.log_event(
                log_category="INFO",
                message=f"Loaded market cap values for {valid_count} coins",
                path=log_path
            )
            return market_cap_map
        except Exception as e:
            logger.log_event(
                log_category="ERROR",
                message=f"Error loading market cap data: {e}",
                path=log_path
            )
            return {}


class BinanceDataFetcher:
    """Handles asynchronous fetching and parsing of Binance OHLCV data"""

    @staticmethod
    async def fetch_ohlcv(
        session: aiohttp.ClientSession,
        symbol: str,
        interval: str = "1h",
        limit: int = 200,
        log_path: str = None
    ) -> Tuple[str, Optional[List[List[Any]]]]:
        """
        Fetch OHLCV data for a single symbol
        :param session: aiohttp ClientSession
        :param symbol: Symbol to fetch
        :param interval: Timeframe (e.g., '1h', '1d')
        :param limit: Number of candles
        :param log_path: Path to log file
        :return: Tuple of (symbol, data or None)
        """
        params: Dict[str, Any] = {
            "symbol": symbol,
            "interval": interval,
            "limit": limit
        }
        try:
            async with session.get(
                ConfigManager.get_binance_base_url(),
                params=params,
                timeout=10
            ) as response:
                if response.status == 200:
                    data: List[List[Any]] = await response.json()
                    if log_path:
                        logger.log_event(
                            log_category="INFO",
                            message=f"Successfully fetched OHLCV for symbol {symbol}",
                            path=log_path
                        )
                    return symbol, data
                else:
                    if log_path:
                        logger.log_event(
                            log_category="ERROR",
                            message=(
                                f"Failed to fetch OHLCV for symbol {symbol} "
                                f"with status {response.status}"
                            ),
                            path=log_path
                        )
                    return symbol, None
        except Exception as e:
            if log_path:
                logger.log_event(
                    log_category="ERROR",
                    message=f"Failed to fetch OHLCV for symbol {symbol}. Error: {e}",
                    path=log_path
                )
            return symbol, None

    @staticmethod
    async def get_coin_data(
        symbols: List[str],
        interval: str = "1h",
        limit: int = 200,
        max_concurrent: int = 20,
        log_path: str = None
    ) -> Dict[str, Optional[List[List[Any]]]]:
        """
        Async fetch OHLCV data for all symbols
        :param symbols: List of symbols
        :param interval: Timeframe
        :param limit: Number of candles
        :param max_concurrent: Maximum concurrent requests
        :param log_path: Path to log file
        :return: Dictionary mapping symbol to data
        """
        connector: aiohttp.TCPConnector = aiohttp.TCPConnector(limit=max_concurrent)
        timeout: aiohttp.ClientTimeout = aiohttp.ClientTimeout(total=30)

        async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
            sem: asyncio.Semaphore = asyncio.Semaphore(max_concurrent)

            async def sem_task(symbol: str) -> Tuple[str, Optional[List[List[Any]]]]:
                async with sem:
                    return await BinanceDataFetcher.fetch_ohlcv(
                        session, symbol, interval, limit, log_path
                    )

            tasks: List[asyncio.Task] = [sem_task(symbol) for symbol in symbols]
            results: List[Tuple[str, Optional[List[List[Any]]]]] = await asyncio.gather(*tasks)
            return dict(results)

    @staticmethod
    def parse_raw_data_to_dataframe(
        symbol: str,
        raw_data: Optional[List[List[Any]]],
        log_path: str = None
    ) -> Optional[pd.DataFrame]:
        """
        Convert raw API response to DataFrame with proper formatting
        :param symbol: Symbol name
        :param raw_data: List of raw data or None
        :param log_path: Path to log file
        :return: DataFrame or None
        """
        if not raw_data:
            return None
        try:
            # Validate raw data format
            try:
                OHLCVData(symbol=symbol, interval="1h", candles=raw_data)
            except Exception as e:
                if log_path:
                    logger.log_event(
                        log_category="WARNING",
                        message=f"OHLCV validation warning for {symbol}: {e}",
                        path=log_path
                    )
            
            df: pd.DataFrame = pd.DataFrame(raw_data, columns=[
                "timestamp", "open", "high", "low", "close", "volume",
                "close_time", "quote_asset_volume", "number_of_trades",
                "taker_buy_base_volume", "taker_buy_quote_volume", "ignore"
            ])
            df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")
            df['timestamp'] = df['timestamp'].dt.tz_localize('UTC')
            df['timestamp'] = df['timestamp'].dt.tz_localize(None)

            # Convert numeric columns
            df[['open', 'high', 'low', 'close', 'volume']] = (
                df[['open', 'high', 'low', 'close', 'volume']].astype(float)
            )
            
            # Validate converted data
            validation_result = validate_dataframe_schema(
                df,
                required_columns=["timestamp", "open", "high", "low", "close", "volume"]
            )
            if not validation_result.valid:
                if log_path:
                    logger.log_event(
                        log_category="WARNING",
                        message=f"DataFrame schema validation for {symbol}: {validation_result.errors}",
                        path=log_path
                    )

            return df
        except Exception as e:
            if log_path:
                logger.log_event(
                    log_category="ERROR",
                    message=f"Failed to parse data for {symbol}. Error: {e}",
                    path=log_path
                )
            return None


class IndicatorCalculator:
    """Calculates technical indicators and trend analysis"""

    @staticmethod
    def calculate_indicators_in_memory(
        in_memory_data: Dict[str, pd.DataFrame],
        log_path: str = None
    ) -> Dict[str, pd.DataFrame]:
        """
        Calculate indicators for all symbols and store in memory
        :param in_memory_data: Dictionary {symbol: DataFrame}
        :param log_path: Path to log file
        :return: Dictionary {symbol: DataFrame} with indicators added
        """
        indicators_data: Dict[str, pd.DataFrame] = {}
        validation_issues: List[str] = []

        try:
            for symbol, df in in_memory_data.items():
                try:
                    df_sorted: pd.DataFrame = df.sort_values('timestamp').copy()
                    df_sorted['timestamp'] = pd.to_datetime(df_sorted['timestamp'])
                    df_sorted.set_index('timestamp', inplace=True)

                    close: pd.Series = df_sorted['close']
                    df_sorted['sma20'] = close.rolling(window=20).mean()
                    df_sorted['sma50'] = close.rolling(window=50).mean()
                    df_sorted['sma100'] = close.rolling(window=100).mean()
                    df_sorted['rsi14'] = ta.rsi(close, length=14)

                    volume: pd.Series = df_sorted['volume']
                    df_sorted['volume_sma20'] = volume.rolling(window=20).mean()

                    df_sorted.reset_index(inplace=True)
                    
                    # Validate indicator data
                    try:
                        for idx, row in df_sorted.iterrows():
                            try:
                                IndicatorData(
                                    symbol=symbol,
                                    timestamp=row['timestamp'],
                                    open=float(row['open']),
                                    high=float(row['high']),
                                    low=float(row['low']),
                                    close=float(row['close']),
                                    volume=float(row['volume']),
                                    sma20=float(row['sma20']) if pd.notna(row['sma20']) else None,
                                    sma50=float(row['sma50']) if pd.notna(row['sma50']) else None,
                                    sma100=float(row['sma100']) if pd.notna(row['sma100']) else None,
                                    rsi14=float(row['rsi14']) if pd.notna(row['rsi14']) else None,
                                    volume_sma20=float(row['volume_sma20']) if pd.notna(row['volume_sma20']) else None
                                )
                            except Exception as e:
                                # Log but continue - partial validation failures are non-fatal
                                if idx == 0:  # Log only first issue per symbol
                                    validation_issues.append(f"{symbol}: {str(e)[:50]}")
                                break
                    except Exception as e:
                        if log_path:
                            logger.log_event(
                                log_category="WARNING",
                                message=f"Indicator validation error for {symbol}: {e}",
                                path=log_path
                            )
                    
                    indicators_data[symbol] = df_sorted

                    if log_path:
                        logger.log_event(
                            log_category="INFO",
                            message=f"Successfully calculated indicators for symbol {symbol}",
                            path=log_path
                        )
                except Exception as e:
                    if log_path:
                        logger.log_event(
                            log_category="ERROR",
                            message=f"Failed to calculate indicators for {symbol}: {e}",
                            path=log_path
                        )
                    continue

            if validation_issues and log_path:
                logger.log_event(
                    log_category="INFO",
                    message=f"Validation checks performed on indicator data ({len(validation_issues)} symbols had partial issues)",
                    path=log_path
                )
            
            return indicators_data
        except Exception as e:
            if log_path:
                logger.log_event(
                    log_category="ERROR",
                    message=f"Failed to calculate indicators. Error: {e}",
                    path=log_path
                )
            return {}

    @staticmethod
    def determine_trend(row: pd.Series) -> str:
        """
        Classify trend based on moving averages
        :param row: DataFrame row with SMA values
        :return: Trend category
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

    @staticmethod
    def calculate_trend_counts(
        indicators_data: Dict[str, pd.DataFrame],
        log_path: str = None
    ) -> Optional[pd.DataFrame]:
        """
        Calculate trend counts per timestamp across all symbols
        :param indicators_data: Dictionary {symbol: DataFrame}
        :param log_path: Path to log file
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
                df['trend'] = df.apply(IndicatorCalculator.determine_trend, axis=1)

                for idx, row in df.iterrows():
                    ts, trend = row['timestamp'], row['trend']
                    trend_counter[ts][trend] += 1

            # Convert to DataFrame
            trend_df = pd.DataFrame.from_dict(trend_counter, orient='index')
            trend_df.index.name = 'timestamp'
            trend_df = trend_df.sort_index()

            # Ensure all columns are int type
            for col in trend_df.columns:
                trend_df[col] = trend_df[col].astype('int64')

            if log_path:
                logger.log_event(
                    log_category="INFO",
                    message="Successfully calculated trend counts",
                    path=log_path
                )
            return trend_df
        except Exception as e:
            if log_path:
                logger.log_event(
                    log_category="ERROR",
                    message=f"Failed to calculate trend counts. Error: {e}",
                    path=log_path
                )
            return None


class MathUtility:
    """Utility functions for mathematical calculations"""

    @staticmethod
    def calculate_percentage(numerator: float, denominator: float) -> str:
        """
        Calculate percentage between two numbers
        :param numerator: Numerator value
        :param denominator: Denominator value
        :return: Formatted percentage string
        """
        if denominator == 0:
            return "0.00 %"
        result = numerator / denominator
        percentage = result * 100
        return "{:.2f} %".format(percentage)

    @staticmethod
    def calculate_price_change_percent(
        current_price: float,
        previous_price: float
    ) -> Optional[float]:
        """
        Calculate percentage change in price
        :param current_price: Current price
        :param previous_price: Previous price
        :return: Percentage change or None
        """
        if pd.notna(current_price) and pd.notna(previous_price) and previous_price != 0:
            return ((current_price - previous_price) / previous_price * 100)
        return None


class S3Manager:
    """Manages S3 upload operations"""

    @staticmethod
    def upload_dataframe_to_s3(
        dataframe: pd.DataFrame,
        s3_key: str,
        log_path: str = None
    ) -> bool:
        """
        Upload DataFrame directly to S3 as CSV without saving locally
        :param dataframe: Pandas DataFrame to upload
        :param s3_key: S3 key path (e.g., "coin-data/coin_data.csv")
        :param log_path: Path to log file
        :return: True if successful, False otherwise
        """
        try:
            # Initialize S3 client
            s3_client = boto3.client(
                's3',
                region_name=ConfigManager.get_aws_region()
            )

            # Convert DataFrame to CSV in memory
            csv_buffer: BytesIO = BytesIO()
            dataframe.to_csv(csv_buffer, index=False)
            csv_buffer.seek(0)

            # Upload to S3
            s3_client.upload_fileobj(
                csv_buffer,
                ConfigManager.get_s3_bucket(),
                s3_key
            )

            if log_path:
                logger.log_event(
                    log_category="INFO",
                    message=(
                        f"Successfully uploaded {s3_key} to S3 bucket "
                        f"{ConfigManager.get_s3_bucket()}"
                    ),
                    path=log_path
                )
            print(f"[OK] Uploaded {s3_key} to S3")
            return True

        except Exception as e:
            if log_path:
                logger.log_event(
                    log_category="ERROR",
                    message=f"Failed to upload {s3_key} to S3. Error: {e}",
                    path=log_path
                )
            print(f"[ERROR] Failed to upload {s3_key} to S3: {e}")
            return False

    @staticmethod
    def save_dataframe_locally(
        dataframe: pd.DataFrame,
        file_path: str,
        log_path: str = None
    ) -> bool:
        """
        Save DataFrame to local CSV file
        :param dataframe: Pandas DataFrame
        :param file_path: Local file path
        :param log_path: Path to log file
        :return: True if successful, False otherwise
        """
        try:
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            dataframe.to_csv(file_path, index=False)

            if log_path:
                logger.log_event(
                    log_category="INFO",
                    message=f"Successfully saved data to {file_path}",
                    path=log_path
                )
            print(f"[OK] Results saved locally to {file_path}")
            return True
        except Exception as e:
            if log_path:
                logger.log_event(
                    log_category="ERROR",
                    message=f"Failed to save data to {file_path}. Error: {e}",
                    path=log_path
                )
            print(f"Error saving to CSV: {e}")
            return False
