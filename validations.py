"""
Pydantic validation models for runtime data validation across all collectors/fetchers.
Ensures data integrity and type safety for all data operations.
"""

from pydantic import BaseModel, Field, field_validator, model_validator
from typing import List, Dict, Any, Optional, Union
from datetime import datetime
import pandas as pd


# ============================================================================
# Coin Data Models
# ============================================================================

class CoinDataModel(BaseModel):
    """Validates coin data from coin_data_collector.py"""
    coin: str = Field(..., min_length=1, max_length=20, description="Coin symbol")
    market_cap: Union[str, float] = Field(..., description="Market cap value or 'N/A'")
    category: str = Field(default="N/A", max_length=100, description="Market cap category")
    
    class Config:
        use_enum_values = True
        validate_assignment = True
    
    @field_validator('coin')
    @classmethod
    def validate_coin_symbol(cls, v):
        """Validate coin symbol contains only ASCII alphanumeric"""
        if not all(c.isascii() and c.isalnum() for c in v):
            raise ValueError("Coin symbol must contain only ASCII alphanumeric characters")
        return v.upper()


class CoinListResponse(BaseModel):
    """Validates list of coins returned from data sources"""
    coins: List[str] = Field(..., min_items=1, description="List of coin symbols")
    count: Optional[int] = Field(None, ge=0, description="Total number of coins")
    
    @model_validator(mode='after')
    def validate_count(self):
        """Verify count matches list length if provided"""
        if self.count and self.count != len(self.coins):
            raise ValueError("Count must match length of coins list")
        return self


# ============================================================================
# OHLCV Data Models (Binance API responses)
# ============================================================================

class OHLCVCandle(BaseModel):
    """Validates individual OHLCV candle data from Binance"""
    timestamp: int = Field(..., ge=0, description="Timestamp in milliseconds")
    open: float = Field(..., gt=0, description="Open price")
    high: float = Field(..., gt=0, description="High price")
    low: float = Field(..., gt=0, description="Low price")
    close: float = Field(..., gt=0, description="Close price")
    volume: float = Field(..., ge=0, description="Volume")
    close_time: Optional[int] = Field(None, ge=0, description="Close time in milliseconds")
    quote_asset_volume: Optional[float] = Field(None, ge=0, description="Quote asset volume")
    number_of_trades: Optional[int] = Field(None, ge=0, description="Number of trades")
    taker_buy_base_volume: Optional[float] = Field(None, ge=0, description="Taker buy base volume")
    taker_buy_quote_volume: Optional[float] = Field(None, ge=0, description="Taker buy quote volume")
    
    @field_validator('high')
    @classmethod
    def validate_high(cls, v, info):
        """High should be >= low"""
        if 'low' in info.data:
            if v < info.data['low']:
                raise ValueError("High price must be >= low price")
        return v
    
    @field_validator('close')
    @classmethod
    def validate_close(cls, v, info):
        """Close should be between high and low"""
        if 'high' in info.data and 'low' in info.data:
            if not (info.data['low'] <= v <= info.data['high']):
                raise ValueError("Close price must be between low and high")
        return v


class OHLCVData(BaseModel):
    """Validates raw OHLCV data from API"""
    symbol: str = Field(..., min_length=3, description="Trading symbol")
    interval: str = Field(..., regex=r"^(\d+[mhd])$", description="Timeframe (e.g., 1h, 1d)")
    candles: List[Union[List, OHLCVCandle]] = Field(..., min_items=1, description="List of candles")
    
    class Config:
        arbitrary_types_allowed = True
    
    @field_validator('symbol')
    @classmethod
    def validate_symbol(cls, v):
        """Validate symbol format"""
        if not v.isupper():
            raise ValueError("Symbol must be uppercase")
        return v


# ============================================================================
# Price Change Data Models
# ============================================================================

class PriceChangeData(BaseModel):
    """Validates price change calculations"""
    symbol: str = Field(..., min_length=3, max_length=20, description="Trading symbol")
    timestamp: datetime = Field(..., description="Data timestamp")
    close: float = Field(..., gt=0, description="Close price")
    previous_close: float = Field(..., gt=0, description="Previous close price")
    price_change: float = Field(..., description="Price change percentage")
    trend_category: str = Field(
        default="N/A",
        regex=r"^(uptrend|downtrend|pullback|reversal-up|reversal-down|uncategorized|N/A)$",
        description="Trend category"
    )
    market_cap_category: str = Field(default="N/A", description="Market cap category")
    
    class Config:
        validate_assignment = True
    
    @field_validator('price_change')
    @classmethod
    def validate_price_change(cls, v):
        """Validate price change is reasonable (not inf or nan)"""
        if not (-1000 < v < 1000):  # Allow large swings but catch inf/nan
            raise ValueError("Price change must be between -1000% and 1000%")
        return v


class PriceChangeList(BaseModel):
    """Validates list of price changes"""
    data: List[PriceChangeData] = Field(..., min_items=1)
    timestamp: Optional[datetime] = Field(None)


# ============================================================================
# Market Breadth & Sentiment Models
# ============================================================================

class MarketBreadthData(BaseModel):
    """Validates market breadth metrics"""
    timestamp: datetime = Field(..., description="Data timestamp")
    total_coins: int = Field(..., ge=1, description="Total number of coins analyzed")
    positive_coins: int = Field(..., ge=0, le=lambda: 100, description="Coins with positive change")
    negative_coins: int = Field(..., ge=0, description="Coins with negative change")
    unchanged_coins: int = Field(default=0, ge=0, description="Coins with no change")
    uptrend_count: int = Field(default=0, ge=0, description="Coins in uptrend")
    downtrend_count: int = Field(default=0, ge=0, description="Coins in downtrend")
    pullback_count: int = Field(default=0, ge=0, description="Coins in pullback")
    reversal_up_count: int = Field(default=0, ge=0, description="Coins in reversal up")
    reversal_down_count: int = Field(default=0, ge=0, description="Coins in reversal down")
    uncategorized_count: int = Field(default=0, ge=0, description="Coins uncategorized")
    btc_change: Optional[float] = Field(None, description="BTC price change %")
    btc_dominance_change: Optional[float] = Field(None, description="BTC dominance change %")
    
    @model_validator(mode='after')
    def validate_totals(self):
        """Validate that trend counts don't exceed total coins"""
        total = self.total_coins
        trend_total = (
            self.uptrend_count +
            self.downtrend_count +
            self.pullback_count +
            self.reversal_up_count +
            self.reversal_down_count +
            self.uncategorized_count
        )
        if trend_total > total:
            raise ValueError("Sum of trend counts cannot exceed total coins")
        return self


class TrendCounts(BaseModel):
    """Validates trend count per timestamp"""
    timestamp: datetime = Field(..., index=True)
    uptrend: int = Field(default=0, ge=0)
    pullback: int = Field(default=0, ge=0)
    downtrend: int = Field(default=0, ge=0)
    reversal_up: int = Field(default=0, ge=0)
    reversal_down: int = Field(default=0, ge=0)
    uncategorized: int = Field(default=0, ge=0)


# ============================================================================
# Technical Indicator Models
# ============================================================================

class IndicatorData(BaseModel):
    """Validates technical indicator calculations"""
    symbol: str = Field(..., description="Trading symbol")
    timestamp: datetime = Field(..., description="Data timestamp")
    open: float = Field(..., gt=0)
    high: float = Field(..., gt=0)
    low: float = Field(..., gt=0)
    close: float = Field(..., gt=0)
    volume: float = Field(..., ge=0)
    sma20: Optional[float] = Field(None, description="20-period SMA")
    sma50: Optional[float] = Field(None, description="50-period SMA")
    sma100: Optional[float] = Field(None, description="100-period SMA")
    rsi14: Optional[float] = Field(None, ge=0, le=100, description="14-period RSI")
    volume_sma20: Optional[float] = Field(None, ge=0, description="20-period volume SMA")
    trend: Optional[str] = Field(
        None,
        regex=r"^(uptrend|downtrend|pullback|reversal-up|reversal-down|uncategorized)$"
    )
    
    @field_validator('high')
    @classmethod
    def validate_high(cls, v, info):
        """High >= low and >= close"""
        if 'low' in info.data and v < info.data['low']:
            raise ValueError("High must be >= low")
        if 'close' in info.data and v < info.data['close']:
            raise ValueError("High must be >= close")
        return v
    
    @field_validator('low')
    @classmethod
    def validate_low(cls, v, info):
        """Low <= open, close, and high"""
        if 'close' in info.data and v > info.data['close']:
            raise ValueError("Low must be <= close")
        return v


# ============================================================================
# Open Interest Change Models
# ============================================================================

class OIChangeData(BaseModel):
    """Validates open interest change data"""
    symbol: str = Field(..., description="Trading symbol")
    timestamp: datetime = Field(..., description="Data timestamp")
    current_oi: float = Field(..., ge=0, description="Current open interest")
    previous_oi: float = Field(..., ge=0, description="Previous open interest")
    oi_change: float = Field(..., description="OI change percentage")
    oi_change_abs: float = Field(..., description="Absolute OI change")
    market_cap_category: str = Field(default="N/A")
    
    @field_validator('oi_change')
    @classmethod
    def validate_oi_change(cls, v):
        """Validate OI change is reasonable"""
        if not (-500 < v < 500):  # Allow for large moves
            raise ValueError("OI change must be between -500% and 500%")
        return v


class OIChangeList(BaseModel):
    """Validates list of OI changes"""
    data: List[OIChangeData] = Field(..., min_items=1)
    timestamp: Optional[datetime] = Field(None)
    total_analyzed: Optional[int] = Field(None)


# ============================================================================
# Market Cap Data Models
# ============================================================================

class MarketCapData(BaseModel):
    """Validates market cap information"""
    coin: str = Field(..., description="Coin symbol")
    market_cap: Optional[Union[float, str]] = Field(None, description="Market cap value")
    category: str = Field(default="N/A", description="Market cap category")
    
    @field_validator('market_cap')
    @classmethod
    def validate_market_cap(cls, v):
        """Market cap should be positive if numeric"""
        if isinstance(v, (int, float)) and v < 0:
            raise ValueError("Market cap must be positive")
        return v


class MarketCapDataDict(BaseModel):
    """Validates dictionary of market cap data"""
    data: Dict[str, Optional[float]] = Field(..., description="Coin -> market cap mapping")
    
    class Config:
        arbitrary_types_allowed = True


# ============================================================================
# API Request/Response Models
# ============================================================================

class BinanceAPIRequest(BaseModel):
    """Validates Binance API request parameters"""
    symbol: str = Field(..., regex=r"^[A-Z0-9]+USDT$")
    interval: str = Field(..., regex=r"^(\d+[mhd])$")
    limit: int = Field(default=200, ge=1, le=1500)
    
    class Config:
        validate_assignment = True


class BinanceAPIResponse(BaseModel):
    """Validates Binance API response structure"""
    symbol: str = Field(...)
    interval: str = Field(...)
    data: List[List[Union[int, str, float]]] = Field(..., min_items=1)
    status_code: Optional[int] = Field(None, ge=200, le=299)
    error: Optional[str] = Field(None)


# ============================================================================
# Utility Validation Functions
# ============================================================================

class ValidationResult(BaseModel):
    """Result of validation operation"""
    valid: bool = Field(...)
    errors: List[str] = Field(default_factory=list)
    warnings: List[str] = Field(default_factory=list)
    validated_count: int = Field(default=0)


def validate_dataframe_schema(
    df: pd.DataFrame,
    required_columns: List[str],
    optional_columns: Optional[List[str]] = None
) -> ValidationResult:
    """
    Validate DataFrame schema matches requirements
    :param df: Pandas DataFrame
    :param required_columns: List of required column names
    :param optional_columns: List of optional column names
    :return: ValidationResult with errors and warnings
    """
    errors = []
    warnings = []
    
    if df is None or df.empty:
        errors.append("DataFrame is None or empty")
        return ValidationResult(valid=False, errors=errors)
    
    missing_columns = set(required_columns) - set(df.columns)
    if missing_columns:
        errors.append(f"Missing required columns: {missing_columns}")
    
    if optional_columns:
        missing_optional = set(optional_columns) - set(df.columns)
        if missing_optional:
            warnings.append(f"Missing optional columns: {missing_optional}")
    
    return ValidationResult(
        valid=len(errors) == 0,
        errors=errors,
        warnings=warnings,
        validated_count=len(df)
    )


def validate_batch_data(
    data_list: List[Dict[str, Any]],
    model: type,
    fail_fast: bool = False
) -> ValidationResult:
    """
    Validate a batch of data items against a Pydantic model
    :param data_list: List of data dictionaries
    :param model: Pydantic model class
    :param fail_fast: Stop on first error
    :return: ValidationResult
    """
    errors = []
    valid_count = 0
    
    for idx, item in enumerate(data_list):
        try:
            model.parse_obj(item)
            valid_count += 1
        except Exception as e:
            error_msg = f"Item {idx}: {str(e)}"
            errors.append(error_msg)
            if fail_fast:
                break
    
    return ValidationResult(
        valid=len(errors) == 0,
        errors=errors,
        validated_count=valid_count
    )
