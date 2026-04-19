"""
Unit tests for pydantic validation models in validations.py
Tests all validation models and utility functions for runtime data validation
"""

import pytest
import sys
import os
from datetime import datetime
from typing import List, Dict, Any
import pandas as pd

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from validations import (
    CoinDataModel, CoinListResponse, OHLCVCandle, OHLCVData, 
    PriceChangeData, MarketBreadthData, TrendCounts, IndicatorData,
    OIChangeData, OIChangeList, MarketCapData, BinanceAPIRequest,
    BinanceAPIResponse, ValidationResult, validate_dataframe_schema, 
    validate_batch_data
)


# ============================================================================
# CoinDataModel Tests
# ============================================================================

class TestCoinDataModel:
    """Test suite for CoinDataModel validation"""

    def test_valid_coin_data(self):
        """Test CoinDataModel with valid data"""
        data = CoinDataModel(
            coin="BTC",
            market_cap=1000000000,
            category="Large Cap"
        )
        
        assert data.coin == "BTC"
        assert data.market_cap == 1000000000
        assert data.category == "Large Cap"

    def test_coin_symbol_uppercase_conversion(self):
        """Test that coin symbol is converted to uppercase"""
        data = CoinDataModel(
            coin="btc",
            market_cap="N/A",
            category="N/A"
        )
        
        assert data.coin == "BTC"

    def test_market_cap_as_string(self):
        """Test CoinDataModel accepts market cap as string"""
        data = CoinDataModel(
            coin="ETH",
            market_cap="500000000",
            category="Large Cap"
        )
        
        assert data.market_cap == "500000000"

    def test_coin_with_invalid_characters(self):
        """Test CoinDataModel rejects coin with invalid characters"""
        with pytest.raises(ValueError):
            CoinDataModel(
                coin="BTC-USD",
                market_cap=1000000000,
                category="Large Cap"
            )

    def test_coin_with_unicode_characters(self):
        """Test CoinDataModel rejects unicode characters"""
        with pytest.raises(ValueError):
            CoinDataModel(
                coin="BTC€",
                market_cap=1000000000,
                category="Large Cap"
            )

    def test_default_category_value(self):
        """Test that category has default value N/A"""
        data = CoinDataModel(
            coin="ADA",
            market_cap="N/A"
        )
        
        assert data.category == "N/A"


# ============================================================================
# CoinListResponse Tests
# ============================================================================

class TestCoinListResponse:
    """Test suite for CoinListResponse validation"""

    def test_valid_coin_list(self):
        """Test CoinListResponse with valid data"""
        response = CoinListResponse(
            coins=["BTC", "ETH", "BNB"],
            count=3
        )
        
        assert len(response.coins) == 3
        assert response.count == 3

    def test_coin_count_mismatch_raises_error(self):
        """Test that mismatched count raises error"""
        with pytest.raises(ValueError):
            CoinListResponse(
                coins=["BTC", "ETH", "BNB"],
                count=5
            )

    def test_count_not_provided(self):
        """Test that count can be optional"""
        response = CoinListResponse(coins=["BTC", "ETH"])
        
        assert response.count is None

    def test_empty_coins_raises_error(self):
        """Test that empty coin list raises error"""
        with pytest.raises(ValueError):
            CoinListResponse(coins=[])


# ============================================================================
# OHLCVCandle Tests
# ============================================================================

class TestOHLCVCandle:
    """Test suite for OHLCVCandle validation"""

    def test_valid_candle_data(self):
        """Test OHLCVCandle with valid OHLCV data"""
        candle = OHLCVCandle(
            timestamp=1704067200000,
            open=45000.0,
            high=45500.0,
            low=44500.0,
            close=45200.0,
            volume=1000000.0
        )
        
        assert candle.close == 45200.0
        assert candle.volume == 1000000.0

    def test_high_less_than_low_raises_error(self):
        """Test that high < low raises error"""
        with pytest.raises(ValueError):
            OHLCVCandle(
                timestamp=1704067200000,
                open=45000.0,
                high=44000.0,  # Low than low
                low=45000.0,
                close=45200.0,
                volume=1000000.0
            )

    def test_close_outside_high_low_raises_error(self):
        """Test that close outside [low, high] raises error"""
        with pytest.raises(ValueError):
            OHLCVCandle(
                timestamp=1704067200000,
                open=45000.0,
                high=45500.0,
                low=44500.0,
                close=46000.0,  # Above high
                volume=1000000.0
            )

    def test_zero_or_negative_prices_raise_error(self):
        """Test that zero or negative prices raise errors"""
        with pytest.raises(ValueError):
            OHLCVCandle(
                timestamp=1704067200000,
                open=0,  # Invalid
                high=45500.0,
                low=44500.0,
                close=45200.0,
                volume=1000000.0
            )

    def test_negative_volume_raises_error(self):
        """Test that negative volume raises error"""
        with pytest.raises(ValueError):
            OHLCVCandle(
                timestamp=1704067200000,
                open=45000.0,
                high=45500.0,
                low=44500.0,
                close=45200.0,
                volume=-100.0  # Invalid
            )

    def test_optional_fields(self):
        """Test that optional fields work correctly"""
        candle = OHLCVCandle(
            timestamp=1704067200000,
            open=45000.0,
            high=45500.0,
            low=44500.0,
            close=45200.0,
            volume=1000000.0,
            number_of_trades=500
        )
        
        assert candle.number_of_trades == 500


# ============================================================================
# PriceChangeData Tests
# ============================================================================

class TestPriceChangeData:
    """Test suite for PriceChangeData validation"""

    def test_valid_price_change_data(self):
        """Test PriceChangeData with valid data"""
        now = datetime.now()
        data = PriceChangeData(
            symbol="BTCUSDT",
            timestamp=now,
            close=45000.0,
            previous_close=44000.0,
            price_change=2.27,
            trend_category="uptrend",
            market_cap_category="Large Cap"
        )
        
        assert data.symbol == "BTCUSDT"
        assert data.price_change == 2.27

    def test_invalid_trend_category_raises_error(self):
        """Test that invalid trend category raises error"""
        with pytest.raises(ValueError):
            PriceChangeData(
                symbol="BTCUSDT",
                timestamp=datetime.now(),
                close=45000.0,
                previous_close=44000.0,
                price_change=2.27,
                trend_category="invalid_trend"
            )

    def test_valid_trend_categories(self):
        """Test all valid trend categories"""
        valid_trends = ["uptrend", "downtrend", "pullback", "reversal-up", "reversal-down", "uncategorized", "N/A"]
        
        for trend in valid_trends:
            data = PriceChangeData(
                symbol="BTCUSDT",
                timestamp=datetime.now(),
                close=45000.0,
                previous_close=44000.0,
                price_change=2.27,
                trend_category=trend
            )
            assert data.trend_category == trend

    def test_extreme_price_change_raises_error(self):
        """Test that extremely large price changes raise error"""
        with pytest.raises(ValueError):
            PriceChangeData(
                symbol="BTCUSDT",
                timestamp=datetime.now(),
                close=45000.0,
                previous_close=44000.0,
                price_change=2000.0  # Outside [-1000, 1000]
            )

    def test_price_change_boundary_values(self):
        """Test price change at boundary values"""
        # Exactly at boundary should work
        data = PriceChangeData(
            symbol="BTCUSDT",
            timestamp=datetime.now(),
            close=45000.0,
            previous_close=44000.0,
            price_change=999.99
        )
        assert data.price_change == 999.99
        
        data = PriceChangeData(
            symbol="BTCUSDT",
            timestamp=datetime.now(),
            close=100.0,
            previous_close=100.0,
            price_change=-999.99
        )
        assert data.price_change == -999.99


# ============================================================================
# MarketBreadthData Tests
# ============================================================================

class TestMarketBreadthData:
    """Test suite for MarketBreadthData validation"""

    def test_valid_market_breadth_data(self):
        """Test MarketBreadthData with valid data"""
        data = MarketBreadthData(
            timestamp=datetime.now(),
            total_coins=100,
            positive_coins=60,
            negative_coins=40,
            uptrend_count=40,
            downtrend_count=30,
            pullback_count=15,
            reversal_up_count=10,
            reversal_down_count=5
        )
        
        assert data.total_coins == 100
        assert data.positive_coins == 60

    def test_trend_counts_exceed_total_raises_error(self):
        """Test that trend counts exceeding total raises error"""
        with pytest.raises(ValueError):
            MarketBreadthData(
                timestamp=datetime.now(),
                total_coins=100,
                positive_coins=60,
                negative_coins=40,
                uptrend_count=50,  # Too high
                downtrend_count=60  # Combined exceed 100
            )

    def test_default_values(self):
        """Test that numeric fields have default values"""
        data = MarketBreadthData(
            timestamp=datetime.now(),
            total_coins=100,
            positive_coins=60,
            negative_coins=40
        )
        
        assert data.uptrend_count == 0
        assert data.downtrend_count == 0
        assert data.pullback_count == 0


# ============================================================================
# IndicatorData Tests
# ============================================================================

class TestIndicatorData:
    """Test suite for IndicatorData validation"""

    def test_valid_indicator_data(self):
        """Test IndicatorData with valid technical indicators"""
        data = IndicatorData(
            symbol="BTCUSDT",
            timestamp=datetime.now(),
            open=45000.0,
            high=45500.0,
            low=44500.0,
            close=45200.0,
            volume=1000000.0,
            sma20=44800.0,
            sma50=44000.0,
            sma100=43000.0,
            rsi14=65.5,
            volume_sma20=950000.0,
            trend="uptrend"
        )
        
        assert data.rsi14 == 65.5
        assert data.sma20 == 44800.0

    def test_rsi_range_validation(self):
        """Test that RSI is validated to be between 0 and 100"""
        with pytest.raises(ValueError):
            IndicatorData(
                symbol="BTCUSDT",
                timestamp=datetime.now(),
                open=45000.0,
                high=45500.0,
                low=44500.0,
                close=45200.0,
                volume=1000000.0,
                rsi14=150.0  # Invalid
            )

    def test_high_less_than_low_raises_error(self):
        """Test OHLC validation in IndicatorData"""
        with pytest.raises(ValueError):
            IndicatorData(
                symbol="BTCUSDT",
                timestamp=datetime.now(),
                open=45000.0,
                high=44000.0,  # Below low
                low=45000.0,
                close=45200.0,
                volume=1000000.0
            )

    def test_optional_indicators(self):
        """Test that indicators can be optional"""
        data = IndicatorData(
            symbol="BTCUSDT",
            timestamp=datetime.now(),
            open=45000.0,
            high=45500.0,
            low=44500.0,
            close=45200.0,
            volume=1000000.0
        )
        
        assert data.sma20 is None
        assert data.rsi14 is None


# ============================================================================
# OIChangeData Tests
# ============================================================================

class TestOIChangeData:
    """Test suite for OIChangeData validation"""

    def test_valid_oi_change_data(self):
        """Test OIChangeData with valid data"""
        data = OIChangeData(
            symbol="BTCUSDT",
            timestamp=datetime.now(),
            current_oi=1000000000.0,
            previous_oi=900000000.0,
            oi_change=11.11,
            oi_change_abs=100000000.0
        )
        
        assert data.oi_change == 11.11
        assert data.current_oi == 1000000000.0

    def test_oi_change_extreme_values_raise_error(self):
        """Test that extreme OI changes raise error"""
        with pytest.raises(ValueError):
            OIChangeData(
                symbol="BTCUSDT",
                timestamp=datetime.now(),
                current_oi=1000000000.0,
                previous_oi=900000000.0,
                oi_change=600.0  # Exceeds limit
            )

    def test_negative_oi_values_raise_error(self):
        """Test that negative OI values raise error"""
        with pytest.raises(ValueError):
            OIChangeData(
                symbol="BTCUSDT",
                timestamp=datetime.now(),
                current_oi=-1000000000.0,  # Invalid
                previous_oi=900000000.0,
                oi_change=11.11
            )


# ============================================================================
# BinanceAPIRequest Tests
# ============================================================================

class TestBinanceAPIRequest:
    """Test suite for BinanceAPIRequest validation"""

    def test_valid_api_request(self):
        """Test BinanceAPIRequest with valid parameters"""
        request = BinanceAPIRequest(
            symbol="BTCUSDT",
            interval="1h",
            limit=200
        )
        
        assert request.symbol == "BTCUSDT"
        assert request.interval == "1h"
        assert request.limit == 200

    def test_invalid_symbol_format_raises_error(self):
        """Test that invalid symbol format raises error"""
        with pytest.raises(ValueError):
            BinanceAPIRequest(
                symbol="BTC",  # Missing USDT
                interval="1h"
            )

    def test_invalid_interval_format_raises_error(self):
        """Test that invalid interval format raises error"""
        with pytest.raises(ValueError):
            BinanceAPIRequest(
                symbol="BTCUSDT",
                interval="invalid"
            )

    def test_valid_intervals(self):
        """Test various valid interval formats"""
        valid_intervals = ["1m", "5m", "1h", "1d", "1w"]
        
        for interval in valid_intervals:
            request = BinanceAPIRequest(
                symbol="BTCUSDT",
                interval=interval,
                limit=100
            )
            assert request.interval == interval

    def test_limit_boundaries(self):
        """Test limit parameter boundaries"""
        # Minimum limit
        request = BinanceAPIRequest(
            symbol="BTCUSDT",
            interval="1h",
            limit=1
        )
        assert request.limit == 1
        
        # Maximum limit
        request = BinanceAPIRequest(
            symbol="BTCUSDT",
            interval="1h",
            limit=1500
        )
        assert request.limit == 1500


# ============================================================================
# Utility Function Tests
# ============================================================================

class TestValidateDataframeSchema:
    """Test suite for validate_dataframe_schema utility"""

    def test_valid_dataframe_schema(self):
        """Test schema validation with valid DataFrame"""
        df = pd.DataFrame({
            'symbol': ['BTC', 'ETH'],
            'timestamp': ['2024-01-01', '2024-01-02'],
            'close': [45000.0, 2500.0]
        })
        
        result = validate_dataframe_schema(
            df,
            required_columns=['symbol', 'timestamp', 'close']
        )
        
        assert result.valid is True
        assert len(result.errors) == 0
        assert result.validated_count == 2

    def test_missing_required_columns(self):
        """Test schema validation with missing required columns"""
        df = pd.DataFrame({
            'symbol': ['BTC', 'ETH'],
            'timestamp': ['2024-01-01', '2024-01-02']
        })
        
        result = validate_dataframe_schema(
            df,
            required_columns=['symbol', 'timestamp', 'close']
        )
        
        assert result.valid is False
        assert len(result.errors) > 0
        assert 'close' in str(result.errors)

    def test_empty_dataframe(self):
        """Test schema validation with empty DataFrame"""
        df = pd.DataFrame()
        
        result = validate_dataframe_schema(
            df,
            required_columns=['symbol']
        )
        
        assert result.valid is False

    def test_optional_columns(self):
        """Test schema validation with optional columns"""
        df = pd.DataFrame({
            'symbol': ['BTC', 'ETH'],
            'timestamp': ['2024-01-01', '2024-01-02']
        })
        
        result = validate_dataframe_schema(
            df,
            required_columns=['symbol', 'timestamp'],
            optional_columns=['close', 'volume']
        )
        
        assert result.valid is True
        assert len(result.warnings) > 0


class TestValidateBatchData:
    """Test suite for validate_batch_data utility"""

    def test_valid_batch_data(self):
        """Test batch validation with all valid items"""
        data_list = [
            {'coin': 'BTC', 'market_cap': 1000000000, 'category': 'Large'},
            {'coin': 'ETH', 'market_cap': 500000000, 'category': 'Large'}
        ]
        
        result = validate_batch_data(data_list, CoinDataModel)
        
        assert result.valid is True
        assert result.validated_count == 2
        assert len(result.errors) == 0

    def test_batch_with_invalid_items(self):
        """Test batch validation with some invalid items"""
        data_list = [
            {'coin': 'BTC', 'market_cap': 1000000000, 'category': 'Large'},
            {'coin': 'BTC-USD', 'market_cap': 1000000000, 'category': 'Large'}  # Invalid
        ]
        
        result = validate_batch_data(data_list, CoinDataModel)
        
        assert result.valid is False
        assert len(result.errors) > 0
        assert result.validated_count == 1

    def test_fail_fast_mode(self):
        """Test batch validation with fail_fast=True"""
        data_list = [
            {'coin': 'BTC€', 'market_cap': 1000000000, 'category': 'Large'},  # Invalid
            {'coin': 'ETH€', 'market_cap': 500000000, 'category': 'Large'}   # Also invalid
        ]
        
        result = validate_batch_data(data_list, CoinDataModel, fail_fast=True)
        
        assert result.valid is False
        assert len(result.errors) == 1  # Only first error


# ============================================================================
# Integration Tests
# ============================================================================

class TestValidationIntegration:
    """Integration tests for validation across multiple models"""

    def test_price_change_to_market_breadth_flow(self):
        """Test validation flow from price changes to market breadth"""
        now = datetime.now()
        
        # Create valid price changes
        price_changes = [
            PriceChangeData(
                symbol="BTCUSDT",
                timestamp=now,
                close=45000.0,
                previous_close=44000.0,
                price_change=2.27,
                trend_category="uptrend"
            ),
            PriceChangeData(
                symbol="ETHUSDT",
                timestamp=now,
                close=2500.0,
                previous_close=2400.0,
                price_change=4.17,
                trend_category="uptrend"
            )
        ]
        
        # Create market breadth from price changes
        positive_count = len([p for p in price_changes if p.price_change > 0])
        total_count = len(price_changes)
        
        breadth = MarketBreadthData(
            timestamp=now,
            total_coins=total_count,
            positive_coins=positive_count,
            negative_coins=0,
            uptrend_count=2
        )
        
        assert breadth.positive_coins == 2
        assert breadth.total_coins == 2

    def test_ohlcv_to_indicator_flow(self):
        """Test validation flow from OHLCV to indicator data"""
        now = datetime.now()
        
        # Create valid OHLCV
        ohlcv = OHLCVCandle(
            timestamp=int(now.timestamp() * 1000),
            open=45000.0,
            high=45500.0,
            low=44500.0,
            close=45200.0,
            volume=1000000.0
        )
        
        # Create indicator from OHLCV
        indicator = IndicatorData(
            symbol="BTCUSDT",
            timestamp=now,
            open=ohlcv.open,
            high=ohlcv.high,
            low=ohlcv.low,
            close=ohlcv.close,
            volume=ohlcv.volume,
            sma20=44800.0,
            rsi14=65.5,
            trend="uptrend"
        )
        
        assert indicator.close == ohlcv.close
        assert indicator.rsi14 == 65.5


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
