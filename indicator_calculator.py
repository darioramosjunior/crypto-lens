from datetime import timedelta

import vectorbt as vbt
import pandas as pd
import pandas_ta as ta
import os
import logger

script_dir = os.path.dirname(os.path.abspath(__file__))
log_path = os.path.join(script_dir, "logs", "indicator_calculator_logs.txt")
indicators_path = os.path.join(script_dir, "indicators_data")
coin_list_path = os.path.join(script_dir, "coin_list.txt")
data_path = os.path.join(script_dir, "hourly_data")


def calculate_and_save_indicators(symbol):
    try:
        symbol_csv = os.path.join(data_path, f"{symbol}.csv")
        df = pd.read_csv(symbol_csv)

        df["timestamp"] = pd.to_datetime(df["timestamp"])
        df["date_only"] = df["timestamp"].dt.date
        df.set_index("timestamp", inplace=True)

        close = df['close']
        df['sma20'] = vbt.MA.run(close, window=20).ma
        df['sma50'] = vbt.MA.run(close, window=50).ma
        df['sma100'] = vbt.MA.run(close, window=100).ma
        # df['rsi14'] = vbt.RSI.run(close, window=14).rsi
        # This one matches with tradingview instead of using vbt
        df['rsi14'] = ta.rsi(close, length=14)

        day_open = df.groupby('date_only')['open'].transform('first')
        df['day_change_percent'] = ((df['close'] - day_open) / day_open) * 100

        volume = df['volume']
        df['volume_sma20'] = vbt.MA.run(volume, window=20).ma

        indicators_csv = os.path.join(indicators_path, f"{symbol}_indicators.csv")
        df.to_csv(indicators_csv, mode='w')
        logger.log_event(log_category="INFO", message=f"Successfully saved indicators for symbol {symbol}", path=log_path)
    except Exception as e:
        logger.log_event(log_category="ERROR", message=f"Failed to calculate & save indicators for symbol {symbol}. Error={e}", path=log_path)


# def calculate_day_status(dataframe):
#     for index, row in dataframe.iloc[::-1].iterrows():
#         current_timestamp = row['timestamp']
#         current_date_only = pd.to_datetime(current_timestamp, unit='ms').date()
#         previous_day_date = current_date_only - timedelta(days=1)
#
#     return dataframe

if __name__ == "__main__":
    print(f"Running {__file__}...")

