import pandas as pd
import os
from glob import glob
import matplotlib.pyplot as plt
from discord_integrator import upload_to_discord
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

script_path = os.path.dirname(os.path.abspath(__file__))
indicators_path = os.path.join(script_path, "indicators_data")
daily_ohlcv_path = os.path.join(script_path, "ohlcv_categorization")
daily_ohlcv_pulse = os.path.join(daily_ohlcv_path, "daily_ohlcv.png")

# Read webhook from environment (DAILY_OHLCV_WEBHOOK); fallback kept for backward-compatibility
discord_webhook_url = os.getenv("DAILY_OHLCV_WEBHOOK", "https://discord.com/api/webhooks/1369672316887367761/zlxHjxikEEhSOK-TcRmz37jH-2kVl8NAiB_BIMdXd0TAco9DnfI5MYGa8Nuuy34poarQ")
if not os.getenv("DAILY_OHLCV_WEBHOOK"):
    print("Warning: DAILY_OHLCV_WEBHOOK not set; using fallback hard-coded webhook. Consider setting DAILY_OHLCV_WEBHOOK in .env or CI secrets.")

colors = '#00B894'
plt.rcParams['font.family'] = 'sans-serif'


def read_file(path):
    with open(path, 'r') as file:
        content = file.read()
        return content


if __name__ == "__main__":
    for path in glob(os.path.join(indicators_path, "*.csv")):
        filename = os.path.basename(path)
        new_filename = filename.replace("_indicators", "_daily_ohlcv")
        output_path = os.path.join(daily_ohlcv_path, new_filename)

        df = pd.read_csv(path, parse_dates=['timestamp'])

        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df.set_index('timestamp', inplace=True)

        df['date'] = df.index.date

        daily_high = df.groupby('date')['high'].cummax()
        daily_low = df.groupby('date')['low'].cummin()

        daily_summary = df.groupby('date').agg({
            'high': 'max',
            'low': 'min',
            'close': 'last'
        }).shift(1)

        df['prev_day_high'] = df['date'].map(daily_summary['high'])
        df['prev_day_low'] = df['date'].map(daily_summary['low'])
        df['prev_day_close'] = df['date'].map(daily_summary['close'])

        df['day_high_so_far'] = daily_high
        df['day_low_so_far'] = daily_low
        df['current_close'] = df['close']

        def categorize(row):
            if pd.isna(row['prev_day_high']) or pd.isna(row['prev_day_low']) or pd.isna(row['prev_day_close']):
                return 'NoPrevDayData'

            day_high = row['day_high_so_far']
            day_low = row['day_low_so_far']
            close = row['current_close']
            prev_d_high = row['prev_day_high']
            prev_d_low = row['prev_day_low']
            prev_d_close = row['prev_day_close']

            bull_day = (day_high > prev_d_high) and (close > prev_d_high)
            bear_sweep = (day_high > prev_d_high) and (close < prev_d_high)
            bear_day = (day_low < prev_d_low) and (close < prev_d_low)
            bull_sweep = (day_low < prev_d_low) and (close > prev_d_low)
            bull_inside_day = (day_high < prev_d_high) and (day_low > prev_d_low) and (close > prev_d_close)
            bear_inside_day = (day_high < prev_d_high) and (day_low > prev_d_low) and (close < prev_d_close)

            if bull_day:
                return 'Bullish Day'
            elif bear_sweep:
                return 'Bearish Sweep'
            elif bear_day:
                return 'Bearish Day'
            elif bull_sweep:
                return 'Bullish Sweep'
            elif bull_inside_day:
                return 'Bullish Inside Day'
            elif bear_inside_day:
                return 'Bearish Inside Day'
            else:
                return 'Uncategorized'

        df['category'] = df.apply(categorize, axis=1)

        df.to_csv(output_path)

    csv_files = glob(os.path.join(daily_ohlcv_path, "*.csv"))

    df_list = [pd.read_csv(path).tail(100) for path in csv_files]
    combined_df = pd.concat(df_list, ignore_index=True)
    combined_df['timestamp'] = pd.to_datetime(combined_df['timestamp'])

    category_counts = (
        combined_df.groupby(['timestamp', 'category'])
        .size()
        .unstack(fill_value=0)
    )

    latest_data = category_counts.iloc[-1]
    sorted_latest_data = latest_data.sort_values(ascending=False)
    sorted_latest_data = sorted_latest_data[sorted_latest_data > 0]
    print(sorted_latest_data)

    plt.figure(figsize=(10, 6))
    sorted_latest_data.plot(kind='bar', color=colors, edgecolor='white', linewidth=0.5)
    plt.title(f'Category Count at {sorted_latest_data.name}', fontsize=14, fontweight='bold')
    plt.xlabel('Category', fontsize=12)
    plt.ylabel('Count', fontsize=12)
    plt.xticks(rotation=45, ha='right')
    plt.yticks(fontsize=10)
    plt.grid(axis='y', linestyle='--', alpha=0.3)

    # Set white background
    plt.gca().set_facecolor('white')
    plt.gcf().set_facecolor('white')

    plt.tight_layout()

    # Save the plot to a PNG file
    plt.savefig(daily_ohlcv_pulse)

    now = datetime.now()

    try:
        upload_to_discord(discord_webhook_url, image_path=daily_ohlcv_pulse, message=f"{now} - Daily ohlcv_categorization")
    except Exception as e:
        print(f"Failed to upload to discord. Error: {e}")


