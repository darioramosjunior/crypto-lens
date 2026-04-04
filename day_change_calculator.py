import os
from dotenv import load_dotenv
load_dotenv()
from discord_integrator import send_to_discord
import pandas as pd
import glob
from itertools import islice
from datetime import datetime
import csv


script_dir = os.path.dirname(os.path.abspath(__file__))
indicators_path = os.path.join(script_dir, "indicators_data")
daily_data_path = os.path.join(script_dir, "daily_data")
daily_change_csv = os.path.join(script_dir, "day_change.csv")
market_cap_csv = os.path.join(script_dir, "market_cap_data.csv")
# Read webhook from environment (DAY_CHANGE_WEBHOOK); fallback kept for backward-compatibility
discord_webhook_url = os.getenv("DAY_CHANGE_WEBHOOK", "https://discord.com/api/webhooks/1375363041599950912/3vx1qI7OQAoIwz4TFV4hhwiK1uIZkr_vu3peoBvn4PO0YpF8z4yN410HC9kJkD4NhSWH")
if not os.getenv("DAY_CHANGE_WEBHOOK"):
    print("Warning: DAY_CHANGE_WEBHOOK not set; using fallback hard-coded webhook. Consider setting DAY_CHANGE_WEBHOOK in .env or CI secrets.")

day_change_dict = {}
category_dict = {}


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
            return category_map
        else:
            print(f"Warning: {market_cap_csv_path} not found. Using N/A for all categories.")
            return {}
    except Exception as e:
        print(f"Error loading category data: {e}")
        return {}


def get_day_change_daily(csv):
    """
    Calculate daily change as: (today's close - yesterday's close) / yesterday's close * 100
    Reads from daily OHLCV data files where each row represents one day.
    """
    df = pd.read_csv(csv)
    df.columns = df.columns.str.strip()
    
    if len(df) < 2:
        # Not enough data to calculate day change
        return 0.0
    
    today_row = df.iloc[-1]
    yesterday_row = df.iloc[-2]

    today_close = today_row.get('close')
    yesterday_close = yesterday_row.get('close')

    if today_close is None or yesterday_close is None or pd.isna(today_close) or pd.isna(yesterday_close):
        return 0.0

    day_change_percent = ((today_close - yesterday_close) / yesterday_close) * 100
    return round(float(day_change_percent), 2)


def sort_top_gainers(day_change_dict):
    sorted_day_change = dict(sorted(day_change_dict.items(), key=lambda item: item[1], reverse=True))
    return sorted_day_change


def sort_top_losers(day_change_dict):
    sorted_day_change = dict(sorted(day_change_dict.items(), key=lambda item: item[1], reverse=False))
    return sorted_day_change


def format_message(input_dict, gainers=True, category_data=None):
    title = ""

    if gainers:
        title = "===== TOP GAINERS ====="
    else:
        title = "===== TOP LOSERS ====="

    now = datetime.now()

    message = f"{now}\n{title}\n"
    for key in input_dict:
        coin = key
        percent_change = input_dict[key]
        category = category_data.get(coin, 'N/A') if category_data else 'N/A'
        message = message + f"{coin} - {percent_change}% [{category}]\n"
    return message


def update_market_cap_with_day_change(market_cap_csv_path, day_change_dict):
    """
    Update market_cap_data.csv with day_change_percent column.
    Reads the existing CSV, adds/updates the day_change_percent column, and saves it back.
    """
    try:
        # Read existing market_cap_data.csv
        if os.path.exists(market_cap_csv_path):
            df = pd.read_csv(market_cap_csv_path)
            df.columns = df.columns.str.strip()
        else:
            print(f"Warning: {market_cap_csv_path} not found.")
            return
        
        # Add or update day_change_percent column
        df['day_change_percent'] = df['coin'].apply(
            lambda coin: day_change_dict.get(coin, 0.0)
        )
        
        # Save updated CSV
        df.to_csv(market_cap_csv_path, index=False)
        print(f"Updated {market_cap_csv_path} with day_change_percent column")
    except Exception as e:
        print(f"Error updating market cap CSV with day change: {e}")

if __name__ == "__main__":
    # Load category data from market_cap_data.csv
    category_dict = load_category_data(market_cap_csv)
    print(f"Loaded categories for {len(category_dict)} coins")

    for path in glob.glob(os.path.join(daily_data_path, "*.csv")):
        file_name = os.path.basename(path)
        coin_name = file_name.replace(".csv","")
        day_change_percent = get_day_change_daily(path)
        day_change_dict[coin_name] = day_change_percent

    print(day_change_dict)

    sorted_top_gainers = sort_top_gainers(day_change_dict)
    sorted_top_losers = sort_top_losers(day_change_dict)

    csv_columns = ["COIN", "Day Change", "Category"]

    with open(daily_change_csv, 'w', newline="") as file:
        writer = csv.writer(file)

        writer.writerow(["COIN", "Day Change", "Category"])

        for coin, change in sorted_top_gainers.items():
            category = category_dict.get(coin, 'N/A')
            writer.writerow([coin, change, category])

    # Update market_cap_data.csv with day change percentages
    update_market_cap_with_day_change(market_cap_csv, day_change_dict)

    top_30_gainers = dict(islice(sorted_top_gainers.items(), 30))
    top_30_losers = dict(islice(sorted_top_losers.items(), 30))

    print(sorted_top_gainers)
    print(top_30_gainers)
    print(top_30_losers)

    top_30_gainers_formatted = format_message(top_30_gainers, gainers=True, category_data=category_dict)
    print(top_30_gainers_formatted)

    top_30_losers_formatted = format_message(top_30_losers, gainers=False, category_data=category_dict)
    print(top_30_losers_formatted)

    full_message = top_30_gainers_formatted + "\n\n" + top_30_losers_formatted
    print(full_message)

    send_to_discord(discord_webhook_url, message=full_message)

