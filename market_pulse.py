from scipy.stats import skew
import numpy as np
import pandas as pd
import os
from glob import glob
from collections import defaultdict
import matplotlib.pyplot as plt

import logger
from discord_integrator import upload_to_discord
from dotenv import load_dotenv

load_dotenv()

print(f"Running file: {__file__}")

script_path = os.path.dirname(os.path.abspath(__file__))
logfile_path = os.path.join(script_path, "logs", "market_pulse_logs.txt")
indicators_path = os.path.join(script_path, "indicators_data")
pulse_path = os.path.join(script_path, "hourly_market_pulse", "Market_Pulse.csv")
image_path = os.path.join(script_path, "hourly_market_pulse", "market_pulse.png")
rsi_sentiment_path = os.path.join(script_path, "hourly_market_pulse", "rsi_sentiment.png")

# Read webhook from environment; prefer setting MARKET_PULSE_WEBHOOK in .env or CI secrets.
discord_webhook_url = os.getenv("MARKET_PULSE_WEBHOOK", "https://discord.com/api/webhooks/1369672316887367761/zlxHjxikEEhSOK-TcRmz37jH-2kVl8NAiB_BIMdXd0TAco9DnfI5MYGa8Nuuy34poarQ")
if not os.getenv("MARKET_PULSE_WEBHOOK"):
    logger.log_event(log_category="WARNING", message="MARKET_PULSE_WEBHOOK not set; using fallback hard-coded webhook. Consider setting MARKET_PULSE_WEBHOOK in .env or CI secrets.", path=logfile_path)

rsi_values = []
coin_names = []

def calculate_percentage(numerator, denominator):
    result = numerator / denominator
    percentage = result * 100
    return "{:.2f} %".format(percentage)


trend_counter = defaultdict(lambda: {
    'uptrend': 0,
    'pullback': 0,
    'downtrend': 0,
    'reversal-up': 0,
    'reversal-down': 0,
    'uncategorized': 0
})

# Process CSV files and classify trends
for file in glob(os.path.join(indicators_path, '*.csv')):

    try:
        df = pd.read_csv(file, parse_dates=['timestamp'])
        logger.log_event(log_category="INFO", message=f"Reading {file}...", path=logfile_path)
    except Exception as e:
        logger.log_event(log_category="ERROR", message=f"Failed to read {file}. Error: [{e}]", path=logfile_path)

    def determine_trend(row):
        ma20, ma50, ma100, close, low = row['sma20'], row['sma50'], row['sma100'], row['close'], row['low']

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

    try:
        df['trend'] = df.apply(determine_trend, axis=1)

        for idx, row in df.iterrows():
            ts, trend = row['timestamp'], row['trend']
            trend_counter[ts][trend] += 1

        logger.log_event(log_category="INFO", message="Successfully applied trend category", path=logfile_path)
    except Exception as e:
        logger.log_event(log_category="ERROR", message=f"Failed to apply trend category. Error: [{e}]", path=logfile_path)

# Convert to DataFrame
try:
    trend_df = pd.DataFrame.from_dict(trend_counter, orient='index')
    trend_df.index.name = 'timestamp'
    trend_df = trend_df.sort_index()
    trend_df.to_csv(pulse_path, mode='w')
    logger.log_event(log_category="INFO", message=f"Successfully saved summary to {pulse_path}", path=logfile_path)
except Exception as e:
    logger.log_event(log_category="ERROR", message=f"Failed to save summary to {pulse_path}", path=logfile_path)

# Read last 100 rows for plotting
df = pd.read_csv(pulse_path, parse_dates=['timestamp']).sort_values('timestamp').tail(100)

latest = df.iloc[-1]
total = (latest['uptrend'] + latest['pullback'] + latest['downtrend'] + latest['reversal-down'] + latest['reversal-up']
         + latest['uncategorized'])


try:
    # Plot using matplotlib
    plt.figure(figsize=(12, 6))
    plt.plot(df['timestamp'], df['uptrend'], label=f"Uptrend - {latest['uptrend']} ({calculate_percentage(latest['uptrend'], total)})", color='green')
    plt.plot(df['timestamp'], df['pullback'], label=f"Pullback - {latest['pullback']} ({calculate_percentage(latest['pullback'], total)})", color='yellow')
    plt.plot(df['timestamp'], df['downtrend'], label=f"Downtrend - {latest['downtrend']} ({calculate_percentage(latest['downtrend'], total)})", color='red')
    plt.plot(df['timestamp'], df['reversal-down'], label=f"Reversing down - {latest['reversal-down']} ({calculate_percentage(latest['reversal-down'], total)})", color='orange')
    plt.plot(df['timestamp'], df['reversal-up'], label=f"Reversing up - {latest['reversal-up']} ({calculate_percentage(latest['reversal-up'], total)})", color='blue')
    plt.plot(df['timestamp'], df['uncategorized'], label=f"Uncategorized - {latest['uncategorized']} ({calculate_percentage(latest['uncategorized'], total)})", color='gray')

    plt.title('Hourly Market hourly_market_pulse')
    plt.xlabel('Timestamp')
    plt.ylabel('Number of Symbols')
    plt.xticks(rotation=45)
    plt.legend(loc='upper left')
    plt.tight_layout()

    # Save to PNG
    plt.savefig(image_path)
    plt.close()

    logger.log_event(log_category="INFO", message=f"Successfully saved market pulse summary to {image_path}", path=logfile_path)
except Exception as e:
    logger.log_event(log_category="ERROR", message=f"Failed to save market pulse summary to {image_path}. Error: [{e}]", path=logfile_path)

try:
    upload_to_discord(discord_webhook_url, image_path=image_path)
    logger.log_event(log_category="INFO", message="Successfully uploaded image to discord...", path=logfile_path)
except Exception as e:
    logger.log_event(log_category="ERROR", message=f"Failed to upload image to discord. Error: [{e}]", path=logfile_path)


for file in glob(os.path.join(indicators_path, '*.csv')):

    file_name = os.path.basename(file)
    coin = file_name.replace("_indicators.csv", "")

    try:
        df = pd.read_csv(file, parse_dates=['timestamp'])
        logger.log_event(log_category="INFO", message=f"Reading {file}...", path=logfile_path)
    except Exception as e:
        logger.log_event(log_category="ERROR", message=f"Failed to read {file}. Error: [{e}]", path=logfile_path)

    last_row = df.iloc[-1]

    if "rsi14" in last_row:
        rsi_values.append(last_row["rsi14"])
        coin_names.append(coin)

plt.figure(figsize=(12,6))
plt.hist(rsi_values, bins=30, color="green", edgecolor="white", alpha=0.7)

plt.axvline(70, color="red", linestyle="--", linewidth=0.5)
plt.axvline(30, color="green", linestyle="--", linewidth=0.5)
plt.axvline(50, color="gray", linestyle="--", linewidth=0.5)

mean_rsi = np.mean(rsi_values)
median_rsi = np.median(rsi_values)

total_count = len(rsi_values)
above_rsi50 = 0
below_rsi50 = 0
above_rsi70 = 0
below_rsi30 = 0

for rsi in rsi_values:
    if 50 < rsi < 70:
        above_rsi50 = above_rsi50 + 1
    elif 50 > rsi < 30:
        below_rsi50 = below_rsi50 + 1
    elif rsi > 70:
        above_rsi70 = above_rsi70 + 1
    elif rsi < 30:
        below_rsi30 = below_rsi30 + 1

rsi_arr = np.array(rsi_values)
rsi_skew = skew(rsi_arr)

if rsi_skew < -0.5:
    sentiment = "Bullish"
elif rsi_skew > 0.5:
    sentiment = "Bearish"
else:
    sentiment = "Neutral"

plt.axvline(mean_rsi, color="blue", linestyle="-.", linewidth=1, label=f"Mean RSI = {mean_rsi:.2f}")
plt.axvline(median_rsi, color="purple", linestyle="-.", linewidth=1, label=f"Median RSI = {median_rsi:.2f}")


plt.title(f"Hourly Market Sentiment: {sentiment} (Skew = {rsi_skew:.2f})")
plt.xlabel("RSI(14)")
plt.ylabel("Number of Coins")
plt.legend()

plt.savefig(rsi_sentiment_path)
plt.close()

try:
    upload_to_discord(discord_webhook_url, image_path=rsi_sentiment_path)
    logger.log_event(log_category="INFO", message="Successfully uploaded image to discord...", path=logfile_path)
except Exception as e:
    logger.log_event(log_category="ERROR", message=f"Failed to upload image to discord. Error: [{e}]", path=logfile_path)


