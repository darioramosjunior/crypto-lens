import os
from glob import glob
import pandas as pd
from discord_integrator import send_to_discord
import logger

from dotenv import load_dotenv
load_dotenv()

script_path = os.path.dirname(os.path.abspath(__file__))
logs_path = os.path.join(script_path, "logs")
logfile_path = os.path.join(logs_path, "screener_logs.txt")
data_daily_path = os.path.join(script_path, "daily_data")
indicators_path = os.path.join(script_path, "indicators_data")
data_4h_path = os.path.join(script_path, "four-hour_data")
screener_path = os.path.join(script_path, "Screener")

# Read webhooks from environment (SCREENER_WEBHOOK_DAILY, SCREENER_WEBHOOK_4H)
webhook_url_daily = os.getenv("SCREENER_WEBHOOK_DAILY", "https://discord.com/api/webhooks/1379449817436131360/SCpFFQClIzmwmMOPuj83EFh9roLDyiiZ8wpDmkUYGnjcO6Ip-eSqsX8rVnO-Wiw2RMp9")
webhook_url_4h = os.getenv("SCREENER_WEBHOOK_4H", "https://discord.com/api/webhooks/1380159262348677120/Wm5fkQB5KMN_jAgP4vCkmJJsKjJ4UrrjLrr_NXtUKepQkRDnRsfH4JcFiNqVM4Hcnp-l")
if not os.getenv("SCREENER_WEBHOOK_DAILY") or not os.getenv("SCREENER_WEBHOOK_4H"):
    logger.log_event(log_category="WARNING", message="One or more SCREENER webhooks not set; using fallback hard-coded webhooks. Consider setting SCREENER_WEBHOOK_DAILY and SCREENER_WEBHOOK_4H in .env or CI secrets.", path=logfile_path)

data_path_mapping = {
    '1d': data_daily_path,
    '1h': indicators_path,
    '4h': data_4h_path
}

rsi50_daily_coins = []
rsi50_4h_coins = []
rsi50_hourly_coins = []
rsi50_daily_hourly = []


def get_symbol(path):
    filename = os.path.basename(path)
    coin = filename.replace(".csv", "")
    return coin


def format_list(list):
    message = ""
    for item in list:
        message += f"{item}\n"

    return message


def get_rsi50_coins(timeframe, mode="normal"):
    """
    Get the coins that currently have an RSI50 in a variable timeframe
    :param timeframe: expects - '1d', '1h', '4h'
    :param mode: default - normal, other option is enhanced
    :return: list
    """

    rsi50_coins = []
    data_directory = data_path_mapping[timeframe]

    for path in glob(os.path.join(data_directory, '*.csv')):
        symbol = get_symbol(path)

        logger.log_event(log_category="INFO", message=f"Reading {path}...", path=logfile_path)
        df = pd.read_csv(path)
        rsi_value = df["rsi14"].iloc[-1]

        if timeframe in ['1d', '4h']:
            if 50 < rsi_value < 55:
                rsi50_coins.append(symbol)
        elif timeframe == '1h':
            if 48 < rsi_value < 55:
                rsi50_coins.append(symbol)

    return rsi50_coins


if __name__ == "__main__":
    print(f"Running {__file__}...")

    try:
        rsi50_daily_coins = get_rsi50_coins('1d')
    except Exception as e:
        logger.log_event(log_category="ERROR", message=f"Failed to process daily data. Error: {e}", path=logfile_path)

    try:
        rsi50_hourly_coins = get_rsi50_coins('1h')
    except Exception as e:
        logger.log_event(log_category="ERROR", message=f"Failed to process hourly data. Error: {e}", path=logfile_path)

    try:
        rsi50_4h_coins = get_rsi50_coins('4h')
    except Exception as e:
        logger.log_event(log_category="ERROR", message=f"Failed to process 4h data. Error: {e}", path=logfile_path)

    for coin in rsi50_hourly_coins:
        if coin in rsi50_daily_coins:
            rsi50_daily_hourly.append(coin)

    confluence_list = format_list(rsi50_daily_hourly)
    daily_confluence_message = "\n===== RSI50 Daily + Hourly Confluence =====\n\n" + confluence_list

    daily_list = format_list(rsi50_daily_coins)
    daily_message = "\n\n===== RSI50 DAILY =====\n\n" + daily_list

    four_hour_list = format_list(rsi50_4h_coins)
    four_hour_message = "\n\n===== RSI50 4H =====\n\n" + four_hour_list

    send_to_discord(webhook_url_daily, daily_confluence_message)
    send_to_discord(webhook_url_daily, daily_message)
    send_to_discord(webhook_url_4h, four_hour_message)
