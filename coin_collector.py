import ccxt
import os
import logger

script_dir = os.path.dirname(os.path.abspath(__file__))
log_path = os.path.join(script_dir, "logs", "coin_collector_logs.txt")
coin_list_path = os.path.join(script_dir, "coin_list.txt")


def get_coins():
    """
    Get all active futures coins from binance
    :return: list[] of all active coins
    """

    try:
        binance = ccxt.binance({
            'options': {
                'defaultType': 'future'
            }
        })

        markets = binance.load_markets()

        usdt_perps = [
            symbol for symbol, market in markets.items()
            if market['contract'] and market['linear'] and market['quote'] == 'USDT' and market['active']
        ]

        coin_count = len(usdt_perps)
        if coin_count != 0:
            logger.log_event(log_category="INFO", message=f"Successfully retrieved {coin_count} coins", path=log_path)

        return usdt_perps
    except Exception as e:
        logger.log_event(log_category="ERROR", message=f"Failed to retrieve coins because of error: {e}", path=log_path)


def save_list(coins):
    """
    Save list of active coins
    :return: none
    """
    try:
        with open(coin_list_path, 'w') as file:
            for coin in coins:
                formatted_coin = coin.replace("/USDT:", "")
                if "-" not in formatted_coin:
                    file.write(f"{formatted_coin}\n")
        logger.log_event(log_category="INFO", message="Successfully saved coin list", path=log_path)
    except Exception as e:
        logger.log_event(log_category="ERROR", message=f"Failed to save coin list. Error: {e}", path=log_path)


if __name__ == "__main__":
    coins_list = get_coins()
    save_list(coins_list)
