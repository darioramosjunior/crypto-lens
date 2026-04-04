import json
import ssl
import urllib.parse
import urllib.request
import csv
import os
from pathlib import Path

import certifi
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.environ.get("cmc_api_key")

if not API_KEY:
    print("Error: cmc_api_key environment variable not set.")
    print("Please set the CMC API key: set cmc_api_key=your_key_here")
    exit(1)

# Read coin list from file
coin_list_file = "coin_list.txt"
try:
    with open(coin_list_file, "r") as f:
        coins = [line.strip() for line in f if line.strip()]
except FileNotFoundError:
    print(f"Error: {coin_list_file} not found.")
    exit(1)

print(f"Loaded {len(coins)} coins from {coin_list_file}")

# Batch coins into groups of 100
batch_size = 100
batches = [coins[i : i + batch_size] for i in range(0, len(coins), batch_size)]

context = ssl.create_default_context(cafile=certifi.where())

# Store results
results = []

for batch_num, batch in enumerate(batches, 1):
    print(f"Processing batch {batch_num}/{len(batches)} ({len(batch)} coins)...")
    
    # Create comma-separated symbol list (remove USDT suffix for CMC API)
    symbols = ",".join([coin.replace("USDT", "") for coin in batch])
    
    params = urllib.parse.urlencode(
        {
            "symbol": symbols,
            "convert": "USD",
        }
    )

    try:
        request = urllib.request.Request(
            f"https://pro-api.coinmarketcap.com/v2/cryptocurrency/quotes/latest?{params}",
            headers={
                "Accept": "application/json",
                "X-CMC_PRO_API_KEY": API_KEY,
            },
        )

        with urllib.request.urlopen(request, context=context) as response:
            data = json.load(response)

        # Process response
        if "data" in data:
            # Track which coins were found in the response
            found_coins = set()
            
            for symbol_key, coin_data_list in data["data"].items():
                original_symbol = f"{symbol_key}USDT"
                
                # Check if the list has data (API returns list of matches)
                if coin_data_list and len(coin_data_list) > 0:
                    coin_data = coin_data_list[0]  # Get the first (best match)
                    found_coins.add(symbol_key)
                    
                    try:
                        price = coin_data.get("quote", {}).get("USD", {}).get("price")
                        circulating_supply = coin_data.get("circulating_supply", 0)
                        
                        if price and circulating_supply:
                            market_cap = price * circulating_supply
                        else:
                            market_cap = None
                    except (KeyError, TypeError):
                        market_cap = None
                    
                    # Categorize market cap
                    if market_cap is None:
                        market_cap_str = "N/A"
                        category = "N/A"
                    else:
                        market_cap_str = f"{market_cap:.2f}"
                        if market_cap > 10_000_000_000:  # >10B
                            category = "Large Cap"
                        elif market_cap >= 1_000_000_000:  # 1B-10B
                            category = "Mid Cap"
                        else:  # <1B
                            category = "Small Cap"
                    
                    results.append([original_symbol, market_cap_str, category])
                else:
                    # Empty list means coin not found in API
                    results.append([original_symbol, "N/A", "N/A"])
                
    except Exception as e:
        print(f"Error processing batch {batch_num}: {e}")
        # Add N/A for all coins in this batch
        for coin in batch:
            results.append([coin, "N/A", "N/A"])

# Save to CSV
output_file = "market_cap_data.csv"
try:
    with open(output_file, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["coin", "market_cap", "category"])
        writer.writerows(results)
    print(f"\nResults saved to {output_file}")
    print(f"Total coins processed: {len(results)}")
except Exception as e:
    print(f"Error saving to CSV: {e}")
    exit(1)