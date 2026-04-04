import os
import glob
from datetime import datetime
import csv

try:
    import pandas as pd
except Exception:
    pd = None

try:
    from dotenv import load_dotenv
except Exception:
    def load_dotenv():
        return None

from discord_integrator import send_to_discord


load_dotenv()


def compute_day_change_from_csv(path):
    # Prefer pandas when available for robustness, else fallback to csv reader
    if pd is not None:
        try:
            df = pd.read_csv(path)
            df.columns = df.columns.str.strip()
            if len(df) < 2:
                return None
            today_close = float(df.iloc[-1].get('close'))
            yesterday_close = float(df.iloc[-2].get('close'))
            if yesterday_close == 0:
                return None
            pct = (today_close - yesterday_close) / yesterday_close * 100.0
            return round(pct, 2)
        except Exception:
            return None

    # Fallback using csv module (no pandas installed)
    try:
        import csv
        closes = []
        with open(path, 'r', newline='') as fh:
            reader = csv.DictReader(fh)
            for row in reader:
                # strip keys to handle inconsistent headers
                key_map = {k.strip(): v for k, v in row.items()}
                if 'close' in key_map and key_map['close'] not in (None, ''):
                    try:
                        closes.append(float(key_map['close']))
                    except Exception:
                        continue
        if len(closes) < 2:
            return None
        today_close = closes[-1]
        yesterday_close = closes[-2]
        if yesterday_close == 0:
            return None
        pct = (today_close - yesterday_close) / yesterday_close * 100.0
        return round(pct, 2)
    except Exception:
        return None


def main():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    daily_data_path = os.path.join(script_dir, "daily_data")
    summary_csv = os.path.join(script_dir, "day_change_summary.csv")

    webhook_url = os.getenv("MARKET_BREADTH_WEBHOOK") or os.getenv("DAY_CHANGE_WEBHOOK")
    if not webhook_url:
        print("Warning: MARKET_BREADTH_WEBHOOK and DAY_CHANGE_WEBHOOK are not set; message will not be sent.")

    patterns = os.path.join(daily_data_path, "*.csv")
    files = glob.glob(patterns)

    day_changes = {}
    for f in files:
        name = os.path.basename(f).upper()
        coin = name.replace('.CSV', '')
        pct = compute_day_change_from_csv(f)
        if pct is None:
            continue
        day_changes[coin] = pct

    # Separate BTC files
    btc_key = 'BTCUSDT'
    btcd_key = 'BTCDOMUSDT'

    btc_change = day_changes.get(btc_key)
    btcd_change = day_changes.get(btcd_key)

    # Exclude BTC and BTCDOM from market breadth calculation
    excluded = {btc_key, btcd_key}
    filtered = {k: v for k, v in day_changes.items() if k not in excluded}

    total = len(filtered)
    positive = sum(1 for v in filtered.values() if v > 0)

    market_breadth = round((positive / total) * 100.0, 2) if total > 0 else 0.0

    now = datetime.utcnow().isoformat()

    # Write summary CSV
    try:
        with open(summary_csv, 'w', newline='') as fh:
            writer = csv.writer(fh)
            writer.writerow(['timestamp', 'market_breadth_pct', 'positive_count', 'total_count', 'btc_pct', 'btcd_pct'])
            writer.writerow([now, market_breadth, positive, total, btc_change if btc_change is not None else '', btcd_change if btcd_change is not None else ''])
    except Exception:
        pass

    # Prepare message
    lines = []
    lines.append(f"Market Breadth: {market_breadth}% ({positive}/{total} coins positive)")
    if btc_change is not None:
        lines.append(f"BTC Day Change: {btc_change}%")
    else:
        lines.append("BTC Day Change: N/A")
    if btcd_change is not None:
        lines.append(f"BTCDOM Day Change: {btcd_change}%")
    else:
        lines.append("BTCDOM Day Change: N/A")

    message = "\n".join(lines)

    print(message)

    if webhook_url:
        send_to_discord(webhook_url, message=message)


if __name__ == '__main__':
    main()
