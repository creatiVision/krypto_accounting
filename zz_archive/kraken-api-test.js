import json
import requests
from datetime import datetime, timezone
from pathlib import Path

# Load config
CONFIG_FILE = Path(__file__).parent / "config.json"
with open(CONFIG_FILE, 'r') as f:
    config = json.load(f)
API_KEY = config["API_KEY"]
API_SECRET = config["API_SECRET"]

# Function to generate signature
def get_signature(endpoint, data):
    import hmac, hashlib, base64
    encoded = (data["nonce"] + "&".join(f"{k}={v}" for k, v in data.items())).encode()
    message = f"/0 private/{endpoint}".encode() + hashlib.sha256(encoded).digest()
    signature = hmac.new(base64.b64decode(API_SECRET), message, hashlib.sha512).digest()
    return base64.b64encode(signature).decode()

# Function to make API call
def kraken_request(endpoint, data):
    url = f"https://api.kraken.com/0 private/{endpoint}"
    data["nonce"] = str(int(time.time() * 100000))
    signature = get_signature(endpoint, data)
    headers = {"API-Key": API_KEY, "API-Sign": signature}
    response = requests.post(url, headers=headers, data=data)
    return response.json()

# Define date range for 2024
start_date = "2024-01-01"
end_date = "2024-12-31"
start_ts = int(datetime.strptime(start_date, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp())
end_ts = int(datetime.strptime(end_date, "%Y-%m-%d").replace(hour=23, minute=59, second=59, tzinfo=timezone.utc).timestamp())

# Fetch all trades for 2024
def get_all_trades(start_ts, end_ts):
    trades = []
    offset = 0
    while True:
        params = {"trades": "true", "start": start_ts, "end": end_ts, "ofs": offset}
        response = kraken_request("TradesHistory", params)
        if "error" in response:
            print(f"Error fetching trades: {response['error']}")
            break
        batch_trades = response["result"]["trades"]
        if not batch_trades:
            break
        trades.extend(batch_trades.values())
        offset += len(batch_trades)
    return trades

# Fetch all ledger entries for 2024
def get_all_ledger(start_ts, end_ts):
    ledger = []
    offset = 0
    while True:
        params = {"start": start_ts, "end": end_ts, "ofs": offset}
        response = kraken_request("Ledgers", params)
        if "error" in response:
            print(f"Error fetching ledger: {response['error']}")
            break
        batch_ledger = response["result"]["ledger"]
        if not batch_ledger:
            break
        ledger.extend(batch_ledger.values())
        offset += len(batch_ledger)
    return ledger

# Main function
def main():
    print("Fetching trades for 2024...")
    trades_2024 = get_all_trades(start_ts, end_ts)
    print(f"Fetched {len(trades_2024)} trades for 2024.")
    with open("trades_2024.json", "w") as f:
        json.dump(trades_2024, f, indent=4)
    print("Fetching ledger entries for 2024...")
    ledger_2024 = get_all_ledger(start_ts, end_ts)
    print(f"Fetched {len(ledger_2024)} ledger entries for 2024.")
    with open("ledger_2024.json", "w") as f:
        json.dump(ledger_2024, f, indent=4)

if __name__ == "__main__":
    main()