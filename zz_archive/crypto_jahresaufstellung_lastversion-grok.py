#!/usr/bin/env python3
import subprocess
import sys
import json
from pathlib import Path
from datetime import datetime, timezone  # Added timezone
import time
import hmac
import hashlib
import base64
import requests
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import locale
import traceback  # Import traceback for better error logging

# --- Package Installation ---


def install_packages(packages):
    """Installs required Python packages using pip."""
    try:
        print(f"Attempting to install/update: {', '.join(packages)}...")
        # Use --quiet to reduce console noise, remove if you want to see pip output
        subprocess.check_call(
            [sys.executable, "-m", "pip", "install", "--quiet"] + packages)
        print(f"Successfully installed/verified: {', '.join(packages)}")
        # Log event *after* successful installation attempt if logger is available
        # Note: log_event might not be defined yet if this runs very early
        if 'log_event' in globals():
            log_event("Package Install",
                      f"Successfully installed/verified: {', '.join(packages)}")
    except subprocess.CalledProcessError as e:
        print(f"ERROR: Failed to install packages: {', '.join(packages)}.")
        print(f"pip error output: {e}")
        print(f"Please try installing manually: pip install {
              ' '.join(packages)}")
        # Log event before exiting if logger is available
        if 'log_event' in globals():
            log_event("Package Install Error", f"Failed to install {
                      ', '.join(packages)}: {e}")
        # Exit because core functionality might depend on these packages
        sys.exit(1)
    except Exception as e:
        print(
            f"ERROR: An unexpected error occurred during package installation: {e}")
        if 'log_event' in globals():
            log_event("Package Install Error", f"Unexpected error installing {
                      ', '.join(packages)}: {e}")
        sys.exit(1)


# Check and install required packages
required_packages = ["requests", "google-auth", "google-auth-oauthlib",
    "google-auth-httplib2", "google-api-python-client"]
for package in required_packages:
    try:
        # Handle package names that differ from import names (e.g., google-api-python-client -> googleapiclient)
        import_name = package.replace('-', '_')
        __import__(import_name)
    except ImportError:
        print(f"Package '{package}' not found. Installing...")
        install_packages([package])  # Call the install function

# --- Configuration Loading ---
CONFIG_FILE = Path(__file__).parent / "config.json"
# Adjust path if needed
CREDENTIALS_FILE = Path(__file__).parent / \
                        "mbay-tax-sheet-for-kryptos-7fc01e35fb9a.json"

try:
    with CONFIG_FILE.open('r') as f:
        config = json.load(f)
    API_KEY = config["API_KEY"]
    API_SECRET = config["API_SECRET"]
    SHEET_ID = config["SHEET_ID"]
    THEFT_TXIDS = config.get("theft_txids", [])
    # Load optional start/end dates
    START_DATE_STR = config.get("start_date")
    END_DATE_STR = config.get("end_date")
    START_TIMESTAMP = None
    END_TIMESTAMP = None
    if START_DATE_STR:
        try:
            # Assume date is midnight UTC on that day
            START_TIMESTAMP = int(datetime.strptime(
                START_DATE_STR, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp())
        except ValueError:
            print(f"Error: Invalid start_date format '{
                  START_DATE_STR}'. Use YYYY-MM-DD. Ignoring.")
    if END_DATE_STR:
        try:
            # Assume date is end of the day UTC
            END_TIMESTAMP = int((datetime.strptime(END_DATE_STR, "%Y-%m-%d").replace(
                hour=23, minute=59, second=59, tzinfo=timezone.utc)).timestamp())
        except ValueError:
            print(f"Error: Invalid end_date format '{
                  END_DATE_STR}'. Use YYYY-MM-DD. Ignoring.")

except FileNotFoundError:
    raise FileNotFoundError(f"Config file not found at: {CONFIG_FILE}")
except KeyError as e:
    raise KeyError(f"Missing required key in config.json: {str(e)}")
except Exception as e:
    print(f"Error loading configuration: {e}")
    sys.exit(1)


# --- Google API Setup ---
try:
    SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_file(
        str(CREDENTIALS_FILE), scopes=SCOPES)
    service = build("sheets", "v4", credentials=creds)
except Exception as e:
    print(f"Error setting up Google API service: {e}")
    print("Please ensure the credentials file path is correct and the service account has necessary permissions.")
    sys.exit(1)

# --- Global Variables & Logging ---
HOLDINGS = {}  # Asset -> list of {"amount": float, "price_eur": float, "timestamp": float, "fee_eur": float, "refid": str}
LOG_DATA = [["Timestamp", "Event", "Details"]]
PRICE_CACHE = {}
# Define headers globally once after imports and basic setup
HEADERS = [
    "Zeile", "Typ", "Datum", "Asset", "Anzahl",
    "Kaufdatum", "Kaufpreis (€)/Stk", "Verkaufsdatum", "Verkaufspreis (€)/Stk",
    "Kosten (€)", "Erlös (€)", "Gebühr (€)", "Gewinn / Verlust (€)",
    "Haltedauer (Tage)", "Steuerpflichtig", "Notizen / FIFO-Details"
]


def log_event(event, details):
    """Appends an event to the global log data."""
    timestamp = datetime.now().strftime(
        "%Y-%m-%d %H:%M:%S.%f")[:-3]  # Added milliseconds
    LOG_DATA.append([timestamp, event, str(details)])
    # print(f"LOG: {timestamp} - {event} - {details}") # Optional: print logs immediately

# --- Kraken API Request Function ---


def kraken_request(endpoint, data=None):
    """Sends an authenticated request to the Kraken API."""
    url = f"https://api.kraken.com/0/private/{endpoint}"
    if data is None:
        data = {}
    # Use a unique nonce for each request
    data["nonce"] = str(int(time.time() * 100000))  # Increased precision nonce
    post_data = "&".join([f"{k}={v}" for k, v in data.items()])

    try:
        encoded = (str(data["nonce"]) + post_data).encode()
        message = f"/0/private/{endpoint}".encode() + \
                                                  hashlib.sha256(
                                                      encoded).digest()
        signature = hmac.new(base64.b64decode(
            API_SECRET), message, hashlib.sha512)
        sig = base64.b64encode(signature.digest()).decode()
    except Exception as e:
        log_event("HMAC Error", f"Failed to generate signature: {e}")
         raise Exception(f"HMAC signature generation failed: {e}")

    headers = {"API-Key": API_KEY, "API-Sign": sig}

    max_retries = 3
    wait_time = 5  # seconds
    for attempt in range(max_retries):
        try:
            response = requests.post(
                url, headers=headers, data=data, timeout=45)  # Increased timeout
            log_event(f"API Call: {endpoint}", f"Attempt: {attempt+1}, Nonce: {data['nonce']}, Params: {
                      data}, Status: {response.status_code}, Response: {response.text[:200]}...")
            response.raise_for_status()  # Raise HTTPError for bad responses (4xx or 5xx)

            result = response.json()
            if result.get("error"):
                error_messages = result["error"]
                 # Specific handling for nonce errors
                 if any("Nonce" in e for e in error_messages):
                     log_event("API Error - Nonce", f"Nonce {
                               data['nonce']} likely too low or duplicate. Waiting and retrying. Error: {error_messages}")
                     if attempt < max_retries - 1:
                         # Exponential backoff might be better
                         time.sleep(wait_time * (attempt + 1))
                         # Generate new nonce for retry
                         data["nonce"] = str(int(time.time() * 100000))
                         # Regenerate post_data
                         post_data = "&".join(
                             [f"{k}={v}" for k, v in data.items()])
                         encoded = (str(data["nonce"]) +
                                    post_data).encode()  # Re-encode
                         # Recalculate message
                         message = f"/0/private/{endpoint}".encode() + \
                                                                   hashlib.sha256(
                                                                       encoded).digest()
                         signature = hmac.new(base64.b64decode(
                             API_SECRET), message, hashlib.sha512)  # Recalculate signature
                         sig = base64.b64encode(signature.digest()).decode()
                         headers["API-Sign"] = sig  # Update header
                         continue  # Retry with the new nonce and signature
                     else:
                         log_event("API Error - Nonce Fatal", f"Nonce error persisted after {
                                   max_retries} attempts. Last error: {error_messages}")
                          raise Exception(f"API nonce error after retries: {
                                          error_messages}")
                 # Handle other specific errors if needed (e.g., permission denied)
                 elif any("Permission denied" in e for e in error_messages):
                     log_event("API Error - Permissions", f"Permission denied for {
                               endpoint}. Check API key permissions. Error: {error_messages}")
                      raise Exception(f"API Permission Denied for {
                                      endpoint}: {error_messages}")
                 else:
                     log_event("API Error - Generic",
                               f"Endpoint: {endpoint}, Error: {error_messages}")
                      raise Exception(f"API error: {error_messages}")
            # If no error, return the result
            return result
        except requests.exceptions.Timeout as e:
            log_event("API Request Error - Timeout",
                      f"Endpoint: {endpoint}, Attempt: {attempt+1}, Error: {str(e)}")
            if attempt == max_retries - 1:
                log_event("API Request Error - Timeout Fatal",
                          f"Request timed out after {max_retries} attempts for {endpoint}.")
                raise Exception(f"API request timed out after {
                                max_retries} attempts: {str(e)}")
            time.sleep(wait_time * (attempt + 1))
        except requests.exceptions.RequestException as e:
            log_event("API Request Error - Connection/Other",
                      f"Endpoint: {endpoint}, Attempt: {attempt+1}, Error: {str(e)}")
            if attempt == max_retries - 1:
                log_event("API Request Error - Connection/Other Fatal",
                          f"Request failed after {max_retries} attempts for {endpoint}.")
                raise Exception(f"API request failed after {
                                max_retries} attempts: {str(e)}")
            time.sleep(wait_time * (attempt + 1))
        # Catch other potential errors (like JSON decoding)
        except Exception as e:
            log_event("API Call Exception - Unexpected",
                      f"Endpoint: {endpoint}, Attempt: {attempt+1}, Error: {str(e)}")
             # Include traceback for unexpected errors
             log_event("Traceback", traceback.format_exc())
             raise e  # Re-raise other exceptions

    # This should only be reached if all retries fail for connection/timeout issues
    log_event("API Call Fatal Error", f"API call {
              endpoint} failed after all retries.")
    raise Exception(f"API call {endpoint} failed after all retries.")


# --- Data Fetching Functions ---
def get_trades(start_ts=None, end_ts=None):
    """Fetches trade history from Kraken, handling pagination and deduplication."""
    trades_dict = {}  # Use dict for deduplication based on txid
    offset = 0
    count = -1  # Initialize count to know when the first call returns
    fetch_params = {"trades": "true"}  # Ensure trades are included
    if start_ts:
        fetch_params["start"] = start_ts
    if end_ts:
        fetch_params["end"] = end_ts

    date_range_log = f" (Range: {datetime.fromtimestamp(start_ts, timezone.utc).strftime(
        '%Y-%m-%d') if start_ts else 'N/A'} - {datetime.fromtimestamp(end_ts, timezone.utc).strftime('%Y-%m-%d') if end_ts else 'N/A'})"
    log_event("Fetching Trades",
              f"Starting trade history retrieval{date_range_log}.")

    while True:
        try:
            current_params = {"ofs": offset, **fetch_params}
            result = kraken_request("TradesHistory", current_params)
            batch_dict = result.get("result", {}).get("trades", {})
            # Update count only on the first call or if it changes (shouldn't change with fixed range)
            if count == -1:
                count = int(result.get("result", {}).get("count", 0))
                log_event("Fetching Trades Info", f"API reports total {
                          count} trades for the query parameters.")

            if not batch_dict:
                log_event("Fetching Trades", f"No more trades found in batch. Total unique fetched: {
                          len(trades_dict)}. Expected count by API: {count}.")
                break

            # Add/overwrite entries in the dictionary
            trades_dict.update(batch_dict)
            log_event("Trades Fetched Batch", f"Offset: {offset}, Batch Size: {len(
                batch_dict)}, Total Unique Fetched: {len(trades_dict)}, Expected Total: {count}")
            # Increment offset by items *received in this batch*
            offset += len(batch_dict)

            # Stop if we have fetched equal to or more than the initially reported count
            if count != -1 and offset >= count:
                log_event("Fetching Trades", f"Offset ({
                          offset}) reached/exceeded reported count ({count}). Stopping.")
                break

            time.sleep(1.1)  # Be nice to the API

        except Exception as e:
            log_event("Error Fetching Trades", f"Failed at offset {
                      offset}. Error: {str(e)}")
            raise  # Stop execution if fetching fails

    final_trades = list(trades_dict.values())
    log_event("Fetching Trades Completed",
              f"Total unique trades fetched: {len(final_trades)}")
    return final_trades


def get_ledger(start_ts=None, end_ts=None):
    """Fetches ledger history from Kraken, handling pagination and deduplication."""
    ledger_dict = {}  # Use dict for deduplication based on ledger id (key)
    offset = 0
    count = -1  # Initialize count
    fetch_params = {}
    if start_ts:
        fetch_params["start"] = start_ts
    if end_ts:
        fetch_params["end"] = end_ts

    date_range_log = f" (Range: {datetime.fromtimestamp(start_ts, timezone.utc).strftime(
        '%Y-%m-%d') if start_ts else 'N/A'} - {datetime.fromtimestamp(end_ts, timezone.utc).strftime('%Y-%m-%d') if end_ts else 'N/A'})"
    log_event("Fetching Ledger",
              f"Starting ledger history retrieval{date_range_log}.")

    while True:
        try:
            current_params = {"ofs": offset, **fetch_params}
            result = kraken_request("Ledgers", current_params)
            # batch_dict is {ledger_id: entry_data}
            batch_dict = result.get("result", {}).get("ledger", {})
            if count == -1:
                count = int(result.get("result", {}).get("count", 0))
                log_event("Fetching Ledger Info", f"API reports total {
                          count} ledger entries for the query parameters.")

            if not batch_dict:
                log_event("Fetching Ledger", f"No more ledger entries found in batch. Total unique fetched: {
                          len(ledger_dict)}. Expected count by API: {count}.")
                break

            # Add/overwrite entries based on ledger ID key
            ledger_dict.update(batch_dict)
            log_event("Ledger Fetched Batch", f"Offset: {offset}, Batch Size: {len(
                batch_dict)}, Total Unique Fetched: {len(ledger_dict)}, Expected Total: {count}")
            offset += len(batch_dict)  # Increment offset correctly

            if count != -1 and offset >= count:
                log_event("Fetching Ledger", f"Offset ({
                          offset}) reached/exceeded reported count ({count}). Stopping.")
                break

            time.sleep(1.1)  # Be nice to the API

        except Exception as e:
            log_event("Error Fetching Ledger", f"Failed at offset {
                      offset}. Error: {str(e)}")
            raise  # Stop execution if fetching fails

    final_ledger = list(ledger_dict.values())
    log_event("Fetching Ledger Completed", f"Total unique ledger entries fetched: {
              len(final_ledger)}")  # Log the count AFTER deduplication
    return final_ledger


# --- Grouping Function ---
def group_by_year(trades, ledger):
    """Groups trades and ledger entries by calendar year (UTC)."""
    trades_by_year = {}
    ledger_by_year = {}
    for trade in trades:
        try:
            # Use timezone-aware datetime for grouping
            year = datetime.fromtimestamp(
                float(trade["time"]), timezone.utc).year
            trades_by_year.setdefault(year, []).append(trade)
        except Exception as e:
            log_event("Grouping Error", f"Could not process trade time for grouping: {
                      trade.get('ordertxid', 'N/A')}, Time: {trade.get('time', 'N/A')}, Error: {e}")
    for entry in ledger:
        try:
            year = datetime.fromtimestamp(
                float(entry["time"]), timezone.utc).year
            ledger_by_year.setdefault(year, []).append(entry)
        except Exception as e:
            log_event("Grouping Error", f"Could not process ledger time for grouping: {
                      entry.get('refid', 'N/A')}, Time: {entry.get('time', 'N/A')}, Error: {e}")

    all_years = set(trades_by_year.keys()).union(ledger_by_year.keys())
    log_event("Data Grouping", f"Grouped data for years: {
              sorted(list(all_years))}")
    # Sort within each year before returning
    return {year: (sorted(trades_by_year.get(year, []), key=lambda x: float(x["time"])),
                   sorted(ledger_by_year.get(year, []), key=lambda x: float(x["time"])))
            for year in sorted(all_years)}


# --- Price Fetching Function ---
def get_market_price(asset, timestamp):
    """Fetches market price near a given timestamp using Kraken public API."""
    # Map internal Kraken assets to public API pairs (Focus on EUR pairs)
    # Needs to be comprehensive for assets traded
    asset_map = {
        "ETH": "ETH/EUR", "XETH": "ETH/EUR",
        "XBT": "XBT/EUR", "XXBT": "XBT/EUR", "BTC": "XBT/EUR",
        "XRP": "XRP/EUR", "XXRP": "XRP/EUR",
        "ADA": "ADA/EUR",
        "LTC": "LTC/EUR",
        "XLM": "XLM/EUR",
        "EOS": "EOS/EUR",
        "ETC": "ETC/EUR",
        "AVAX": "AVAX/EUR",
        "ARB": "ARB/EUR",
        # Add other asset mappings as needed...
        "EUR": None, "ZEUR": None,  # EUR has no pair to fetch against itself
        "KFEE": None  # Kraken Fee Credits
    }
    # Normalize asset name (e.g., XXBT -> XBT, ZEUR -> EUR)
    normalized_asset = asset.replace('Z', '', 1) if len(
        asset) == 4 and asset.startswith('Z') else asset
    normalized_asset = normalized_asset.replace('X', '', 1) if len(
        normalized_asset) == 4 and normalized_asset.startswith('X') else normalized_asset

    pair = asset_map.get(normalized_asset)

    if not pair:
        log_event("Price Fetch Error", f"Unsupported asset or EUR: {
                  asset}. Cannot determine market price.")
        return 0  # Assume 0 price if mapping doesn't exist or it's EUR

    timestamp_int = int(timestamp)
    # Cache price per hour using integer division for the key component
    cache_key = (pair, timestamp_int // 3600)

    if cache_key in PRICE_CACHE:
        log_event("Price Cache Hit", f"Using cached price for {pair} around {
                  datetime.fromtimestamp(timestamp_int, timezone.utc)}")
         return PRICE_CACHE[cache_key]

    # Kraken public API uses 'since' based on time. Fetch trades around that time.
    # Fetch trades from 1 hour before to the exact time.
    since_time = timestamp_int - 3600  # 1 hour before
    url = "https://api.kraken.com/0/public/Trades"
    # Using seconds for 'since' seems reliable
    params = {"pair": pair, "since": str(since_time)}

    log_event("Public API Call: Trades", f"Fetching price for {pair} at {datetime.fromtimestamp(
        timestamp, timezone.utc)} (Since: {datetime.fromtimestamp(since_time, timezone.utc)})")

    try:
        response = requests.get(url, params=params, timeout=15)
        response.raise_for_status()
        data = response.json()

        if data.get("error"):
            error_messages = data["error"]
            # Handle specific errors like "Unknown asset pair"
            if any("Unknown asset pair" in e for e in error_messages):
                log_event("Price Fetch Warning", f"Asset pair {
                          pair} unknown to public API. Assuming 0 EUR price.")
                 PRICE_CACHE[cache_key] = 0
                 return 0
            raise Exception(f"Public API Error: {error_messages}")

        # Result structure: {"error": [], "result": {"PAIR_NAME": [[price, volume, time, side, type, misc]], "last": "..."}}
        result_keys = list(data.get("result", {}).keys())
        # Find the actual pair data key
        pair_data_key = next(
            (key for key in result_keys if key != 'last'), None)
        if not pair_data_key:
            log_event("Price Fetch Warning", f"No trade data key found in result for {pair} around {
                      datetime.fromtimestamp(timestamp, timezone.utc)}. Assuming 0 EUR price.")
             PRICE_CACHE[cache_key] = 0
             return 0

        pair_trades = data["result"].get(pair_data_key, [])

        if not pair_trades:
            log_event("Price Fetch Warning", f"No public trades array found for {pair} around {
                      datetime.fromtimestamp(timestamp, timezone.utc)}. Assuming 0 EUR price.")
            # Attempt fetch further back? For now, assume 0.
            PRICE_CACHE[cache_key] = 0
            return 0

        # Find the last trade at or before the target timestamp
        relevant_trade = None
        for trade in reversed(pair_trades):
            # Trade time is float epoch timestamp
            trade_time = float(trade[2])
            if trade_time <= timestamp:
                relevant_trade = trade
                break

        trade_price = 0  # Default to 0
        if relevant_trade:
            trade_price = float(relevant_trade[0])
            log_event("Price Found", f"Using trade price {trade_price} EUR for {pair} at {datetime.fromtimestamp(
                relevant_trade[2], timezone.utc)} (Target: {datetime.fromtimestamp(timestamp, timezone.utc)})")
        else:
            # If no trade before, use the earliest trade fetched IF it's reasonably close
            if pair_trades:
                earliest_trade_time = float(pair_trades[0][2])
                 # Only use the first trade if it's within e.g., 2 hours of our 'since' time
                 if earliest_trade_time < since_time + 7200:  # Allow up to 2 hours difference
                     trade_price = float(pair_trades[0][0])
                      log_event("Price Found (Approx)", f"No trade exactly before {datetime.fromtimestamp(timestamp, timezone.utc)}. Using earliest fetched trade price {
                                trade_price} EUR for {pair} at {datetime.fromtimestamp(earliest_trade_time, timezone.utc)}")
                 else:
                     log_event("Price Found (Stale)", f"No trade found before {datetime.fromtimestamp(
                         timestamp, timezone.utc)} and earliest fetched trade is too far in the future ({datetime.fromtimestamp(earliest_trade_time, timezone.utc)}). Assuming 0 EUR.")
                      trade_price = 0
            else:
                log_event("Price Fetch Error", f"Logic error: No trades available after initial check for {
                          pair}. Assuming 0 EUR.")
                 trade_price = 0

        PRICE_CACHE[cache_key] = trade_price
        return trade_price

    except requests.exceptions.RequestException as e:
        log_event("Price Fetch HTTP Error", f"Failed for {
                  pair} at {timestamp}: {str(e)}. Assuming 0 EUR.")
        PRICE_CACHE[cache_key] = 0
        return 0
    except Exception as e:
        log_event("Price Fetch General Error", f"Failed for {
                  pair} at {timestamp}: {str(e)}. Assuming 0 EUR.")
        PRICE_CACHE[cache_key] = 0
        return 0


# --- Main Processing Function ---
def process_for_tax(trades, ledger, year):
    """Processes transactions for a given year using FIFO and generates tax report data."""
    tax_data = [HEADERS]  # Start with header row

    # Combine trades and ledger entries into a single list of events, adding type hints
    events = []
    for trade in trades:
        events.append({"type": "trade", "data": trade,
                      "time": float(trade["time"])})
    for entry in ledger:
        events.append({"type": "ledger", "data": entry,
                      "time": float(entry["time"])})

    # Sort all events chronologically
    events.sort(key=lambda x: x["time"])

    summaries = {}  # Asset -> {gains, tax_free_gains, losses, taxable_days, tax_free_days}
    total_taxable_gains = 0
    total_tax_free_gains = 0
    total_losses = 0
    total_fees_eur = 0
    # Start from 1 for data rows (header is row 0 in list, row 1 in sheet)
    line_num = 1

    processed_refids = set()  # To avoid double-processing ledger items involved in trades

    log_event(f"Processing Year {year}", f"Starting with {
              len(events)} combined events.")
    # Log holdings *at the start* of this year's processing (inherited from previous year)
    log_event(f"Start of Year Holdings {year}", f"{
              json.dumps(HOLDINGS, indent=2)}")  # Pretty print holdings

    for event_index, event in enumerate(events):
        timestamp = event["time"]
        # Skip events outside the target year STRICTLY
        event_year = datetime.fromtimestamp(timestamp, timezone.utc).year
        if event_year != year:
            # Should not happen if group_by_year worked, but safeguard
            # log_event("Processing Skip", f"Skipping event from wrong year ({event_year}) during {year} processing. Time: {timestamp}")
            continue

        date_str = datetime.fromtimestamp(timestamp, timezone.utc).strftime(
            "%Y-%m-%d %H:%M:%S")  # Use UTC consistently
        data = event["data"]

        row_base = [""] * len(HEADERS)  # Initialize empty row
        row_base[0] = line_num + 1  # Sheet row number

        # --- Handle Trade Events ---
        if event["type"] == "trade":
            trade = data
            pair = trade.get("pair", "N/A")
            trade_type = trade.get("type", "N/A")  # 'buy' or 'sell'
            amount = float(trade.get("vol", 0))
            price_per_unit = float(trade.get("price", 0))
            fee_paid = float(trade.get("fee", 0))
            # Total EUR value of the trade
            cost_or_proceeds = float(trade.get("cost", 0))
            # Use ordertxid if available
            trade_refid = trade.get("ordertxid", f"trade_{timestamp}")

            if trade_refid in processed_refids:
                continue  # Already handled via ledger likely

            asset_base = ""  # The crypto asset
            asset_quote = ""  # Usually EUR or another crypto

            # --- Determine Base and Quote Assets ---
            known_eur_endings = ["ZEUR", "EUR"]
            parsed = False
            for eur in known_eur_endings:
                if pair.endswith(eur):
                     asset_base = pair[:-len(eur)]
                      asset_quote = "EUR"
                      parsed = True
                      break
            if not parsed and len(pair) >= 6:  # Basic check for crypto-crypto
                # Simple crypto-crypto pair detection (e.g., ETHXBT, ADABTC)
                # Assume common base/quote lengths (3 or 4 chars for X prefixed)
                potential_split = -1
                 if len(pair) == 6:
                     potential_split = 3
                 elif len(pair) == 7: potential_split = 4 if pair.startswith(('X', 'Z')) else 3
                 elif len(pair) == 8:
                     potential_split = 4

                 if potential_split > 0:
                     asset_base = pair[:potential_split]
                     asset_quote = pair[potential_split:]
                     parsed = True
                 else:
                     log_event("Trade Parse Warning", f"Cannot reliably parse trade pair '{
                               pair}'. Ref: {trade_refid}. Relying on ledger.")
                      # Mark as processed so ledger doesn't try again unnecessarily? Maybe not.
                      continue  # Skip detailed trade processing, rely on ledger
            elif not parsed:
                log_event("Trade Parse Warning", f"Cannot parse pair '{
                          pair}'. Ref: {trade_refid}")
                 continue  # Skip if pair format is unexpected

            # --- Normalize asset names ---
            asset_base_norm = asset_base.replace('Z', '', 1).replace('X', '', 1) if len(
                asset_base) >= 4 and asset_base[0] in ('X', 'Z') else asset_base
            asset_quote_norm = asset_quote.replace('Z', '', 1).replace('X', '', 1) if len(
                asset_quote) >= 4 and asset_quote[0] in ('X', 'Z') else asset_quote

            # --- Process EUR Buys specifically ---
            if trade_type == "buy" and asset_quote_norm == "EUR":
                asset = asset_base_norm
                buy_entry = {"amount": amount, "price_eur": price_per_unit,
                    "timestamp": timestamp, "fee_eur": fee_paid, "refid": trade_refid}
                HOLDINGS.setdefault(asset, []).append(buy_entry)
                HOLDINGS[asset].sort(key=lambda x: x["timestamp"])

                row_base[1] = "Kauf (Trade)"  # Typ
                row_base[2] = date_str  # Datum
                row_base[3] = asset  # Asset
                row_base[4] = amount  # Anzahl
                row_base[5] = date_str  # Kaufdatum
                row_base[6] = price_per_unit  # Kaufpreis (€)/Stk
                # row_base[7,8] = Verkaufsdatum, Verkaufspreis = ""
                row_base[9] = cost_or_proceeds  # Kosten (€)
                # row_base[10] = Erlös = ""
                row_base[11] = fee_paid  # Gebühr (€)
                # row_base[12,13] = Gewinn/Verlust, Haltedauer = ""
                row_base[14] = "N/A"  # Steuerpflichtig (buy event)
                row_base[15] = f"Trade Ref: {trade_refid}"  # Notizen

                tax_data.append(row_base)
                log_event("Buy Recorded (Trade)", f"{date_str}, Asset: {asset}, Amount: {amount}, Price: {
                          price_per_unit:.4f}, Cost: {cost_or_proceeds:.2f}, Fee: {fee_paid:.4f}, Ref: {trade_refid}")
                line_num += 1
                # Mark associated order ID as processed
                processed_refids.add(trade_refid)

            # --- Handle other trades via Ledger ---
            else:
                # Log non-EUR-buy trades if necessary, but ledger processing is primary
                if trade_refid not in processed_refids:
                   log_event("Generic Trade Logged", f"Ref: {trade_refid}, Type: {
                             trade_type}, Pair: {pair}, Amount: {amount}. Details expected in Ledger.")
                   # Mark refid to avoid double logging if ledger also has it
                   processed_refids.add(trade_refid)

        # --- Handle Ledger Events ---
        elif event["type"] == "ledger":
            entry = data
            entry_type = entry.get("type", "N/A").capitalize()
            asset = entry.get("asset", "N/A")
            asset_norm = asset.replace('Z', '', 1) if len(
                asset) == 4 and asset.startswith('Z') else asset
            asset_norm = asset_norm.replace('X', '', 1) if len(
                asset_norm) == 4 and asset_norm.startswith('X') else asset_norm
            amount = float(entry.get("amount", 0))
            fee_paid = float(entry.get("fee", 0))
            # Use refid if available
            refid = entry.get("refid", f"ledger_{timestamp}")

            if refid in processed_refids:
                continue  # Already handled
            if asset_norm == "KFEE":
                continue  # Skip fee credits
            if entry_type in ["Transfer", "Margin"]:
                log_event("Ledger Skip (Transfer/Margin)", f"Ref: {refid}, Type: {
                          entry_type}, Asset: {asset_norm}, Amount: {amount}")
                 processed_refids.add(refid)
                 continue

            is_theft = refid in THEFT_TXIDS
            notes = f"Ledger Ref: {refid}" + (" | THEFT" if is_theft else "")

            # --- Detect Sell Event / Crypto-Crypto Trade ---
            is_sale_or_trade_processed = False
            # Check for Spend/Trade of non-EUR crypto
            if (entry_type == "Spend" or entry_type == "Trade") and amount < 0 and asset_norm != "EUR":

                corresponding_receive = None
                # Search subsequent events for matching Receive/Trade
                for next_event_index in range(event_index + 1, len(events)):
                    next_event = events[next_event_index]
                    # Optimization: stop searching if time difference is too large
                    # Check events within 15 seconds
                    if next_event["time"] - timestamp > 15:
                        break
                    if next_event["type"] == "ledger":
                        next_entry = next_event["data"]
                         if next_entry.get("refid") == refid and \
                            next_entry.get("type", "").capitalize() in ["Receive", "Trade"] and \
                            float(next_entry.get("amount", -1)) > 0:
                               corresponding_receive = next_entry
                                break  # Found it

                if corresponding_receive:
                    # --- Process the Sale / Trade ---
                    is_sale_or_trade_processed = True  # Mark this event pair as handled
                    sell_asset = asset_norm
                    sell_amount = abs(amount)
                    receive_asset_raw = corresponding_receive.get(
                        "asset", "N/A")
                    receive_asset = receive_asset_raw.replace(
                        'Z', '', 1).replace('X', '', 1)  # Normalize
                    receive_amount = float(
                        corresponding_receive.get("amount", 0))
                    # Use fee from receive side if available, else spend side fee
                    receive_fee = float(corresponding_receive.get("fee", 0))
                    fee_eur = receive_fee if receive_fee != 0 else fee_paid
                    total_fees_eur += fee_eur  # Accumulate total fees

                    # --- Determine Sell Price & Proceeds ---
                    sell_price_eur_per_unit = 0
                    total_proceeds_eur = 0
                    is_crypto_to_crypto = (receive_asset != "EUR")

                    if not is_crypto_to_crypto:  # Sale to EUR
                        total_proceeds_eur = receive_amount
                        sell_price_eur_per_unit = total_proceeds_eur / \
                            sell_amount if sell_amount > 1e-12 else 0
                        log_event("Sale Detected (Crypto->EUR)", f"{date_str}, Sold: {sell_amount:.8f} {
                                  sell_asset}, Received: {total_proceeds_eur:.2f} EUR, Fee: {fee_eur:.4f}, Ref: {refid}")
                    else:  # Crypto-to-Crypto Trade
                        receive_asset_price_eur = get_market_price(
                            receive_asset_raw, timestamp)
                        total_proceeds_eur = receive_amount * receive_asset_price_eur
                        sell_price_eur_per_unit = total_proceeds_eur / \
                            sell_amount if sell_amount > 1e-12 else 0
                        log_event("Trade Detected (Crypto->Crypto)", f"{date_str}, Sold: {sell_amount:.8f} {sell_asset}, Received: {receive_amount:.8f} {
                                  receive_asset} (@{receive_asset_price_eur:.4f} EUR/unit Est.), Est. Proceeds: {total_proceeds_eur:.2f} EUR, Fee: {fee_eur:.4f}, Ref: {refid}")
                        # Add received crypto to holdings
                        cost_basis_per_unit_received = receive_asset_price_eur
                        receive_entry = {"amount": receive_amount, "price_eur": cost_basis_per_unit_received,
                            "timestamp": timestamp, "fee_eur": 0, "refid": refid + "-rcv"}
                        HOLDINGS.setdefault(
                            receive_asset, []).append(receive_entry)
                        HOLDINGS[receive_asset].sort(
                            key=lambda x: x["timestamp"])
                        log_event("Trade Receive Leg", f"Added received {receive_asset} to holdings. Amount: {
                                  receive_amount:.8f}, Cost Basis Price: {cost_basis_per_unit_received:.4f} EUR/unit, Ref: {refid}-rcv")

                    # --- Apply FIFO Logic ---
                    remaining_sell_amount = sell_amount
                    total_cost_basis_eur = 0
                    fifo_details = []
                    earliest_buy_timestamp = timestamp
                    sell_precision_tolerance = 1e-9

                    if sell_asset in HOLDINGS and HOLDINGS[sell_asset]:
                        items_consumed_indices = []
                        # Iterate directly on list, track indices to modify later
                        for i, buy_lot in enumerate(HOLDINGS[sell_asset]):
                            if remaining_sell_amount <= sell_precision_tolerance:
                                break

                            buy_amount = buy_lot["amount"]
                            buy_price = buy_lot["price_eur"]
                            buy_time = buy_lot["timestamp"]
                            buy_ref = buy_lot.get("refid", "N/A")

                            if earliest_buy_timestamp == timestamp or buy_time < earliest_buy_timestamp:
                                earliest_buy_timestamp = buy_time

                            amount_to_use = min(
                                buy_amount, remaining_sell_amount)
                            cost_basis_part = amount_to_use * buy_price
                            total_cost_basis_eur += cost_basis_part
                            fifo_details.append(f"Lot {i+1}: {amount_to_use:.8f} from {datetime.fromtimestamp(
                                buy_time, timezone.utc).strftime('%Y-%m-%d')} @ {buy_price:.4f} EUR (Ref: {buy_ref})")
                            items_consumed_indices.append(
                                {"index": i, "amount_used": amount_to_use})
                            remaining_sell_amount -= amount_to_use
                            # Log inside loop can be very verbose, consider logging summary after loop
                            # log_event("FIFO Match", f"Ref: {refid}, Used {amount_to_use:.8f} {sell_asset}, Cost Part: {cost_basis_part:.2f} EUR")

                        # Update original HOLDINGS list based on consumption (process in reverse index order)
                        for consumed_item in sorted(items_consumed_indices, key=lambda x: x['index'], reverse=True):
                            idx = consumed_item["index"]
                            amount_used = consumed_item["amount_used"]
                            # Check if the entire lot was consumed (within tolerance)
                            if abs(HOLDINGS[sell_asset][idx]["amount"] - amount_used) < sell_precision_tolerance:
                                del HOLDINGS[sell_asset][idx]
                            else:
                                HOLDINGS[sell_asset][idx]["amount"] -= amount_used
                        # Clean up asset entry if holdings list becomes empty
                        if sell_asset in HOLDINGS and not HOLDINGS[sell_asset]:
                            del HOLDINGS[sell_asset]

                        if remaining_sell_amount > sell_precision_tolerance:
                            log_event("FIFO Warning", f"Ref: {refid}, Sold {sell_asset}, but only found holdings for {
                                      sell_amount - remaining_sell_amount:.8f}. Assuming 0 cost basis for remaining {remaining_sell_amount:.8f}.")
                            fifo_details.append(f"Warning: {remaining_sell_amount:.8f} {
                                                sell_asset} sold with no matching buy record (assumed 0 cost).")
                    else:
                        log_event("FIFO Warning", f"Ref: {refid}, Sold {
                                  sell_asset}, but no holdings found for this asset. Assuming 0 cost basis for entire amount {sell_amount:.8f}.")
                        total_cost_basis_eur = 0
                        earliest_buy_timestamp = timestamp
                        fifo_details.append(f"Warning: Entire amount {sell_amount:.8f} {
                                            sell_asset} sold with no matching buy record (assumed 0 cost).")

                    # --- Calculate Gain/Loss and Holding Period ---
                    gain_loss = total_proceeds_eur - total_cost_basis_eur - fee_eur
                    holding_period_days = (timestamp - earliest_buy_timestamp) / (
                        24 * 3600) if earliest_buy_timestamp < timestamp else 0
                    # Potentially taxable gain or offsetable loss
                    is_taxable_gain_rule = holding_period_days <= 365

                    # --- Update Summaries ---
                    summaries.setdefault(sell_asset, {
                                         "gains": 0, "tax_free_gains": 0, "losses": 0, "taxable_days": [], "tax_free_days": []})
                    if gain_loss >= 0:  # Gains or Zero
                        if is_taxable_gain_rule:
                            summaries[sell_asset]["gains"] += gain_loss
                            summaries[sell_asset]["taxable_days"].append(
                                holding_period_days)
                            total_taxable_gains += gain_loss
                            log_event("Taxable Gain/Zero", f"Ref: {refid}, Asset: {sell_asset}, Gain: {
                                      gain_loss:.2f}, Holding: {holding_period_days:.0f} days")
                        else:  # Tax-free gain (>1 year)
                            summaries[sell_asset]["tax_free_gains"] += gain_loss
                            summaries[sell_asset]["tax_free_days"].append(
                                holding_period_days)
                            total_tax_free_gains += gain_loss
                            log_event("Tax-Free Gain", f"Ref: {refid}, Asset: {sell_asset}, Gain: {
                                      gain_loss:.2f}, Holding: {holding_period_days:.0f} days")
                    else:  # Losses
                        if not is_theft:
                            summaries[sell_asset]["losses"] += gain_loss
                             total_losses += gain_loss
                             log_event("Loss Recorded", f"Ref: {refid}, Asset: {sell_asset}, Loss: {
                                       gain_loss:.2f}, Holding: {holding_period_days:.0f} days (Offsettable)")
                        else:
                            notes += " | Theft loss - Non-deductible per §23 EStG"
                            log_event("Theft Loss Excluded", f"Ref: {refid}, Asset: {
                                      sell_asset}, Loss: {gain_loss:.2f} flagged as theft.")

                    # --- Populate Row for Sheet ---
                    # Typ
                    row_base[1] = "Verkauf" if not is_crypto_to_crypto else "Tausch"
                    row_base[2] = date_str  # Datum
                    row_base[3] = sell_asset  # Asset sold
                    row_base[4] = sell_amount  # Anzahl sold
                    row_base[5] = datetime.fromtimestamp(earliest_buy_timestamp, timezone.utc).strftime(
                        "%Y-%m-%d %H:%M:%S") if earliest_buy_timestamp < timestamp else "N/A"  # Kaufdatum
                    # Avg. Kaufpreis
                    row_base[6] = total_cost_basis_eur / \
                        sell_amount if sell_amount > 1e-12 else 0
                    row_base[7] = date_str  # Verkaufsdatum
                    row_base[8] = sell_price_eur_per_unit  # Verkaufspreis
                    row_base[9] = total_cost_basis_eur  # Kosten (€)
                    row_base[10] = total_proceeds_eur  # Erlös (€)
                    row_base[11] = fee_eur  # Gebühr (€)
                    # Store calculated value for formula check / logging
                    row_base[12] = gain_loss
                    row_base[13] = round(holding_period_days)  # Haltedauer
                    # Steuerpflichtig flag
                    row_base[14] = "Ja" if is_taxable_gain_rule else "Nein"
                    row_base[15] = notes + " | FIFO: " + \
                        " | ".join(fifo_details)  # Notizen

                    tax_data.append(row_base)
                    line_num += 1
                    processed_refids.add(refid)
                    # Ensure corresponding receive refid is also marked processed
                    processed_refids.add(
                        corresponding_receive.get("refid", refid))

            # --- Handle Unmatched Spends (Withdrawals / Potential Missing Buys) ---
            elif not is_sale_or_trade_processed and (entry_type == "Spend" or entry_type == "Trade") and amount < 0:
                if asset_norm == "EUR":
                     row_base[1] = "EUR Spend (Unmatched)"
                     row_base[2] = date_str
                     row_base[3] = "EUR"
                     row_base[4] = abs(amount)
                     row_base[11] = fee_paid
                     row_base[14] = "N/A"
                     row_base[15] = notes + \
                         " | Possible crypto buy missing from API history?"
                     tax_data.append(row_base)
                     log_event("Unmatched EUR Spend", f"{date_str}, Amount: {abs(amount):.2f}, Fee: {
                               fee_paid:.2f}, Ref: {refid}. Suggests missing buy history.")
                     line_num += 1
                     processed_refids.add(refid)
                 else:  # Spend of Crypto = Withdrawal
                     log_event("Withdrawal Detected", f"{date_str}, Asset: {asset_norm}, Amount: {abs(
                         amount)}, Ref: {refid}. Processing as non-taxable disposal (FIFO cost basis reduction).")
                     # --- Apply FIFO to reduce holdings for withdrawal ---
                     remaining_withdrawal_amount = abs(amount)
                     total_cost_basis_withdrawn = 0
                     fifo_details = []
                     withdrawal_precision_tolerance = 1e-9

                     if asset_norm in HOLDINGS and HOLDINGS[asset_norm]:
                         items_consumed_indices = []
                         for i, buy_lot in enumerate(HOLDINGS[asset_norm]):
                             if remaining_withdrawal_amount <= withdrawal_precision_tolerance:
                                 break
                             buy_amount = buy_lot["amount"]
                             buy_price = buy_lot["price_eur"]
                             buy_ref = buy_lot.get("refid", "N/A")
                             amount_to_use = min(
                                 buy_amount, remaining_withdrawal_amount)
                             cost_basis_part = amount_to_use * buy_price
                             total_cost_basis_withdrawn += cost_basis_part
                             fifo_details.append(
                                 f"Lot {i+1}: {amount_to_use:.8f} @ {buy_price:.4f} EUR (Ref: {buy_ref}) removed")
                             items_consumed_indices.append(
                                 {"index": i, "amount_used": amount_to_use})
                             remaining_withdrawal_amount -= amount_to_use

                         # Update original HOLDINGS list
                         for consumed_item in sorted(items_consumed_indices, key=lambda x: x['index'], reverse=True):
                             idx = consumed_item["index"]
                             amount_used = consumed_item["amount_used"]
                             if abs(HOLDINGS[asset_norm][idx]["amount"] - amount_used) < withdrawal_precision_tolerance:
                                 del HOLDINGS[asset_norm][idx]
                             else:
                                 HOLDINGS[asset_norm][idx]["amount"] -= amount_used
                         if asset_norm in HOLDINGS and not HOLDINGS[asset_norm]:
                             del HOLDINGS[asset_norm]

                         if remaining_withdrawal_amount > withdrawal_precision_tolerance:
                             log_event("Withdrawal Warning", f"Ref: {refid}, Withdrew {abs(amount)} {
                                       asset_norm}, but only found holdings for {abs(amount) - remaining_withdrawal_amount:.8f}. Check history.")
                             fifo_details.append(f"Warning: {remaining_withdrawal_amount:.8f} {
                                                 asset_norm} withdrawn with no matching buy record found.")
                     else:
                         log_event("Withdrawal Warning", f"Ref: {refid}, Withdrew {asset_norm}, but no holdings found for this asset.")
                          fifo_details.append(f"Warning: Entire amount {abs(amount):.8f} {
                                              asset_norm} withdrawn with no matching buy record.")

                     row_base[1] = "Auszahlung"  # Typ
                     row_base[2] = date_str  # Datum
                     row_base[3] = asset_norm  # Asset
                     row_base[4] = abs(amount)  # Anzahl
                     # row_base[5-8] Kauf/Verkauf details N/A for withdrawal
                     row_base[9] = total_cost_basis_withdrawn  # Kosten (€) - Record cost basis removed
                     # row_base[10] Erlös = 0
                     row_base[11] = fee_paid  # Gebühr (€)
                     # row_base[12,13] Gewinn/Verlust, Haltedauer N/A
                     row_base[14] = "N/A"  # Steuerpflichtig
                     row_base[15] = notes + " | FIFO Cost Basis Removed: " + " | ".join(fifo_details)  # Notizen
                     tax_data.append(row_base)
                     line_num += 1
                     processed_refids.add(refid)

            # --- Handle Crypto Receive (Deposit or Trade Receive not matched above) ---
            elif entry_type == "Receive" and amount > 0 and asset_norm != "EUR":
                if refid not in processed_refids:
                     log_event("Deposit Detected", f"{date_str}, Asset: {asset_norm}, Amount: {
                               amount}, Ref: {refid}. Adding to holdings with 0 cost basis.")
                     deposit_entry = {"amount": amount, "price_eur": 0,
                         "timestamp": timestamp, "fee_eur": fee_paid, "refid": refid}
                     HOLDINGS.setdefault(asset_norm, []).append(deposit_entry)
                     HOLDINGS[asset_norm].sort(key=lambda x: x["timestamp"])

                     row_base[1] = "Einzahlung/Receive"  # Typ
                     row_base[2] = date_str  # Datum
                     row_base[3] = asset_norm  # Asset
                     row_base[4] = amount  # Anzahl
                     row_base[5] = date_str  # Kaufdatum = receive date
                     row_base[6] = 0  # Kaufpreis (€)/Stk = 0 for deposit
                     # row_base[7-8] Verkauf = N/A
                     row_base[9] = 0  # Kosten (€) = 0
                     # row_base[10] Erlös = N/A
                     row_base[11] = fee_paid  # Gebühr (€)
                     # row_base[12,13] Gewinn/Verlust, Haltedauer N/A
                     row_base[14] = "N/A"  # Steuerpflichtig
                     row_base[15] = notes + " | Assuming deposit (0 cost basis)."  # Notizen
                     tax_data.append(row_base)
                     line_num += 1
                     processed_refids.add(refid)

            # --- Handle EUR Deposit ---
            elif asset_norm == "EUR" and entry_type == "Deposit" and refid not in processed_refids:
                row_base[1] = "EUR Einzahlung" # Typ
                 row_base[2] = date_str  # Datum
                 row_base[3] = "EUR"  # Asset
                 row_base[4] = amount  # Anzahl (Positive)
                 # row_base[5-10] Kauf/Verkauf/Kosten/Erlös N/A
                 row_base[11] = fee_paid  # Gebühr (€)
                 # row_base[12,13] Gewinn/Verlust, Haltedauer N/A
                 row_base[14] = "N/A"  # Steuerpflichtig
                 row_base[15] = notes  # Notizen
                 tax_data.append(row_base)
                 line_num += 1
                 processed_refids.add(refid)

            # --- Catch other ledger types if necessary ---
            elif refid not in processed_refids:
                log_event("Unhandled Ledger Type", f"Type: {entry_type}, Asset: {asset_norm}, Amount: {amount}, Ref: {refid}. Adding basic row.")
                 row_base[1] = f"Ledger ({entry_type})"  # Typ
                 row_base[2] = date_str  # Datum
                 row_base[3] = asset_norm  # Asset
                 row_base[4] = amount  # Anzahl
                 # row_base[5-10] Kauf/Verkauf/Kosten/Erlös N/A
                 row_base[11] = fee_paid  # Gebühr (€)
                 # row_base[12,13] Gewinn/Verlust, Haltedauer N/A
                 row_base[14] = "N/A"  # Steuerpflichtig
                 row_base[15] = notes + f" | Unhandled type: {entry_type}"  # Notizen
                 tax_data.append(row_base)
                 line_num += 1
                 processed_refids.add(refid)  # Mark as handled

    # Log holdings *at the end* of this year's processing
    log_event(f"End of Year Holdings {year}", f"{json.dumps(HOLDINGS, indent=2)}")  # Pretty print

    # --- Add Sum Row ---
    sum_row_num = len(tax_data) + 1  # Row number for the sum row
    sum_row = [""] * len(HEADERS)
    sum_row[0] = sum_row_num
    sum_row[1] = "Summe"
    last_data_row = len(tax_data)  # Index of the last data row before adding sum row
    if last_data_row >= 1:  # Check if there are any data rows (index >= 1 means at least one row after header)
        # Formulas reference columns by letter: J=Kosten, K=Erlös, M=Gewinn/Verlust
        # Assume data starts at sheet row 2 (index 1 in tax_data list)
        # Sum Kosten (Col J) - Range includes last data row + 1 (sheet notation)
        sum_row[9] = f"=SUM(J2:J{last_data_row+1})"
        sum_row[10] = f"=SUM(K2:K{last_data_row+1})"  # Sum Erlös (Col K)
        sum_row[12] = f"=SUM(M2:M{last_data_row+1})"  # Sum Gewinn/Verlust (Col M)
    tax_data.append(sum_row)

    # --- Add Detailed Summary Section ---
    summary_start_sheet_row = len(tax_data) + 2  # Add a blank line after Summe
    summary_rows = [[""] * len(HEADERS)]  # Blank separator row
    summary_rows.append(
        [summary_start_sheet_row, "--- Steuerliche Zusammenfassung ---"] + [""] * (len(HEADERS) - 2))
    current_summary_row = summary_start_sheet_row + 1

    freigrenze = 600 if year < 2024 else 1000
    net_taxable_gains_final = total_taxable_gains + total_losses  # Losses are negative
    taxable_amount_for_freigrenze = max(0, net_taxable_gains_final)
    final_taxable_amount = taxable_amount_for_freigrenze if taxable_amount_for_freigrenze > freigrenze else 0

    if net_taxable_gains_final > freigrenze:
        freigrenze_note = f"Netto {net_taxable_gains_final:.2f} € > {
            freigrenze}€ Freigrenze -> Voll steuerpflichtig"
    elif net_taxable_gains_final > 0:
        freigrenze_note = f"Netto {net_taxable_gains_final:.2f} € <= {
            freigrenze}€ Freigrenze -> Steuerfrei"
    else:
        freigrenze_note = f"Netto Ergebnis {
            net_taxable_gains_final:.2f} € -> Kein steuerpflichtiger Gewinn"
        if net_taxable_gains_final < 0:
            freigrenze_note += " (Verlustvortrag möglich)"

    log_event("Freigrenze Check", f"Year: {year}, Taxable Gains: {total_taxable_gains:.2f}, TaxFree Gains: {total_tax_free_gains:.2f}, Losses: {
              total_losses:.2f}, Net Result: {net_taxable_gains_final:.2f}, Taxable after Freigrenze: {final_taxable_amount:.2f}, Freigrenze: {freigrenze}€, Note: {freigrenze_note}")

    # Overall Totals Summary
    summary_rows.extend([
        [current_summary_row, "GESAMT", "Steuerpfl. Gewinne (<1J, nach Gebühren)", f"{total_taxable_gains:.2f}", "", "", "", "", "", f"{
                                                             total_fees_eur:.2f}", "", "", "", "", "Ja", "Relevant für Anlage SO"],
        [current_summary_row + 1, "GESAMT", "Steuerfreie Gewinne (>1J, nach Gebühren)", f"{
                                                                  total_tax_free_gains:.2f}", "", "", "", "", "", "", "", "", "", "", "Nein", "Nicht steuerbar"],
        [current_summary_row + 2, "GESAMT", "Verluste (nach Gebühren)", f"{
                                                       total_losses:.2f}", "", "", "", "", "", "", "", "", "", "", "", "Relevant für Anlage SO (Verlustverrechnung)"],
        [current_summary_row + 3, "GESAMT", "Netto Ergebnis (§23 EStG)", f"{
                                                             net_taxable_gains_final:.2f}", "", "", "", "", "", "", "", "", "", "", "", freigrenze_note],
        [current_summary_row + 4, "GESAMT", f"Zu versteuern nach Freigrenze ({freigrenze}€)", f"{
                                                                             final_taxable_amount:.2f}", "", "", "", "", "", "", "", "", "", "", "Ja", "Betrag für Anlage SO"],
        [""] * len(HEADERS),  # Blank line
        [current_summary_row + 6, "INFO", "Erstellt am", datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                                                                      "", "", "", "", "", "", "", "", "", "", "", "Berichtsgenerierung"],
        [current_summary_row + 7, "INFO", "Methode", "FIFO", "", "",
            "", "", "", "", "", "", "", "", "", "Gemäß BMF-Schreiben"],
        [current_summary_row + 8, "INFO", "Hinweis",
            "Diebstahlverluste sind nicht abzugsfähig (§23 EStG)", "", "", "", "", "", "", "", "", "", "", "", ""],
    ])

    tax_data.extend(summary_rows)
    return tax_data


# --- Sheet Writing Functions ---
def write_to_sheets(data, year):
    """Writes the processed tax data to a Google Sheet for the given year."""
    # Check if there's more than just the header row
    if len(data) <= 1:
        log_event("Sheet Write Skip", f"No data rows generated for {year}. Skipping sheet writing.")
         print(f"Skipping sheet generation for {
               year} as no transaction data was processed.")
         return

    sheet_name = f"Steuer {year}"  # German name
    sheet_name, sheet_id = get_or_create_sheet(SHEET_ID, sheet_name)

    num_rows = len(data)
    num_cols = len(HEADERS)  # Use global HEADERS length

    # --- Prepare data for sheet, including formulas ---
    sheet_data_payload = []  # This will hold the rows for the batchUpdate request
    # Header row first
    header_row_values = [({"stringValue": str(h)}) for h in data[0]]
    sheet_data_payload.append({"values": header_row_values})

    # Process data rows
    for r, row_data in enumerate(data[1:], start=1):  # Start from index 1 (row 2 in sheet)
        new_row_values = []
        is_sum_row = row_data[1] == "Summe"
        is_summary_row = row_data[1] in ["--- Steuerliche Zusammenfassung ---", "GESAMT", "INFO", ""] or r >= num_rows - 10  # Heuristic for summary rows

        for c, cell_value in enumerate(row_data):
            cell_data = {}  # Holds the value/formula for the cell

            # --- Apply Formula for Gewinn/Verlust (Column M, index 12) ---
            # Apply only to actual data rows, not sum/summary
            if c == 12 and not is_sum_row and not is_summary_row:
                current_sheet_row_num = r + 1 # Sheet row numbers are 1-based
                 # Formula: =IF(ISBLANK(K{row}), "", K{row} - J{row} - L{row}) -> Erlös - Kosten - Gebühr
                 # Check if dependent cells likely contain numbers before adding formula
                 k_val = row_data[10]  # Erlös (K)
                 j_val = row_data[9]  # Kosten (J)
                 l_val = row_data[11]  # Gebühr (L)
                 # Basic check if they look like numbers or are empty
                 if isinstance(k_val, (int, float)) or isinstance(j_val, (int, float)) or isinstance(l_val, (int, float)) or k_val in [None, ""]:
                     formula_str = f"=IF(ISBLANK(K{current_sheet_row_num}), \"\", ROUND(K{current_sheet_row_num} - J{current_sheet_row_num} - L{current_sheet_row_num}, 2))"
                      cell_data["formulaValue"] = formula_str
                 else:
                     # If dependent cells don't look numeric, just put the pre-calculated python value (if any) or empty
                     cell_data["numberValue" if isinstance(cell_value, (int, float)) else "stringValue"] = cell_value if cell_value is not None else ""

            # --- Apply SUM Formulas in Summe Row ---
            elif is_sum_row and isinstance(cell_value, str) and cell_value.startswith("=SUM("):
                cell_data["formulaValue"] = cell_value

            # --- Handle standard cell values ---
            else:
                if isinstance(cell_value, float):
                    # Handle potential NaN or Inf values if they occur
                    if cell_value != cell_value or cell_value == float('inf') or cell_value == float('-inf'):
                         cell_data["stringValue"] = "Error" # Or some indicator
                     else:
                         cell_data["numberValue"] = cell_value
                elif isinstance(cell_value, int):
                    cell_data["numberValue"] = cell_value
                else:
                    cell_data["stringValue"] = str(cell_value) if cell_value is not None else ""

            new_row_values.append(cell_data)
        sheet_data_payload.append({"values": new_row_values})

    # --- Perform Sheet Updates ---
    try:
        # Clear previous content (make sure range is appropriate)
        clear_range = f"{sheet_name}!A1:{chr(ord('A') + num_cols - 1)}{num_rows + 5}"  # Clear a bit extra
        log_event("Sheet Clear", f"Clearing range {
                  clear_range} for sheet: {sheet_name}")
        service.spreadsheets().values().clear(spreadsheetId=SHEET_ID, range=sheet_name).execute()  # Clear entire sheet safer

        # Update with new data using batchUpdate for cell data to send formulas correctly
        update_body = {
            "valueInputOption": "USER_ENTERED",  # IMPORTANT for formulas
            "data": [
                # Specify range for the entire data block
                 {"range": f"{sheet_name}!A1", "rows": sheet_data_payload}
            ]
        }
        log_event("Sheet Update", f"Sending batchUpdate for {
                  sheet_name} data ({len(sheet_data_payload)} rows).")
        service.spreadsheets().values().batchUpdate(
            spreadsheetId=SHEET_ID, body=update_body).execute()
        log_event("Sheet Update Success",
                  f"Successfully updated data for {sheet_name}")

        # --- Formatting Requests ---
        # (Define formatting_requests list as before, including currency, dates, bolding, conditionals, background, etc.)
        formatting_requests = [
            {"repeatCell": {"range": {"sheetId": sheet_id, "startRowIndex": 0, "endRowIndex": 1}, "cell": {
                "userEnteredFormat": {"textFormat": {"bold": True}}}, "fields": "userEnteredFormat.textFormat.bold"}},
            {"repeatCell": {"range": {"sheetId": sheet_id, "startRowIndex": 1, "startColumnIndex": 6, "endColumnIndex": 7}, "cell": {
                "userEnteredFormat": {"numberFormat": {"type": "NUMBER", "pattern": "#,##0.00 €"}}}, "fields": "userEnteredFormat.numberFormat"}},
            {"repeatCell": {"range": {"sheetId": sheet_id, "startRowIndex": 1, "startColumnIndex": 8, "endColumnIndex": 9}, "cell": {
                "userEnteredFormat": {"numberFormat": {"type": "NUMBER", "pattern": "#,##0.00 €"}}}, "fields": "userEnteredFormat.numberFormat"}},
            {"repeatCell": {"range": {"sheetId": sheet_id, "startRowIndex": 1, "startColumnIndex": 9, "endColumnIndex": 13}, "cell": {
                "userEnteredFormat": {"numberFormat": {"type": "NUMBER", "pattern": "#,##0.00 €"}}}, "fields": "userEnteredFormat.numberFormat"}},
            {"repeatCell": {"range": {"sheetId": sheet_id, "startRowIndex": 1, "startColumnIndex": 4, "endColumnIndex": 5}, "cell": {
                "userEnteredFormat": {"numberFormat": {"type": "NUMBER", "pattern": "#,##0.00######"}}}, "fields": "userEnteredFormat.numberFormat"}},
            {"repeatCell": {"range": {"sheetId": sheet_id, "startRowIndex": 1, "startColumnIndex": 13, "endColumnIndex": 14}, "cell": {
                "userEnteredFormat": {"numberFormat": {"type": "NUMBER", "pattern": "0"}}}, "fields": "userEnteredFormat.numberFormat"}},
            # Date Formatting (Columns C, F, H - Indices 2, 5, 7)
            {"repeatCell": {"range": {"sheetId": sheet_id, "startRowIndex": 1, "startColumnIndex": 2, "endColumnIndex": 3}, "cell": {"userEnteredFormat": {"numberFormat": {"type": "DATE_TIME", "pattern": "yyyy-mm-dd hh:mm:ss"}}}, "fields": "userEnteredFormat.numberFormat"}},  # Datum
            {"repeatCell": {"range": {"sheetId": sheet_id, "startRowIndex": 1, "startColumnIndex": 5, "endColumnIndex": 6}, "cell": {"userEnteredFormat": {"numberFormat": {"type": "DATE_TIME", "pattern": "yyyy-mm-dd hh:mm:ss"}}}, "fields": "userEnteredFormat.numberFormat"}},  # Kaufdatum
            {"repeatCell": {"range": {"sheetId": sheet_id, "startRowIndex": 1, "startColumnIndex": 7, "endColumnIndex": 8}, "cell": {"userEnteredFormat": {"numberFormat": {"type": "DATE_TIME", "pattern": "yyyy-mm-dd hh:mm:ss"}}}, "fields": "userEnteredFormat.numberFormat"}},  # Verkaufsdatum
            # Conditional Formatting
            {"addConditionalFormatRule": {"rule": {"ranges": [{"sheetId": sheet_id, "startRowIndex": 1, "endRowIndex": num_rows, "startColumnIndex": 14, "endColumnIndex": 15}], "booleanRule": {
                "condition": {"type": "TEXT_EQ", "values": [{"userEnteredValue": "Ja"}]}, "format": {"backgroundColor": {"red": 1.0, "green": 0.8, "blue": 0.8}}}}, "index": 0}},
            {"addConditionalFormatRule": {"rule": {"ranges": [{"sheetId": sheet_id, "startRowIndex": 1, "endRowIndex": num_rows, "startColumnIndex": 14, "endColumnIndex": 15}], "booleanRule": {
                "condition": {"type": "TEXT_EQ", "values": [{"userEnteredValue": "Nein"}]}, "format": {"backgroundColor": {"red": 0.8, "green": 1.0, "blue": 0.8}}}}, "index": 1}},
            {"addConditionalFormatRule": {"rule": {"ranges": [{"sheetId": sheet_id, "startRowIndex": 1, "endRowIndex": num_rows, "startColumnIndex": 12, "endColumnIndex": 13}], "booleanRule": {
                "condition": {"type": "NUMBER_GREATER", "values": [{"userEnteredValue": "0"}]}, "format": {"textFormat": {"foregroundColor": {"red": 0.0, "green": 0.6, "blue": 0.0}}}}}, "index": 2}},
            {"addConditionalFormatRule": {"rule": {"ranges": [{"sheetId": sheet_id, "startRowIndex": 1, "endRowIndex": num_rows, "startColumnIndex": 12, "endColumnIndex": 13}], "booleanRule": {
                "condition": {"type": "NUMBER_LESS", "values": [{"userEnteredValue": "0"}]}, "format": {"textFormat": {"foregroundColor": {"red": 0.8, "green": 0.0, "blue": 0.0}}}}}, "index": 3}},
            # Text wrapping for notes column (P, index 15)
            {"repeatCell": {"range": {"sheetId": sheet_id, "startColumnIndex": 15, "endColumnIndex": 16, "startRowIndex": 1},
                             "cell": {"userEnteredFormat": {"wrapStrategy": "WRAP"}},
                             "fields": "userEnteredFormat.wrapStrategy"}},
            # Auto Resize Columns (Apply last)
             {"autoResizeDimensions": {"dimensions": {"sheetId": sheet_id,
                 "dimension": "COLUMNS", "startIndex": 0, "endIndex": num_cols}}},
        ]

        # Find summary/sum rows for background/bold formatting
        summe_row_index = -1
        summary_start_index = -1
        for i, row_content in enumerate(data):  # Iterate original data to find row types
            if i == 0:
                continue
            if row_content[1] == "Summe": summe_row_index = i
            if row_content[1] == "--- Steuerliche Zusammenfassung ---":
                 summary_start_index = i - 1 # Start from the separator row
                 break  # Assume only one summary block

        # Add specific formatting for Summe row and Summary Section
        if summe_row_index != -1:
            formatting_requests.insert(1,
                                        {"repeatCell": {"range": {"sheetId": sheet_id, "startRowIndex": summe_row_index, "endRowIndex": summe_row_index + 1}, "cell": {"userEnteredFormat": {"textFormat": {"bold": True}}}, "fields": "userEnteredFormat.textFormat.bold"}}
             )
        if summary_start_index != -1:
            formatting_requests.insert(1,
                                       {"repeatCell": {"range": {"sheetId": sheet_id, "startRowIndex": summary_start_index, "endRowIndex": num_rows}, "cell": {"userEnteredFormat": {"backgroundColor": {"red": 0.92, "green": 0.92, "blue": 0.92}}}, "fields": "userEnteredFormat.backgroundColor"}}
                )

        # Apply formatting
        if formatting_requests:
            log_event("Sheet Formatting", f"Applying {
                      len(formatting_requests)} formatting rules to {sheet_name}")
            service.spreadsheets().batchUpdate(spreadsheetId=SHEET_ID, body={
                                 "requests": formatting_requests}).execute()
            log_event("Sheet Formatting Success",
                      f"Applied formatting rules to {sheet_name}")

    except HttpError as e:
        error_details = "Unknown Error"
        try:
            error_details = json.loads(e.content.decode('utf-8'))
        except Exception:
            error_details = str(e)
        log_event("Sheet Write/Format Error", f"Sheet: {sheet_name}, Error: {error_details}")
        print(
            f"ERROR writing/formatting sheet '{sheet_name}': {error_details}")
        # Optional: raise e
    except Exception as e:
        log_event("Sheet Write/Format Error",
                  f"Sheet: {sheet_name}, Unexpected Error: {str(e)}")
        print(
            f"ERROR writing/formatting sheet '{sheet_name}': Unexpected error {str(e)}")
        # Optional: raise e


def get_or_create_sheet(spreadsheet_id, sheet_name):
    """Gets the ID of an existing sheet or creates it if not found."""
    try:
        spreadsheet = service.spreadsheets().get(
            spreadsheetId=spreadsheet_id).execute()
        sheets = spreadsheet.get('sheets', [])
        existing_sheet = next(
            (sheet for sheet in sheets if sheet['properties']['title'] == sheet_name), None)

        if existing_sheet:
            log_event("Sheet Found", f"Using existing sheet: '{sheet_name}' (ID: {existing_sheet['properties']['sheetId']})")
             return sheet_name, existing_sheet['properties']['sheetId']
        else:
            log_event("Sheet Creation", f"Creating new sheet: '{sheet_name}'")
            request = {"addSheet": {"properties": {"title": sheet_name, "gridProperties": {
                "rowCount": 2000, "columnCount": len(HEADERS)}}}}
            response = service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id,
                                            body={"requests": [request]}).execute()
            new_sheet_id = response['replies'][0]['addSheet']['properties']['sheetId']
            log_event("Sheet Created", f"New sheet '{
                      sheet_name}' created with ID: {new_sheet_id}")
            return sheet_name, new_sheet_id

    except HttpError as e:
        log_event("Sheet Discovery Error",
                  f"Failed to get/create sheet '{sheet_name}'. Error: {str(e)}")
        print(f"ERROR: Could not get or create sheet '{
              sheet_name}'. Check Sheet ID and permissions.")
        raise


def write_log_sheet():
    """Writes the collected log data to the 'Log' sheet."""
    sheet_name = "Log"
    if not LOG_DATA or len(LOG_DATA) <= 1:  # Check if there's anything to log besides header
        print("No log entries generated.")
        return

    try:
        # Attempt to get/create log sheet, continue even if it fails to log locally
        try:
            sheet_name, sheet_id = get_or_create_sheet(SHEET_ID, sheet_name)
        except Exception as sheet_err:
            print(f"WARNING: Could not get/create log sheet '{
                  sheet_name}': {sheet_err}. Will only log locally.")
            sheet_id = None  # Flag that sheet ops should be skipped

        if sheet_id:
            # Ensure all log data items are strings for writing
            log_values = [[str(item) for item in row] for row in LOG_DATA]
            body = {"values": log_values}
            # Clear only the content area
            service.spreadsheets().values().clear(spreadsheetId=SHEET_ID,
                                 range=f"{sheet_name}!A1:C").execute()
            # Update using USER_ENTERED
            service.spreadsheets().values().update(spreadsheetId=SHEET_ID, range=f"{
                                 sheet_name}!A1", valueInputOption="USER_ENTERED", body=body).execute()
            # Formatting
            formatting_requests = [
                {"autoResizeDimensions": {"dimensions": {"sheetId": sheet_id,
                    "dimension": "COLUMNS", "startIndex": 0, "endIndex": 3}}},
                {"repeatCell": {"range": {"sheetId": sheet_id, "startColumnIndex": 2, "endColumnIndex": 3, "startRowIndex": 0}, # Details column C
                                 "cell": {"userEnteredFormat": {"wrapStrategy": "WRAP"}},
                                 "fields": "userEnteredFormat.wrapStrategy"}}
            ]
            service.spreadsheets().batchUpdate(spreadsheetId=SHEET_ID, body={
                                 "requests": formatting_requests}).execute()
            print(f"Log written to Google Sheet: https://docs.google.com/spreadsheets/d/{
                  SHEET_ID}/edit#gid={sheet_id}")

    except Exception as e:
        print(f"CRITICAL: Error during log sheet processing: {str(e)}")
        print("Attempting to write log to local file 'tax_script_error.log' as fallback.")
        try:
            # Use 'w' to overwrite previous log file for the current run, 'a' to append
            with open("tax_script_error.log", "w", encoding='utf-8') as f:
                 f.write("Timestamp\tEvent\tDetails\n")  # Write header
                 for log_entry in LOG_DATA:
                     f.write("\t".join(map(str, log_entry)) + "\n")
                 f.write(
                     f"--- ERROR DURING LOG SHEET PROCESSING: {str(e)} ---\n")
             print("Log data saved to tax_script_error.log")
        except Exception as log_file_e:
            print(f"CRITICAL: Could not write log to file either: {log_file_e}")


# --- Main Execution Block ---
def main():
    """Main function to orchestrate fetching, processing, and writing."""
    global HOLDINGS  # Allow modification of global holdings state across years
    run_start_time = datetime.now()
    # Initialize logging immediately
    log_event("Script Started", f"Execution Time: {
              run_start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Script started at {run_start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    date_range_msg = f" (Using date range: {START_DATE_STR or 'Default History'} to {
                         END_DATE_STR or 'Present'})"
    print(f"Fetching data from Kraken{date_range_msg}...")

    all_trades = []
    all_ledger = []
    try:
        print("Fetching Kraken trade history...")
        all_trades = get_trades(START_TIMESTAMP, END_TIMESTAMP)
        print(f"Retrieved {len(all_trades)} unique trades.")

        print("Fetching Kraken ledger data...")
        all_ledger = get_ledger(START_TIMESTAMP, END_TIMESTAMP)
        print(f"Retrieved {len(all_ledger)} unique ledger entries.")

        if not all_trades and not all_ledger:
            print("\nNo trade or ledger data found for the specified period or default history.")
             log_event("Processing Abort",
                       "No trade or ledger data returned by API.")
             # Log writing happens in finally block
             return

        print("Grouping data by year...")
        data_by_year = group_by_year(all_trades, all_ledger)

        if not data_by_year:
            print("No data found for any year within the fetched results.")
             log_event("Processing Abort",
                       "Fetched data contained no processable years.")
             # Log writing happens in finally block
             return

        print(f"Found data for years: {sorted(list(data_by_year.keys()))}")

        # --- Process years chronologically ---
        all_years = sorted(data_by_year.keys())
        HOLDINGS = {}  # Reset holdings before processing the first year
        log_event("Global Holdings Reset",
                  "Resetting holdings before processing the first year.")

        for year in all_years:
            year_trades, year_ledger = data_by_year[year]
            print(f"\n--- Processing Year {year} ---")
            log_event(f"Year Start {year}", f"Input: {len(year_trades)} trades, {
                      len(year_ledger)} ledger entries.")

            print(f"Processing data for German tax declaration with FIFO...")
            tax_data = process_for_tax(year_trades, year_ledger, year)

            # Check if meaningful data rows were generated
            meaningful_rows = [row for i, row in enumerate(tax_data) if i > 0 and row[1] not in ["Summe", "--- Steuerliche Zusammenfassung ---", "GESAMT", "INFO", ""]]
            if not meaningful_rows:
                print(f"No significant transaction rows generated for {year}.")
                 log_event(f"Year Skip {
                           year}", f"No data rows generated after processing. Sheet not written.")
                 continue  # Skip writing sheet

            print(f"Writing tax report for {year} to Google Sheets...")
            write_to_sheets(tax_data, year)
            print(f"Tax report for {
                  year} written. Check Google Sheet: https://docs.google.com/spreadsheets/d/{SHEET_ID}")
            time.sleep(2)  # Pause between sheet writes

        print("\nProcessing complete.")

    except Exception as e:
        print(f"\n--- SCRIPT ERROR ---")
        print(f"An error occurred: {str(e)}")
        print("Traceback:")
        # Print traceback directly to console for immediate feedback
        traceback.print_exc()
        # Log the error and traceback
        log_event("CRITICAL ERROR", f"{str(e)}\n{traceback.format_exc()}")
        print("\nPlease check logs for details. Attempting to write final logs...")
        # Log writing happens in finally block

    finally:
        # Always attempt to write the log sheet at the very end
        run_end_time = datetime.now()
        duration = run_end_time - run_start_time
        log_event("Script Finished", f"Execution Time: {
                  run_end_time.strftime('%Y-%m-%d %H:%M:%S')}, Duration: {duration}")
        print(f"\nScript finished at {run_end_time.strftime(
            '%Y-%m-%d %H:%M:%S')} (Duration: {duration}).")
        print("Attempting to write final logs...")
        write_log_sheet()


if __name__ == "__main__":
    # Optional: Set locale if needed for specific string formatting, but API formatting is preferred
    # try:
    #     locale.setlocale(locale.LC_ALL, 'de_DE.UTF-8')
    # except locale.Error:
    #     print("Warning: German locale 'de_DE.UTF-8' not available. Using default.")
    main()
