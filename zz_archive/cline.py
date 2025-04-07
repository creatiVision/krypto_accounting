#!/usr/bin/env python3
import subprocess
import sys
import json
from pathlib import Path
from datetime import datetime, timezone
import time
import hmac
import hashlib
import base64
import requests
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import locale
import traceback

# --- Global Variables & Initial Logging Setup ---
# Define LOG_DATA early so log_event works during setup
LOG_DATA = [["Timestamp", "Event", "Details"]]

def log_event(event, details):
    """Appends an event to the global log data."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    LOG_DATA.append([timestamp, event, str(details)])
    # print(f"LOG: {timestamp} - {event} - {details}") # Optional

# --- Package Installation ---
def install_packages(packages):
    """Installs required Python packages using pip."""
    try:
        print(f"Attempting to install/update: {', '.join(packages)}...")
        subprocess.check_call([sys.executable, "-m", "pip", "install", "--quiet"] + packages)
        print(f"Successfully installed/verified: {', '.join(packages)}")
        log_event("Package Install", f"Successfully installed/verified: {', '.join(packages)}")
    except subprocess.CalledProcessError as e:
        print(f"ERROR: Failed to install packages: {', '.join(packages)}.")
        print(f"pip error output: {e}")
        print(f"Please try installing manually: pip install {' '.join(packages)}")
        log_event("Package Install Error", f"Failed to install {', '.join(packages)}: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"ERROR: An unexpected error occurred during package installation: {e}")
        log_event("Package Install Error", f"Unexpected error installing {', '.join(packages)}: {e}")
        sys.exit(1)

required_packages = ["requests", "google-auth", "google-auth-oauthlib", "google-auth-httplib2", "google-api-python-client"]
for package in required_packages:
    try:
        import_name = package.replace('-', '_')
        __import__(import_name)
    except ImportError:
        print(f"Package '{package}' not found. Installing...")
        install_packages([package])

# --- Configuration Loading ---
CONFIG_FILE = Path(__file__).parent / "config.json"
CREDENTIALS_FILE = Path(__file__).parent / "mbay-tax-sheet-for-kryptos-7fc01e35fb9a.json"
print(f"Looking for config file at: {CONFIG_FILE}")
print(f"Looking for credentials file at: {CREDENTIALS_FILE}")

API_KEY, API_SECRET, SHEET_ID, THEFT_TXIDS = None, None, None, []
START_DATE_STR, END_DATE_STR = None, None
START_TIMESTAMP, END_TIMESTAMP = None, None

try:
    with CONFIG_FILE.open('r') as f:
        config = json.load(f)
    API_KEY = config["API_KEY"]
    API_SECRET = config["API_SECRET"]
    SHEET_ID = config["SHEET_ID"]
    THEFT_TXIDS = config.get("theft_txids", [])
    START_DATE_STR = config.get("start_date")
    END_DATE_STR = config.get("end_date")

    if START_DATE_STR:
        try:
            START_TIMESTAMP = int(datetime.strptime(START_DATE_STR, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp())
        except ValueError:
            print(f"Error: Invalid start_date format '{START_DATE_STR}'. Use YYYY-MM-DD. Ignoring.")
    if END_DATE_STR:
        try:
            END_TIMESTAMP = int((datetime.strptime(END_DATE_STR, "%Y-%m-%d").replace(hour=23, minute=59, second=59, tzinfo=timezone.utc)).timestamp())
        except ValueError:
            print(f"Error: Invalid end_date format '{END_DATE_STR}'. Use YYYY-MM-DD. Ignoring.")
except FileNotFoundError:
    print(f"ERROR: Config file not found at: {CONFIG_FILE}")
    log_event("Config Error", f"Config file not found at: {CONFIG_FILE}")
    sys.exit(1)
except KeyError as e:
    print(f"ERROR: Missing required key in config.json: {str(e)}")
    log_event("Config Error", f"Missing key in config.json: {e}")
    sys.exit(1)
except Exception as e:
    print(f"Error loading configuration: {e}")
    log_event("Config Error", f"Error loading config: {e}")
    sys.exit(1)

# --- Google API Setup ---
creds, service = None, None
try:
    SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_file(str(CREDENTIALS_FILE), scopes=SCOPES)
    service = build("sheets", "v4", credentials=creds)
except FileNotFoundError:
    print(f"ERROR: Credentials file not found at: {CREDENTIALS_FILE}")
    log_event("Credentials Error", f"Credentials file not found: {CREDENTIALS_FILE}")
    sys.exit(1)
except Exception as e:
    print(f"Error setting up Google API service: {e}")
    log_event("Google API Error", f"Error setting up service: {e}")
    print("Please ensure credentials file path is correct and service account has permissions.")
    sys.exit(1)

# --- Global Variables (cont.) ---
HOLDINGS = {}
PRICE_CACHE = {}
HEADERS = ["Zeile", "Typ", "Datum", "Asset", "Anzahl", "Kaufdatum", "Kaufpreis (€)/Stk", "Verkaufsdatum", "Verkaufspreis (€)/Stk", "Kosten (€)", "Erlös (€)", "Gebühr (€)", "Gewinn / Verlust (€)", "Haltedauer (Tage)", "Steuerpflichtig", "Notizen / FIFO-Details"]

# --- Kraken API Request Function ---
def kraken_request(endpoint, data=None):
    """Sends an authenticated request to the Kraken API."""
    url = f"https://api.kraken.com/0/private/{endpoint}"
    if data is None:
        data = {}
    data["nonce"] = str(int(time.time() * 100000))
    post_data = "&".join([f"{k}={v}" for k, v in data.items()])
    try:
        encoded = (str(data["nonce"]) + post_data).encode()
        message = f"/0/private/{endpoint}".encode() + hashlib.sha256(encoded).digest()
        signature = hmac.new(base64.b64decode(API_SECRET), message, hashlib.sha512)
        sig = base64.b64encode(signature.digest()).decode()
    except Exception as e:
        log_event("HMAC Error", f"Failed to generate signature: {e}")
        raise Exception(f"HMAC signature generation failed: {e}")
    headers = {"API-Key": API_KEY, "API-Sign": sig}
    max_retries, wait_time = 3, 5
    for attempt in range(max_retries):
        try:
            response = requests.post(url, headers=headers, data=data, timeout=45)
            log_event(f"API Call: {endpoint}", f"Attempt: {attempt+1}, Nonce: {data['nonce']}, Status: {response.status_code}...")
            response.raise_for_status()
            result = response.json()
            if result.get("error"):
                error_messages = result["error"]
                if any("Nonce" in e for e in error_messages):
                    log_event("API Error - Nonce", f"Nonce {data['nonce']} invalid. Retrying. Error: {error_messages}")
                    if attempt < max_retries - 1:
                        time.sleep(wait_time * (attempt + 1))
                        data["nonce"] = str(int(time.time() * 100000))
                        post_data = "&".join([f"{k}={v}" for k, v in data.items()])
                        try: # Regenerate Signature
                            encoded = (str(data["nonce"]) + post_data).encode()
                            message = f"/0/private/{endpoint}".encode() + hashlib.sha256(encoded).digest()
                            signature = hmac.new(base64.b64decode(API_SECRET), message, hashlib.sha512)
                            sig = base64.b64encode(signature.digest()).decode()
                            headers["API-Sign"] = sig
                        except Exception as sig_e:
                            log_event("HMAC Error on Retry", f"Failed to regenerate signature: {sig_e}")
                            raise Exception(f"HMAC signature generation failed on retry: {sig_e}")
                        continue # Retry
                    else:
                        log_event("API Error - Nonce Fatal", f"Nonce error persisted. Error: {error_messages}")
                        raise Exception(f"API nonce error after retries: {error_messages}")
                elif any("Permission denied" in e for e in error_messages):
                    log_event("API Error - Permissions", f"Permission denied for {endpoint}. Error: {error_messages}")
                    raise Exception(f"API Permission Denied for {endpoint}: {error_messages}")
                else:
                    log_event("API Error - Generic", f"Endpoint: {endpoint}, Error: {error_messages}")
                    raise Exception(f"API error: {error_messages}")
            return result # Success
        except requests.exceptions.Timeout as e:
            log_event("API Request Error - Timeout", f"Attempt: {attempt+1}, Error: {e}")
            if attempt == max_retries - 1:
                raise Exception(f"API timeout after {max_retries} attempts: {e}")
            time.sleep(wait_time * (attempt + 1))
        except requests.exceptions.RequestException as e:
            log_event("API Request Error - Connection/Other", f"Attempt: {attempt+1}, Error: {e}")
            if attempt == max_retries - 1:
                raise Exception(f"API request failed after {max_retries} attempts: {e}")
            time.sleep(wait_time * (attempt + 1))
        except Exception as e:
            log_event("API Call Exception - Unexpected", f"Attempt: {attempt+1}, Error: {e}\n{traceback.format_exc()}")
            raise e
    raise Exception(f"API call {endpoint} failed after all retries.")

# --- Data Fetching Functions ---
def get_trades(start_ts=None, end_ts=None):
    trades_dict = {}
    offset, count = 0, -1
    fetch_params = {"trades": "true"}
    if start_ts:
        fetch_params["start"] = start_ts
    if end_ts:
        fetch_params["end"] = end_ts
    log_event("Fetching Trades", "Starting trade history retrieval.")
    while True:
        try:
            current_params = {"ofs": offset, **fetch_params}
            result = kraken_request("TradesHistory", current_params)
            batch_dict = result.get("result", {}).get("trades", {})
            if count == -1:
                count = int(result.get("result", {}).get("count", 0))
                log_event("Fetching Trades Info", f"API reports total {count} trades for query.")
            if not batch_dict:
                break
            trades_dict.update(batch_dict)
            log_event("Trades Fetched Batch", f"Offset:{offset}, Batch:{len(batch_dict)}, Total:{len(trades_dict)}, Expected:{count}")
            offset += len(batch_dict)
            if count != -1 and offset >= count:
                break
            time.sleep(1.1)
        except Exception as e:
            log_event("Error Fetching Trades", f"Failed at offset {offset}. Error: {e}")
            raise
    final_trades = list(trades_dict.values())
    log_event("Fetching Trades Completed", f"Fetched: {len(final_trades)}")
    return final_trades

def get_ledger(start_ts=None, end_ts=None):
    ledger_dict = {}
    offset, count = 0, -1
    fetch_params = {}
    if start_ts:
        fetch_params["start"] = start_ts
    if end_ts:
        fetch_params["end"] = end_ts
    log_event("Fetching Ledger", "Starting ledger history retrieval.")
    while True:
        try:
            current_params = {"ofs": offset, **fetch_params}
            result = kraken_request("Ledgers", current_params)
            batch_dict = result.get("result", {}).get("ledger", {})
            if count == -1:
                count = int(result.get("result", {}).get("count", 0))
                log_event("Fetching Ledger Info", f"API reports total {count} entries.")
            if not batch_dict:
                break
            ledger_dict.update(batch_dict)
            log_event("Ledger Fetched Batch", f"Offset:{offset}, Batch:{len(batch_dict)}, Total:{len(ledger_dict)}, Expected:{count}")
            offset += len(batch_dict)
            if count != -1 and offset >= count:
                break
            time.sleep(1.1)
        except Exception as e:
            log_event("Error Fetching Ledger", f"Failed at offset {offset}. Error: {e}")
            raise
    final_ledger = list(ledger_dict.values())
    log_event("Fetching Ledger Completed", f"Fetched: {len(final_ledger)}")
    return final_ledger

# --- Grouping Function ---
def group_by_year(trades, ledger):
    trades_by_year, ledger_by_year = {}, {}
    for trade in trades:
        try:
            year = datetime.fromtimestamp(float(trade["time"]), timezone.utc).year
            trades_by_year.setdefault(year, []).append(trade)
        except Exception as e:
            log_event("Grouping Error", f"Trade Time Error: {trade.get('ordertxid', 'N/A')}, Error: {e}")
    for entry in ledger:
        try:
            year = datetime.fromtimestamp(float(entry["time"]), timezone.utc).year
            ledger_by_year.setdefault(year, []).append(entry)
        except Exception as e:
            log_event("Grouping Error", f"Ledger Time Error: {entry.get('refid', 'N/A')}, Error: {e}")
    all_years = set(trades_by_year.keys()).union(ledger_by_year.keys())
    log_event("Data Grouping", f"Grouped for years: {sorted(list(all_years))}")
    return {year: (sorted(trades_by_year.get(year, []), key=lambda x: float(x["time"])),
                   sorted(ledger_by_year.get(year, []), key=lambda x: float(x["time"])))
            for year in sorted(all_years)}

# --- Price Fetching Function ---
def get_market_price(asset, timestamp):
    asset_map={"ETH":"ETH/EUR","XETH":"ETH/EUR","XBT":"XBT/EUR","XXBT":"XBT/EUR","BTC":"XBT/EUR","XRP":"XRP/EUR","XXRP":"XRP/EUR","ADA":"ADA/EUR","LTC":"LTC/EUR","XLM":"XLM/EUR","EOS":"EOS/EUR","ETC":"ETC/EUR","AVAX":"AVAX/EUR","ARB":"ARB/EUR","EUR":None,"ZEUR":None,"KFEE":None}
    normalized_asset = asset.replace('Z','',1) if len(asset)==4 and asset.startswith('Z') else asset
    normalized_asset = normalized_asset.replace('X','',1) if len(normalized_asset)==4 and normalized_asset.startswith('X') else normalized_asset
    pair = asset_map.get(normalized_asset)
    if not pair:
        log_event("Price Fetch Error", f"Unsupported asset: {asset}")
        return 0
    timestamp_int = int(timestamp)
    cache_key = (pair, timestamp_int // 3600)
    if cache_key in PRICE_CACHE:
        log_event("Price Cache Hit", f"Using cached price for {pair}")
        return PRICE_CACHE[cache_key]
    since_time = timestamp_int - 3600
    url = "https://api.kraken.com/0/public/Trades"
    params = {"pair": pair, "since": str(since_time)}
    log_event("Public API Call: Trades", f"Fetching price for {pair} at {datetime.fromtimestamp(timestamp, timezone.utc)}")
    try:
        response = requests.get(url, params=params, timeout=15)
        response.raise_for_status()
        data = response.json()
        if data.get("error"):
            if any("Unknown asset pair" in e for e in data["error"]):
                log_event("Price Fetch Warning", f"{pair} unknown. 0 EUR.")
                PRICE_CACHE[cache_key]=0
                return 0
            raise Exception(f"Public API Error: {data['error']}")
        result_keys = list(data.get("result",{}).keys())
        pair_data_key = next((k for k in result_keys if k!='last'), None)
        if not pair_data_key:
            log_event("Price Fetch Warning", f"No trade key for {pair}. 0 EUR.")
            PRICE_CACHE[cache_key]=0
            return 0
        pair_trades = data["result"].get(pair_data_key,[])
        if not pair_trades:
            log_event("Price Fetch Warning", f"No trades array for {pair}. 0 EUR.")
            PRICE_CACHE[cache_key]=0
            return 0
        relevant_trade = None
        for trade in reversed(pair_trades):
            if float(trade[2]) <= timestamp:
                relevant_trade = trade
                break
        trade_price = 0
        if relevant_trade:
            trade_price = float(relevant_trade[0])
            log_event("Price Found", f"Using price {trade_price} EUR for {pair} at {datetime.fromtimestamp(relevant_trade[2], timezone.utc)}")
        elif pair_trades:
            earliest_trade_time = float(pair_trades[0][2])
            if earliest_trade_time < since_time + 7200:
                trade_price = float(pair_trades[0][0])
                log_event("Price Found (Approx)", f"Using earliest price {trade_price} EUR for {pair} at {datetime.fromtimestamp(earliest_trade_time, timezone.utc)}")
            else:
                log_event("Price Found (Stale)", "Earliest trade too far. 0 EUR.")
                trade_price = 0
        else:
            log_event("Price Fetch Error", "Logic error. 0 EUR.")
            trade_price = 0
        PRICE_CACHE[cache_key] = trade_price
        return trade_price
    except requests.exceptions.RequestException as e:
        log_event("Price Fetch HTTP Error", f"Failed for {pair}: {e}. 0 EUR.")
        PRICE_CACHE[cache_key]=0
        return 0
    except Exception as e:
        log_event("Price Fetch General Error", f"Failed for {pair}: {e}. 0 EUR.")
        PRICE_CACHE[cache_key]=0
        return 0

# --- Main Processing Function ---
def process_for_tax(trades, ledger, year):
    tax_data = [HEADERS]
    events = []
    for trade in trades:
        events.append({"type": "trade", "data": trade, "time": float(trade.get("time", 0))})
    for entry in ledger:
        events.append({"type": "ledger", "data": entry, "time": float(entry.get("time", 0))})
    valid_events = [e for e in events if e["time"] > 0]
    if len(valid_events) != len(events):
        log_event("Data Warning", f"Removed {len(events) - len(valid_events)} events with invalid timestamp.")
    events = valid_events
    events.sort(key=lambda x: x["time"])
    summaries = {}
    total_taxable_gains, total_tax_free_gains, total_losses, total_fees_eur = 0,0,0,0
    line_num = 1
    processed_refids = set()
    log_event(f"Processing Year {year}", f"Starting with {len(events)} events.")
    log_event(f"Start Holdings {year}", f"{json.dumps(HOLDINGS, indent=2)}")

    for event_index, event in enumerate(events):
        timestamp = event["time"]
        event_year = datetime.fromtimestamp(timestamp, timezone.utc).year
        if event_year != year:
            continue
        date_str = datetime.fromtimestamp(timestamp, timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        data = event["data"]
        row_base = [""] * len(HEADERS)
        row_base[0] = line_num + 1

        if event["type"] == "trade":
            trade = data
            pair = trade.get("pair", "N/A")
            trade_type = trade.get("type", "N/A")
            amount_traded = float(trade.get("vol", 0))
            price_per_unit = float(trade.get("price", 0))
            fee_paid = float(trade.get("fee", 0))
            cost_or_proceeds = float(trade.get("cost", 0))
            trade_refid = trade.get("ordertxid", f"trade_{timestamp}")

            if trade_refid in processed_refids:
                continue

            asset_base, asset_quote, parsed = "", "", False
            known_eur_endings = ["ZEUR", "EUR"]
            for eur in known_eur_endings:
                if pair.endswith(eur):
                    asset_base = pair[:-len(eur)]
                    asset_quote = "EUR"
                    parsed = True
                    break
            if not parsed and len(pair) >= 6:
                potential_split = -1
                if len(pair) == 6:
                    potential_split = 3
                elif len(pair) == 7:
                    potential_split = 4 if pair.startswith(('X', 'Z')) else 3
                elif len(pair) == 8:
                    potential_split = 4

                if potential_split > 0:
                    asset_base = pair[:potential_split]
                    asset_quote = pair[potential_split:]
                    parsed = True
                else:
                    log_event("Trade Parse Warning", f"Cannot reliably parse trade pair '{pair}'. Ref: {trade_refid}.")
            elif not parsed:
                log_event("Trade Parse Warning", f"Cannot parse pair '{pair}'. Ref: {trade_refid}")

            if not parsed:
                continue

            asset_base_norm = asset_base.replace('Z','',1).replace('X','',1) if len(asset_base)>=4 and asset_base[0] in ('X','Z') else asset_base
            asset_quote_norm = asset_quote.replace('Z','',1).replace('X','',1) if len(asset_quote)>=4 and asset_quote[0] in ('X','Z') else asset_quote

            if trade_type == "buy" and asset_quote_norm == "EUR":
                asset = asset_base_norm
                buy_entry = {"amount": amount_traded, "price_eur": price_per_unit, "timestamp": timestamp, "fee_eur": fee_paid, "refid": trade_refid}
                HOLDINGS.setdefault(asset, []).append(buy_entry)
                HOLDINGS[asset].sort(key=lambda x: x["timestamp"])
                row_base[1]="Kauf (Trade)"; row_base[2]=date_str; row_base[3]=asset; row_base[4]=amount_traded; row_base[5]=date_str; row_base[6]=price_per_unit; row_base[9]=cost_or_proceeds; row_base[11]=fee_paid; row_base[14]="N/A"; row_base[15]=f"Trade Ref: {trade_refid}"
                tax_data.append(row_base)
                log_event("Buy Recorded (Trade)", f"{date_str}, Asset:{asset}, Amount:{amount_traded:.8f}, Price:{price_per_unit:.4f}, Cost:{cost_or_proceeds:.2f}, Fee:{fee_paid:.4f}, Ref:{trade_refid}")
                line_num += 1
                processed_refids.add(trade_refid)
                # Attempt to link ledger spend
                try:
                    cost_tolerance=cost_or_proceeds*0.01; time_window_ledger=15; found_ledger_spend=False
                    for idx in range(max(0, event_index - 5), min(event_index + 10, len(events))):
                        if idx == event_index: continue
                        le=events[idx]
                        if abs(le["time"] - timestamp) > time_window_ledger: continue
                        if le["type"] == "ledger":
                            l_entry=le["data"]; l_refid=l_entry.get("refid"); l_asset=l_entry.get("asset",""); l_type=l_entry.get("type","")
                            if l_asset in ["ZEUR","EUR"] and l_type=="spend" and l_refid not in processed_refids:
                                l_amount=abs(float(l_entry.get("amount",0)))
                                if abs(l_amount - cost_or_proceeds) <= cost_tolerance:
                                    log_event("Buy/Spend Link", f"Linked Buy {trade_refid} to Ledger Spend {l_refid}.")
                                    processed_refids.add(l_refid); found_ledger_spend=True; break
                    if not found_ledger_spend:
                        log_event("Buy/Spend Link", f"No matching Ledger Spend for Buy {trade_refid} found.")
                except Exception as link_e:
                    log_event("Buy/Spend Link Error", f"Error linking buy {trade_refid}: {link_e}")
            else: # Log other trades
                if trade_refid not in processed_refids:
                    log_event("Generic Trade Logged", f"Ref: {trade_refid}, Type: {trade_type}, Pair: {pair}, Amount: {amount_traded:.8f}. Details in Ledger.")
                    processed_refids.add(trade_refid)

        elif event["type"] == "ledger":
            entry=data; entry_type=entry.get("type","N/A").capitalize(); asset=entry.get("asset","N/A"); asset_norm=asset.replace('Z','',1).replace('X','',1) if len(asset)>=4 and asset[0] in ('X','Z') else asset; amount_ledger=float(entry.get("amount",0)); fee_paid=float(entry.get("fee",0)); refid=entry.get("refid",f"ledger_{timestamp}")
            if refid in processed_refids: continue
            if asset_norm == "KFEE": continue
            if entry_type in ["Transfer", "Margin"]:
                log_event("Ledger Skip (Transfer/Margin)", f"Ref: {refid}, Type: {entry_type}, Asset: {asset_norm}, Amount: {amount_ledger}")
                processed_refids.add(refid); continue
            if entry_type == "Trade":
                log_event("Ledger Trade Detected", f"Ref: {refid}, Asset: {asset_norm}, Amount: {amount_ledger}. Processing based on sign.")

            is_theft = refid in THEFT_TXIDS
            notes = f"Ledger Ref: {refid}" + (" | THEFT" if is_theft else "")
            is_sale_or_trade_processed = False

            if (entry_type == "Spend" or entry_type == "Trade") and amount_ledger < 0 and asset_norm != "EUR":
                log_event("Sale Detection Start", f"Checking for sale/trade: Ref={refid}, Asset={asset_norm}, Amount={amount_ledger}, Time={timestamp}")
                corresponding_receive=None; time_window_sale=120
                for next_idx in range(event_index + 1, len(events)):
                    next_event = events[next_idx]; time_diff = next_event["time"] - timestamp
                    log_event("Sale Detection Debug", f"  Checking vs event {next_idx}: TimeDiff={time_diff:.2f}s, Type={next_event['type']}")
                    if time_diff > time_window_sale: log_event("Sale Detection Debug", f"  -> Time window exceeded."); break
                    if next_event["type"] == "ledger":
                        next_entry=next_event["data"]; next_refid = next_entry.get("refid"); next_type = next_entry.get("type","").capitalize(); next_amount = float(next_entry.get("amount",-1))
                        log_event("Sale Detection Debug", f"  -> Ledger Candidate: Ref={next_refid}, Type={next_type}, Amount={next_amount}")
                        if next_refid == refid and next_type in ["Receive","Trade"] and next_amount > 0:
                            corresponding_receive=next_entry; log_event("Sale Detection Debug", f"  -> MATCH FOUND for {refid}!"); break
                if not corresponding_receive and (entry_type == "Spend" or entry_type == "Trade"):
                    log_event("Sale Detection Result", f"No corresponding receive for spend {refid} ({asset_norm}, {amount_ledger})")

                if corresponding_receive:
                    is_sale_or_trade_processed=True; sell_asset=asset_norm; sell_amount=abs(amount_ledger)
                    rcv_asset_raw=corresponding_receive.get("asset","N/A"); rcv_asset=rcv_asset_raw.replace('Z','',1).replace('X','',1); rcv_amount=float(corresponding_receive.get("amount",0)); rcv_fee=float(corresponding_receive.get("fee",0)); fee_eur=rcv_fee if rcv_fee!=0 else fee_paid; total_fees_eur+=fee_eur
                    proceeds, price_pu = 0, 0; is_crypto_crypto=(rcv_asset!="EUR")
                    if not is_crypto_crypto: proceeds=rcv_amount; price_pu=proceeds/sell_amount if sell_amount > 1e-12 else 0; log_event("Sale Detected (Crypto->EUR)", f"{date_str}, Sold:{sell_amount:.8f} {sell_asset}, Recv:{proceeds:.2f} EUR, Fee:{fee_eur:.4f}, Ref:{refid}")
                    else: price_rcv=get_market_price(rcv_asset_raw, timestamp); proceeds=rcv_amount*price_rcv; price_pu=proceeds/sell_amount if sell_amount > 1e-12 else 0; log_event("Trade Detected (Crypto->Crypto)", f"{date_str}, Sold:{sell_amount:.8f} {sell_asset}, Recv:{rcv_amount:.8f} {rcv_asset} (@{price_rcv:.4f}), Proceeds:{proceeds:.2f}, Fee:{fee_eur:.4f}, Ref:{refid}"); cost_rcv=price_rcv; rcv_entry={"amount":rcv_amount,"price_eur":cost_rcv,"timestamp":timestamp,"fee_eur":0,"refid":refid+"-rcv"}; HOLDINGS.setdefault(rcv_asset,[]).append(rcv_entry); HOLDINGS[rcv_asset].sort(key=lambda x: x["timestamp"]); log_event("Trade Receive Leg", f"Added {rcv_asset}. Amt:{rcv_amount:.8f}, CostPrice:{cost_rcv:.4f}, Ref:{refid}-rcv")
                    # FIFO Logic
                    log_event(f"FIFO Start ({'Sale' if not is_crypto_crypto else 'Trade'})", f"Ref:{refid}, Selling {sell_amount:.8f} {sell_asset}. Holdings BEFORE: {json.dumps(HOLDINGS.get(sell_asset,[]))}")
                    rem_sell=sell_amount; cost_basis=0; fifo_details=[]; buy_ts=timestamp; tol=1e-9; consumed_idx=[]
                    if sell_asset in HOLDINGS and HOLDINGS[sell_asset]:
                        for i, lot in enumerate(HOLDINGS[sell_asset]):
                            if rem_sell <= tol: break
                            b_amt, b_pr, b_t, b_ref = lot.get("amount",0), lot.get("price_eur",0), lot.get("timestamp",0), lot.get("refid","N/A")
                            if buy_ts == timestamp or b_t < buy_ts: buy_ts = b_t
                            use=min(b_amt, rem_sell); cost_part=use*b_pr; cost_basis+=cost_part;
                            log_event("FIFO Applied", f"Ref: {refid}, Used {use:.8f} from lot bought {datetime.fromtimestamp(b_t, timezone.utc).strftime('%Y-%m-%d')} (Ref:{b_ref}), Cost Basis Part: {cost_part:.2f} EUR")
                            fifo_details.append(f"Lot {i+1}: {use:.8f}@{b_pr:.4f}(Ref:{b_ref})")
                            consumed_idx.append({"index":i,"amount_used":use}); rem_sell-=use
                        for consumed in sorted(consumed_idx, key=lambda x: x['index'], reverse=True):
                            idx, used = consumed["index"], consumed["amount_used"]
                            if abs(HOLDINGS[sell_asset][idx]["amount"] - used) < tol: del HOLDINGS[sell_asset][idx]
                            else: HOLDINGS[sell_asset][idx]["amount"] -= used
                        if sell_asset in HOLDINGS and not HOLDINGS[sell_asset]: del HOLDINGS[sell_asset]
                        if rem_sell > tol: fifo_details.append(f"Warn:{rem_sell:.8f} sold w/o buy.")
                    else: log_event("FIFO Warning", f"Ref:{refid}, Sold {sell_asset}, no holdings."); cost_basis, buy_ts=0,timestamp; fifo_details.append(f"Warn:Entire {sell_amount:.8f} sold w/o buy.")
                    log_event(f"FIFO End ({'Sale' if not is_crypto_crypto else 'Trade'})", f"Ref:{refid}, Sold {sell_amount:.8f} {sell_asset}. Holdings AFTER: {json.dumps(HOLDINGS.get(sell_asset,[]))}")
                    # Calculate P/L, holding, summaries
                    gain_loss=proceeds-cost_basis-fee_eur; hold_days=(timestamp-buy_ts)/(24*3600) if buy_ts<timestamp else 0; is_taxable=hold_days<=365
                    summaries.setdefault(sell_asset, {"gains":0,"tax_free_gains":0,"losses":0,"taxable_days":[],"tax_free_days":[]})
                    if gain_loss>=0:
                        if is_taxable: summaries[sell_asset]["gains"]+=gain_loss; summaries[sell_asset]["taxable_days"].append(hold_days); total_taxable_gains+=gain_loss; log_event("Taxable Gain/Zero", f"Ref:{refid} Asset:{sell_asset} Gain:{gain_loss:.2f} Days:{hold_days:.0f}")
                        else: summaries[sell_asset]["tax_free_gains"]+=gain_loss; summaries[sell_asset]["tax_free_days"].append(hold_days); total_tax_free_gains+=gain_loss; log_event("Tax-Free Gain", f"Ref:{refid} Asset:{sell_asset} Gain:{gain_loss:.2f} Days:{hold_days:.0f}")
                    else: # Loss
                        if not is_theft: summaries[sell_asset]["losses"]+=gain_loss; total_losses+=gain_loss; log_event("Loss Recorded", f"Ref:{refid} Asset:{sell_asset} Loss:{gain_loss:.2f} Days:{hold_days:.0f}")
                        else: notes += " | Theft loss - Non-deductible"; log_event("Theft Loss Excluded", f"Ref:{refid} Asset:{sell_asset} Loss:{gain_loss:.2f}")
                    # Populate Sheet Row
                    row_base[1]="Verkauf" if not is_crypto_crypto else "Tausch"; row_base[2]=date_str; row_base[3]=sell_asset; row_base[4]=sell_amount; row_base[5]=datetime.fromtimestamp(buy_ts,timezone.utc).strftime("%Y-%m-%d %H:%M:%S") if buy_ts<timestamp else "N/A"; row_base[6]=cost_basis/sell_amount if sell_amount>1e-12 else 0; row_base[7]=date_str; row_base[8]=price_pu; row_base[9]=cost_basis; row_base[10]=proceeds; row_base[11]=fee_eur; row_base[12]=gain_loss; row_base[13]=round(hold_days); row_base[14]="Ja" if is_taxable else "Nein"; row_base[15]=notes + " | FIFO: " + " | ".join(fifo_details)
                    log_event("Sale/Trade Row Added", f"Ref: {refid}, Asset: {sell_asset}, Qty: {sell_amount:.8f}, BuyDateUsed: {row_base[5]}, P/L: {gain_loss:.2f}, Holding: {round(hold_days)}")
                    tax_data.append(row_base); line_num+=1; processed_refids.add(refid); processed_refids.add(corresponding_receive.get("refid", refid))

            elif not is_sale_or_trade_processed and (entry_type == "Spend" or entry_type == "Trade") and amount_ledger < 0: # Unmatched Spend/Trade
                if asset_norm == "EUR":
                    row_base[1]="EUR Spend (Unmatched)"; row_base[2]=date_str; row_base[3]="EUR"; row_base[4]=abs(amount_ledger); row_base[11]=fee_paid; row_base[14]="N/A"; row_base[15]=notes + " | Possible crypto buy missing?"
                    tax_data.append(row_base); log_event("Unmatched EUR Spend", f"{date_str}, Amt:{abs(amount_ledger):.2f}, Fee:{fee_paid:.2f}, Ref:{refid}. Missing buy?"); line_num+=1; processed_refids.add(refid)
                else: # Withdrawal
                    log_event("Withdrawal Detected", f"{date_str}, Asset:{asset_norm}, Amt:{abs(amount_ledger):.8f}, Ref:{refid}. Reducing holdings.")
                    log_event("Withdrawal FIFO Start", f"Ref:{refid}, Withdrawing {abs(amount_ledger):.8f} {asset_norm}. Holdings BEFORE: {json.dumps(HOLDINGS.get(asset_norm,[]))}")
                    rem_wd=abs(amount_ledger); cost_wd=0; fifo_details=[]; tol=1e-9; consumed_idx=[]
                    if asset_norm in HOLDINGS and HOLDINGS[asset_norm]:
                        for i, lot in enumerate(HOLDINGS[asset_norm]):
                            if rem_wd <= tol: break
                            use=min(lot["amount"], rem_wd); cost_wd+=use*lot["price_eur"]
                            log_event("Withdrawal FIFO Applied", f"Ref: {refid}, Removed {use:.8f} from lot bought {datetime.fromtimestamp(lot['timestamp'], timezone.utc).strftime('%Y-%m-%d')} (Ref:{lot.get('refid','N/A')})")
                            fifo_details.append(f"Lot {i+1}: {use:.8f} @ {lot['price_eur']:.4f} EUR removed")
                            consumed_idx.append({"index":i,"amount_used":use}); rem_wd-=use
                        for consumed in sorted(consumed_idx, key=lambda x: x['index'], reverse=True):
                            idx, used = consumed["index"], consumed["amount_used"]
                            if abs(HOLDINGS[asset_norm][idx]["amount"] - used) < tol: del HOLDINGS[asset_norm][idx]
                            else: HOLDINGS[asset_norm][idx]["amount"] -= used
                        if asset_norm in HOLDINGS and not HOLDINGS[asset_norm]: del HOLDINGS[asset_norm]
                        if rem_wd > tol: log_event("Withdrawal Warning", f"Ref:{refid}, Withdrew {abs(amount_ledger)} {asset_norm}, insufficient holdings for {rem_wd:.8f}.")
                    else: log_event("Withdrawal Warning", f"Ref:{refid}, Withdrew {asset_norm}, no holdings.")
                    log_event("Withdrawal FIFO End", f"Ref:{refid}, Withdrew {abs(amount_ledger):.8f} {asset_norm}. Holdings AFTER: {json.dumps(HOLDINGS.get(asset_norm,[]))}")
                    row_base[1]="Auszahlung"; row_base[2]=date_str; row_base[3]=asset_norm; row_base[4]=abs(amount_ledger); row_base[9]=cost_wd; row_base[11]=fee_paid; row_base[14]="N/A"; row_base[15]=notes + " | FIFO Cost Removed: " + " | ".join(fifo_details)
                    tax_data.append(row_base); line_num+=1; processed_refids.add(refid)

            elif (entry_type == "Receive" or (entry_type == "Trade" and amount_ledger > 0)) and asset_norm != "EUR": # Unmatched Receive/Pos-Trade
                if refid not in processed_refids:
                    log_event("Deposit/Receive/Pos-Trade Detected", f"{date_str}, Asset:{asset_norm}, Amt:{amount_ledger:.8f}, Ref:{refid}. Assuming 0 cost basis.")
                    deposit_entry={"amount":amount_ledger,"price_eur":0,"timestamp":timestamp,"fee_eur":fee_paid,"refid":refid}
                    HOLDINGS.setdefault(asset_norm,[]).append(deposit_entry); HOLDINGS[asset_norm].sort(key=lambda x: x["timestamp"])
                    log_event("Deposit Holding Added", f"Asset: {asset_norm}, Details: {json.dumps(deposit_entry)}")
                    row_base[1]=f"{entry_type} (0 Cost)"; row_base[2]=date_str; row_base[3]=asset_norm; row_base[4]=amount_ledger; row_base[5]=date_str; row_base[6]=0; row_base[9]=0; row_base[11]=fee_paid; row_base[14]="N/A"; row_base[15]=notes + f" | {entry_type} (0 cost basis)."
                    tax_data.append(row_base); line_num+=1; processed_refids.add(refid)

            elif asset_norm == "EUR" and entry_type == "Deposit" and refid not in processed_refids: # EUR Deposit
                row_base[1]="EUR Einzahlung"; row_base[2]=date_str; row_base[3]="EUR"; row_base[4]=amount_ledger; row_base[11]=fee_paid; row_base[14]="N/A"; row_base[15]=notes
                tax_data.append(row_base); line_num+=1; processed_refids.add(refid)

            elif refid not in processed_refids: # Other unhandled
                log_event("Unhandled Ledger Type", f"Type:{entry_type}, Asset:{asset_norm}, Amt:{amount_ledger}, Ref:{refid}. Basic log.")
                row_base[1]=f"Ledger ({entry_type})"; row_base[2]=date_str; row_base[3]=asset_norm; row_base[4]=amount_ledger; row_base[11]=fee_paid; row_base[14]="N/A"; row_base[15]=notes + f" | Unhandled: {entry_type}"
                tax_data.append(row_base); line_num+=1; processed_refids.add(refid)

    # --- Final Calcs & Summary ---
    log_event(f"End of Year Holdings {year}", f"{json.dumps(HOLDINGS, indent=2)}")
    sum_row_num = len(tax_data) + 1; sum_row = [""] * len(HEADERS); sum_row[0]=sum_row_num; sum_row[1]="Summe"
    last_data_row_idx = len(tax_data) - 1
    if last_data_row_idx >= 1:
        last_sheet_row = last_data_row_idx + 1
        sum_row[9]=f"=SUM(J2:J{last_sheet_row})" # Kosten
        sum_row[10]=f"=SUM(K2:K{last_sheet_row})" # Erlös
        sum_row[12]=f"=SUM(M2:M{last_sheet_row})" # Gewinn/Verlust
    tax_data.append(sum_row)
    summary_start_sheet_row = len(tax_data) + 2; summary_rows = [[""] * len(HEADERS)]; summary_rows.append([summary_start_sheet_row, "--- Steuerliche Zusammenfassung ---"] + [""]*(len(HEADERS)-2)); current_summary_row = summary_start_sheet_row + 1
    freigrenze=600 if year<2024 else 1000; net_taxable=total_taxable_gains+total_losses; taxable_for_freigrenze=max(0, net_taxable); final_taxable=taxable_for_freigrenze if taxable_for_freigrenze>freigrenze else 0
    if net_taxable>freigrenze: note=f"Netto {net_taxable:.2f}€ > {freigrenze}€ -> Voll steuerpfl."
    elif net_taxable>0: note=f"Netto {net_taxable:.2f}€ <= {freigrenze}€ -> Steuerfrei"
    else: note=f"Netto {net_taxable:.2f}€ -> Kein steuerpfl. Gewinn"+(" (Verlustvortrag)" if net_taxable<0 else "")
    log_event("Freigrenze Check", f"Y:{year}, TaxG:{total_taxable_gains:.2f}, TF:{total_tax_free_gains:.2f}, Loss:{total_losses:.2f}, Net:{net_taxable:.2f}, Final:{final_taxable:.2f}, Note:{note}")
    summary_rows.extend([
        [current_summary_row, "GESAMT","Steuerpfl. Gewinne (<1J)","{:.2f}".format(total_taxable_gains),"","","","","", "{:.2f}".format(total_fees_eur),"","","","","Ja","Anlage SO"],
        [current_summary_row+1,"GESAMT","Steuerfreie Gewinne (>1J)","{:.2f}".format(total_tax_free_gains),"","","","","","","","","","","Nein","N/A"],
        [current_summary_row+2,"GESAMT","Verluste","{:.2f}".format(total_losses),"","","","","","","","","","","","Anlage SO"],
        [current_summary_row+3,"GESAMT","Netto Ergebnis (§23 EStG)","{:.2f}".format(net_taxable),"","","","","","","","","","","",note],
        [current_summary_row+4,"GESAMT",f"Zu versteuern ({freigrenze}€ Grenze)","{:.2f}".format(final_taxable),"","","","","","","","","","","Ja","Anlage SO"],
        [""] * len(HEADERS),
        [current_summary_row+6,"INFO","Erstellt am",datetime.now().strftime("%Y-%m-%d %H:%M:%S"),"","","","","","","","","","","",""],
        [current_summary_row+7,"INFO","Methode","FIFO","","","","","","","","","","","","Gemäß BMF"],
        [current_summary_row+8,"INFO","Hinweis","Diebstahlverluste nicht abzugsfähig","","","","","","","","","","","","§23 EStG"],
    ])
    tax_data.extend(summary_rows)
    return tax_data

# --- Sheet Writing Functions ---
def write_to_sheets(data, year):
    if len(data) <= 1: log_event("Sheet Write Skip", f"No data rows for {year}."); print(f"Skipping sheet generation for {year}: No data."); return
    sheet_name = f"Steuer {year}"
    try: sheet_name, sheet_id = get_or_create_sheet(SHEET_ID, sheet_name)
    except Exception as e: log_event("Sheet Write Error", f"Failed get/create sheet '{sheet_name}': {e}"); print(f"ERROR: Could not get/create sheet '{sheet_name}'."); return
    num_rows, num_cols = len(data), len(HEADERS)
    header_values = [str(h) for h in data[0]]; data_rows_values = []
    for r, row_data in enumerate(data[1:], start=1):
        current_row_vals = []; is_sum = row_data[1]=="Summe"; is_summary = row_data[1] in ["--- Steuerliche Zusammenfassung ---","GESAMT","INFO",""] or r>=num_rows-10
        for c, cell_val in enumerate(row_data):
            if c==12 and not is_sum and not is_summary: # Gewinn/Verlust Formula
                k,j,l=row_data[10],row_data[9],row_data[11]; formula_ok=False
                try:
                    if k in [None,""] or isinstance(k,(int,float)) or (isinstance(k,str) and k.replace('.','',1).replace(',','',1).replace('-','',1).isdigit()):
                        if j in [None,""] or isinstance(j,(int,float)) or (isinstance(j,str) and j.replace('.','',1).replace(',','',1).replace('-','',1).isdigit()):
                            if l in [None,""] or isinstance(l,(int,float)) or (isinstance(l,str) and l.replace('.','',1).replace(',','',1).replace('-','',1).isdigit()): formula_ok=True
                except Exception: pass
                if formula_ok: formula=f"=IF(ISBLANK(K{r+1}),\"\",ROUND(IFERROR(K{r+1},0)-IFERROR(J{r+1},0)-IFERROR(L{r+1},0),2))"; current_row_vals.append(formula)
                else: current_row_vals.append(cell_val if cell_val is not None else "")
            elif is_sum and isinstance(cell_val, str) and cell_val.startswith("=SUM("): current_row_vals.append(cell_val)
            else: # Standard values
                if isinstance(cell_val, float) and (cell_val != cell_val or cell_val == float('inf') or cell_val == float('-inf')): current_row_vals.append("Error")
                else: current_row_vals.append(cell_val if isinstance(cell_val,(int,float)) else (str(cell_val) if cell_val is not None else ""))
        data_rows_values.append(current_row_vals)
    try: # Perform Sheet Updates
        log_event("Sheet Clear", f"Clearing sheet: {sheet_name}"); service.spreadsheets().values().clear(spreadsheetId=SHEET_ID, range=sheet_name).execute(); time.sleep(1)
        header_body = {"range": f"{sheet_name}!A1", "majorDimension": "ROWS", "values": [header_values]}
        data_body = {"range": f"{sheet_name}!A2", "majorDimension": "ROWS", "values": data_rows_values}
        log_event("Sheet Update", f"Updating {sheet_name} header."); service.spreadsheets().values().update(spreadsheetId=SHEET_ID, range=header_body["range"], valueInputOption="USER_ENTERED", body=header_body).execute()
        log_event("Sheet Update", f"Updating {sheet_name} data ({len(data_rows_values)} rows)."); service.spreadsheets().values().update(spreadsheetId=SHEET_ID, range=data_body["range"], valueInputOption="USER_ENTERED", body=data_body).execute()
        log_event("Sheet Update Success", f"Data updated for {sheet_name}"); time.sleep(1)
        # --- Apply Formatting ---
        formatting_requests = [
            {"repeatCell": {"range": {"sheetId": sheet_id, "startRowIndex": 0, "endRowIndex": 1}, "cell": {"userEnteredFormat": {"textFormat": {"bold": True}}}, "fields": "userEnteredFormat.textFormat.bold"}},
            {"repeatCell": {"range": {"sheetId": sheet_id, "startRowIndex": 1, "startColumnIndex": 6, "endColumnIndex": 7}, "cell": {"userEnteredFormat": {"numberFormat": {"type": "NUMBER", "pattern": "#,##0.00 €"}}}, "fields": "userEnteredFormat.numberFormat"}},
            {"repeatCell": {"range": {"sheetId": sheet_id, "startRowIndex": 1, "startColumnIndex": 8, "endColumnIndex": 9}, "cell": {"userEnteredFormat": {"numberFormat": {"type": "NUMBER", "pattern": "#,##0.00 €"}}}, "fields": "userEnteredFormat.numberFormat"}},
            {"repeatCell": {"range": {"sheetId": sheet_id, "startRowIndex": 1, "startColumnIndex": 9, "endColumnIndex": 13}, "cell": {"userEnteredFormat": {"numberFormat": {"type": "NUMBER", "pattern": "#,##0.00 €"}}}, "fields": "userEnteredFormat.numberFormat"}},
            {"repeatCell": {"range": {"sheetId": sheet_id, "startRowIndex": 1, "startColumnIndex": 4, "endColumnIndex": 5}, "cell": {"userEnteredFormat": {"numberFormat": {"type": "NUMBER", "pattern": "#,##0.00######"}}}, "fields": "userEnteredFormat.numberFormat"}},
            {"repeatCell": {"range": {"sheetId": sheet_id, "startRowIndex": 1, "startColumnIndex": 13, "endColumnIndex": 14}, "cell": {"userEnteredFormat": {"numberFormat": {"type": "NUMBER", "pattern": "0"}}}, "fields": "userEnteredFormat.numberFormat"}},
            {"repeatCell": {"range": {"sheetId": sheet_id, "startRowIndex": 1, "startColumnIndex": 2, "endColumnIndex": 3}, "cell": {"userEnteredFormat": {"numberFormat": {"type": "DATE_TIME", "pattern": "yyyy-mm-dd hh:mm:ss"}}}, "fields": "userEnteredFormat.numberFormat"}},
            {"repeatCell": {"range": {"sheetId": sheet_id, "startRowIndex": 1, "startColumnIndex": 5, "endColumnIndex": 6}, "cell": {"userEnteredFormat": {"numberFormat": {"type": "DATE_TIME", "pattern": "yyyy-mm-dd hh:mm:ss"}}}, "fields": "userEnteredFormat.numberFormat"}},
            {"repeatCell": {"range": {"sheetId": sheet_id, "startRowIndex": 1, "startColumnIndex": 7, "endColumnIndex": 8}, "cell": {"userEnteredFormat": {"numberFormat": {"type": "DATE_TIME", "pattern": "yyyy-mm-dd hh:mm:ss"}}}, "fields": "userEnteredFormat.numberFormat"}},
            {"addConditionalFormatRule": {"rule": {"ranges": [{"sheetId": sheet_id, "startRowIndex": 1, "endRowIndex": num_rows, "startColumnIndex": 14, "endColumnIndex": 15}], "booleanRule": {"condition": {"type": "TEXT_EQ", "values": [{"userEnteredValue": "Ja"}]}, "format": {"backgroundColor": {"red": 1.0, "green": 0.85, "blue": 0.85}}}}, "index": 0}},
            {"addConditionalFormatRule": {"rule": {"ranges": [{"sheetId": sheet_id, "startRowIndex": 1, "endRowIndex": num_rows, "startColumnIndex": 14, "endColumnIndex": 15}], "booleanRule": {"condition": {"type": "TEXT_EQ", "values": [{"userEnteredValue": "Nein"}]}, "format": {"backgroundColor": {"red": 0.85, "green": 1.0, "blue": 0.85}}}}, "index": 1}},
            {"addConditionalFormatRule": {"rule": {"ranges": [{"sheetId": sheet_id, "startRowIndex": 1, "endRowIndex": num_rows, "startColumnIndex": 12, "endColumnIndex": 13}], "booleanRule": {"condition": {"type": "NUMBER_GREATER", "values": [{"userEnteredValue": "0"}]}, "format": {"textFormat": {"foregroundColor": {"red": 0.0, "green": 0.5, "blue": 0.0}}}}}, "index": 2}},
            {"addConditionalFormatRule": {"rule": {"ranges": [{"sheetId": sheet_id, "startRowIndex": 1, "endRowIndex": num_rows, "startColumnIndex": 12, "endColumnIndex": 13}], "booleanRule": {"condition": {"type": "NUMBER_LESS", "values": [{"userEnteredValue": "0"}]}, "format": {"textFormat": {"foregroundColor": {"red": 0.7, "green": 0.0, "blue": 0.0}}}}}, "index": 3}},
            {"repeatCell": { "range": {"sheetId": sheet_id, "startColumnIndex": 15, "endColumnIndex": 16, "startRowIndex": 1}, "cell": {"userEnteredFormat": {"wrapStrategy": "WRAP"}}, "fields": "userEnteredFormat.wrapStrategy"}},
        ]
        summe_row_idx, summary_start_idx = -1, -1
        for i, r in enumerate(data):
            if len(r) > 1 and r[1] == "Summe": summe_row_idx = i
            if len(r) > 1 and r[1] == "--- Steuerliche Zusammenfassung ---": summary_start_idx = i - 1; break
        if summe_row_idx != -1: formatting_requests.insert(1, {"repeatCell": {"range": {"sheetId": sheet_id, "startRowIndex": summe_row_idx, "endRowIndex": summe_row_idx + 1}, "cell": {"userEnteredFormat": {"textFormat": {"bold": True}}}, "fields": "userEnteredFormat.textFormat.bold"}})
        if summary_start_idx != -1: formatting_requests.insert(1, {"repeatCell": {"range": {"sheetId": sheet_id, "startRowIndex": summary_start_idx, "endRowIndex": num_rows}, "cell": {"userEnteredFormat": {"backgroundColor": {"red": 0.92, "green": 0.92, "blue": 0.92}}}, "fields": "userEnteredFormat.backgroundColor"}})
        formatting_requests.append({"autoResizeDimensions": {"dimensions": {"sheetId": sheet_id, "dimension": "COLUMNS", "startIndex": 0, "endIndex": num_cols}}})
        if formatting_requests:
            log_event("Sheet Formatting", f"Applying {len(formatting_requests)} rules to {sheet_name}")
            service.spreadsheets().batchUpdate(spreadsheetId=SHEET_ID, body={"requests": formatting_requests}).execute()
            log_event("Sheet Formatting Success", f"Applied rules to {sheet_name}")
    except HttpError as e:
        error_details = str(e)
        try:
            error_details = json.loads(e.content.decode('utf-8'))
        except Exception:
            pass
        log_event("Sheet Write/Format Error", f"Sheet: {sheet_name}, Error: {error_details}")
        print(f"ERROR writing/formatting sheet '{sheet_name}': {error_details}")
    except Exception as e:
        log_event("Sheet Write/Format Error", f"Sheet: {sheet_name}, Unexpected Error: {e}\n{traceback.format_exc()}")
        print(f"ERROR writing/formatting sheet '{sheet_name}': Unexpected error {e}")

# --- Utility Functions ---
def get_or_create_sheet(spreadsheet_id, sheet_name):
    try:
        spreadsheet = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
        sheets = spreadsheet.get('sheets', [])
        existing_sheet = next((s for s in sheets if s['properties']['title'] == sheet_name), None)
        if existing_sheet:
            log_event("Sheet Found", f"Using sheet: '{sheet_name}' (ID: {existing_sheet['properties']['sheetId']})")
            return sheet_name, existing_sheet['properties']['sheetId']
        else:
            log_event("Sheet Creation", f"Creating sheet: '{sheet_name}'")
            req = {"addSheet": {"properties": {"title": sheet_name, "gridProperties": {"rowCount": 2000, "columnCount": len(HEADERS)}}}}
            res = service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body={"requests": [req]}).execute()
            sheet_id = res['replies'][0]['addSheet']['properties']['sheetId']
            log_event("Sheet Created", f"New sheet '{sheet_name}' ID: {sheet_id}")
            return sheet_name, sheet_id
    except HttpError as e:
        log_event("Sheet Discovery Error", f"Failed get/create sheet '{sheet_name}'. Error: {e}")
        print(f"ERROR: Could not get or create sheet '{sheet_name}'.")
        raise

def write_raw_data_sheet(trades, ledger):
    """Writes all fetched trades and ledger entries to a 'Raw Data' sheet."""
    sheet_name = "Raw Data"
    log_event("Raw Data Sheet", f"Preparing raw data sheet '{sheet_name}'...")
    raw_headers = ["Timestamp", "DateTime (UTC)", "Source", "Type", "Asset", "Amount", "Fee", "Ref/OrderID", "Pair", "Price", "Cost/Proceeds", "Raw Data"]
    raw_data_rows = [raw_headers]
    combined_events = []
    for trade in trades: combined_events.append({"time": float(trade.get("time", 0)), "source": "Trade", "data": trade})
    for entry in ledger: combined_events.append({"time": float(entry.get("time", 0)), "source": "Ledger", "data": entry})
    valid_events = [e for e in combined_events if e["time"] > 0]
    valid_events.sort(key=lambda x: x["time"])
    for event in valid_events:
        ts = event["time"]; dt_utc = datetime.fromtimestamp(ts, timezone.utc).strftime("%Y-%m-%d %H:%M:%S"); source = event["source"]; data = event["data"]
        row = [""] * len(raw_headers)
        row[0] = ts; row[1] = dt_utc; row[2] = source
        if source == "Trade":
            row[3] = data.get("type"); pair = data.get("pair",""); asset = pair[:3] if pair and not pair.startswith('X') else (pair[:4] if pair else ""); row[4] = asset
            row[5] = data.get("vol"); row[6] = data.get("fee"); row[7] = data.get("ordertxid"); row[8] = data.get("pair"); row[9] = data.get("price"); row[10] = data.get("cost")
        else: # Ledger
            row[3] = data.get("type"); row[4] = data.get("asset"); row[5] = data.get("amount"); row[6] = data.get("fee"); row[7] = data.get("refid")
        row[11] = json.dumps(data) # Store full raw JSON
        raw_data_rows.append(row)
    if len(raw_data_rows) <= 1: log_event("Raw Data Sheet", "No valid raw data found."); return
    try:
        sheet_name, sheet_id = get_or_create_sheet(SHEET_ID, sheet_name)
        log_event("Raw Data Sheet", f"Clearing sheet: {sheet_name}")
        service.spreadsheets().values().clear(spreadsheetId=SHEET_ID, range=sheet_name).execute(); time.sleep(1)
        body = {"range": f"{sheet_name}!A1", "majorDimension": "ROWS", "values": raw_data_rows}
        log_event("Raw Data Sheet", f"Writing {len(raw_data_rows)} rows to {sheet_name}.")
        service.spreadsheets().values().update(spreadsheetId=SHEET_ID, range=body["range"], valueInputOption="RAW", body=body).execute()
        log_event("Raw Data Sheet", "Raw data written successfully.")
        formatting_reqs = [{"repeatCell": {"range": {"sheetId": sheet_id, "startRowIndex": 0, "endRowIndex": 1}, "cell": {"userEnteredFormat": {"textFormat": {"bold": True}}}, "fields": "userEnteredFormat.textFormat.bold"}}, {"autoResizeDimensions": {"dimensions": {"sheetId": sheet_id, "dimension": "COLUMNS", "startIndex": 0, "endIndex": len(raw_headers)}}}]
        service.spreadsheets().batchUpdate(spreadsheetId=SHEET_ID, body={"requests": formatting_reqs}).execute()
    except Exception as e:
        log_event("Raw Data Sheet Error", f"Failed to write raw data: {e}\n{traceback.format_exc()}")
        print(f"ERROR: Failed to write raw data sheet: {e}")

def write_log_sheet():
    sheet_name = "Log"; log_file_path = Path(__file__).parent / "tax_script_error.log"
    if not LOG_DATA or len(LOG_DATA) <= 1: print("No log entries."); return
    try:
        sheet_id = None
        try: sheet_name, sheet_id = get_or_create_sheet(SHEET_ID, sheet_name)
        except Exception as sheet_err: print(f"WARNING: Log sheet error: {sheet_err}. Logging locally only.")
        if sheet_id:
            log_values = [[str(item) for item in row] for row in LOG_DATA]
            body = {"values": log_values}
            service.spreadsheets().values().clear(spreadsheetId=SHEET_ID, range=f"{sheet_name}!A1:C").execute()
            service.spreadsheets().values().update(spreadsheetId=SHEET_ID, range=f"{sheet_name}!A1", valueInputOption="USER_ENTERED", body=body).execute()
            fmt_reqs = [{"autoResizeDimensions": {"dimensions": {"sheetId": sheet_id, "dimension": "COLUMNS", "startIndex": 0, "endIndex": 3}}}, {"repeatCell": { "range": {"sheetId": sheet_id, "startColumnIndex": 2, "endColumnIndex": 3, "startRowIndex": 0}, "cell": {"userEnteredFormat": {"wrapStrategy": "WRAP"}}, "fields": "userEnteredFormat.wrapStrategy"}}]
            service.spreadsheets().batchUpdate(spreadsheetId=SHEET_ID, body={"requests": fmt_reqs}).execute()
            print(f"Log written to Google Sheet: https://docs.google.com/spreadsheets/d/{SHEET_ID}/edit#gid={sheet_id}")
    except Exception as e:
        print(f"CRITICAL: Error during log sheet processing: {e}")
        print(f"Attempting to write log to local file '{log_file_path}' as fallback.")
        try:
             with open(log_file_path, "w", encoding='utf-8') as f:
                 f.write("Timestamp\tEvent\tDetails\n")
                 for entry in LOG_DATA: f.write("\t".join(map(str, entry)) + "\n")
                 f.write(f"--- ERROR DURING LOG SHEET PROCESSING: {e} ---\n")
             print(f"Log data saved to {log_file_path}")
        except Exception as log_e: print(f"CRITICAL: Could not write log to file: {log_e}")

# --- Main Execution Block ---
def main():
    global HOLDINGS
    run_start_time = datetime.now()
    log_event("Script Started", f"Execution Time: {run_start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Script started at {run_start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    date_range_msg = f" (Using range: {START_DATE_STR or 'Default'} to {END_DATE_STR or 'Present'})"
    print(f"Fetching data from Kraken{date_range_msg}...")
    all_trades, all_ledger = [], []
    try:
        print("Fetching trades...")
        all_trades = get_trades(START_TIMESTAMP, END_TIMESTAMP)
        print(f"Retrieved {len(all_trades)} trades.")
        print("Fetching ledger...")
        all_ledger = get_ledger(START_TIMESTAMP, END_TIMESTAMP)
        print(f"Retrieved {len(all_ledger)} entries.")
        # Write Raw Data Sheet
        if all_trades or all_ledger:
            print("Writing raw data sheet...")
            write_raw_data_sheet(all_trades, all_ledger)
        else:
            print("\nNo data found.")
            log_event("Abort", "No API data.")
            return # Log writing happens in finally
        print("Grouping by year...")
        data_by_year = group_by_year(all_trades, all_ledger)
        if not data_by_year:
            print("No data found for any year.")
            log_event("Abort", "No processable years.")
            return # Log writing happens in finally
        print(f"Found data for years: {sorted(list(data_by_year.keys()))}")
        all_years = sorted(list(data_by_year.keys()))
        HOLDINGS = {}
        log_event("Global Holdings Reset", "Resetting holdings.")
        for year in all_years:
            year_trades, year_ledger = data_by_year[year]
            print(f"\n--- Processing Year {year} ---")
            log_event(f"Year Start {year}", f"Input: {len(year_trades)} trades, {len(year_ledger)} entries.")
            print(f"Processing FIFO...")
            tax_data = process_for_tax(year_trades, year_ledger, year)
            meaningful_rows = [r for i, r in enumerate(tax_data) if i>0 and len(r)>1 and r[1] not in ["Summe", "--- Steuerliche Zusammenfassung ---", "GESAMT", "INFO", ""]]
            if not meaningful_rows:
                print(f"No significant transaction rows generated for {year}.")
                log_event(f"Year Skip {year}", "No data rows generated.")
                continue
            print(f"Writing report for {year}...")
            write_to_sheets(tax_data, year)
            print(f"Sheet written for {year}.")
            time.sleep(2) # Pause
        print("\nProcessing complete.")
    except Exception as e:
        print(f"\n--- SCRIPT ERROR ---")
        print(f"Error: {e}")
        print("Traceback:")
        traceback.print_exc()
        log_event("CRITICAL ERROR", f"{e}\n{traceback.format_exc()}")
        print("\nPlease check logs.")
    finally:
        run_end_time = datetime.now()
        duration = run_end_time - run_start_time
        log_event("Script Finished", f"Execution Time: {run_end_time.strftime('%Y-%m-%d %H:%M:%S')}, Duration: {duration}")
        print(f"\nScript finished at {run_end_time.strftime('%Y-%m-%d %H:%M:%S')} (Duration: {duration}).")
        print("Writing final logs...")
        write_log_sheet()

if __name__ == "__main__":
    main()