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
from typing import Dict, List, Any, Tuple, Optional, Union

# --- Log Data Storage ---
LOG_DATA = []

# --- Global Variables ---
HOLDINGS: Dict[str, List[Dict[str, Any]]] = {}
PRICE_CACHE: Dict[Tuple[str, int], float] = {}
HEADERS = [
    "Zeile", "Typ", "Transaktionsdatum", "Asset", "Anzahl", "Kaufdatum", "Kaufpreis (€)/Stk", "Verkaufsdatum",
    "Verkaufspreis (€)/Stk", "Kosten (€)", "Erlös (€)", "Gebühr (€)", "Gewinn / Verlust (€)",
    "Haltedauer (Tage)", "Steuerpflichtig", "Notizen / FIFO-Details"
]
RAW_HEADERS = [
    "Type", "Time", "Asset", "Amount", "Fee", "Cost", "Price",
    "Vol", "Ordertxid", "Refid", "Subtype", "Aclass", "Balance"
]

# --- Define log_event before it is used ---
def log_event(event: str, details: str) -> None:
    """Log an event with a timestamp for debugging purposes."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    LOG_DATA.append([timestamp, event, details])

# --- Package Installation ---
def install_packages(packages: List[str]) -> None:
    """Installs required Python packages using pip."""
    try:
        print(f"Attempting to install/update: {', '.join(packages)}...")
        subprocess.check_call(
            [sys.executable, "-m", "pip", "install", "--quiet"] + packages)
        print(f"Successfully installed/verified: {', '.join(packages)}")
        log_event("Package Install",
                  f"Successfully installed/verified: {', '.join(packages)}")
    except subprocess.CalledProcessError as e:
        print(
            f"ERROR: Failed to install packages: {', '.join(packages)}. Error: {e}")
        log_event("Package Install Error",
                  f"Failed to install {', '.join(packages)}: {e}")
        sys.exit(1)

# --- Package List and Import Logic ---
required_packages = ["requests", "google-auth", "google-auth-oauthlib",
                     "google-auth-httplib2", "google-api-python-client"]
for package in required_packages:
    try:
        if package == "google-auth":
            import google.auth as google_auth  # noqa: F401
        else:
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

try:
    with CONFIG_FILE.open('r') as f:
        config = json.load(f)
    API_KEY = config["API_KEY"]
    API_SECRET = config["API_SECRET"]
    SHEET_ID = config["SHEET_ID"]
    THEFT_TXIDS = config.get("theft_txids", [])
    START_DATE_STR = config.get("start_date")
    END_DATE_STR = config.get("end_date", None)
    START_TIMESTAMP = None
    END_TIMESTAMP = int(datetime.now(timezone.utc).timestamp()) if not END_DATE_STR else None
    if START_DATE_STR:
        START_TIMESTAMP = int(datetime.strptime(
            START_DATE_STR, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp())
    if END_DATE_STR:
        END_TIMESTAMP = int(datetime.strptime(END_DATE_STR, "%Y-%m-%d").replace(
            hour=23, minute=59, second=59, tzinfo=timezone.utc).timestamp())
except FileNotFoundError:
    print(f"ERROR: Config file not found at: {CONFIG_FILE}")
    sys.exit(1)
except KeyError as e:
    print(f"ERROR: Missing required key in config.json: {e}")
    sys.exit(1)

# --- Google API Setup ---
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
creds = Credentials.from_service_account_file(
    str(CREDENTIALS_FILE), scopes=SCOPES)
service = build("sheets", "v4", credentials=creds)

# --- Kraken API Request Function ---
def kraken_request(endpoint: str, data: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
    """Make a signed request to the Kraken private API."""
    url = f"https://api.kraken.com/0/private/{endpoint}"
    if data is None:
        data = {}
    data["nonce"] = str(int(time.time() * 100000))
    post_data = "&".join([f"{k}={v}" for k, v in data.items()])
    encoded = (str(data["nonce"]) + post_data).encode()
    message = f"/0/private/{endpoint}".encode() + \
        hashlib.sha256(encoded).digest()
    signature = hmac.new(base64.b64decode(API_SECRET), message, hashlib.sha512)
    sig = base64.b64encode(signature.digest()).decode()
    headers = {"API-Key": API_KEY, "API-Sign": sig}
    max_retries, wait_time = 3, 5
    for attempt in range(max_retries):
        try:
            response = requests.post(
                url, headers=headers, data=data, timeout=45)
            response.raise_for_status()
            result = response.json()
            if result.get("error"):
                error_messages = result["error"]
                if any("Nonce" in e for e in error_messages) and attempt < max_retries - 1:
                    time.sleep(wait_time * (attempt + 1))
                    data["nonce"] = str(int(time.time() * 100000))
                    continue
                raise Exception(f"API error: {error_messages}")
            return result
        except requests.exceptions.RequestException as e:
            if attempt == max_retries - 1:
                raise Exception(
                    f"API request failed after {max_retries} attempts: {e}")
            time.sleep(wait_time * (attempt + 1))
    raise Exception(f"API call {endpoint} failed after all retries.")

# --- Data Fetching Functions ---
def get_trades(start_ts: Optional[int] = None, end_ts: Optional[int] = None) -> List[Dict[str, Any]]:
    """Fetch trade history from Kraken API with pagination."""
    trades_dict = {}
    offset = 0
    fetch_params: Dict[str, str] = {"trades": "true"}
    if start_ts:
        fetch_params["start"] = str(start_ts)
    if end_ts:
        fetch_params["end"] = str(end_ts)
    while True:
        current_params = {"ofs": str(offset), **fetch_params}
        result = kraken_request("TradesHistory", current_params)
        batch_dict = result.get("result", {}).get("trades", {})
        trades_dict.update(batch_dict)
        offset += len(batch_dict)
        if not batch_dict or offset >= int(result.get("result", {}).get("count", 0)):
            break
        time.sleep(1.1)
    return list(trades_dict.values())

def get_ledger(start_ts: Optional[int] = None, end_ts: Optional[int] = None) -> List[Dict[str, Any]]:
    """Fetch ledger entries from Kraken API with pagination."""
    ledger_dict = {}
    offset = 0
    fetch_params: Dict[str, str] = {}
    if start_ts:
        fetch_params["start"] = str(start_ts)
    if end_ts:
        fetch_params["end"] = str(end_ts)
    while True:
        current_params = {"ofs": str(offset), **fetch_params}
        result = kraken_request("Ledgers", current_params)
        batch_dict = result.get("result", {}).get("ledger", {})
        ledger_dict.update(batch_dict)
        offset += len(batch_dict)
        if not batch_dict or offset >= int(result.get("result", {}).get("count", 0)):
            break
        time.sleep(1.1)
    return list(ledger_dict.values())

# --- Write Raw Transactions to Sheets ---
def write_raw_transactions_to_sheets(transactions: List[Dict[str, Any]]) -> None:
    """Write raw transaction data to a Google Sheet for reference."""
    sheet_name = "Raw Transactions"
    sheet_name, sheet_id = get_or_create_sheet(SHEET_ID, sheet_name)

    # Clear the sheet before writing new data
    service.spreadsheets().values().clear(
        spreadsheetId=SHEET_ID, range=sheet_name).execute()

    # Prepare data rows
    data_rows = [RAW_HEADERS]
    for tx in transactions:
        row = [
            tx.get("type", ""),
            datetime.fromtimestamp(float(tx.get("time", 0)), timezone.utc).strftime(
                "%Y-%m-%d %H:%M:%S") if tx.get("time") else "",
            tx.get("asset", ""),
            tx.get("amount", ""),
            tx.get("fee", ""),
            tx.get("cost", ""),
            tx.get("price", ""),
            tx.get("vol", ""),
            tx.get("ordertxid", ""),
            tx.get("refid", ""),
            tx.get("subtype", ""),
            tx.get("aclass", ""),
            tx.get("balance", "")
        ]
        data_rows.append(row)

    # Write data to the sheet
    body = {"values": [list(map(str, row)) for row in data_rows]}
    service.spreadsheets().values().update(
        spreadsheetId=SHEET_ID, range=f"{sheet_name}!A1", valueInputOption="USER_ENTERED", body=body
    ).execute()
    
    # Format the raw transactions sheet
    format_requests = {
        "requests": [
            # Format header row
            {
                "repeatCell": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": 0,
                        "endRowIndex": 1,
                        "startColumnIndex": 0,
                        "endColumnIndex": len(RAW_HEADERS)
                    },
                    "cell": {
                        "userEnteredFormat": {
                            "backgroundColor": {
                                "red": 0.8,
                                "green": 0.8,
                                "blue": 0.8
                            },
                            "horizontalAlignment": "CENTER",
                            "textFormat": {
                                "bold": True
                            }
                        }
                    },
                    "fields": "userEnteredFormat(backgroundColor,textFormat,horizontalAlignment)"
                }
            },
            # Auto-resize columns
            {
                "autoResizeDimensions": {
                    "dimensions": {
                        "sheetId": sheet_id,
                        "dimension": "COLUMNS",
                        "startIndex": 0,
                        "endIndex": len(RAW_HEADERS)
                    }
                }
            }
        ]
    }
    service.spreadsheets().batchUpdate(
        spreadsheetId=SHEET_ID, body=format_requests).execute()
    
    print("Raw Transactions sheet written.")

# --- Grouping Function ---
def group_by_year(trades: List[Dict[str, Any]], ledger: List[Dict[str, Any]]) -> Dict[int, Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]]:
    """Group trades and ledger entries by year."""
    trades_by_year: Dict[int, List[Dict[str, Any]]] = {}
    ledger_by_year: Dict[int, List[Dict[str, Any]]] = {}
    
    for trade in trades:
        year = datetime.fromtimestamp(float(trade["time"]), timezone.utc).year
        trades_by_year.setdefault(year, []).append(trade)
    
    for entry in ledger:
        year = datetime.fromtimestamp(float(entry["time"]), timezone.utc).year
        ledger_by_year.setdefault(year, []).append(entry)
    
    all_years = set(trades_by_year.keys()).union(ledger_by_year.keys())
    
    return {
        year: (
            sorted(trades_by_year.get(year, []),
                   key=lambda x: float(x["time"])),
            sorted(ledger_by_year.get(year, []),
                   key=lambda x: float(x["time"]))
        )
        for year in sorted(all_years)
    }

# --- Price Fetching Function ---
def get_market_price(asset: str, timestamp: float) -> float:
    """Fetch market price for an asset at a given timestamp."""
    asset_map = {
        "ETH": "ETH/EUR", "XETH": "ETH/EUR", "XBT": "XBT/EUR", "XXBT": "XBT/EUR", "BTC": "XBT/EUR",
        "XRP": "XRP/EUR", "XXRP": "XRP/EUR", "ADA": "ADA/EUR", "LTC": "LTC/EUR", "XLM": "XLM/EUR",
        "EOS": "EOS/EUR", "ETC": "ETC/EUR", "AVAX": "AVAX/EUR", "ARB": "ARB/EUR", "EUR": None,
        "ZEUR": None, "KFEE": None
    }
    pair = asset_map.get(asset)
    if not pair:
        return 0
    timestamp_int = int(timestamp)
    cache_key = (pair, timestamp_int // 3600)
    if cache_key in PRICE_CACHE:
        return PRICE_CACHE[cache_key]
    since_time = timestamp_int - 3600
    url = "https://api.kraken.com/0/public/Trades"
    params = {"pair": pair, "since": str(since_time)}
    try:
        response = requests.get(url, params=params, timeout=15)
        response.raise_for_status()
        data = response.json()
        pair_data_key = next((k for k in data.get(
            "result", {}).keys() if k != 'last'), None)
        if not pair_data_key or not data["result"].get(pair_data_key):
            PRICE_CACHE[cache_key] = 0
            return 0
        pair_trades = data["result"][pair_data_key]
        for trade in reversed(pair_trades):
            if float(trade[2]) <= timestamp:
                PRICE_CACHE[cache_key] = float(trade[0])
                return PRICE_CACHE[cache_key]
        PRICE_CACHE[cache_key] = float(pair_trades[0][0]) if pair_trades else 0
        return PRICE_CACHE[cache_key]
    except requests.exceptions.RequestException:
        PRICE_CACHE[cache_key] = 0
        return 0

# --- Main Processing Function ---
def process_for_tax(trades: List[Dict[str, Any]], ledger: List[Dict[str, Any]], year: int) -> List[List[Any]]:
    """Process trades and ledger entries for tax reporting using FIFO."""
    tax_data = [HEADERS]
    events = [
        {"type": "trade", "data": trade, "time": float(trade.get("time", 0))} for trade in trades
    ] + [
        {"type": "ledger", "data": entry, "time": float(entry.get("time", 0))} for entry in ledger
    ]
    events = sorted([e for e in events if e["time"] > 0],
                    key=lambda x: x["time"])
    line_num = 1
    processed_refids = set()
    
    # Initialize HOLDINGS from previous years' data if this is not the first year
    # This ensures we have the correct cost basis for assets purchased in previous years
    global HOLDINGS
    
    # Process all events for the current year
    for event in events:
        timestamp = event["time"]
        if datetime.fromtimestamp(timestamp, timezone.utc).year != year:
            continue
        
        date_str = datetime.fromtimestamp(
            timestamp, timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        data = event["data"]
        row_base = [""] * len(HEADERS)
        row_base[0] = line_num + 1

        # Handle EUR deposits and withdrawals
        if event["type"] == "ledger" and data.get("asset") in ["EUR", "ZEUR"]:
            entry_type = data.get("type", "").capitalize()
            amount = float(data.get("amount", 0))
            fee = float(data.get("fee", 0))
            refid = data.get("refid", f"ledger_{timestamp}")

            if refid in processed_refids:
                continue

            row_base[1] = "EUR Einzahlung" if entry_type == "Deposit" else "EUR Auszahlung" if entry_type == "Withdrawal" else f"Ledger ({entry_type})"
            row_base[2] = date_str
            row_base[3] = "EUR"
            row_base[4] = amount
            row_base[5] = "N/A"
            row_base[6] = 0.0
            row_base[7] = "N/A"
            row_base[8] = 0.0
            row_base[9] = 0.0
            row_base[10] = 0.0
            row_base[11] = fee
            row_base[12] = 0.0
            row_base[13] = 0
            row_base[14] = "Nein"
            row_base[15] = f"Ledger Ref: {refid} | {'Unhandled: ' + entry_type if entry_type not in ['Deposit', 'Withdrawal'] else ''}"
            tax_data.append(row_base)
            line_num += 1
            processed_refids.add(refid)
            continue

        # Handle Buy Transactions (Trades)
        if event["type"] == "trade" and data.get("type") == "buy" and "EUR" in data.get("pair", ""):
            trade_refid = data.get("ordertxid", f"trade_{timestamp}")
            if trade_refid not in processed_refids:
                asset = data["pair"].replace("ZEUR", "").replace("EUR", "")
                amount_traded = float(data.get("vol", 0))
                price_per_unit = float(data.get("price", 0))
                fee_paid = float(data.get("fee", 0))
                cost_or_proceeds = float(data.get("cost", 0))
                
                # Add to holdings for FIFO tracking
                HOLDINGS.setdefault(asset, []).append({
                    "amount": amount_traded, 
                    "price_eur": price_per_unit, 
                    "timestamp": timestamp,
                    "fee_eur": fee_paid, 
                    "refid": trade_refid,
                    "year": year  # Track which year this purchase was made
                })
                
                row_base[1] = "Kauf (Trade)"
                row_base[2] = date_str
                row_base[3] = asset
                row_base[4] = amount_traded
                row_base[5] = date_str
                row_base[6] = price_per_unit
                row_base[7] = "N/A"
                row_base[8] = 0.0
                row_base[9] = cost_or_proceeds
                row_base[10] = 0.0
                row_base[11] = fee_paid
                row_base[12] = 0.0  # No gain/loss on purchase
                row_base[13] = 0    # No holding period yet
                row_base[14] = "N/A"
                row_base[15] = f"Trade Ref: {trade_refid}"
                tax_data.append(row_base)
                line_num += 1
                processed_refids.add(trade_refid)
                continue

        # Handle Receive (0 Cost) Transactions
        if event["type"] == "ledger" and data.get("type", "").capitalize() == "Receive" and data.get("asset") not in ["EUR", "ZEUR"]:
            asset = data.get("asset", "N/A")
            asset_norm = asset.replace('Z', '', 1).replace('X', '', 1) if len(
                asset) >= 4 and asset[0] in ('X', 'Z') else asset
            amount = float(data.get("amount", 0))
            fee = float(data.get("fee", 0))
            refid = data.get("refid", f"ledger_{timestamp}")

            if refid in processed_refids:
                continue

            # Add to holdings with 0 cost basis
            HOLDINGS.setdefault(asset_norm, []).append({
                "amount": amount,
                "price_eur": 0.0,
                "timestamp": timestamp,
                "fee_eur": fee,
                "refid": refid,
                "year": year
            })

            row_base[1] = "Receive (0 Cost)"
            row_base[2] = date_str
            row_base[3] = asset_norm
            row_base[4] = amount
            row_base[5] = date_str
            row_base[6] = 0.0
            row_base[7] = "N/A"
            row_base[8] = 0.0
            row_base[9] = 0.0
            row_base[10] = 0.0
            row_base[11] = fee
            row_base[12] = 0.0
            row_base[13] = 0
            row_base[14] = "N/A"
            row_base[15] = f"Ledger Ref: {refid} | Receive (0 cost basis)"
            tax_data.append(row_base)
            line_num += 1
            processed_refids.add(refid)
            continue

        # Handle Sell Transactions (Spend of Crypto)
        if event["type"] == "ledger" and data.get("type", "").capitalize() == "Spend" and data.get("asset") not in ["EUR", "ZEUR"]:
            asset = data.get("asset", "N/A")
            asset_norm = asset.replace('Z', '', 1).replace('X', '', 1) if len(
                asset) >= 4 and asset[0] in ('X', 'Z') else asset
            amount_ledger = float(data.get("amount", 0))
            fee_paid = float(data.get("fee", 0))
            refid = data.get("refid", f"ledger_{timestamp}")

            if refid in processed_refids or asset_norm == "KFEE":
                continue

            # Look for a matching EUR Receive event (indicating a sale)
            proceeds = 0.0
            eur_receive_refid = None
            
            for next_event in events[events.index(event) + 1:]:
                if next_event["time"] - timestamp > 600:  # Within 10 minutes
                    break
                if (next_event["type"] == "ledger" and
                    next_event["data"]["type"].capitalize() == "Receive" and
                        next_event["data"]["asset"] in ["ZEUR", "EUR"]):
                    proceeds = float(next_event["data"]["amount"])
                    eur_receive_refid = next_event["data"]["refid"]
                    break

            if proceeds == 0.0:
                # If no matching EUR Receive, estimate proceeds using market price
                market_price = get_market_price(asset_norm, timestamp)
                proceeds = abs(amount_ledger) * market_price
                print(f"Warning: No matching EUR receive found for {asset_norm} sale. Using market price: {market_price} EUR")

            sell_amount = abs(amount_ledger)
            price_pu = proceeds / sell_amount if sell_amount > 1e-12 else 0
            cost_basis = 0.0
            fifo_details = []
            buy_timestamp = timestamp
            purchase_year = year
            
            # Apply FIFO method to determine cost basis
            if asset_norm in HOLDINGS and HOLDINGS[asset_norm]:
                rem_sell = sell_amount
                for i, lot in enumerate(HOLDINGS[asset_norm]):
                    if rem_sell <= 1e-9:
                        break
                    use = min(lot["amount"], rem_sell)
                    cost_basis += use * lot["price_eur"]
                    purchase_year = lot.get("year", year)
                    fifo_details.append(
                        f"Lot {i+1}: {use:.8f}@{lot['price_eur']:.4f} ({datetime.fromtimestamp(lot['timestamp'], timezone.utc).strftime('%Y-%m-%d')})")
                    lot["amount"] -= use
                    rem_sell -= use
                    if i == 0:  # Use the timestamp of the first lot for holding period
                        buy_timestamp = lot["timestamp"]
                
                # Remove empty lots
                HOLDINGS[asset_norm] = [
                    lot for lot in HOLDINGS[asset_norm] if lot["amount"] > 1e-9]
                if not HOLDINGS[asset_norm]:
                    del HOLDINGS[asset_norm]
            else:
                # No holdings found - this is unusual and might indicate missing data
                print(f"Warning: No holdings found for {asset_norm} when processing sale. Cost basis will be 0.")
                fifo_details.append("No matching purchase found - cost basis set to 0")

            gain_loss = proceeds - cost_basis - fee_paid
            hold_days = (timestamp - buy_timestamp) / (24 * 3600) if buy_timestamp < timestamp else 0
            tax_status = "Ja" if hold_days <= 365 else "Nein"

            row_base[1] = "Verkauf"
            row_base[2] = date_str
            row_base[3] = asset_norm
            row_base[4] = sell_amount
            row_base[5] = datetime.fromtimestamp(buy_timestamp, timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
            row_base[6] = cost_basis / sell_amount if sell_amount > 1e-12 else 0
            row_base[7] = date_str
            row_base[8] = price_pu
            row_base[9] = cost_basis
            row_base[10] = proceeds
            row_base[11] = fee_paid
            row_base[12] = gain_loss
            row_base[13] = round(hold_days)
            row_base[14] = tax_status
            row_base[15] = f"Ledger Ref: {refid} | FIFO: {' | '.join(fifo_details)}"
            
            if purchase_year != year:
                row_base[15] += f" | Purchased in {purchase_year}"
            
            tax_data.append(row_base)
            line_num += 1
            processed_refids.add(refid)
            if eur_receive_refid:
                processed_refids.add(eur_receive_refid)
            continue

        # Handle Unmatched EUR Spends
        if event["type"] == "ledger" and data.get("type", "").capitalize() == "Spend" and data.get("asset") in ["EUR", "ZEUR"]:
            amount = float(data.get("amount", 0))
            fee = float(data.get("fee", 0))
            refid = data.get("refid", f"ledger_{timestamp}")

            if refid in processed_refids:
                continue

            row_base[1] = "EUR Spend (Unmatched)"
            row_base[2] = date_str
            row_base[3] = "EUR"
            row_base[4] = amount
            row_base[5] = "N/A"
            row_base[6] = 0.0
            row_base[7] = "N/A"
            row_base[8] = 0.0
            row_base[9] = 0.0
            row_base[10] = 0.0
            row_base[11] = fee
            row_base[12] = 0.0
            row_base[13] = 0
            row_base[14] = "Nein"
            row_base[15] = f"Ledger Ref: {refid} | Possible crypto buy missing?"
            tax_data.append(row_base)
            line_num += 1
            processed_refids.add(refid)
            continue

    # Calculate summary values from the processed data
    tax_gains_short = 0.0
    tax_gains_long = 0.0
    losses = 0.0
    total_costs = 0.0
    total_proceeds = 0.0
    total_fees = 0.0
    total_gain_loss = 0.0
    
    for row in tax_data[1:]:  # Skip header row
        if row[1] == "Verkauf":
            gain_loss = float(row[12]) if row[12] else 0.0
            hold_days = float(row[13]) if row[13] else 0.0
            
            if gain_loss > 0:
                if hold_days <= 365:
                    tax_gains_short += gain_loss
                else:
                    tax_gains_long += gain_loss
            else:
                losses += abs(gain_loss)
        
        # Calculate totals from original data
        costs = float(row[9]) if row[9] and row[9] != "N/A" else 0.0
        proceeds = float(row[10]) if row[10] and row[10] != "N/A" else 0.0
        fees = float(row[11]) if row[11] and row[11] != "N/A" else 0.0
        gain_loss = float(row[12]) if row[12] and row[12] != "N/A" else 0.0
        
        total_costs += costs
        total_proceeds += proceeds
        total_fees += fees
        total_gain_loss += gain_loss

    # Add summary rows to the data
    tax_data.append([""] * len(HEADERS))  # Empty row
    
    # Summary row
    summary_row = ["Summe", "", "", "", "", "", "", "", ""]
    summary_row.extend([total_costs, total_proceeds, total_fees, total_gain_loss, "", "", ""])
    tax_data.append(summary_row)
    
    # Tax summary section
    tax_data.append([""] * len(HEADERS))  # Empty row
    tax_data.append(["--- Steuerliche Zusammenfassung ---"] + [""] * (len(HEADERS) - 1))
    
    # Total income and expenses
    total_income_row = ["GESAMT", "Gesamteinnahmen", "", "", "", "", "", "", ""]
    total_income_row.extend([total_proceeds, "", "", "", "", "", "Summe aller Verkaufserlöse"])
    tax_data.append(total_income_row)
    
    total_expenses_row = ["GESAMT", "Gesamtausgaben", "", "", "", "", "", "", ""]
    total_expenses_row.extend([total_costs, "", "", "", "", "", "Summe aller Anschaffungskosten"])
    tax_data.append(total_expenses_row)
    
    total_fees_row = ["GESAMT", "Gesamtgebühren", "", "", "", "", "", "", ""]
    total_fees_row.extend([total_fees, "", "", "", "", "", "Summe aller Gebühren"])
    tax_data.append(total_fees_row)
    
    # Taxable gains (short-term)
    tax_short_row = ["GESAMT", "Steuerpfl. Gewinne (<1J)", "", "", "", "", "", "", ""]
    tax_short_row.extend([tax_gains_short, "", "", "", "", "Ja", "Anlage SO"])
    tax_data.append(tax_short_row)
    
    # Tax-free gains (long-term)
    tax_long_row = ["GESAMT", "Steuerfreie Gewinne (>1J)", "", "", "", "", "", "", ""]
    tax_long_row.extend([tax_gains_long, "", "", "", "", "Nein", ""])
    tax_data.append(tax_long_row)
    
    # Losses
    losses_row = ["GESAMT", "Verluste", "", "", "", "", "", "", ""]
    losses_row.extend([losses, "", "", "", "", "", "Anlage SO"])
    tax_data.append(losses_row)
    
    # Net result
    net_result = tax_gains_short + tax_gains_long - losses
    net_result_row = ["GESAMT", "Netto Ergebnis (§23 EStG)", "", "", "", "", "", "", ""]
    net_result_row.extend([net_result, "", "", "", "", "", 
                          f"Netto {net_result:.2f}€ -> {'Kein steuerpfl. Gewinn' if net_result <= 0 else 'Steuerpfl. Gewinn'}"])
    tax_data.append(net_result_row)
    
    # Taxable amount with threshold
    freigrenze = 1000 if year >= 2024 else 600
    taxable_amount = max(0, tax_gains_short - freigrenze)
    taxable_row = ["GESAMT", f"Zu versteuern ({freigrenze}€ Grenze)", "", "", "", "", "", "", ""]
    taxable_row.extend([taxable_amount, "", "", "", "", "Ja", f"Freigrenze: {freigrenze}€ gemäß §23 EStG"])
    tax_data.append(taxable_row)
    
    # Info section
    tax_data.append([""] * len(HEADERS))  # Empty row
    tax_data.append(["INFO", "Erstellt am", datetime.now().strftime("%Y-%m-%d %H:%M:%S"), 
                    "", "", "", "", "", "", "", "", "", "", "", "", ""])
    tax_data.append(["INFO", "Methode", "FIFO", "", "", "", "", "", "", "", "", "", "", "", "", "Gemäß BMF"])
    tax_data.append(["INFO", "Hinweis", "Diebstahlverluste nicht abzugsfähig", 
                    "", "", "", "", "", "", "", "", "", "", "", "", "§23 EStG"])

    return tax_data

# --- Utility Function ---
def get_or_create_sheet(spreadsheet_id: str, sheet_name: str) -> Tuple[str, int]:
    """Get or create a sheet in the Google Spreadsheet."""
    spreadsheet = service.spreadsheets().get(
        spreadsheetId=spreadsheet_id).execute()
    sheets = spreadsheet.get('sheets', [])
    existing_sheet = next(
        (s for s in sheets if s['properties']['title'] == sheet_name), None)
    if existing_sheet:
        return sheet_name, existing_sheet['properties']['sheetId']
    req = {"addSheet": {"properties": {"title": sheet_name,
                                       "gridProperties": {"rowCount": 2000, "columnCount": len(HEADERS)}}}}
    res = service.spreadsheets().batchUpdate(
        spreadsheetId=spreadsheet_id, body={"requests": [req]}).execute()
    sheet_id = res['replies'][0]['addSheet']['properties']['sheetId']
    return sheet_name, sheet_id

# --- Sheet Writing Function ---
def write_to_sheets(data: List[List[Any]], year: int) -> None:
    """Write processed tax data to a Google Sheet with formulas and formatting."""
    if len(data) <= 1:
        print(f"Skipping sheet generation for {year}: No data.")
        return
    
    sheet_name = f"Steuer {year}"
    sheet_name, sheet_id = get_or_create_sheet(SHEET_ID, sheet_name)
    
    # Clear the sheet
    service.spreadsheets().values().clear(
        spreadsheetId=SHEET_ID, range=sheet_name).execute()
    
    # First, write the headers
    header_row = data[0]
    header_range = f"{sheet_name}!A1:{chr(65 + len(header_row) - 1)}1"
    header_body = {"values": [header_row]}
    service.spreadsheets().values().update(
        spreadsheetId=SHEET_ID, range=header_range, valueInputOption="RAW", body=header_body
    ).execute()
    
    # Format the header row
    format_header_request = {
        "requests": [
            {
                "repeatCell": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": 0,
                        "endRowIndex": 1,
                        "startColumnIndex": 0,
                        "endColumnIndex": len(header_row)
                    },
                    "cell": {
                        "userEnteredFormat": {
                            "backgroundColor": {
                                "red": 0.8,
                                "green": 0.8,
                                "blue": 0.8
                            },
                            "horizontalAlignment": "CENTER",
                            "textFormat": {
                                "bold": True
                            }
                        }
                    },
                    "fields": "userEnteredFormat(backgroundColor,textFormat,horizontalAlignment)"
                }
            }
        ]
    }
    service.spreadsheets().batchUpdate(
        spreadsheetId=SHEET_ID, body=format_header_request).execute()
    
    # Now process and write the data rows with formulas
    formula_rows = []
    
    for i, row in enumerate(data[1:], start=1):
        # Skip empty rows
        if not row or row[0] == "":
            formula_rows.append(row)
            continue
        
        # Create a new row with formulas
        formula_row = row.copy()
        row_idx = i + 1  # +1 because of 1-indexed in sheets
        
        # For date fields, ensure they're properly formatted
        if isinstance(row[2], str) and row[2] != "N/A":
            try:
                date_obj = datetime.strptime(row[2], "%Y-%m-%d %H:%M:%S")
                formula_row[2] = f"=DATE({date_obj.year},{date_obj.month},{date_obj.day})"
            except ValueError:
                pass
        
        if isinstance(row[5], str) and row[5] != "N/A":
            try:
                date_obj = datetime.strptime(row[5], "%Y-%m-%d %H:%M:%S")
                formula_row[5] = f"=DATE({date_obj.year},{date_obj.month},{date_obj.day})"
            except ValueError:
                pass
        
        if isinstance(row[7], str) and row[7] != "N/A":
            try:
                date_obj = datetime.strptime(row[7], "%Y-%m-%d %H:%M:%S")
                formula_row[7] = f"=DATE({date_obj.year},{date_obj.month},{date_obj.day})"
            except ValueError:
                pass
        
        # For all transactions, ensure date fields are properly formatted
        if isinstance(row[2], str) and row[2] != "N/A":
            try:
                date_obj = datetime.strptime(row[2], "%Y-%m-%d %H:%M:%S")
                formula_row[2] = f"=DATE({date_obj.year},{date_obj.month},{date_obj.day})"
            except ValueError:
                pass
        
        if isinstance(row[5], str) and row[5] != "N/A":
            try:
                date_obj = datetime.strptime(row[5], "%Y-%m-%d %H:%M:%S")
                formula_row[5] = f"=DATE({date_obj.year},{date_obj.month},{date_obj.day})"
            except ValueError:
                pass
        
        if isinstance(row[7], str) and row[7] != "N/A":
            try:
                date_obj = datetime.strptime(row[7], "%Y-%m-%d %H:%M:%S")
                formula_row[7] = f"=DATE({date_obj.year},{date_obj.month},{date_obj.day})"
            except ValueError:
                pass
        
        # For Verkauf (sell) transactions, use formulas for all calculations
        if row[1] == "Verkauf":
            # Make sure purchase date is filled
            if formula_row[5] == "N/A" or not formula_row[5]:
                # Try to find a purchase date from FIFO details
                fifo_details = row[15]
                if "Lot" in fifo_details and "(" in fifo_details and ")" in fifo_details:
                    date_match = fifo_details.split("(")[1].split(")")[0]
                    try:
                        date_obj = datetime.strptime(date_match, "%Y-%m-%d")
                        formula_row[5] = f"=DATE({date_obj.year},{date_obj.month},{date_obj.day})"
                    except ValueError:
                        pass
            
            # Make sure sale date is filled
            if formula_row[7] == "N/A" or not formula_row[7]:
                formula_row[7] = formula_row[2]  # Use transaction date as sale date
            
            # For Verkauf (sell) transactions, we need to ensure the calculations are correct
            # Always use the original values to avoid circular dependencies
            
            # Purchase price per unit - Use the original value directly
            if isinstance(row[6], (int, float)) and row[6] != 0:
                formula_row[6] = row[6]  # Use the original purchase price per unit
            else:
                # If no original value, just use a static value of 0
                formula_row[6] = 0
            formula_row[15] += " | Kaufpreis/Stk (original value)"
            
            # Sale price per unit - Use the original value directly
            if isinstance(row[8], (int, float)) and row[8] != 0:
                formula_row[8] = row[8]  # Use the original sale price per unit
            else:
                # If no original value, just use a static value of 0
                formula_row[8] = 0
            formula_row[15] += " | Verkaufspreis/Stk (original value)"
            
            # Cost calculation - Amount multiplied by purchase price per unit
            if isinstance(row[9], (int, float)) and row[9] != 0:
                formula_row[9] = row[9]  # Use the original cost
            else:
                formula_row[9] = f"=ROUND(E{row_idx}*G{row_idx},2)"
            formula_row[15] += " | Kosten = Anzahl × Kaufpreis/Stk"
            
            # Proceeds calculation - Amount multiplied by sale price per unit
            if isinstance(row[10], (int, float)) and row[10] != 0:
                formula_row[10] = row[10]  # Use the original proceeds
            else:
                formula_row[10] = f"=ROUND(E{row_idx}*I{row_idx},2)"
            formula_row[15] += " | Erlös = Anzahl × Verkaufspreis/Stk"
            
            # Profit/Loss calculation - Proceeds minus costs minus fee
            formula_row[12] = f"=K{row_idx}-J{row_idx}-L{row_idx}"
            formula_row[15] += " | Gewinn/Verlust = Erlös - Kosten - Gebühr"
            
            # Holding period calculation - Sale date minus purchase date in days
            formula_row[13] = f"=IF(AND(F{row_idx}<>\"\",H{row_idx}<>\"\"),H{row_idx}-F{row_idx},0)"
            formula_row[15] += " | Haltedauer = Verkaufsdatum - Kaufdatum"
            
            # Tax status based on holding period - Yes if <= 365 days, No if > 365 days
            formula_row[14] = f"=IF(N{row_idx}>365,\"Nein\",\"Ja\")"
            formula_row[15] += " | Steuerpflichtig = Ja wenn Haltedauer ≤ 365 Tage"
        
        # For Kauf (buy) transactions, use formulas for all calculations
        elif row[1] == "Kauf (Trade)":
            # Make sure purchase date is filled
            if formula_row[5] == "N/A" or not formula_row[5]:
                formula_row[5] = formula_row[2]  # Use transaction date as purchase date
            
            # Purchase price per unit
            formula_row[6] = f"=IF(E{row_idx}>0,J{row_idx}/E{row_idx},0)"
            
            # Cost calculation (Amount * Purchase Price)
            formula_row[9] = f"=E{row_idx}*G{row_idx}"
        
        # For EUR transactions, ensure proper formatting
        elif row[1].startswith("EUR"):
            # No purchase/sale dates needed
            formula_row[5] = ""
            formula_row[7] = ""
            
            # No prices per unit needed
            formula_row[6] = ""
            formula_row[8] = ""
            
            # No costs/proceeds needed
            formula_row[9] = "0"
            formula_row[10] = "0"
            
            # Keep fees as is - important for total fee calculation
            
            # No profit/loss or holding period
            formula_row[12] = "0"
            formula_row[13] = "0"
            formula_row[14] = "Nein"
        
        # For Receive transactions, ensure proper formatting
        elif row[1] == "Receive (0 Cost)":
            # Make sure receive date is filled
            if formula_row[5] == "N/A" or not formula_row[5]:
                formula_row[5] = formula_row[2]  # Use transaction date as receive date
            
            # Purchase price is 0
            formula_row[6] = "0"
            
            # Cost is 0
            formula_row[9] = "0"
        
        formula_rows.append(formula_row)
    
    # Add summary row with formulas
    summary_row_idx = 0
    for i, row in enumerate(formula_rows):
        if isinstance(row[0], str) and row[0] == "Summe":
            summary_row_idx = i + 2  # +2 because of 1-indexed in sheets and header row
            
            # Replace static values with formulas
            formula_rows[i][9] = f"=SUM(J2:J{summary_row_idx-1})"  # Total costs
            formula_rows[i][10] = f"=SUM(K2:K{summary_row_idx-1})"  # Total proceeds
            formula_rows[i][11] = f"=SUM(L2:L{summary_row_idx-1})"  # Total fees
            formula_rows[i][12] = f"=SUM(M2:M{summary_row_idx-1})"  # Total profit/loss
            break
    
    # Add tax summary rows with formulas
    tax_summary_start_idx = 0
    for i, row in enumerate(formula_rows):
        if isinstance(row[1], str) and row[1] == "Steuerpfl. Gewinne (<1J)":
            tax_summary_start_idx = i + 2  # +2 because of 1-indexed in sheets and header row
            
            # Replace static values with formulas
            formula_rows[i][9] = f"=SUMIF(O2:O{summary_row_idx-2},\"Ja\",M2:M{summary_row_idx-2})"
            
            # Tax-free gains
            formula_rows[i+1][9] = f"=SUMIF(O2:O{summary_row_idx-2},\"Nein\",M2:M{summary_row_idx-2})"
            
            # Losses - Use SUMIF with proper filtering to avoid #N/A errors
            formula_rows[i+2][9] = f"=IF(COUNTIF(M2:M{summary_row_idx-2},\"<0\")>0,SUMIF(M2:M{summary_row_idx-2},\"<0\",ABS(M2:M{summary_row_idx-2})),0)"
            
            # Net result - Ensure we have valid values to avoid #N/A errors
            formula_rows[i+3][9] = f"=IF(AND(ISNUMBER(J{tax_summary_start_idx}),ISNUMBER(J{tax_summary_start_idx+1}),ISNUMBER(J{tax_summary_start_idx+2})),J{tax_summary_start_idx}+J{tax_summary_start_idx+1}-J{tax_summary_start_idx+2},0)"
            
            # Taxable amount
            freigrenze = 1000 if year >= 2024 else 600
            formula_rows[i+4][9] = f"=MAX(0,J{tax_summary_start_idx}-{freigrenze})"
            break
    
    # Write the data with formulas
    data_range = f"{sheet_name}!A2:{chr(65 + len(header_row) - 1)}{len(formula_rows) + 1}"
    data_body = {"values": [list(map(lambda x: "" if x is None else x, row)) for row in formula_rows]}
    service.spreadsheets().values().update(
        spreadsheetId=SHEET_ID, range=data_range, valueInputOption="USER_ENTERED", body=data_body
    ).execute()
    
    # Format the data columns
    format_requests = {
        "requests": [
            # Format currency columns (J, K, L, M)
            {
                "repeatCell": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": 1,
                        "endRowIndex": len(formula_rows) + 1,
                        "startColumnIndex": 9,  # Column J
                        "endColumnIndex": 13  # Column M
                    },
                    "cell": {
                        "userEnteredFormat": {
                            "numberFormat": {
                                "type": "CURRENCY",
                                "pattern": "#,##0.00 €"
                            }
                        }
                    },
                    "fields": "userEnteredFormat.numberFormat"
                }
            },
            # Format date columns (C, F, H)
            {
                "repeatCell": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": 1,
                        "endRowIndex": len(formula_rows) + 1,
                        "startColumnIndex": 2,  # Column C
                        "endColumnIndex": 3  # Column C
                    },
                    "cell": {
                        "userEnteredFormat": {
                            "numberFormat": {
                                "type": "DATE",
                                "pattern": "dd.mm.yyyy"
                            }
                        }
                    },
                    "fields": "userEnteredFormat.numberFormat"
                }
            },
            {
                "repeatCell": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": 1,
                        "endRowIndex": len(formula_rows) + 1,
                        "startColumnIndex": 5,  # Column F
                        "endColumnIndex": 6  # Column F
                    },
                    "cell": {
                        "userEnteredFormat": {
                            "numberFormat": {
                                "type": "DATE",
                                "pattern": "dd.mm.yyyy"
                            }
                        }
                    },
                    "fields": "userEnteredFormat.numberFormat"
                }
            },
            {
                "repeatCell": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": 1,
                        "endRowIndex": len(formula_rows) + 1,
                        "startColumnIndex": 7,  # Column H
                        "endColumnIndex": 8  # Column H
                    },
                    "cell": {
                        "userEnteredFormat": {
                            "numberFormat": {
                                "type": "DATE",
                                "pattern": "dd.mm.yyyy"
                            }
                        }
                    },
                    "fields": "userEnteredFormat.numberFormat"
                }
            },
            # Format number columns (E, G, I)
            {
                "repeatCell": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": 1,
                        "endRowIndex": len(formula_rows) + 1,
                        "startColumnIndex": 4,  # Column E
                        "endColumnIndex": 5  # Column E
                    },
                    "cell": {
                        "userEnteredFormat": {
                            "numberFormat": {
                                "type": "NUMBER",
                                "pattern": "#,##0.00000000"
                            }
                        }
                    },
                    "fields": "userEnteredFormat.numberFormat"
                }
            },
            {
                "repeatCell": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": 1,
                        "endRowIndex": len(formula_rows) + 1,
                        "startColumnIndex": 6,  # Column G
                        "endColumnIndex": 7  # Column G
                    },
                    "cell": {
                        "userEnteredFormat": {
                            "numberFormat": {
                                "type": "CURRENCY",
                                "pattern": "#,##0.00 €"
                            }
                        }
                    },
                    "fields": "userEnteredFormat.numberFormat"
                }
            },
            {
                "repeatCell": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": 1,
                        "endRowIndex": len(formula_rows) + 1,
                        "startColumnIndex": 8,  # Column I
                        "endColumnIndex": 9  # Column I
                    },
                    "cell": {
                        "userEnteredFormat": {
                            "numberFormat": {
                                "type": "CURRENCY",
                                "pattern": "#,##0.00 €"
                            }
                        }
                    },
                    "fields": "userEnteredFormat.numberFormat"
                }
            },
            # Format holding period column (N)
            {
                "repeatCell": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": 1,
                        "endRowIndex": len(formula_rows) + 1,
                        "startColumnIndex": 13,  # Column N
                        "endColumnIndex": 14  # Column N
                    },
                    "cell": {
                        "userEnteredFormat": {
                            "numberFormat": {
                                "type": "NUMBER",
                                "pattern": "#,##0"
                            }
                        }
                    },
                    "fields": "userEnteredFormat.numberFormat"
                }
            },
            # Auto-resize columns
            {
                "autoResizeDimensions": {
                    "dimensions": {
                        "sheetId": sheet_id,
                        "dimension": "COLUMNS",
                        "startIndex": 0,
                        "endIndex": len(header_row)
                    }
                }
            }
        ]
    }
    
    service.spreadsheets().batchUpdate(
        spreadsheetId=SHEET_ID, body=format_requests).execute()
    
    print(f"Sheet written for {year}.")

# --- Write Log Data to Sheet ---
def write_log_to_sheet() -> None:
    """Write log data to a Google Sheet for reference."""
    sheet_name = "Log"
    sheet_name, sheet_id = get_or_create_sheet(SHEET_ID, sheet_name)

    # Clear the sheet before writing new data
    service.spreadsheets().values().clear(
        spreadsheetId=SHEET_ID, range=sheet_name).execute()

    # Prepare log headers and data
    log_headers = ["Timestamp", "Event", "Details"]
    data_rows = [log_headers] + LOG_DATA

    # Write data to the sheet
    body = {"values": [list(map(str, row)) for row in data_rows]}
    service.spreadsheets().values().update(
        spreadsheetId=SHEET_ID, range=f"{sheet_name}!A1", valueInputOption="USER_ENTERED", body=body
    ).execute()
    
    # Format the log sheet
    format_requests = {
        "requests": [
            # Format header row
            {
                "repeatCell": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": 0,
                        "endRowIndex": 1,
                        "startColumnIndex": 0,
                        "endColumnIndex": len(log_headers)
                    },
                    "cell": {
                        "userEnteredFormat": {
                            "backgroundColor": {
                                "red": 0.8,
                                "green": 0.8,
                                "blue": 0.8
                            },
                            "horizontalAlignment": "CENTER",
                            "textFormat": {
                                "bold": True
                            }
                        }
                    },
                    "fields": "userEnteredFormat(backgroundColor,textFormat,horizontalAlignment)"
                }
            },
            # Auto-resize columns
            {
                "autoResizeDimensions": {
                    "dimensions": {
                        "sheetId": sheet_id,
                        "dimension": "COLUMNS",
                        "startIndex": 0,
                        "endIndex": len(log_headers)
                    }
                }
            }
        ]
    }
    service.spreadsheets().batchUpdate(
        spreadsheetId=SHEET_ID, body=format_requests).execute()
    
    print("Log sheet written.")

# --- Main Execution Block ---
def main() -> None:
    print(f"Script started at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log_event("Script Start", "Script execution started")

    # Fetch data
    log_event("Data Fetch", "Fetching trade history and ledger entries from Kraken API")
    all_trades = get_trades(START_TIMESTAMP, END_TIMESTAMP)
    all_ledger = get_ledger(START_TIMESTAMP, END_TIMESTAMP)
    log_event("Data Fetch Complete", f"Retrieved {len(all_trades)} trades and {len(all_ledger)} ledger entries")

    # Create and write raw transactions
    log_event("Raw Transactions", "Writing raw transaction data to sheet")
    raw_transactions = [
        {"type": "trade", **trade} for trade in all_trades
    ] + [
        {"type": "ledger", **entry} for entry in all_ledger
    ]
    write_raw_transactions_to_sheets(raw_transactions)
    log_event("Raw Transactions Complete", f"Wrote {len(raw_transactions)} raw transactions to sheet")

    # Continue with existing tax processing
    data_by_year = group_by_year(all_trades, all_ledger)
    log_event("Year Grouping", f"Grouped data into {len(data_by_year)} years: {', '.join(map(str, sorted(data_by_year.keys())))}")
    
    for year in sorted(data_by_year.keys()):
        year_trades, year_ledger = data_by_year[year]
        log_event(f"Processing Year {year}", f"Processing {len(year_trades)} trades and {len(year_ledger)} ledger entries for {year}")
        print(f"Processing Year {year}")
        tax_data = process_for_tax(year_trades, year_ledger, year)
        write_to_sheets(tax_data, year)
        log_event(f"Year {year} Complete", f"Processed {len(tax_data) - 1} tax entries for {year}")
    
    # Write log data to sheet
    log_event("Script Complete", "All processing completed successfully")
    write_log_to_sheet()
    
    print("Processing complete.")

if __name__ == "__main__":
    main()
