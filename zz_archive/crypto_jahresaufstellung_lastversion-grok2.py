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
from typing import Dict, List, Any, Tuple

# --- Log Data Storage ---
LOG_DATA = []

# --- Global Variables ---
HOLDINGS: Dict[str, List[Dict[str, Any]]] = {}
PRICE_CACHE: Dict[Tuple[str, int], float] = {}
HEADERS = [
    "Zeile", "Typ", "Datum", "Asset", "Anzahl", "Kaufdatum", "Kaufpreis (€)/Stk", "Verkaufsdatum",
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

# --- Write Raw Transactions to Sheets ---


def write_raw_transactions_to_sheets(transactions: List[Dict[str, Any]]) -> None:
    sheet_name = "Raw Transactions"

    # Create or get the sheet
    def get_or_create_sheet(spreadsheet_id: str, sheet_name: str) -> Tuple[str, int]:
        spreadsheet = service.spreadsheets().get(
            spreadsheetId=spreadsheet_id).execute()
        sheets = spreadsheet.get('sheets', [])
        existing_sheet = next(
            (s for s in sheets if s['properties']['title'] == sheet_name), None)
        if existing_sheet:
            return sheet_name, existing_sheet['properties']['sheetId']
        req = {
            "addSheet": {
                "properties": {"title": sheet_name, "gridProperties": {"rowCount": 2000, "columnCount": len(RAW_HEADERS)}}
            }
        }
        res = service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id, body={"requests": [req]}).execute()
        sheet_id = res['replies'][0]['addSheet']['properties']['sheetId']
        return sheet_name, sheet_id

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
    # Fixed: Removed unnecessary f-string
    print("Raw Transactions sheet written.")

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
CREDENTIALS_FILE = Path(__file__).parent / \
    "mbay-tax-sheet-for-kryptos-7fc01e35fb9a.json"
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
    END_TIMESTAMP = int(datetime.now(timezone.utc).timestamp()
                        ) if not END_DATE_STR else None
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


def kraken_request(endpoint: str, data: Dict[str, str] = None) -> Dict[str, Any]:
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


def get_trades(start_ts: int = None, end_ts: int = None) -> List[Dict[str, Any]]:
    """Fetch trade history from Kraken API with pagination."""
    trades_dict = {}
    offset = 0
    fetch_params = {"trades": "true"}
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


def get_ledger(start_ts: int = None, end_ts: int = None) -> List[Dict[str, Any]]:
    """Fetch ledger entries from Kraken API with pagination."""
    ledger_dict = {}
    offset = 0
    fetch_params = {}
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

    for event in events:
        timestamp = event["time"]
        if datetime.fromtimestamp(timestamp, timezone.utc).year != year:
            continue
        date_str = datetime.fromtimestamp(
            timestamp, timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        data = event["data"]
        row_base = [""] * len(HEADERS)
        row_base[0] = line_num + 1

        if event["type"] == "trade" and data.get("type") == "buy" and "EUR" in data.get("pair", ""):
            trade_refid = data.get("ordertxid", f"trade_{timestamp}")
            if trade_refid not in processed_refids:
                asset = data["pair"].replace("ZEUR", "").replace("EUR", "")
                amount_traded = float(data.get("vol", 0))
                price_per_unit = float(data.get("price", 0))
                fee_paid = float(data.get("fee", 0))
                cost_or_proceeds = float(data.get("cost", 0))
                HOLDINGS.setdefault(asset, []).append({
                    "amount": amount_traded, "price_eur": price_per_unit, "timestamp": timestamp,
                    "fee_eur": fee_paid, "refid": trade_refid
                })
                row_base[1] = "Kauf (Trade)"
                row_base[2] = date_str
                row_base[3] = asset
                row_base[4] = amount_traded
                row_base[5] = date_str
                row_base[6] = price_per_unit
                row_base[9] = cost_or_proceeds
                row_base[11] = fee_paid
                row_base[14] = "N/A"
                row_base[15] = f"Trade Ref: {trade_refid}"
                tax_data.append(row_base)
                line_num += 1
                processed_refids.add(trade_refid)

        elif event["type"] == "ledger":
            entry_type = data.get("type", "").capitalize()
            asset = data.get("asset", "N/A")
            asset_norm = asset.replace('Z', '', 1).replace('X', '', 1) if len(
                asset) >= 4 and asset[0] in ('X', 'Z') else asset
            amount_ledger = float(data.get("amount", 0))
            fee_paid = float(data.get("fee", 0))
            refid = data.get("refid", f"ledger_{timestamp}")

            if refid in processed_refids or asset_norm == "KFEE":
                continue

            if entry_type == "Spend" and amount_ledger < 0 and asset_norm != "EUR":
                for next_event in events[events.index(event) + 1:]:
                    if next_event["time"] - timestamp > 600:
                        break
                    if (next_event["type"] == "ledger" and
                        next_event["data"]["type"].capitalize() == "Receive" and
                            next_event["data"]["asset"] in ["ZEUR", "EUR"]):
                        proceeds = float(next_event["data"]["amount"])
                        sell_amount = abs(amount_ledger)
                        price_pu = proceeds / sell_amount if sell_amount > 1e-12 else 0
                        cost_basis = 0
                        fifo_details = []
                        if asset_norm in HOLDINGS:
                            rem_sell = sell_amount
                            for i, lot in enumerate(HOLDINGS[asset_norm]):
                                if rem_sell <= 1e-9:
                                    break
                                use = min(lot["amount"], rem_sell)
                                cost_basis += use * lot["price_eur"]
                                fifo_details.append(
                                    f"Lot {i+1}: {use:.8f}@{lot['price_eur']:.4f}")
                                lot["amount"] -= use
                                rem_sell -= use
                            HOLDINGS[asset_norm] = [
                                lot for lot in HOLDINGS[asset_norm] if lot["amount"] > 1e-9]
                            if not HOLDINGS[asset_norm]:
                                del HOLDINGS[asset_norm]
                        gain_loss = proceeds - cost_basis - fee_paid
                        buy_ts = min((lot["timestamp"] for lot in HOLDINGS.get(
                            asset_norm, [])), default=timestamp)
                        hold_days = (timestamp - buy_ts) / \
                            (24 * 3600) if buy_ts < timestamp else 0
                        row_base[1] = "Verkauf"
                        row_base[2] = date_str
                        row_base[3] = asset_norm
                        row_base[4] = sell_amount
                        row_base[5] = datetime.fromtimestamp(
                            buy_ts, timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
                        row_base[6] = cost_basis / \
                            sell_amount if sell_amount > 1e-12 else 0
                        row_base[7] = date_str
                        row_base[8] = price_pu
                        row_base[9] = cost_basis
                        row_base[10] = proceeds
                        row_base[11] = fee_paid
                        row_base[12] = gain_loss
                        row_base[13] = round(hold_days)
                        row_base[14] = "Ja" if hold_days <= 365 else "Nein"
                        row_base[15] = f"Ledger Ref: {refid} | FIFO: {' | '.join(fifo_details)}"
                        tax_data.append(row_base)
                        line_num += 1
                        processed_refids.add(refid)
                        processed_refids.add(next_event["data"]["refid"])
                        break

    return tax_data

# --- Sheet Writing Function ---


def write_to_sheets(data: List[List[Any]], year: int) -> None:
    """Write processed tax data to a Google Sheet."""
    if len(data) <= 1:
        print(f"Skipping sheet generation for {year}: No data.")
        return
    sheet_name = f"Steuer {year}"
    sheet_name, sheet_id = get_or_create_sheet(SHEET_ID, sheet_name)
    service.spreadsheets().values().clear(
        spreadsheetId=SHEET_ID, range=sheet_name).execute()
    body = {"values": [list(map(str, row)) for row in data]}
    service.spreadsheets().values().update(
        spreadsheetId=SHEET_ID, range=f"{sheet_name}!A1", valueInputOption="USER_ENTERED", body=body
    ).execute()
    print(f"Sheet written for {year}.")

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

# --- Main Execution Block ---


def main() -> None:
    print(f"Script started at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # Fetch data
    all_trades = get_trades(START_TIMESTAMP, END_TIMESTAMP)
    all_ledger = get_ledger(START_TIMESTAMP, END_TIMESTAMP)

    # Create and write raw transactions
    raw_transactions = [
        {"type": "trade", **trade} for trade in all_trades
    ] + [
        {"type": "ledger", **entry} for entry in all_ledger
    ]
    write_raw_transactions_to_sheets(raw_transactions)

    # Continue with existing tax processing
    data_by_year = group_by_year(all_trades, all_ledger)
    for year in sorted(data_by_year.keys()):
        year_trades, year_ledger = data_by_year[year]
        print(f"Processing Year {year}")
        tax_data = process_for_tax(year_trades, year_ledger, year)
        write_to_sheets(tax_data, year)
    print("Processing complete.")


if __name__ == "__main__":
    main()
