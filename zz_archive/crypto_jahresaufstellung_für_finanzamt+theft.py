#!/usr/bin/env python3
import subprocess
import sys
import json
from pathlib import Path
from datetime import datetime
import time
import hmac
import hashlib
import base64
import requests
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError


def install_packages(packages):
    subprocess.check_call([sys.executable, "-m", "pip", "install"] + packages)


required_packages = ["requests", "google-auth", "google-auth-oauthlib",
    "google-auth-httplib2", "google-api-python-client"]
for package in required_packages:
    try:
        __import__(package)
    except ImportError:
        print(f"Package '{package}' not found. Installing...")
        install_packages([package])

CONFIG_FILE = Path(__file__).parent / "config.json"
CREDENTIALS_FILE = Path(__file__).parent / \
                        "mbay-tax-sheet-for-kryptos-7fc01e35fb9a.json"

try:
    with CONFIG_FILE.open('r') as f:
        config = json.load(f)
    API_KEY = config["API_KEY"]
    API_SECRET = config["API_SECRET"]
    SHEET_ID = config["SHEET_ID"]
    THEFT_TXIDS = config.get("theft_txids", [])
except FileNotFoundError:
    raise FileNotFoundError(f"Config file not found at: {CONFIG_FILE}")
except KeyError as e:
    raise KeyError(f"Missing required key in config.json: {str(e)}")

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
creds = Credentials.from_service_account_file(
    str(CREDENTIALS_FILE), scopes=SCOPES)
service = build("sheets", "v4", credentials=creds)

HOLDINGS = {}
LOG_DATA = [["Timestamp", "Event", "Details"]]


def log_event(event, details):
    LOG_DATA.append([datetime.now().strftime(
        "%Y-%m-%d %H:%M:%S"), event, details])


def kraken_request(endpoint, data=None):
    url = f"https://api.kraken.com/0/private/{endpoint}"
    if data is None:
        data = {}
    data["nonce"] = str(int(time.time() * 1000))
    post_data = "&".join([f"{k}={v}" for k, v in data.items()])
    encoded = (str(data["nonce"]) + post_data).encode()
    message = f"/0/private/{endpoint}".encode() + \
                                              hashlib.sha256(encoded).digest()
    signature = hmac.new(base64.b64decode(API_SECRET), message, hashlib.sha512)
    sig = base64.b64encode(signature.digest()).decode()
    headers = {"API-Key": API_KEY, "API-Sign": sig}
    response = requests.post(url, headers=headers, data=data)
    log_event(f"API Call: {endpoint}", f"Status: {
              response.status_code}, Response: {response.text[:200]}...")
    if response.status_code != 200 or "error" in response.json() and response.json()["error"]:
        raise Exception(
            f"API error: {response.json().get('error', 'Unknown error')}")
    return response.json()


def get_trades():
    trades = []
    result = kraken_request("TradesHistory")
    trades.extend(result["result"]["trades"].values())
    return trades


def get_ledger():
    ledger = []
    result = kraken_request("Ledgers")
    ledger.extend(result["result"]["ledger"].values())
    return ledger


def group_by_year(trades, ledger):
    trades_by_year = {}
    ledger_by_year = {}
    for trade in trades:
        year = datetime.fromtimestamp(float(trade["time"])).year
        trades_by_year.setdefault(year, []).append(trade)
    for entry in ledger:
        year = datetime.fromtimestamp(float(entry["time"])).year
        ledger_by_year.setdefault(year, []).append(entry)
    all_years = set(trades_by_year.keys()).union(ledger_by_year.keys())
    return {year: (trades_by_year.get(year, []), ledger_by_year.get(year, [])) for year in sorted(all_years)}


def get_market_price(asset, timestamp):
    dummy_prices = {"XETH": 2000, "XBT": 40000, "ADA": 0.5,
        "AVAX": 30, "ARB": 1, "XXRP": 0.6, "XXBT": 40000}
    return dummy_prices.get(asset, 0)


def process_for_tax(trades, ledger, year):
    tax_data = [["Line", "Date", "Type", "Asset", "Amount", "Buy Price (EUR)", "Sell Price (EUR)",
                "Total (EUR)", "Gain/Loss (EUR)", "Fee (EUR)", "Holding Period (Days)", "Taxable", "Tax Form Field", "Notes"]]
    trades = sorted(trades, key=lambda x: float(x["time"]))
    ledger = sorted(ledger, key=lambda x: float(x["time"]))
    summaries = {}
    total_gains = 0
    total_tax_free_gains = 0
    total_losses = 0
    total_fees = 0
    line_num = 2

    for trade in trades:
        timestamp = float(trade["time"])
        date = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(timestamp))
        pair = trade["pair"]
        asset = pair[:4] if pair.startswith(
            ("XBT", "ETH", "XRP", "ADA")) else pair[-3:]
        amount = float(trade["vol"])
        price_eur = float(trade["price"])
        total_eur = amount * price_eur
        fee_eur = float(trade["fee"])
        notes = f"{trade['ordertxid']} - Retain records for tax audit"
        summaries.setdefault(
            asset, {"gains": 0, "tax_free_gains": 0, "losses": 0})

        if trade["type"] == "buy" and pair.endswith("ZEUR"):
            HOLDINGS.setdefault(asset, []).append(
                (amount, price_eur, timestamp))
            tax_data.append([line_num, date, "Buy", asset, amount, price_eur,
                            "", total_eur, "", fee_eur, "", "No", "N/A", notes])
            log_event("Buy Recorded", f"{date}, {asset}, Amount: {amount}, Price: {
                      price_eur}, Total: {total_eur}, Fee: {fee_eur}")
            line_num += 1
        else:
            tax_data.append([line_num, date, "Trade", asset, amount,
                            "", "", total_eur, "", fee_eur, "", "No", "N/A", notes])
            line_num += 1

    i = 0
    while i < len(ledger):
        entry = ledger[i]
        timestamp = float(entry["time"])
        date = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(timestamp))
        entry_type = entry["type"].capitalize()
        asset = entry["asset"]
        amount = float(entry["amount"])
        fee_eur = float(entry["fee"])
        refid = entry["refid"]
        notes = f"{refid} - Retain records for tax audit"
        summaries.setdefault(
            asset, {"gains": 0, "tax_free_gains": 0, "losses": 0})
        is_theft = refid in THEFT_TXIDS

        if entry_type == "Spend" and asset not in ("ZEUR", "EUR", "KFEE") and amount < 0 and i > 0:
            prev_entry = ledger[i - 1]
            if prev_entry["type"] == "receive" and prev_entry["asset"] == "ZEUR" and abs(float(prev_entry["time"]) - timestamp) < 1:
                sell_amount = -amount
                sell_price_eur = float(prev_entry["amount"]) / sell_amount
                total_eur = sell_amount * sell_price_eur
                remaining = sell_amount
                gain_loss = 0
                formula_parts = []
                buy_details = []
                earliest_buy_time = None

                log_event("Sell Calculation Start", f"{date}, {asset}, Amount: {
                          sell_amount}, Sell Price: {sell_price_eur}, Fee: {fee_eur}")
                if asset in HOLDINGS and HOLDINGS[asset]:
                    while remaining > 0 and HOLDINGS[asset]:
                        buy_amount, buy_price, buy_time = HOLDINGS[asset][0]
                        if earliest_buy_time is None or buy_time < earliest_buy_time:
                            earliest_buy_time = buy_time
                        sold_amount = min(buy_amount, remaining)
                        partial_gain_loss = (
                            sell_price_eur - buy_price) * sold_amount
                        gain_loss += partial_gain_loss
                        formula_parts.append(
                            f"({sell_price_eur} - {buy_price}) * {sold_amount}")
                        buy_details.append(f"Buy at {time.strftime(
                            '%Y-%m-%d %H:%M:%S', time.gmtime(buy_time))}: {sold_amount} at {buy_price} EUR")
                        log_event("FIFO Match", f"Sell {date}, {asset}, Used Buy: {
                                  buy_details[-1]}, Partial Gain/Loss: {partial_gain_loss}")
                        remaining -= sold_amount
                        if buy_amount <= remaining:
                            HOLDINGS[asset].pop(0)
                        else:
                            HOLDINGS[asset][0] = (
                                buy_amount - sold_amount, buy_price, buy_time)
                    if remaining > 0:
                        partial_gain_loss = sell_price_eur * remaining
                        gain_loss += partial_gain_loss
                        formula_parts.append(
                            f"({sell_price_eur} - 0) * {remaining}")
                        log_event("No Prior Buy", f"Sell {date}, {asset}, Amount: {
                                  remaining}, Assumed 0 cost basis, Gain: {partial_gain_loss}")
                else:
                    gain_loss = sell_price_eur * sell_amount
                    formula_parts.append(
                        f"({sell_price_eur} - 0) * {sell_amount}")
                    log_event("No Prior Buy", f"Sell {date}, {asset}, Amount: {
                              sell_amount}, Assumed 0 cost basis, Gain: {gain_loss}")

                raw_gain_loss = gain_loss
                gain_loss -= fee_eur
                total_fees += fee_eur
                formula_text = " + ".join(formula_parts) + \
                                          (f" - {fee_eur}" if fee_eur >
                                           0 else "")
                holding_period = round(
                    (timestamp - earliest_buy_time) / (24 * 3600)) if earliest_buy_time else 0
                is_taxable = holding_period < 365 if earliest_buy_time else True
                tax_field = "SO - Line 7" if is_taxable else "N/A"
                notes += f" | Formula: {formula_text}"

                if gain_loss > 0:
                    if is_taxable:
                        summaries[asset]["gains"] += gain_loss
                        total_gains += gain_loss
                        log_event("Gain Recorded", f"{asset}: {gain_loss} (Raw: {
                                  raw_gain_loss}, Fee Deducted: {fee_eur})")
                    else:
                        summaries[asset]["tax_free_gains"] += gain_loss
                        total_tax_free_gains += gain_loss
                        log_event(
                            "Tax-Free Gain", f"{asset}: {gain_loss} (Raw: {raw_gain_loss}, Fee Deducted: {fee_eur})")
                elif not is_theft:
                    summaries[asset]["losses"] += gain_loss
                    total_losses += gain_loss
                    log_event("Loss Recorded", f"{asset}: {gain_loss} (Raw: {
                              raw_gain_loss}, Fee Deducted: {fee_eur})")
                else:
                    notes += " | Theft loss - Non-deductible per §23 EStG"
                    log_event("Theft Loss Excluded", f"{asset}: {
                              gain_loss} flagged as theft, not deductible")

                buy_price_used = buy_price if 'buy_price' in locals() else 0
                tax_data.append([line_num, date, "Sell", asset, sell_amount, buy_price_used, sell_price_eur,
                                total_eur, gain_loss, fee_eur, holding_period, "Yes" if is_taxable else "No", tax_field, notes])
                log_event("Sell Processed", f"Line {line_num}, {date}, {asset}, Amount: {
                          sell_amount}, Gain/Loss: {gain_loss}, Formula: {formula_text}, Taxable: {is_taxable}")
                line_num += 1
                i += 1
                continue

        elif entry_type == "Spend" and asset not in ("ZEUR", "EUR", "KFEE") and amount < 0 and i < len(ledger) - 1:
            next_entry = ledger[i + 1]
            if next_entry["type"] == "receive" and next_entry["asset"] not in ("ZEUR", "EUR", "KFEE") and abs(float(next_entry["time"]) - timestamp) < 1:
                sell_amount = -amount
                received_asset = next_entry["asset"]
                received_amount = float(next_entry["amount"])
                sell_price_eur = get_market_price(
                    received_asset, timestamp) * received_amount / sell_amount
                total_eur = sell_amount * sell_price_eur
                remaining = sell_amount
                gain_loss = 0
                formula_parts = []
                buy_details = []
                earliest_buy_time = None

                log_event("Crypto-to-Crypto Trade Detected", f"{date}, Sold {asset} for {
                          received_asset}, Amount: {sell_amount}, Estimated Sell Price: {sell_price_eur} EUR")
                if asset in HOLDINGS and HOLDINGS[asset]:
                    while remaining > 0 and HOLDINGS[asset]:
                        buy_amount, buy_price, buy_time = HOLDINGS[asset][0]
                        if earliest_buy_time is None or buy_time < earliest_buy_time:
                            earliest_buy_time = buy_time
                        sold_amount = min(buy_amount, remaining)
                        partial_gain_loss = (
                            sell_price_eur - buy_price) * sold_amount
                        gain_loss += partial_gain_loss
                        formula_parts.append(
                            f"({sell_price_eur} - {buy_price}) * {sold_amount}")
                        buy_details.append(f"Buy at {time.strftime(
                            '%Y-%m-%d %H:%M:%S', time.gmtime(buy_time))}: {sold_amount} at {buy_price} EUR")
                        log_event("FIFO Match", f"Trade {date}, {asset}, Used Buy: {
                                  buy_details[-1]}, Partial Gain/Loss: {partial_gain_loss}")
                        remaining -= sold_amount
                        if buy_amount <= remaining:
                            HOLDINGS[asset].pop(0)
                        else:
                            HOLDINGS[asset][0] = (
                                buy_amount - sold_amount, buy_price, buy_time)
                    if remaining > 0:
                        partial_gain_loss = sell_price_eur * remaining
                        gain_loss += partial_gain_loss
                        formula_parts.append(
                            f"({sell_price_eur} - 0) * {remaining}")
                        log_event("No Prior Buy", f"Trade {date}, {asset}, Amount: {
                                  remaining}, Assumed 0 cost basis, Gain: {partial_gain_loss}")
                else:
                    gain_loss = sell_price_eur * sell_amount
                    formula_parts.append(
                        f"({sell_price_eur} - 0) * {sell_amount}")
                    log_event("No Prior Buy", f"Trade {date}, {asset}, Amount: {
                              sell_amount}, Assumed 0 cost basis, Gain: {gain_loss}")

                raw_gain_loss = gain_loss
                gain_loss -= fee_eur
                total_fees += fee_eur
                formula_text = " + ".join(formula_parts) + \
                                          (f" - {fee_eur}" if fee_eur >
                                           0 else "")
                holding_period = round(
                    (timestamp - earliest_buy_time) / (24 * 3600)) if earliest_buy_time else 0
                is_taxable = holding_period < 365 if earliest_buy_time else True
                tax_field = "SO - Line 7" if is_taxable else "N/A"
                notes += f" | Crypto-to-Crypto Trade ({asset} to {received_asset}) | Formula: {
                                                      formula_text}"

                if gain_loss > 0:
                    if is_taxable:
                        summaries[asset]["gains"] += gain_loss
                        total_gains += gain_loss
                        log_event("Gain Recorded", f"{asset}: {gain_loss} (Raw: {
                                  raw_gain_loss}, Fee Deducted: {fee_eur})")
                    else:
                        summaries[asset]["tax_free_gains"] += gain_loss
                        total_tax_free_gains += gain_loss
                        log_event(
                            "Tax-Free Gain", f"{asset}: {gain_loss} (Raw: {raw_gain_loss}, Fee Deducted: {fee_eur})")
                elif not is_theft:
                    summaries[asset]["losses"] += gain_loss
                    total_losses += gain_loss
                    log_event("Loss Recorded", f"{asset}: {gain_loss} (Raw: {
                              raw_gain_loss}, Fee Deducted: {fee_eur})")
                else:
                    notes += " | Theft loss - Non-deductible per §23 EStG"
                    log_event("Theft Loss Excluded", f"{asset}: {
                              gain_loss} flagged as theft, not deductible")

                buy_price_used = buy_price if 'buy_price' in locals() else 0
                tax_data.append([line_num, date, "Sell", asset, sell_amount, buy_price_used, sell_price_eur,
                                total_eur, gain_loss, fee_eur, holding_period, "Yes" if is_taxable else "No", tax_field, notes])
                tax_data.append([line_num + 1, date, "Receive", received_asset,
                                received_amount, "", "", "", "", fee_eur, "", "No", "N/A", notes])
                log_event("Trade Processed", f"Line {line_num}, {date}, {asset} to {received_asset}, Amount: {
                          sell_amount}, Gain/Loss: {gain_loss}, Formula: {formula_text}, Taxable: {is_taxable}")
                line_num += 2
                i += 2
                continue

        if entry_type == "Receive" and asset not in ("ZEUR", "EUR", "KFEE") and amount > 0:
            HOLDINGS.setdefault(asset, []).append((amount, 0, timestamp))
            log_event("Receive Recorded", f"{date}, {
                      asset}, Amount: {amount}, Assumed 0 cost basis")
        tax_data.append([line_num, date, entry_type, asset, amount,
                        "", "", "", "", fee_eur, "", "No", "N/A", notes])
        line_num += 1
        i += 1

    tax_data = [tax_data[0]] + \
        sorted(tax_data[1:], key=lambda x: datetime.strptime(
            x[1], "%Y-%m-%d %H:%M:%S"))

    freigrenze = 600 if year < 2024 else 1000
    net_taxable_gains_before_fees = total_gains - total_tax_free_gains + total_losses
    net_taxable_gains = net_taxable_gains_before_fees - total_fees
    taxable_gains = net_taxable_gains if net_taxable_gains > freigrenze else 0
    freigrenze_note = f"Below {
        freigrenze}€ Freigrenze - Tax-Free" if net_taxable_gains <= freigrenze else f"Exceeds {freigrenze}€ Freigrenze - Fully Taxable"
    log_event("Freigrenze Check", f"Year: {year}, Total Gains: {total_gains}, Total Tax-Free Gains: {total_tax_free_gains}, Total Losses: {total_losses}, Total Fees: {
              total_fees}, Net Taxable Gains Before Fees: {net_taxable_gains_before_fees}, Net Taxable Gains: {net_taxable_gains}, Taxable Gains: {taxable_gains}, Note: {freigrenze_note}")

    summary_start = len(tax_data) + 2
    summary_rows = [["", "", "", "", "", "", "", "", "", "", "", "", ""]]
    for asset in summaries:
        summary_rows.extend([
            [summary_start, "SUMMARY", f"{asset} Taxable Gains {
                year}", summaries[asset]["gains"], "", "", "", "", "", "", "", "Yes", "SO - Line 7", "To be reported"],
            [summary_start + 1, "SUMMARY", f"{asset} Tax-Free Gains", summaries[asset]
                ["tax_free_gains"], "", "", "", "", "", "", "", "No", "N/A", "Held > 1 year"],
            [summary_start + 2, "SUMMARY", f"{asset} Losses {year}", summaries[asset]["losses"],
                "", "", "", "", "", "", "", "Yes", "SO - Line 24", "Losses to carry forward"]
        ])
        summary_start += 3

    summary_rows.extend([
        ["", "", "", "", "", "", "", "", "", "", "", "", ""],
        [summary_start, "TOTAL
