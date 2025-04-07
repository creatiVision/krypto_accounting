#!/usr/bin/env python3
"""
Comprehensive script to fix all syntax errors in krypto-accounting_german_tax.py
and create a working version of the German crypto tax calculator.
"""

from pathlib import Path
import os
import shutil

def fix_all_syntax_errors():
    """Fix all syntax errors in the tax calculation module."""
    # First, make a backup
    src_file = Path(__file__).parent / "krypto-accounting_german_tax.py"
    backup_file = src_file.with_suffix(".py.bak")
    
    if not backup_file.exists():
        shutil.copy2(src_file, backup_file)
        print(f"Created backup at {backup_file}")
    
    # Create fixed version from scratch using known good content
    with open("krypto-accounting_german_tax.py", "w", encoding="utf-8") as f:
        f.write("""#!/usr/bin/env python3
\"\"\"
German Crypto Tax Reporter
=========================

This module calculates taxable gains and losses from cryptocurrency transactions
according to German tax law (§23 EStG).
\"\"\"

import csv
import json
import os
import sys
import time
from datetime import date, datetime, timezone, timedelta
from enum import Enum
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any

# --- Constants ---
VERSION = "1.0.1"
HOLDING_PERIOD_DAYS = 365  # One year holding period for tax-free status
FREIGRENZE_UNTIL_2023 = 600.0  # Tax exemption limit until 2023
FREIGRENZE_2024_ONWARDS = 1000.0  # New tax exemption limit from 2024 onwards

# Global data structures
HOLDINGS = {}  # Tracks cryptocurrency holdings for FIFO calculation
LOG_DATA = []  # For activity logging
CONFIG = {}    # Configuration data

# Tax category descriptions
TAX_CATEGORIES = {
    "private_sale": "Privates Veräußerungsgeschäft (§23 EStG)",
    "mining": "Mining-Einkünfte (besondere steuerliche Behandlung)",
    "staking": "Staking-Reward (besondere steuerliche Behandlung)",
    "lending": "Lending-Reward (besondere steuerliche Behandlung)"
}

# --- Define log_event before it is used ---
def log_event(event: str, details: str) -> None:
    \"\"\"Log an event with a timestamp for debugging purposes.\"\"\"
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    LOG_DATA.append([timestamp, event, details])

# --- Import required packages directly ---
try:
    import requests
    from google.oauth2 import service_account
    from googleapiclient.discovery import build
except ImportError as e:
    log_event("Import Error", f"Failed to import required packages: {str(e)}")
    print(f"Error: {e}")
    print("Required Python packages not found. Please run 'pip install requests google-auth google-auth-oauthlib google-auth-httplib2 google-api-python-client'")
    sys.exit(1)

# --- Helper functions ---
def load_config() -> Dict:
    \"\"\"Load configuration from config.json\"\"\"
    config_path = Path(__file__).parent / "config.json"
    try:
        with open(config_path, "r", encoding="utf-8") as f:
            config = json.load(f)
        log_event("Config", "Successfully loaded configuration")
        return config
    except Exception as e:
        log_event("Config Error", f"Failed to load configuration: {str(e)}")
        print(f"Error loading configuration: {e}")
        return {}

def get_market_price(asset: str, timestamp: int) -> float:
    \"\"\"Get market price for an asset at a specific timestamp.\"\"\"
    # In a full implementation, this would query a price API
    # For now, return a dummy price of 1.0
    return 1.0

def determine_tax_category(transaction_type: str, asset: str) -> str:
    \"\"\"Determine the tax category based on transaction type and asset.\"\"\"
    # Default to private sale
    if transaction_type.lower() in ["buy", "sell"]:
        return TAX_CATEGORIES["private_sale"]
    elif transaction_type.lower() == "mining":
        return TAX_CATEGORIES["mining"]
    elif transaction_type.lower() == "staking":
        return TAX_CATEGORIES["staking"]
    elif transaction_type.lower() == "lending":
        return TAX_CATEGORIES["lending"]
    else:
        return TAX_CATEGORIES["private_sale"]

def export_detailed_fifo_documentation(year: int) -> str:
    \"\"\"
    Export detailed FIFO calculations to a separate file for tax authority review.
    Returns the path to the exported file.
    \"\"\"
    output_directory = Path(__file__).parent / "export"
    output_directory.mkdir(exist_ok=True)
    
    output_file = output_directory / f"fifo_nachweis_{year}.txt"
    
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(f"FIFO Nachweis für Steuerjahr {year}\\n")
        f.write("=" * 80 + "\\n\\n")
        
        f.write("Gemäß BMF-Schreiben zur steuerlichen Behandlung von Kryptowährungen\\n")
        f.write("werden die Coins nach dem FIFO-Prinzip (First In - First Out) behandelt.\\n\\n")
        
        f.write("Übersicht der Coin-Bestände und Verkäufe:\\n")
        f.write("-" * 80 + "\\n")
    
    log_event("FIFO Documentation", f"Exported detailed FIFO documentation for {year} to {output_file}")
    return str(output_file)

def export_to_google_sheets(data: List[List], year: int) -> str:
    \"\"\"Export data to Google Sheets.\"\"\"
    config = CONFIG.get("google_sheets", {})
    credentials_file = config.get("credentials_file")
    spreadsheet_id = config.get("spreadsheet_id")
    
    if not credentials_file or not spreadsheet_id:
        log_event("Google Sheets", "Google Sheets export skipped - missing configuration")
        return "Google Sheets export skipped - missing configuration"
    
    try:
        credentials_path = Path(__file__).parent / credentials_file
        credentials = service_account.Credentials.from_service_account_file(
            credentials_path,
            scopes=["https://www.googleapis.com/auth/spreadsheets"]
        )
        
        service = build("sheets", "v4", credentials=credentials)
        sheet_name = f"Steuer-{year}"
        
        # Check if the sheet exists
        sheet_metadata = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
        sheets = sheet_metadata.get('sheets', '')
        sheet_exists = False
        
        for sheet in sheets:
            if sheet.get("properties", {}).get("title") == sheet_name:
                sheet_exists = True
                break
        
        # Create sheet if it doesn't exist
        if not sheet_exists:
            request_body = {
                'requests': [{
                    'addSheet': {
                        'properties': {
                            'title': sheet_name,
                            'gridProperties': {"rowCount": 2000, "columnCount": len(HEADERS)}
                        }
                    }
                }]
            }
            service.spreadsheets().batchUpdate(
                spreadsheetId=spreadsheet_id,
                body=request_body
            ).execute()
            log_event("Google Sheets", f"Created new sheet '{sheet_name}'")
        
        # Update the sheet with the data
        data_rows = [HEADERS] + data[1:]  # Use headers as first row
        range_name = f"{sheet_name}!A1:Z{len(data_rows)}"
        
        service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range=range_name,
            valueInputOption="USER_ENTERED",
            body={"values": data_rows}
        ).execute()
        
        # Apply formatting
        # Reset all formatting first
        requests = []
        requests.append({
            "updateCells": {
                "range": {
                    "sheetId": next(
                        sheet.get("properties", {}).get("sheetId")
                        for sheet in sheets
                        if sheet.get("properties", {}).get("title") == sheet_name
                    ),
                    "startRowIndex": 0,
                    "endRowIndex": len(data_rows),
                    "startColumnIndex": 0,
                    "endColumnIndex": len(HEADERS)
                },
                "fields": "userEnteredFormat"
            }
        })
        
        # Format header row
        requests.append({
            "repeatCell": {
                "range": {
                    "sheetId": next(
                        sheet.get("properties", {}).get("sheetId")
                        for sheet in sheets
                        if sheet.get("properties", {}).get("title") == sheet_name
                    ),
                    "startRowIndex": 0,
                    "endRowIndex": 1
                },
                "cell": {
                    "userEnteredFormat": {
                        "backgroundColor": {"red": 0.7, "green": 0.7, "blue": 0.7},
                        "horizontalAlignment": "CENTER",
                        "textFormat": {"bold": True}
                    }
                },
                "fields": "userEnteredFormat(backgroundColor,textFormat,horizontalAlignment)"
            }
        })
        
        # Format taxable vs non-taxable rows
        for i, row in enumerate(data[1:], start=1):
            is_taxable = row[15] if len(row) > 15 else False
            
            if is_taxable and is_taxable.lower() == "ja":
                # Taxable rows - light red background
                requests.append({
                    "repeatCell": {
                        "range": {
                            "sheetId": next(
                                sheet.get("properties", {}).get("sheetId")
                                for sheet in sheets
                                if sheet.get("properties", {}).get("title") == sheet_name
                            ),
                            "startRowIndex": i,
                            "endRowIndex": i + 1
                        },
                        "cell": {
                            "userEnteredFormat": {
                                "backgroundColor": {"red": 1.0, "green": 0.9, "blue": 0.9}
                            }
                        },
                        "fields": "userEnteredFormat(backgroundColor)"
                    }
                })
            elif is_taxable and is_taxable.lower() == "nein":
                # Non-taxable rows - light green background
                requests.append({
                    "repeatCell": {
                        "range": {
                            "sheetId": next(
                                sheet.get("properties", {}).get("sheetId")
                                for sheet in sheets
                                if sheet.get("properties", {}).get("title") == sheet_name
                            ),
                            "startRowIndex": i,
                            "endRowIndex": i + 1
                        },
                        "cell": {
                            "userEnteredFormat": {
                                "backgroundColor": {"red": 0.9, "green": 1.0, "blue": 0.9}
                            }
                        },
                        "fields": "userEnteredFormat(backgroundColor)"
                    }
                })
        
        # Execute all formatting requests
        service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={"requests": requests}
        ).execute()
        
        url = f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/edit#gid=0"
        log_event("Google Sheets", f"Data exported successfully to {url}")
        return url
        
    except Exception as e:
        log_event("Google Sheets Error", f"Failed to export to Google Sheets: {str(e)}")
        return f"Error exporting to Google Sheets: {str(e)}"

def export_to_csv(data: List[List], year: int) -> str:
    \"\"\"Export data to CSV file.\"\"\"
    output_directory = Path(__file__).parent / "export"
    output_directory.mkdir(exist_ok=True)
    
    output_file = output_directory / f"krypto_steuer_{year}.csv"
    
    try:
        with open(output_file, "w", newline="", encoding="utf-8") as csvfile:
            writer = csv.writer(csvfile, delimiter=";", quotechar='"')
            for row in data:
                writer.writerow(row)
        
        log_event("CSV Export", f"Data exported successfully to {output_file}")
        return str(output_file)
    except Exception as e:
        log_event("CSV Export Error", f"Failed to export to CSV: {str(e)}")
        return f"Error exporting to CSV: {str(e)}"

def generate_summary(data: List[List], year: int) -> str:
    \"\"\"Generate a summary of the tax report.\"\"\"
    try:
        # Skip header row
        data_rows = data[1:]
        
        # Filter for sell transactions
        sell_rows = [row for row in data_rows if row[1] == "Verkauf"]
        
        total_gains = sum(float(row[13]) for row in sell_rows if row[13])
        taxable_gains = sum(float(row[13]) for row in sell_rows if row[16] == "Ja" and row[13])
        
        # Determine tax status
        current_year_freigrenze = FREIGRENZE_2024_ONWARDS if year >= 2024 else FREIGRENZE_UNTIL_2023
        is_taxable = taxable_gains > current_year_freigrenze
        
        summary = f"Zusammenfassung für Steuerjahr {year}\\n"
        summary += "=" * 50 + "\\n"
        summary += f"Gesamtgewinne: {total_gains:.2f} EUR\\n"
        summary += f"Davon steuerpflichtig: {taxable_gains:.2f} EUR\\n"
        
        if is_taxable:
            summary += f"Freigrenze ({current_year_freigrenze} EUR) überschritten - voller Betrag ist steuerpflichtig\\n"
        else:
            summary += f"Unterhalb der Freigrenze ({current_year_freigrenze} EUR) - keine Steuerpflicht\\n"
        
        log_event("Summary", f"Generated summary for {year}")
        return summary
    except Exception as e:
        log_event("Summary Error", f"Failed to generate summary: {str(e)}")
        return f"Error generating summary: {str(e)}"

# Define the HEADERS for the report
HEADERS = [
    "Zeile", "Typ", "Steuer-Kategorie", "Transaktionsdatum", "Asset", "Anzahl", 
    "Kaufdatum", "Kaufpreis (€)/Stk", "Verkaufsdatum", "Verkaufspreis (€)/Stk", 
    "Kosten (€)", "Erlös (€)", "Gebühr (€)", "Gewinn / Verlust (€)", "Haltedauer (Tage)", 
    "Haltedauer > 1 Jahr", "Steuerpflichtig", "Steuergrund", "FIFO-Details", "Notizen"
]

def process_for_tax(year: int, data: Dict[str, Any] = None) -> List[List]:
    \"\"\"Process transaction data for tax reporting.\"\"\"
    if data is None:
        data = {}
    
    tax_data = [HEADERS]
    line_num = 1
    processed_refids = set()  # To avoid duplicate entries
    
    config = CONFIG.get("exchanges", {})
    
    # Debug information
    log_event("Processing", f"Starting tax processing for year {year}")
    log_event("Config", f"Loaded {len(config)} exchange configurations")
    
    for exchange_name, exchange_config in config.items():
        log_event("Exchange", f"Processing {exchange_name}")
        
        # Load transaction data from API or file
        transaction_data = []
        if "file" in exchange_config:
            file_path = Path(__file__).parent / exchange_config["file"]
            try:
                with open(file_path, "r", encoding="utf-8") as f:
                    if file_path.suffix.lower() == ".json":
                        transaction_data = json.load(f)
                    elif file_path.suffix.lower() == ".csv":
                        # Process CSV according to exchange format
                        reader = csv.DictReader(f)
                        transaction_data = list(reader)
                    else:
                        log_event("Error", f"Unsupported file format: {file_path.suffix}")
                log_event("Data Load", f"Loaded {len(transaction_data)} transactions from {file_path}")
            except Exception as e:
                log_event("Error", f"Failed to load data for {exchange_name}: {str(e)}")
                continue
        
        # Process transactions
        for tx in transaction_data:
            # Extract transaction data
            timestamp = tx.get("time", 0)
            tx_datetime = datetime.fromtimestamp(timestamp, timezone.utc)
            tx_year = tx_datetime.year
            
            # Skip if not in requested year
            if tx_year != year:
                continue
            
            # Get basic transaction info
            refid = tx.get("refid", "")
            
            # Skip if already processed
            if refid in processed_refids:
                continue
            
            # Process by transaction type
            date_str = tx_datetime.strftime("%Y-%m-%d")
            row_base = [""] * len(HEADERS)
            row_base[0] = str(line_num)
            
            # Get transaction details
            trade_data = tx
            type_ = trade_data.get("type", "")
            amount = float(trade_data.get("amount", 0))
            fee = float(trade_data.get("fee", 0))
            price = float(trade_data.get("price", 0)) if trade_data.get("price") else 1.0
            
            # Determine base and quote asset from pair
            base_asset = trade_data.get("pair", "").split("/")[0] if "/" in trade_data.get("pair", "") else trade_data.get("asset", "")
            quote_asset = trade_data.get("pair", "").split("/")[1] if "/" in trade_data.get("pair", "") else "EUR"
            
            # For simplicity, handle only buy and sell transactions here
            # Buy trades add to holdings
            if type_.lower() == "buy":
                # Add to our holdings for FIFO tracking
                HOLDINGS.setdefault(base_asset, []).append({
                    "amount": abs(amount),
                    "price_eur": price if quote_asset == "EUR" else get_market_price(base_asset, timestamp),
                    "timestamp": timestamp,
                    "refid": refid,
                    "year": year
                })
                
                row_base[1] = "Kauf"
                row_base[2] = determine_tax_category("buy", base_asset)
                row_base[3] = date_str
                row_base[4] = base_asset
                row_base[5] = abs(amount)
                row_base[6] = date_str  # Purchase date
                row_base[7] = price if quote_asset == "EUR" else get_market_price(base_asset, timestamp)
                row_base[8] = "N/A"
                row_base[9] = 0.0
                row_base[10] = abs(amount) * (price if quote_asset == "EUR" else get_market_price(base_asset, timestamp))  # Cost in EUR
                row_base[11] = 0.0
                row_base[12] = fee
                row_base[13] = 0.0  # No gain/loss for purchases
                row_base[14] = 0  # No holding period for purchases
                row_base[15] = "Nein"  # Not taxable
                row_base[16] = "N/A"  # Not applicable for purchases
                row_base[17] = f"Kauf von {base_asset}, Ref: {refid}"
                
                tax_data.append(row_base)
                line_num += 1
                processed_refids.add(refid)
                
            # Sell trades - calculate gain or loss using FIFO
            elif type_.lower() == "sell":
                # For sell transactions, we need to determine cost basis using FIFO
                # and calculate the gain or loss
                
                sell_amount = abs(amount)
                sell_price_eur = price if quote_asset == "EUR" else get_market_price(base_asset, timestamp)
                sell_proceeds = sell_amount * sell_price_eur
                
                # Check if we have holdings for this asset
                if base_asset not in HOLDINGS or not HOLDINGS[base_asset]:
                    row_base[1] = "Verkauf"
                    row_base[2] = determine_tax_category("sell", base_asset)
                    row_base[3] = date_str
                    row_base[4] = base_asset
                    row_base[5] = sell_amount
                    row_base[8] = date_str  # Sale date
                    row_base[9] = sell_price_eur
                    row_base[11] = sell_proceeds
                    row_base[12] = fee
                    row_base[15] = "Unbekannt"  # Cannot determine tax status
                    row_base[16] = "Fehler: Keine Kaufdaten verfügbar"
                    row_base[17] = f"WARNUNG: Keine Kaufdaten für {base_asset} gefunden"
                    
                    tax_data.append(row_base)
                    line_num += 1
                    processed_refids.add(refid)
                    continue
                
                # Sort holdings by acquisition date (FIFO)
                HOLDINGS[base_asset].sort(key=lambda x: x["timestamp"])
                
                total_cost = 0.0
                remaining_to_sell = sell_amount
                matched_lots = []
                lots_to_remove = []
                
                # Match sales against purchases using FIFO
                for idx, lot in enumerate(HOLDINGS[base_asset]):
                    if remaining_to_sell <= 0:
                        break
                    
                    lot_amount = lot["amount"]
                    
                    if lot_amount <= remaining_to_sell:
                        # Use the entire lot
                        matched_amount = lot_amount
                        remaining_to_sell -= lot_amount
                        lots_to_remove.append(idx)
                    else:
                        # Use partial lot
                        matched_amount = remaining_to_sell
                        HOLDINGS[base_asset][idx]["amount"] -= matched_amount
                        remaining_to_sell = 0
                    
                    # Calculate cost basis for this portion
                    lot_cost = matched_amount * lot["price_eur"]
                    total_cost += lot_cost
                    
                    # Calculate holding period
                    holding_days = (tx_datetime - datetime.fromtimestamp(lot["timestamp"], timezone.utc)).days
                    
                    # Record the details for tax reporting
                    matched_lots.append({
                        "amount": matched_amount,
                        "purchase_date": datetime.fromtimestamp(lot["timestamp"], timezone.utc).strftime("%Y-%m-%d"),
                        "purchase_price": lot["price_eur"],
                        "holding_period": holding_days,
                        "cost_basis": lot_cost,
                        "refid": lot["refid"]
                    })
                
                # Remove fully used lots
                for idx in sorted(lots_to_remove, reverse=True):
                    del HOLDINGS[base_asset][idx]
                
                # Check if sell amount was fully matched
                if remaining_to_sell > 0:
                    # Not enough holdings found
                    log_event("FIFO Warning", f"Not enough holdings found for {base_asset}, remaining: {remaining_to_sell}")
                
                # Calculate gain or loss
                gain_loss = sell_proceeds - total_cost - fee
                
                # Determine if the sale is taxable (all lots held less than 1 year)
                is_taxable = all(lot["holding_period"] <= HOLDING_PERIOD_DAYS for lot in matched_lots)
                
                # Create row for the sale
                row_base[1] = "Verkauf"
                row_base[2] = determine_tax_category("sell", base_asset)
                row_base[3] = date_str
                row_base[4] = base_asset
                row_base[5] = sell_amount
                
                # Use the earliest purchase date for FIFO
                earliest_purchase = min([lot["purchase_date"] for lot in matched_lots]) if matched_lots else "Unknown"
                row_base[6] = earliest_purchase
                
                # Average purchase price
                avg_purchase_price = total_cost / sell_amount if sell_amount > 0 else 0
                row_base[7] = avg_purchase_price
                
                row_base[8] = date_str  # Sale date
                row_base[9] = sell_price_eur
                row_base[10] = total_cost
                row_base[11] = sell_proceeds
                row_base[12] = fee
                row_base[13] = gain_loss
                
                # Average holding period
                avg_holding_period = int(sum(lot["holding_period"] for lot in matched_lots) / len(matched_lots)) if matched_lots else 0
                row_base[14] = avg_holding_period
                
                row_base[15] = "Ja" if is_taxable and gain_loss > 0 else "Nein"
                
                # Explanation in the notes field
                if is_taxable:
                    if gain_loss > 0:
                        row_base[16] = "Haltedauer ≤ 1 Jahr, steuerpflichtig"
                    else:
                        row_base[16] = "Verlust, mit anderen Gewinnen verrechenbar"
                else:
                    row_base[16] = "Haltedauer > 1 Jahr, steuerfrei"
                
                # FIFO documentation
                fifo_details = []
                for i, lot in enumerate(matched_lots):
                    fifo_details.append(
                        f"Lot {i+1}: {lot['amount']:.8f} {base_asset} gekauft am {lot['purchase_date']} "
                        f"für {lot['purchase_price']:.2f} €/Stk (Haltedauer: {lot['holding_period']} Tage)"
                    )
                
                row_base[17] = " | ".join(fifo_details)
                
                tax_data.append(row_base)
                line_num += 1
                processed_refids.add(refid)
            
            # Other transaction types could be handled here
    
    return tax_data

def generate_tax_report(year: int) -> Dict[str, str]:
    \"\"\"Generate tax report for the given year.\"\"\"
    results = {}
    
    try:
        # Process transactions for tax reporting
        tax_data = process_for_tax(year)
        
        # Generate FIFO documentation
        fifo_doc_path = export_detailed_fifo_documentation(year)
        results["fifo_documentation"] = fifo_doc_path
        
        # Export to CSV
        csv_path = export_to_csv(tax_data, year)
        results["csv_export"] = csv_path
        
        # Export to Google Sheets if configured
        if CONFIG.get("google_sheets", {}).get("credentials_file"):
            sheets_url = export_to_google_sheets(tax_data, year)
            results["google_sheets"] = sheets_url
        
        # Generate summary
        summary = generate_summary(tax_data, year)
        results["summary"] = summary
        
        # Return all results
        return results
    
    except Exception as e:
        log_event("Error", f"Failed to generate tax report: {str(e)}")
        return {"error": str(e)}

# Main function to generate tax report
def main(year: int = None) -> None:
    \"\"\"Main function to generate tax report.\"\"\"
    print("=" * 80)
    print("Crypto Tax Reporter for German Tax Compliance")
    print("=" * 80)
    
    # Load configuration
    global CONFIG
    CONFIG = load_config()
    
    # Get tax year from arguments if provided, otherwise prompt
    if year is None:
        current_year = datetime.now().year
        year_input = input(f"Enter tax year to generate report for (e.g., {current_year-1}): ")
        try:
            year = int(year_input)
        except ValueError:
            print(f"Invalid year: {year_input}. Using current year instead.")
            year = current_year
    
    # Generate tax report
    report_results = generate_tax_report(year)
    
    # Display results
    if "error" in report_results:
        print(f"Error generating tax report: {report_results['error']}")
    else:
        print(f"\\nTax report for {year} generated successfully!")
        print(f"FIFO documentation: {report_results['fifo_documentation']}")
        print(f"CSV export: {report_results['csv_export']}")
        
# Add this if you want to run the script directly
if __name__ == "__main__":
    main()
""")
    
    print("Successfully created a fixed version of krypto-accounting_german_tax.py")
    return True

if __name__ == "__main__":
    fix_all_syntax_errors()
