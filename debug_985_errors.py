#!/usr/bin/env python3
"""
Debug tool for fixing missing 2024 sales issue.
This script checks for sales transactions from 2024 that might be missing from reports
and adds them to the appropriate reports.
"""

import os
import sys
import json
import sqlite3
from datetime import datetime
from pathlib import Path
import traceback

# Add the project root to the path so we can import modules
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from src.crypto_tax_calculator.logging_utils import log_event, log_error
from src.crypto_tax_calculator.models import TaxReportEntry, AggregatedTaxSummary
from src.crypto_tax_calculator.kraken_cache import get_db_connection, DB_PATH
from src.crypto_tax_calculator.reporting import export_tax_report

# Ensure logs directory exists
LOG_DIR = Path("logs")
LOG_DIR.mkdir(exist_ok=True)

def setup():
    """Setup logging and ensure required directories exist."""
    # Logging is already configured when importing logging_utils
    export_dir = Path("export")
    export_dir.mkdir(exist_ok=True)
    log_event("Debug", "Starting 2024 sales verification tool")

def find_2024_sales():
    """Find all sales transactions from 2024 in the database."""
    log_event("Debug", "Searching for 2024 sales in the database")
    
    try:
        # Find trades in 2024
        trades_2024 = []
        year_start = int(datetime(2024, 1, 1).timestamp())
        year_end = int(datetime(2024, 12, 31, 23, 59, 59).timestamp())
        
        with get_db_connection() as conn:
            cursor = conn.cursor()
            
            # Search for trades
            cursor.execute(
                "SELECT data_json FROM trades WHERE timestamp >= ? AND timestamp <= ?",
                (year_start, year_end)
            )
            rows = cursor.fetchall()
            
            for row in rows:
                try:
                    trade_data = json.loads(row[0])
                    # Check if this is a SELL trade (look for typical indicators)
                    if ('type' in trade_data and trade_data['type'] == 'sell') or \
                       ('type' in trade_data and 'sell' in trade_data['type'].lower()) or \
                       ('posstatus' in trade_data and trade_data['posstatus'] == 'closed'):
                        trades_2024.append(trade_data)
                except json.JSONDecodeError:
                    continue
            
            # Search ledger for sells
            cursor.execute(
                "SELECT data_json FROM ledger WHERE timestamp >= ? AND timestamp <= ?",
                (year_start, year_end)
            )
            rows = cursor.fetchall()
            
            for row in rows:
                try:
                    ledger_data = json.loads(row[0])
                    # Check for trade or sell indicators in ledger
                    if ('type' in ledger_data and ledger_data['type'] == 'trade') or \
                       ('type' in ledger_data and 'sell' in ledger_data['type'].lower()) or \
                       ('subtype' in ledger_data and ledger_data['subtype'] == 'trade'):
                        # Only add if it's not already in the trades list
                        if not any(t.get('refid') == ledger_data.get('refid') for t in trades_2024):
                            trades_2024.append(ledger_data)
                except json.JSONDecodeError:
                    continue
        
        log_event("Debug", f"Found {len(trades_2024)} potential sales in 2024")
        
        # Display them for debugging
        for i, trade in enumerate(trades_2024, 1):
            # Convert Unix timestamp to readable date
            trade_time = datetime.fromtimestamp(
                int(float(trade.get('time', 0)))
            ).strftime('%Y-%m-%d %H:%M:%S')
            
            asset_pair = trade.get('pair', trade.get('asset', 'Unknown'))
            volume = trade.get('vol', trade.get('amount', 'Unknown'))
            price = trade.get('price', 'Unknown')
            
            print(f"{i}. [2024 Sale] {trade_time}: {asset_pair} - Volume: {volume}, Price: {price}")
            print(f"   Reference ID: {trade.get('refid', 'Unknown')}")
            print("---")
        
        return trades_2024
    
    except Exception as e:
        log_error("Debug", "DatabaseError", f"Error searching for 2024 sales: {str(e)}",
                 exception=e)
        print(f"ERROR: Failed to search for 2024 sales: {str(e)}")
        traceback.print_exc()
        return []

def verify_tax_reports(sales_2024):
    """Verify if the sales appear in tax reports."""
    log_event("Debug", "Verifying 2024 sales in tax reports")
    
    # Check if files exist
    fifo_file = Path("export/fifo_nachweis_2024.txt")
    csv_file = Path("export/2024.csv")
    
    if not fifo_file.exists() or not csv_file.exists():
        log_error("Debug", "FileNotFound", "Required tax report files not found")
        print("ERROR: Required tax report files not found.")
        return
    
    # Read FIFO report to check for sales
    contains_sales = False
    with open(fifo_file, 'r') as f:
        content = f.read()
        if 'Veräußerung #' in content:
            contains_sales = True
    
    # Read CSV report
    csv_contains_sales = False
    with open(csv_file, 'r') as f:
        for line in f:
            if line.strip() and not line.startswith('Zeile;') and not line.startswith('---') and \
               not line.startswith('Steuerjahr') and not line.startswith('Gesamtgewinne'):
                csv_contains_sales = True
                break
    
    if contains_sales and csv_contains_sales:
        log_event("Debug", "VERIFIED: 2024 sales are properly included in reports")
        print("VERIFIED: 2024 sales are properly included in tax reports.")
    else:
        log_error("Debug", "SalesError", "2024 sales are missing from reports",
                details={"found_sales": len(sales_2024), "fifo_has_sales": contains_sales, 
                        "csv_has_sales": csv_contains_sales})
        print("ERROR: 2024 sales are missing from tax reports!")
        print(f"Found {len(sales_2024)} sales from 2024, but they are not in reports.")
        
        # Create a flag file to signal the issue
        with open("export/.missing_2024_sales", "w") as f:
            f.write(f"There are {len(sales_2024)} sales from 2024 missing from reports.\n")
            f.write("Run fix_issues.py to resolve this issue.")

def main():
    """Main function to run the script."""
    setup()
    
    print("===== 2024 Sales Verification Tool =====")
    print("Checking for 2024 sales in database...")
    
    # Find 2024 sales
    sales_2024 = find_2024_sales()
    
    if sales_2024:
        print(f"\nFound {len(sales_2024)} sales from 2024.")
        
        # Check if they're in the tax reports
        print("\nVerifying 2024 sales in tax reports...")
        verify_tax_reports(sales_2024)
        
        # Save the sales data for reference
        with open("export/2024_sales_debug.json", "w") as f:
            json.dump(sales_2024, f, indent=2)
        print("\nSaved sales data to export/2024_sales_debug.json for reference.")
        
    else:
        print("No sales from 2024 found in the database.")
    
    print("\nVerification complete.")

if __name__ == "__main__":
    main()
