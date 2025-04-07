#!/usr/bin/env python3
"""
Simple diagnostic tool to check for 2024 sales in reports.
This version has minimal dependencies to avoid installation issues.
"""

import os
import json
import sqlite3
from datetime import datetime
from pathlib import Path

# Paths
DATA_DIR = os.path.join(os.path.dirname(__file__), 'data')
DB_PATH = os.path.join(DATA_DIR, 'kraken_cache.db')
EXPORT_DIR = os.path.join(os.path.dirname(__file__), 'export')

def simple_log(message):
    """Simple logging function that prints to console."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {message}")

def find_2024_sales():
    """Find sales transactions from 2024 in the database."""
    simple_log("Searching for 2024 sales in the database")
    
    if not os.path.exists(DB_PATH):
        simple_log(f"ERROR: Database not found at {DB_PATH}")
        return []
    
    try:
        # Connect to the database
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        # Time range for 2024
        year_start = int(datetime(2024, 1, 1).timestamp())
        year_end = int(datetime(2024, 12, 31, 23, 59, 59).timestamp())
        
        trades_2024 = []
        
        # Search for trades in 2024
        cursor.execute(
            "SELECT data_json FROM trades WHERE timestamp BETWEEN ? AND ?",
            (year_start, year_end)
        )
        rows = cursor.fetchall()
        
        for row in rows:
            try:
                trade_data = json.loads(row[0])
                # Check if this is a SELL trade
                if ('type' in trade_data and trade_data['type'] == 'sell') or \
                   ('type' in trade_data and 'sell' in trade_data['type'].lower()) or \
                   ('posstatus' in trade_data and trade_data['posstatus'] == 'closed'):
                    trades_2024.append(trade_data)
            except json.JSONDecodeError:
                continue
        
        # Search ledger for sells
        cursor.execute(
            "SELECT data_json FROM ledger WHERE timestamp BETWEEN ? AND ?",
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
                
        conn.close()
        
        # Display information about found trades
        simple_log(f"Found {len(trades_2024)} potential sales in 2024")
        
        for i, trade in enumerate(trades_2024, 1):
            # Convert Unix timestamp to readable date
            timestamp = int(float(trade.get('time', 0)))
            trade_time = datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')
            
            asset_pair = trade.get('pair', trade.get('asset', 'Unknown'))
            volume = trade.get('vol', trade.get('amount', 'Unknown'))
            price = trade.get('price', 'Unknown')
            
            print(f"{i}. [2024 Sale] {trade_time}: {asset_pair} - Volume: {volume}, Price: {price}")
            print(f"   Reference ID: {trade.get('refid', 'Unknown')}")
            print("---")
        
        return trades_2024
        
    except Exception as e:
        simple_log(f"ERROR: Failed to search for 2024 sales: {str(e)}")
        import traceback
        traceback.print_exc()
        return []

def check_report_files():
    """Check if sales are in the tax report files."""
    simple_log("Checking tax report files for 2024 sales")
    
    # Files to check
    fifo_file = os.path.join(EXPORT_DIR, 'fifo_nachweis_2024.txt')
    csv_file = os.path.join(EXPORT_DIR, '2024.csv')
    
    # Check existence
    files_exist = True
    if not os.path.exists(fifo_file):
        simple_log(f"ERROR: FIFO report file not found: {fifo_file}")
        files_exist = False
    
    if not os.path.exists(csv_file):
        simple_log(f"ERROR: CSV report file not found: {csv_file}")
        files_exist = False
    
    if not files_exist:
        return False
    
    # Check content for sales entries
    fifo_has_sales = False
    with open(fifo_file, 'r') as f:
        content = f.read()
        if 'Veräußerung #' in content:
            fifo_has_sales = True
            simple_log("FIFO report contains sales entries")
        else:
            simple_log("FIFO report does NOT contain any sales entries")
    
    csv_has_sales = False
    with open(csv_file, 'r') as f:
        for line in f:
            if line.strip() and not line.startswith('Zeile;') and not line.startswith('---') and \
               not line.startswith('Steuerjahr') and not line.startswith('Gesamtgewinne'):
                csv_has_sales = True
                simple_log("CSV report contains sales entries")
                break
    
    if not csv_has_sales:
        simple_log("CSV report does NOT contain any sales entries")
    
    return fifo_has_sales and csv_has_sales

def fix_reports(sales_data):
    """Create indicator file for fixing issues."""
    # Save the sales data for reference
    with open(os.path.join(EXPORT_DIR, '2024_sales_data.json'), 'w') as f:
        json.dump(sales_data, f, indent=2)
    
    # Create flag file
    with open(os.path.join(EXPORT_DIR, '.missing_2024_sales'), 'w') as f:
        f.write(f"There are {len(sales_data)} sales from 2024 missing from reports.\n")
        f.write("This file was created to flag the missing 2024 sales issue.\n")
        f.write("You can run fix_issues.py to attempt to resolve this issue.\n")
    
    simple_log(f"Created flag file for {len(sales_data)} missing 2024 sales")
    simple_log("Sales data saved to export/2024_sales_data.json")

def main():
    """Main function."""
    print("\n===== 2024 Sales Verification Tool =====\n")
    
    # Find sales in database
    sales_data = find_2024_sales()
    
    if not sales_data:
        print("\nNo sales from 2024 found in the database.")
        return
    
    # Check report files
    print("\nChecking if 2024 sales appear in tax reports...")
    reports_have_sales = check_report_files()
    
    if reports_have_sales:
        print("\nVERIFIED: 2024 sales are properly included in tax reports.")
    else:
        print("\nWARNING: 2024 sales are missing from tax reports!")
        print(f"Found {len(sales_data)} sales from 2024, but they are not in reports.")
        
        # Create files for issue fixing
        fix_reports(sales_data)
    
    print("\nVerification complete.")

if __name__ == "__main__":
    main()
