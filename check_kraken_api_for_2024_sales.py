#!/usr/bin/env python3
"""
Check Kraken API directly for 2024 sales and compare with local database.
This script ensures the database is properly synchronized with the Kraken API data.
"""

import os
import sys
import json
import time
import hmac
import base64
import urllib.parse
import hashlib
import sqlite3
import urllib.request
import urllib.error
from datetime import datetime
from pathlib import Path
import traceback

# Add the project root to the path for imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Paths
DATA_DIR = os.path.join(os.path.dirname(__file__), 'data')
DB_PATH = os.path.join(DATA_DIR, 'kraken_cache.db')
EXPORT_DIR = os.path.join(os.path.dirname(__file__), 'export')

def simple_log(message):
    """Simple logging function that prints to console with timestamp."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {message}")

def get_kraken_api_credentials():
    """Get Kraken API credentials from environment or .env file."""
    api_key = os.environ.get('KRAKEN_API_KEY')
    api_secret = os.environ.get('KRAKEN_API_SECRET')
    
    # Try loading from .env file if not in environment
    if not api_key or not api_secret:
        try:
            with open('.env', 'r') as f:
                for line in f:
                    if line.strip().startswith('KRAKEN_API_KEY='):
                        api_key = line.strip().split('=', 1)[1].strip().strip('"\'')
                    elif line.strip().startswith('KRAKEN_API_SECRET='):
                        api_secret = line.strip().split('=', 1)[1].strip().strip('"\'')
        except Exception as e:
            simple_log(f"Error reading .env file: {str(e)}")
    
    return api_key, api_secret

def get_kraken_signature(urlpath, data, secret):
    """Create Kraken API signature."""
    postdata = urllib.parse.urlencode(data)
    encoded = (str(data['nonce']) + postdata).encode()
    message = urlpath.encode() + hashlib.sha256(encoded).digest()
    
    mac = hmac.new(base64.b64decode(secret), message, hashlib.sha512)
    sigdigest = base64.b64encode(mac.digest())
    return sigdigest.decode()

def kraken_api_request(endpoint, data, api_key, api_secret):
    """Make a request to the Kraken API using only standard library."""
    api_url = "https://api.kraken.com"
    url = api_url + endpoint
    
    # Prepare headers
    headers = {
        'API-Key': api_key,
        'API-Sign': get_kraken_signature(endpoint, data, api_secret),
        'Content-Type': 'application/x-www-form-urlencoded'
    }
    
    # Encode data for POST request
    post_data = urllib.parse.urlencode(data).encode('ascii')
    
    try:
        # Create request
        req = urllib.request.Request(url, data=post_data, headers=headers, method='POST')
        
        # Execute request
        with urllib.request.urlopen(req) as response:
            # Read and decode response
            response_data = response.read().decode('utf-8')
            # Parse JSON
            result = json.loads(response_data)
            
            if 'error' in result and result['error']:
                simple_log(f"API Error: {result['error']}")
                return None
                
            return result['result']
    except urllib.error.HTTPError as e:
        simple_log(f"API Error ({e.code}): {e.read().decode('utf-8')}")
        return None
    except urllib.error.URLError as e:
        simple_log(f"Connection Error: {str(e.reason)}")
        return None
    except Exception as e:
        simple_log(f"Request Error: {str(e)}")
        return None

def get_trades_from_kraken(api_key, api_secret, start_time, end_time):
    """Get trades directly from Kraken API."""
    simple_log(f"Fetching trades from Kraken API for period {datetime.fromtimestamp(start_time).strftime('%Y-%m-%d')} to {datetime.fromtimestamp(end_time).strftime('%Y-%m-%d')}")
    
    data = {
        'nonce': str(int(time.time() * 1000)),
        'start': start_time,
        'end': end_time
    }
    
    result = kraken_api_request('/0/private/TradesHistory', data, api_key, api_secret)
    
    if not result:
        simple_log("Failed to retrieve trades from Kraken API")
        return []
        
    trades = []
    for trade_id, trade_data in result.get('trades', {}).items():
        trade_data['refid'] = trade_id  # Add reference ID
        trades.append(trade_data)
    
    simple_log(f"Retrieved {len(trades)} trades from Kraken API")
    return trades

def get_ledger_from_kraken(api_key, api_secret, start_time, end_time):
    """Get ledger entries directly from Kraken API."""
    simple_log(f"Fetching ledger from Kraken API for period {datetime.fromtimestamp(start_time).strftime('%Y-%m-%d')} to {datetime.fromtimestamp(end_time).strftime('%Y-%m-%d')}")
    
    data = {
        'nonce': str(int(time.time() * 1000)),
        'start': start_time,
        'end': end_time
    }
    
    result = kraken_api_request('/0/private/Ledgers', data, api_key, api_secret)
    
    if not result:
        simple_log("Failed to retrieve ledger from Kraken API")
        return []
        
    ledger = []
    for entry_id, entry_data in result.get('ledger', {}).items():
        entry_data['refid'] = entry_id  # Add reference ID
        ledger.append(entry_data)
    
    simple_log(f"Retrieved {len(ledger)} ledger entries from Kraken API")
    return ledger

def find_sales_from_kraken(api_key, api_secret, year=2024):
    """Find all sales from specified year directly from Kraken API."""
    # Time range for specified year
    year_start = int(datetime(year, 1, 1).timestamp())
    year_end = int(datetime(year, 12, 31, 23, 59, 59).timestamp())
    
    # Get trades and ledger entries
    trades = get_trades_from_kraken(api_key, api_secret, year_start, year_end)
    ledger = get_ledger_from_kraken(api_key, api_secret, year_start, year_end)
    
    # Filter for sales
    sales = []
    
    # Check trades
    for trade in trades:
        if ('type' in trade and trade['type'] == 'sell') or \
           ('type' in trade and 'sell' in trade['type'].lower()) or \
           ('posstatus' in trade and trade['posstatus'] == 'closed'):
            sales.append(trade)
    
    # Check ledger
    for entry in ledger:
        if ('type' in entry and entry['type'] == 'trade') or \
           ('type' in entry and 'sell' in entry['type'].lower()) or \
           ('subtype' in entry and entry['subtype'] == 'trade'):
            # Only add if it's not already in the sales list
            if not any(s.get('refid') == entry.get('refid') for s in sales):
                sales.append(entry)
    
    simple_log(f"Identified {len(sales)} sales from {year} in Kraken API data")
    return sales

def find_sales_from_database(year=2024):
    """Find all sales from specified year in the local database."""
    simple_log(f"Searching for {year} sales in local database")
    
    if not os.path.exists(DB_PATH):
        simple_log(f"ERROR: Database not found at {DB_PATH}")
        return []
    
    try:
        # Time range for specified year
        year_start = int(datetime(year, 1, 1).timestamp())
        year_end = int(datetime(year, 12, 31, 23, 59, 59).timestamp())
        
        # Connect to the database
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        sales = []
        
        # Search for trades
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
                    sales.append(trade_data)
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
                    # Only add if it's not already in the sales list
                    if not any(s.get('refid') == ledger_data.get('refid') for s in sales):
                        sales.append(ledger_data)
            except json.JSONDecodeError:
                continue
                
        conn.close()
        
        simple_log(f"Found {len(sales)} sales from {year} in local database")
        return sales
        
    except Exception as e:
        simple_log(f"ERROR: Failed to search database: {str(e)}")
        traceback.print_exc()
        return []

def compare_sales_data(api_sales, db_sales):
    """Compare sales data from API and database."""
    simple_log("Comparing API and database sales data")
    
    # Create dictionaries for comparison
    api_sales_dict = {sale.get('refid'): sale for sale in api_sales if 'refid' in sale}
    db_sales_dict = {sale.get('refid'): sale for sale in db_sales if 'refid' in sale}
    
    # Find missing sales
    missing_in_db = []
    for refid, sale in api_sales_dict.items():
        if refid not in db_sales_dict:
            missing_in_db.append(sale)
    
    # Find extra sales in DB
    extra_in_db = []
    for refid, sale in db_sales_dict.items():
        if refid not in api_sales_dict:
            extra_in_db.append(sale)
    
    # Check for differences in matching sales
    different_sales = []
    for refid in api_sales_dict:
        if refid in db_sales_dict:
            # Compare important fields
            api_sale = api_sales_dict[refid]
            db_sale = db_sales_dict[refid]
            
            # Check key fields only - complete equality is unlikely due to structure differences
            important_fields = ['time', 'type', 'asset', 'amount', 'price']
            
            for field in important_fields:
                if field in api_sale and field in db_sale and api_sale[field] != db_sale[field]:
                    different_sales.append((refid, field, api_sale[field], db_sale[field]))
    
    return {
        'missing_in_db': missing_in_db,
        'extra_in_db': extra_in_db,
        'different_sales': different_sales
    }

def update_report_if_needed(comparison_results, year=2024):
    """Save missing sales data for reporting updates."""
    if comparison_results['missing_in_db']:
        # Create output directory if it doesn't exist
        os.makedirs(EXPORT_DIR, exist_ok=True)
        
        # Save missing sales data
        missing_sales_file = os.path.join(EXPORT_DIR, f'missing_{year}_sales.json')
        with open(missing_sales_file, 'w') as f:
            json.dump(comparison_results['missing_in_db'], f, indent=2)
        
        # Create flag file
        flag_file = os.path.join(EXPORT_DIR, f'.missing_{year}_sales_api')
        with open(flag_file, 'w') as f:
            f.write(f"There are {len(comparison_results['missing_in_db'])} sales from {year} in the Kraken API that are missing from the database.\n")
            f.write("Run fix_issues.py to update the database and regenerate reports.\n")
        
        simple_log(f"Created flag file: {flag_file}")
        simple_log(f"Saved missing sales data to: {missing_sales_file}")
        
        return True
    return False

def main():
    """Main function."""
    print("\n===== Kraken API 2024 Sales Verification Tool =====\n")
    
    # Get API credentials
    api_key, api_secret = get_kraken_api_credentials()
    if not api_key or not api_secret:
        simple_log("ERROR: Kraken API credentials not found. Please set KRAKEN_API_KEY and KRAKEN_API_SECRET in environment or .env file.")
        return 1
    
    try:
        # Get sales from database
        db_sales = find_sales_from_database(year=2024)
        
        # Get sales from API
        api_sales = find_sales_from_kraken(api_key, api_secret, year=2024)
        
        # Compare results
        if not api_sales:
            simple_log("WARNING: Could not retrieve sales from Kraken API. Verification incomplete.")
            return 1
            
        if not db_sales and not api_sales:
            simple_log("No sales found in API or database for 2024.")
            return 0
        
        # Compare sales data
        comparison = compare_sales_data(api_sales, db_sales)
        
        # Print comparison results
        print("\n----- Comparison Results -----")
        
        if comparison['missing_in_db']:
            print(f"\nWARNING: Found {len(comparison['missing_in_db'])} sales in API that are MISSING from database:")
            for i, sale in enumerate(comparison['missing_in_db'], 1):
                sale_time = datetime.fromtimestamp(int(float(sale.get('time', 0)))).strftime('%Y-%m-%d %H:%M:%S')
                print(f"  {i}. [{sale_time}] {sale.get('pair', sale.get('asset', 'Unknown'))} - {sale.get('vol', sale.get('amount', 'Unknown'))}")
                print(f"     RefID: {sale.get('refid', 'Unknown')}")
        else:
            print("\nAll API sales are present in the database.")
            
        if comparison['extra_in_db']:
            print(f"\nNOTE: Found {len(comparison['extra_in_db'])} sales in database not present in API response.")
        
        if comparison['different_sales']:
            print(f"\nWARNING: Found {len(comparison['different_sales'])} sales with differing data:")
            for refid, field, api_value, db_value in comparison['different_sales']:
                print(f"  Sale {refid}: field '{field}' differs - API: {api_value}, DB: {db_value}")
        
        # Update report if needed
        if comparison['missing_in_db']:
            update_report_if_needed(comparison, year=2024)
            print("\nCreated files to flag missing sales. Run fix_issues.py to update database and reports.")
        else:
            print("\nNo action needed - database is in sync with Kraken API for 2024 sales.")
        
        return 0
    
    except Exception as e:
        simple_log(f"ERROR: {str(e)}")
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    sys.exit(main())
