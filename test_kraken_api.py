#!/usr/bin/env python3
"""
Direct Kraken API testing script for accounting_kryptos.
Tests real API calls with actual credentials to identify issues.
"""

import os
import sys
import time
from datetime import datetime, timedelta
import traceback
from decimal import Decimal
import json
from dotenv import load_dotenv

# Add the project root to the Python path
project_root = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, project_root)

# Load environment variables
load_dotenv()

def print_section(title):
    """Print a section header."""
    print("\n" + "=" * 80)
    print(f" {title} ".center(80, "="))
    print("=" * 80)

def test_kraken_request():
    """Test a basic Kraken API public request."""
    print_section("Testing Kraken Public API")
    
    try:
        from src.crypto_tax_calculator.kraken_api import kraken_request
        
        # Test a simple public request
        result = kraken_request('/0/public/Time', {}, "", "", public=True)
        
        # Print result
        if 'error' in result and result['error']:
            print(f"✗ API Error: {result['error']}")
        else:
            print("✓ Kraken Public API is accessible")
            if 'result' in result:
                server_time = datetime.fromtimestamp(result['result']['unixtime'])
                print(f"  Server time: {server_time}")
                
                # Check time drift
                local_time = datetime.now()
                drift = abs((local_time - server_time).total_seconds())
                print(f"  Time drift: {drift:.2f} seconds")
                if drift > 60:
                    print(f"⚠️  Warning: Time drift exceeds 60 seconds, this may cause nonce errors")
        
        return 'error' not in result or not result['error']
    except Exception as e:
        print(f"✗ Error testing Kraken Public API: {str(e)}")
        traceback.print_exc()
        return False

def test_kraken_auth():
    """Test Kraken API authentication."""
    print_section("Testing Kraken API Authentication")
    
    try:
        from src.crypto_tax_calculator.kraken_api import kraken_request, get_safe_nonce
        
        # Get API credentials
        api_key = os.getenv("KRAKEN_API_KEY")
        api_secret = os.getenv("KRAKEN_API_SECRET")
        
        if not api_key or not api_secret:
            print("✗ API credentials not found in environment")
            return False
        
        # Try a simple authenticated request
        nonce = get_safe_nonce()
        result = kraken_request('/0/private/Balance', 
                              {"nonce": nonce}, 
                              api_key, 
                              api_secret)
        
        # Print result
        if 'error' in result and result['error']:
            print(f"✗ API Authentication Error: {result['error']}")
            
            if any("EAPI:Invalid nonce" in str(e) for e in result['error']):
                print("  This is a nonce error. Possible causes:")
                print("  - Local system clock is not synchronized")
                print("  - Another API call is using a higher nonce value")
                print("  - The nonce generation needs improvement")
            
            if any("EAPI:Invalid key" in str(e) for e in result['error']):
                print("  This is an invalid key error. Possible causes:")
                print("  - API key is incorrect")
                print("  - API key does not have required permissions")
                print("  - API key has been revoked or disabled")
            
            return False
        else:
            print("✓ Kraken API authentication successful")
            if 'result' in result:
                print("  Account balances:")
                for asset, balance in result['result'].items():
                    if float(balance) > 0:
                        print(f"    {asset}: {balance}")
            return True
    except Exception as e:
        print(f"✗ Error testing Kraken API authentication: {str(e)}")
        traceback.print_exc()
        return False

def test_trades_api():
    """Test fetching trades from Kraken API."""
    print_section("Testing Kraken Trades API")
    
    try:
        from src.crypto_tax_calculator.kraken_api import get_trades
        
        # Get API credentials
        api_key = os.getenv("KRAKEN_API_KEY")
        api_secret = os.getenv("KRAKEN_API_SECRET")
        
        if not api_key or not api_secret:
            print("✗ API credentials not found in environment")
            return False
        
        # Get trades for a recent period (2 months)
        end_time = int(datetime.now().timestamp())
        start_time = int((datetime.now() - timedelta(days=60)).timestamp())
        
        print(f"Fetching trades from {datetime.fromtimestamp(start_time)} to {datetime.fromtimestamp(end_time)}")
        
        # Time the API call
        start = time.time()
        trades = get_trades(api_key, api_secret, start_time, end_time)
        end = time.time()
        
        if trades:
            if isinstance(trades, dict) and 'error' in trades:
                print(f"✗ API Error: {trades['error']}")
                return False
            
            print(f"✓ Successfully fetched {len(trades)} trades in {end - start:.2f} seconds")
            
            # Print sample trade
            if len(trades) > 0:
                print("\nSample trade:")
                sample = trades[0]
                for key, value in sample.items():
                    print(f"  {key}: {value}")
            
            return True
        else:
            print("✗ No trades returned (empty result)")
            return False
    except Exception as e:
        print(f"✗ Error testing Kraken Trades API: {str(e)}")
        traceback.print_exc()
        return False

def test_ledger_api():
    """Test fetching ledger entries from Kraken API."""
    print_section("Testing Kraken Ledger API")
    
    try:
        from src.crypto_tax_calculator.kraken_api import get_ledger
        
        # Get API credentials
        api_key = os.getenv("KRAKEN_API_KEY")
        api_secret = os.getenv("KRAKEN_API_SECRET")
        
        if not api_key or not api_secret:
            print("✗ API credentials not found in environment")
            return False
        
        # Get ledger entries for a recent period (2 months)
        end_time = int(datetime.now().timestamp())
        start_time = int((datetime.now() - timedelta(days=60)).timestamp())
        
        print(f"Fetching ledger entries from {datetime.fromtimestamp(start_time)} to {datetime.fromtimestamp(end_time)}")
        
        # Time the API call
        start = time.time()
        ledger = get_ledger(api_key, api_secret, start_time, end_time)
        end = time.time()
        
        if ledger:
            if isinstance(ledger, dict) and 'error' in ledger:
                print(f"✗ API Error: {ledger['error']}")
                return False
            
            print(f"✓ Successfully fetched {len(ledger)} ledger entries in {end - start:.2f} seconds")
            
            # Print sample ledger entry
            if len(ledger) > 0:
                print("\nSample ledger entry:")
                sample = ledger[0]
                for key, value in sample.items():
                    print(f"  {key}: {value}")
            
            # Check for missing or empty fields
            missing_fields = set()
            for entry in ledger:
                for field in ['refid', 'time', 'type', 'asset', 'amount', 'fee']:
                    if field not in entry or not entry[field]:
                        missing_fields.add(field)
            
            if missing_fields:
                print(f"⚠️  Warning: Some entries are missing these fields: {', '.join(missing_fields)}")
            
            return True
        else:
            print("✗ No ledger entries returned (empty result)")
            return False
    except Exception as e:
        print(f"✗ Error testing Kraken Ledger API: {str(e)}")
        traceback.print_exc()
        return False

def test_ohlc_api():
    """Test fetching OHLC data from Kraken API."""
    print_section("Testing Kraken OHLC API")
    
    try:
        from src.crypto_tax_calculator.kraken_api import get_kraken_ohlc
        
        # Test pairs
        pairs = ["XXBTZEUR", "XETHZEUR"]
        
        for pair in pairs:
            print(f"Fetching OHLC data for {pair}...")
            
            # Get daily data for the last 30 days
            ohlc_data = get_kraken_ohlc(pair, interval=1440)
            
            if ohlc_data:
                count = len(ohlc_data)
                print(f"✓ Successfully fetched {count} OHLC data points for {pair}")
                
                if count > 0:
                    # Print most recent data point
                    latest = ohlc_data[-1]
                    timestamp = datetime.fromtimestamp(latest[0])
                    open_price = float(latest[1])
                    close_price = float(latest[4])
                    
                    print(f"  Latest data point ({timestamp}):")
                    print(f"    Open: {open_price:.2f} EUR")
                    print(f"    Close: {close_price:.2f} EUR")
            else:
                print(f"✗ Failed to fetch OHLC data for {pair}")
        
        return True
    except Exception as e:
        print(f"✗ Error testing Kraken OHLC API: {str(e)}")
        traceback.print_exc()
        return False

def test_price_api():
    """Test fetching historical prices."""
    print_section("Testing Price API")
    
    try:
        from src.crypto_tax_calculator.price_api import get_historical_price_eur
        
        # Test cases
        test_cases = [
            ("BTC", datetime.now() - timedelta(days=30)),
            ("ETH", datetime.now() - timedelta(days=60)),
            ("ADA", datetime.now() - timedelta(days=90)),
        ]
        
        for asset, dt in test_cases:
            timestamp = int(dt.timestamp())
            date_str = dt.strftime("%Y-%m-%d")
            
            print(f"Fetching price for {asset} on {date_str}...")
            price = get_historical_price_eur(asset, timestamp)
            
            if price is not None:
                print(f"✓ Got price for {asset} on {date_str}: {price:.2f} EUR")
            else:
                print(f"✗ Failed to get price for {asset} on {date_str}")
        
        return True
    except Exception as e:
        print(f"✗ Error testing Price API: {str(e)}")
        traceback.print_exc()
        return False

def test_cache_functionality():
    """Test the caching functionality."""
    print_section("Testing Cache Functionality")
    
    try:
        from src.crypto_tax_calculator.kraken_cache import init_db, get_db_connection, save_entries, load_cached_entries
        
        # Initialize the database
        init_db()
        print("✓ Database initialized")
        
        # Test connection
        conn = get_db_connection()
        if conn:
            print("✓ Database connection successful")
            conn.close()
        else:
            print("✗ Failed to connect to the database")
            return False
        
        # Test retrieving cached data
        # Use a time range that should always have some data (if there is any)
        start_time = int((datetime(2020, 1, 1)).timestamp())
        end_time = int(datetime.now().timestamp())
        
        cached_trades = load_cached_entries('trades', start_time, end_time)
        cached_ledger = load_cached_entries('ledger', start_time, end_time)
        
        print(f"Found {len(cached_trades)} cached trades")
        print(f"Found {len(cached_ledger)} cached ledger entries")
        
        # Test saving some sample data
        test_entry = {
            "refid": f"test_{int(time.time())}",
            "time": str(int(time.time())),
            "type": "test",
            "asset": "TEST",
            "amount": "1.0"
        }
        
        save_entries('trades', [test_entry])
        print("✓ Saved test entry to cache")
        
        # Verify it was saved
        new_cached = load_cached_entries('trades', end_time - 3600, end_time + 3600)
        found = any(e.get('refid') == test_entry['refid'] for e in new_cached)
        
        if found:
            print("✓ Successfully retrieved saved test entry from cache")
        else:
            print("✗ Failed to retrieve saved test entry from cache")
        
        return True
    except Exception as e:
        print(f"✗ Error testing cache functionality: {str(e)}")
        traceback.print_exc()
        return False

def test_end_to_end():
    """Test end-to-end functionality."""
    print_section("Testing End-to-End Functionality")
    
    try:
        # Import required modules
        from src.crypto_tax_calculator.main import process_transactions
        from src.crypto_tax_calculator.kraken_cache import get_trades, get_ledger
        
        # Get API credentials
        api_key = os.getenv("KRAKEN_API_KEY")
        api_secret = os.getenv("KRAKEN_API_SECRET")
        
        if not api_key or not api_secret:
            print("✗ API credentials not found in environment")
            return False
        
        # Using a short test period to keep the test quick
        tax_year = 2023
        start_time = int(datetime(tax_year, 1, 1).timestamp())
        end_time = int(datetime(tax_year + 1, 1, 1).timestamp())
        
        print(f"Testing tax calculations for year {tax_year}")
        
        # Get trades and ledger entries
        print("Fetching trades and ledger entries...")
        trades = get_trades(api_key, api_secret, start_time, end_time)
        ledger = get_ledger(api_key, api_secret, start_time, end_time)
        
        print(f"Found {len(trades)} trades and {len(ledger)} ledger entries for {tax_year}")
        
        # Process transactions
        print("Processing transactions...")
        report_entries = process_transactions(trades, ledger, tax_year)
        
        print(f"Generated {len(report_entries)} tax report entries")
        
        # Print tax summary
        if report_entries:
            total_tax = sum(entry.tax_liability for entry in report_entries)
            print(f"Total tax liability: {total_tax} EUR")
            
            # Print some sample entries
            print("\nSample tax report entries:")
            for i, entry in enumerate(report_entries[:3]):
                date = datetime.fromtimestamp(entry.timestamp).strftime("%Y-%m-%d")
                print(f"Entry {i+1}: {date} - {entry.asset} - Tax: {entry.tax_liability} EUR")
        else:
            print("No tax report entries generated")
        
        return True
    except Exception as e:
        print(f"✗ Error in end-to-end test: {str(e)}")
        traceback.print_exc()
        return False

def main():
    """Run all API tests."""
    print_section("Kraken API Debug Tests")
    print(f"Running tests at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Run tests
    tests = [
        ("Kraken Public API", test_kraken_request),
        ("Kraken Authentication", test_kraken_auth),
        ("Trades API", test_trades_api),
        ("Ledger API", test_ledger_api),
        ("OHLC API", test_ohlc_api),
        ("Price API", test_price_api),
        ("Cache Functionality", test_cache_functionality),
        ("End-to-End Test", test_end_to_end)
    ]
    
    results = {}
    
    for name, test_func in tests:
        print(f"\nRunning test: {name}")
        try:
            result = test_func()
            results[name] = result
        except Exception as e:
            print(f"✗ Test failed with exception: {str(e)}")
            traceback.print_exc()
            results[name] = False
    
    # Print summary
    print_section("Test Results Summary")
    
    passing = 0
    for name, result in results.items():
        status = "✓ PASS" if result else "✗ FAIL"
        print(f"{status} - {name}")
        if result:
            passing += 1
    
    success_rate = (passing / len(tests)) * 100
    print(f"\nSuccess rate: {success_rate:.1f}% ({passing}/{len(tests)})")
    
    if success_rate == 100:
        print("\n🎉 All tests passed! The Kraken API integration is working correctly.")
    else:
        print("\n⚠️ Some tests failed. Review the output above for details on the issues.")

if __name__ == "__main__":
    main()
