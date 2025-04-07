#!/usr/bin/env python3

from datetime import datetime, timezone
import sys
import os

# Add the src directory to the path so we can import the price_api module
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from src.crypto_tax_calculator.price_api import get_historical_price_eur, get_price_from_yahoo, YFINANCE_AVAILABLE

# Test for a future date (2025)
future_date = datetime(2025, 3, 1, tzinfo=timezone.utc)
future_timestamp = int(future_date.timestamp())

# Test assets
assets_to_test = ["BTC", "ETH", "ADA", "AVAX", "ARB", "EUR"]

print(f"Testing price fetching for future date: {future_date.strftime('%Y-%m-%d')}")
print(f"Is yfinance available: {YFINANCE_AVAILABLE}")
print("-" * 50)

for asset in assets_to_test:
    print(f"\nTesting asset: {asset}")
    
    # Try the main price function (should try all sources)
    price = get_historical_price_eur(asset, future_timestamp)
    if price is not None:
        print(f"  Main price function returned: {price:.4f} EUR")
    else:
        print(f"  Main price function couldn't find a price")
    
    # Try Yahoo Finance directly if available
    if YFINANCE_AVAILABLE:
        yahoo_price = get_price_from_yahoo(asset, future_timestamp)
        if yahoo_price is not None:
            print(f"  Yahoo Finance returned: {yahoo_price:.4f} EUR")
        else:
            print(f"  Yahoo Finance couldn't find a price")

print("\nTest completed")
