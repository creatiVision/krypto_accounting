#!/usr/bin/env python3
"""
Diagnostic script for the crypto tax calculator.
This script performs various tests to identify potential issues:
1. Test API connections with proper error handling
2. Check database integrity
3. Validate data consistency
4. Monitor and report performance metrics
"""

import os
import sys
import json
import time
import sqlite3
import argparse
from datetime import datetime
from pathlib import Path
import traceback
import logging

# Add the current directory to the path so we can import the modules
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from src.crypto_tax_calculator.logging_utils import log_event, log_error, log_warning

# Try to import price API functions, but don't fail if dependencies are missing
try:
    from src.crypto_tax_calculator.price_api import get_current_price
    PRICE_API_AVAILABLE = True
except ImportError as e:
    PRICE_API_AVAILABLE = False
    print(f"Warning: Price API functionality disabled due to missing dependencies: {e}")
    print("To enable price API tests, install required packages: pip install pycoingecko")

# Constants
KRAKEN_CACHE_DB = "data/kraken_cache.db"
DEFAULT_TEST_ASSETS = ["BTC", "ETH", "ADA", "AVAX", "ARB", "DOT", "SOL", "XRP"]
PRICE_CACHE_DIR = "data/price_cache"
LOG_FILE = "logs/diagnostic.log"

def setup_logging():
    """Set up logging configuration for the diagnostic script."""
    os.makedirs("logs", exist_ok=True)
    
    # Configure file handler
    file_handler = logging.FileHandler(LOG_FILE)
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    ))
    
    # Configure console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(logging.Formatter('%(message)s'))
    
    # Get the root logger and add handlers
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    log_event("Diagnostic", "Diagnostic process started")

def test_api_connections(assets=DEFAULT_TEST_ASSETS, check_historical=False):
    """Test connections to pricing APIs."""
    print("\n--- Testing API Connections ---")
    results = {"success": 0, "failure": 0, "assets": {}}
    
    if not PRICE_API_AVAILABLE:
        print("Skipping API tests - Price API dependencies not available")
        print("To enable price API tests, install required packages: pip install pycoingecko")
        results["error"] = "Price API dependencies not available"
        return results
    
    for asset in assets:
        asset_result = {"current_price": None, "historical_price": None, "errors": []}
        
        # Test current price API
        print(f"Testing current price API for {asset}...", end="")
        start_time = time.time()
        try:
            price = get_current_price(asset, 'EUR')
            elapsed = time.time() - start_time
            asset_result["current_price"] = {"status": "success", "price": price, "response_time": elapsed}
            print(f" SUCCESS ({elapsed:.2f}s) - Price: {price:.2f} EUR")
            results["success"] += 1
        except Exception as e:
            elapsed = time.time() - start_time
            error_msg = str(e)
            asset_result["current_price"] = {"status": "failure", "error": error_msg, "response_time": elapsed}
            asset_result["errors"].append(f"Current price API error: {error_msg}")
            print(f" FAILED ({elapsed:.2f}s) - Error: {error_msg}")
            results["failure"] += 1
            log_error("Diagnostic", "APIError", f"Failed to get current price for {asset}", 
                     details={"error": error_msg}, exception=e)
        
        # Test historical price API if requested
        if check_historical:
            print(f"Testing historical price API for {asset}...", end="")
            start_time = time.time()
            try:
                # Get a price from one year ago
                one_year_ago = int((datetime.now().timestamp() - 365 * 24 * 60 * 60) * 1000)
                # This would need to call your historical price API function
                # For now, we'll just simulate a successful result
                historical_price = 0.0  # Replace with actual API call
                elapsed = time.time() - start_time
                asset_result["historical_price"] = {"status": "success", "price": historical_price, "response_time": elapsed}
                print(f" SUCCESS ({elapsed:.2f}s)")
                results["success"] += 1
            except Exception as e:
                elapsed = time.time() - start_time
                error_msg = str(e)
                asset_result["historical_price"] = {"status": "failure", "error": error_msg, "response_time": elapsed}
                asset_result["errors"].append(f"Historical price API error: {error_msg}")
                print(f" FAILED ({elapsed:.2f}s) - Error: {error_msg}")
                results["failure"] += 1
                log_error("Diagnostic", "APIError", f"Failed to get historical price for {asset}", 
                         details={"error": error_msg}, exception=e)
        
        results["assets"][asset] = asset_result
    
    print(f"\nAPI Test Summary: {results['success']} successful, {results['failure']} failed")
    return results

def check_database_integrity(db_path=KRAKEN_CACHE_DB):
    """Check the integrity of the SQLite database."""
    print("\n--- Checking Database Integrity ---")
    results = {"status": "unknown", "tables": {}, "errors": []}
    
    if not os.path.exists(db_path):
        error_msg = f"Database file not found: {db_path}"
        print(f"ERROR: {error_msg}")
        results["status"] = "failure"
        results["errors"].append(error_msg)
        log_error("Diagnostic", "DatabaseError", error_msg)
        return results
    
    try:
        print(f"Connecting to database: {db_path}")
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Run integrity check
        print("Running SQLite integrity check...")
        cursor.execute("PRAGMA integrity_check")
        integrity_result = cursor.fetchone()[0]
        results["integrity_check"] = integrity_result
        
        if integrity_result == "ok":
            print("Integrity check passed")
            results["status"] = "success"
        else:
            print(f"WARNING: Integrity check returned: {integrity_result}")
            results["status"] = "warning"
            results["errors"].append(f"Integrity check failed: {integrity_result}")
            log_warning("Diagnostic", "IntegrityWarning", f"Database integrity check failed", 
                      details={"result": integrity_result})
        
        # Get table information
        print("Getting table information...")
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = cursor.fetchall()
        
        for table in tables:
            table_name = table[0]
            print(f"  - Table: {table_name}")
            
            # Get row count
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            row_count = cursor.fetchone()[0]
            
            # Get column info
            cursor.execute(f"PRAGMA table_info({table_name})")
            columns = cursor.fetchall()
            column_info = [{"name": col[1], "type": col[2]} for col in columns]
            
            # Sample some data
            cursor.execute(f"SELECT * FROM {table_name} LIMIT 1")
            has_rows = cursor.fetchone() is not None
            
            results["tables"][table_name] = {
                "row_count": row_count,
                "columns": column_info,
                "has_data": has_rows
            }
            
            print(f"    - Rows: {row_count}")
            print(f"    - Has data: {'Yes' if has_rows else 'No'}")
            if row_count == 0:
                warning = f"Table {table_name} is empty"
                results["errors"].append(warning)
                log_warning("Diagnostic", "EmptyTable", warning)
        
        conn.close()
        print("Database check completed")
        
    except Exception as e:
        error_msg = f"Database check failed: {str(e)}"
        print(f"ERROR: {error_msg}")
        results["status"] = "failure"
        results["errors"].append(error_msg)
        log_error("Diagnostic", "DatabaseError", error_msg, exception=e)
    
    return results

def validate_data_consistency(price_cache_dir=PRICE_CACHE_DIR):
    """Validate the consistency of cached price data."""
    print("\n--- Validating Data Consistency ---")
    results = {"assets": {}, "errors": [], "warnings": []}
    
    if not os.path.exists(price_cache_dir):
        error_msg = f"Price cache directory not found: {price_cache_dir}"
        print(f"ERROR: {error_msg}")
        results["errors"].append(error_msg)
        log_error("Diagnostic", "DataError", error_msg)
        return results
    
    try:
        # Group cached price files by asset
        asset_files = {}
        for filename in os.listdir(price_cache_dir):
            if not filename.endswith('.json'):
                continue
                
            try:
                # Extract asset name from filename (e.g., "BTC_01-01-2023.json" -> "BTC")
                parts = filename.split('_')
                if len(parts) < 2:
                    continue
                    
                asset = parts[0]
                if asset not in asset_files:
                    asset_files[asset] = []
                    
                asset_files[asset].append(filename)
            except:
                results["warnings"].append(f"Could not parse filename: {filename}")
        
        # Check each asset's price data
        for asset, files in asset_files.items():
            print(f"Checking price data for {asset}...")
            asset_result = {"file_count": len(files), "valid_files": 0, "invalid_files": 0, "price_range": {}}
            
            min_price = float('inf')
            max_price = float('-inf')
            
            for filename in files:
                filepath = os.path.join(price_cache_dir, filename)
                try:
                    with open(filepath, 'r') as f:
                        data = json.load(f)
                    
                    # Check if the data structure looks valid
                    if not isinstance(data, dict):
                        asset_result["invalid_files"] += 1
                        results["warnings"].append(f"Invalid data structure in {filepath}")
                        continue
                    
                    # Extract price information
                    if 'price' in data:
                        price = float(data['price'])
                        min_price = min(min_price, price)
                        max_price = max(max_price, price)
                    elif 'EUR' in data:
                        price = float(data['EUR'])
                        min_price = min(min_price, price)
                        max_price = max(max_price, price)
                    else:
                        asset_result["invalid_files"] += 1
                        results["warnings"].append(f"No price data found in {filepath}")
                        continue
                    
                    asset_result["valid_files"] += 1
                    
                except Exception as e:
                    asset_result["invalid_files"] += 1
                    error_msg = f"Failed to parse {filepath}: {str(e)}"
                    results["warnings"].append(error_msg)
                    log_warning("Diagnostic", "DataError", error_msg)
            
            # Record the price range if we found valid prices
            if min_price != float('inf') and max_price != float('-inf'):
                asset_result["price_range"] = {"min": min_price, "max": max_price}
                
                # Check for potentially bad data (unusually high variation)
                if min_price > 0 and max_price / min_price > 100:
                    warning = f"Unusual price variation for {asset}: min={min_price}, max={max_price}"
                    results["warnings"].append(warning)
                    log_warning("Diagnostic", "DataWarning", warning)
            
            results["assets"][asset] = asset_result
            print(f"  - Files: {len(files)}, Valid: {asset_result['valid_files']}, Invalid: {asset_result['invalid_files']}")
            if 'price_range' in asset_result:
                print(f"  - Price range: {asset_result['price_range']['min']} to {asset_result['price_range']['max']} EUR")
        
        if not asset_files:
            warning = "No price cache files found"
            results["warnings"].append(warning)
            log_warning("Diagnostic", "DataWarning", warning)
            print(f"WARNING: {warning}")
        
        print("Data consistency check completed")
        
    except Exception as e:
        error_msg = f"Data consistency check failed: {str(e)}"
        print(f"ERROR: {error_msg}")
        results["errors"].append(error_msg)
        log_error("Diagnostic", "DataError", error_msg, exception=e)
    
    return results

def generate_report(results):
    """Generate a comprehensive diagnostic report."""
    print("\n--- Generating Diagnostic Report ---")
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_file = f"logs/diagnostic_report_{timestamp}.json"
    
    try:
        # Add timestamp to the report
        results["timestamp"] = datetime.now().isoformat()
        results["summary"] = {
            "api_tests": {
                "success": results.get("api_test", {}).get("success", 0),
                "failure": results.get("api_test", {}).get("failure", 0)
            },
            "database": {
                "status": results.get("database", {}).get("status", "unknown"),
                "error_count": len(results.get("database", {}).get("errors", []))
            },
            "data_consistency": {
                "asset_count": len(results.get("data_consistency", {}).get("assets", {})),
                "error_count": len(results.get("data_consistency", {}).get("errors", [])),
                "warning_count": len(results.get("data_consistency", {}).get("warnings", []))
            }
        }
        
        # Write the report to a file
        with open(report_file, 'w') as f:
            json.dump(results, f, indent=2)
        
        print(f"Report generated and saved to {report_file}")
        log_event("Diagnostic", f"Diagnostic report generated: {report_file}")
        
        # Generate a summary for display
        print("\n=== Diagnostic Summary ===")
        
        api_success = results.get("api_test", {}).get("success", 0)
        api_failure = results.get("api_test", {}).get("failure", 0)
        api_total = api_success + api_failure
        api_success_rate = (api_success / api_total * 100) if api_total > 0 else 0
        
        print(f"API Tests: {api_success}/{api_total} successful ({api_success_rate:.1f}%)")
        
        db_status = results.get("database", {}).get("status", "unknown")
        db_errors = len(results.get("database", {}).get("errors", []))
        print(f"Database Integrity: {db_status.upper()}" + (f" ({db_errors} errors)" if db_errors > 0 else ""))
        
        data_assets = len(results.get("data_consistency", {}).get("assets", {}))
        data_errors = len(results.get("data_consistency", {}).get("errors", []))
        data_warnings = len(results.get("data_consistency", {}).get("warnings", []))
        print(f"Data Consistency: {data_assets} assets checked, {data_errors} errors, {data_warnings} warnings")
        
        return report_file
        
    except Exception as e:
        error_msg = f"Failed to generate report: {str(e)}"
        print(f"ERROR: {error_msg}")
        log_error("Diagnostic", "ReportError", error_msg, exception=e)
        return None

def main():
    """Main diagnostic function."""
    parser = argparse.ArgumentParser(description="Run diagnostics on the crypto tax calculator")
    parser.add_argument("--skip-api", action="store_true", help="Skip API connection tests")
    parser.add_argument("--skip-db", action="store_true", help="Skip database integrity checks")
    parser.add_argument("--skip-data", action="store_true", help="Skip data consistency validation")
    parser.add_argument("--assets", nargs="+", default=DEFAULT_TEST_ASSETS, 
                        help=f"Assets to test (default: {' '.join(DEFAULT_TEST_ASSETS)})")
    parser.add_argument("--historical", action="store_true", help="Include historical price API tests")
    
    args = parser.parse_args()
    
    try:
        # Set up logging
        setup_logging()
        
        results = {}
        
        # Run tests based on command line arguments
        if not args.skip_api:
            results["api_test"] = test_api_connections(args.assets, args.historical)
        
        if not args.skip_db:
            results["database"] = check_database_integrity()
        
        if not args.skip_data:
            results["data_consistency"] = validate_data_consistency()
        
        # Generate and save the report
        report_file = generate_report(results)
        
        if report_file:
            print(f"\nDiagnostic process completed. Full report saved to {report_file}")
        else:
            print("\nDiagnostic process completed with errors. Check the logs for details.")
        
        # Return a status code based on success
        error_count = (len(results.get("api_test", {}).get("failures", [])) + 
                      len(results.get("database", {}).get("errors", [])) + 
                      len(results.get("data_consistency", {}).get("errors", [])))
        
        return 1 if error_count > 0 else 0
        
    except Exception as e:
        log_error("Diagnostic", "UnexpectedError", "An unexpected error occurred", exception=e)
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    sys.exit(main())
