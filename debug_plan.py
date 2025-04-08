#!/usr/bin/env python3
"""
Comprehensive debugging script for crypto_tax_calculator issues.
This script addresses several known issues:

1. Variable errors in the reporting module (fixed in reporting.py)
2. FIFO calculation errors where transactions can't find matching lots
3. Missing 2024 sales in reports
4. Database connection and cache issues

Run this script to perform diagnostics and fix the identified issues.
"""

import os
import sys
import json
import sqlite3
from datetime import datetime
from pathlib import Path
import traceback
from decimal import Decimal
import logging
import argparse
from contextlib import contextmanager

# Add the current directory to the path so we can import the modules
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Import modules (with error handling in case of missing dependencies)
try:
    from src.crypto_tax_calculator.logging_utils import log_event, log_error, log_warning
    from src.crypto_tax_calculator.models import TaxReportEntry, AggregatedTaxSummary, MatchedLotInfo, Transaction
    MODULES_AVAILABLE = True
except ImportError as e:
    MODULES_AVAILABLE = False
    print(f"Warning: Some functionality may be limited due to missing dependencies: {e}")
    traceback.print_exc()

# Define DB_PATH directly to avoid dependency on price_api which requires pycoingecko
DATA_DIR = os.path.join(os.path.dirname(__file__), 'data')
DB_PATH = os.path.join(DATA_DIR, 'kraken_cache.db')

# Try to import get_db_connection, or define our own if it fails
try:
    from src.crypto_tax_calculator.kraken_cache import get_db_connection
except ImportError as e:
    # Define a simple version of get_db_connection if the import fails
    @contextmanager
    def get_db_connection():
        """Simple context manager for database connections."""
        conn = None
        try:
            conn = sqlite3.connect(DB_PATH)
            yield conn
        finally:
            if conn:
                conn.close()

# Ensure logs directory exists
LOG_DIR = Path("logs")
LOG_DIR.mkdir(exist_ok=True)

EXPORT_DIR = Path("export")
EXPORT_DIR.mkdir(exist_ok=True)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_DIR / 'debug_fix.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("debug_fix")

class FIFODebugger:
    """Handles debugging and fixing FIFO calculation issues."""
    
    def __init__(self, db_path=DB_PATH):
        self.db_path = db_path
        # Don't initialize FifoCalculator to avoid dependency on price_api
        self.purchases = {}
        self.sales = {}
        
    def load_transactions(self, year=2024):
        """Load all transactions from the database."""
        logger.info(f"Loading transactions for year {year}")
        try:
            # Time range for the specified year
            year_start = int(datetime(year, 1, 1).timestamp())
            year_end = int(datetime(year, 12, 31, 23, 59, 59).timestamp())
            
            purchases = []
            sales = []
            
            with get_db_connection() as conn:
                cursor = conn.cursor()
                
                # Get all trades
                cursor.execute(
                    """SELECT data_json, timestamp FROM trades 
                    WHERE timestamp >= ? AND timestamp <= ?""",
                    (year_start, year_end)
                )
                rows = cursor.fetchall()
                
                for row in rows:
                    try:
                        data = json.loads(row[0])
                        # Add to appropriate list based on type
                        if data.get('type') == 'buy':
                            purchases.append(data)
                        elif data.get('type') == 'sell':
                            sales.append(data)
                    except json.JSONDecodeError:
                        logger.warning(f"Could not parse JSON: {row[0][:50]}...")
                
                # Get ledger entries that might be purchases or sales
                cursor.execute(
                    """SELECT data_json, timestamp FROM ledger
                    WHERE timestamp >= ? AND timestamp <= ?""",
                    (year_start, year_end)
                )
                rows = cursor.fetchall()
                
                for row in rows:
                    try:
                        data = json.loads(row[0])
                        # Determine if it's a purchase or sale
                        if data.get('type') == 'trade':
                            # Look at amount to determine buy/sell
                            amount = Decimal(data.get('amount', 0))
                            if amount > 0:
                                purchases.append(data)
                            else:
                                sales.append(data)
                    except json.JSONDecodeError:
                        logger.warning(f"Could not parse JSON: {row[0][:50]}...")
            
            # Store transactions by asset
            for purchase in purchases:
                asset = purchase.get('asset', '')
                if asset not in self.purchases:
                    self.purchases[asset] = []
                self.purchases[asset].append(purchase)
            
            for sale in sales:
                asset = sale.get('asset', '')
                if asset not in self.sales:
                    self.sales[asset] = []
                self.sales[asset].append(sale)
            
            logger.info(f"Loaded {len(purchases)} purchases and {len(sales)} sales")
            return len(purchases), len(sales)
        
        except Exception as e:
            logger.error(f"Error loading transactions: {str(e)}")
            traceback.print_exc()
            return 0, 0
    
    def inspect_fifo_errors(self):
        """Identify potential FIFO errors."""
        logger.info("Inspecting FIFO errors in transactions")
        
        problematic_assets = set()
        missing_purchases = {}
        
        # Analyze sales to find assets without sufficient purchase records
        for asset, sales_list in self.sales.items():
            if asset not in self.purchases or len(self.purchases[asset]) == 0:
                problematic_assets.add(asset)
                missing_purchases[asset] = sum(Decimal(sale.get('amount', 0)) for sale in sales_list)
                logger.warning(f"Asset {asset} has {len(sales_list)} sales but no purchases")
        
        # Check assets with both purchases and sales to see if sales exceed purchases
        for asset in set(self.purchases.keys()) & set(self.sales.keys()):
            purchase_total = sum(Decimal(purchase.get('amount', 0)) for purchase in self.purchases[asset])
            sales_total = sum(abs(Decimal(sale.get('amount', 0))) for sale in self.sales[asset])
            
            if sales_total > purchase_total:
                problematic_assets.add(asset)
                missing_purchases[asset] = sales_total - purchase_total
                logger.warning(f"Asset {asset} has insufficient purchases: {purchase_total} vs {sales_total} sales")
        
        if problematic_assets:
            logger.info(f"Found {len(problematic_assets)} problematic assets: {', '.join(problematic_assets)}")
            self._write_fifo_issues_report(problematic_assets, missing_purchases)
        else:
            logger.info("No FIFO issues detected")
        
        return problematic_assets, missing_purchases
    
    def _write_fifo_issues_report(self, problematic_assets, missing_purchases):
        """Write a report of FIFO issues."""
        report_path = EXPORT_DIR / "fifo_issues_report.json"
        
        report_data = {
            "timestamp": datetime.now().isoformat(),
            "problematic_assets": list(problematic_assets),
            "missing_purchases": {k: str(v) for k, v in missing_purchases.items()},
            "summary": {
                "total_problematic_assets": len(problematic_assets),
                "total_missing_volume": sum(float(v) for v in missing_purchases.values())
            }
        }
        
        with open(report_path, 'w') as f:
            json.dump(report_data, f, indent=2)
        
        logger.info(f"FIFO issues report written to {report_path}")
    
    def generate_fix_suggestions(self, problematic_assets, missing_purchases):
        """Generate suggestions to fix FIFO issues."""
        logger.info("Generating fix suggestions for FIFO issues")
        
        if not problematic_assets:
            logger.info("No issues to fix")
            return
        
        suggestions = {
            "manual_entries": [],
            "data_corrections": []
        }
        
        for asset in problematic_assets:
            # Get the average purchase price from existing purchases, if any
            avg_price = Decimal('0')
            if asset in self.purchases and self.purchases[asset]:
                prices = [Decimal(p.get('price', '0')) for p in self.purchases[asset]]
                avg_price = sum(prices) / len(prices) if prices else Decimal('0')
            
            # Suggest manual entry
            missing_amount = missing_purchases.get(asset, Decimal('0'))
            
            # Handle both positive and negative missing amounts
            # For fiat currencies like ZEUR, the missing amount might be negative
            # because the system treats sales of ZEUR as purchases of other assets
            abs_missing_amount = abs(missing_amount)
            
            # Skip if the amount is too small
            if abs_missing_amount < Decimal('0.0001'):
                continue
                
            # Determine if this is a fiat currency
            is_fiat = asset in ['EUR', 'ZEUR', 'USD', 'ZUSD', 'GBP', 'ZGBP', 'JPY', 'ZJPY']
            
            # For fiat currencies with negative missing amounts, we need to add a "deposit" entry
            # For crypto assets with positive missing amounts, we need to add a "purchase" entry
            entry_type = "deposit" if is_fiat and missing_amount < 0 else "purchase"
            
            earliest_sale = None
            if asset in self.sales and self.sales[asset]:
                earliest_sale = min(self.sales[asset], key=lambda x: int(x.get('time', 0)))
            
            # If we found a sale, suggest a purchase/deposit before it
            if earliest_sale:
                sale_time = int(earliest_sale.get('time', 0))
                entry_time = max(1546300800, sale_time - 86400)  # One day before sale or Jan 1, 2019
                
                suggestions["manual_entries"].append({
                    "asset": asset,
                    "type": entry_type,
                    "amount": str(abs_missing_amount),
                    "price": "1.0" if is_fiat else str(avg_price),
                    "timestamp": entry_time,
                    "date": datetime.fromtimestamp(entry_time).strftime("%Y-%m-%d"),
                    "refid": f"MANUAL_{asset}_{datetime.fromtimestamp(entry_time).strftime('%Y%m%d')}",
                    "note": f"Manual {entry_type} entry to fix FIFO calculation for {asset}"
                })
        
        # Write suggestions to file
        suggestions_path = EXPORT_DIR / "fifo_fix_suggestions.json"
        with open(suggestions_path, 'w') as f:
            json.dump(suggestions, f, indent=2)
        
        logger.info(f"Generated {len(suggestions['manual_entries'])} manual entry suggestions")
        logger.info(f"Fix suggestions written to {suggestions_path}")
        
        # Also create a SQL script for manual entries
        self._generate_sql_fixes(suggestions["manual_entries"])
        
        return suggestions
    
    def _generate_sql_fixes(self, manual_entries):
        """Generate SQL script to add manual entries to the database."""
        if not manual_entries:
            return
        
        sql_path = EXPORT_DIR / "add_manual_entries.sql"
        with open(sql_path, 'w') as f:
            f.write("-- SQL script to add manual entries for FIFO fixes\n")
            f.write("-- Generated by debug_plan.py\n\n")
            
            for entry in manual_entries:
                # Create a JSON structure for the trade
                trade_data = {
                    "asset": entry["asset"],
                    "type": entry["type"],  # Use the type from the entry (buy or deposit)
                    "time": entry["timestamp"],
                    "refid": entry["refid"],
                    "amount": entry["amount"],
                    "price": entry["price"],
                    "note": entry["note"],
                    "manual_entry": True
                }
                
                # SQL to insert into trades table
                f.write("INSERT INTO trades (refid, data_json, timestamp) VALUES\n")
                f.write(f"('{entry['refid']}', '{json.dumps(trade_data)}', {entry['timestamp']});\n\n")
        
        logger.info(f"SQL fix script written to {sql_path}")

def check_database_consistency():
    """Check the database for consistency issues."""
    logger.info("Checking database consistency")
    
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            
            # Check if tables exist
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            tables = [row[0] for row in cursor.fetchall()]
            
            if 'trades' not in tables or 'ledger' not in tables:
                logger.error(f"Missing essential tables. Found: {tables}")
                return False
            
            # Check row counts
            issues = []
            for table in ['trades', 'ledger']:
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                count = cursor.fetchone()[0]
                logger.info(f"Table '{table}' has {count} rows")
                
                if count == 0:
                    issues.append(f"Table '{table}' is empty")
                
                # Check for malformed JSON
                cursor.execute(f"SELECT refid, data_json FROM {table} LIMIT 5")
                rows = cursor.fetchall()
                for row in rows:
                    try:
                        json.loads(row[1])
                    except json.JSONDecodeError:
                        issues.append(f"Malformed JSON in '{table}' for refid {row[0]}")
            
            # Run PRAGMA integrity check
            cursor.execute("PRAGMA integrity_check")
            integrity = cursor.fetchone()[0]
            if integrity != "ok":
                issues.append(f"Database integrity check failed: {integrity}")
            
            if issues:
                logger.warning("Database consistency issues found:")
                for issue in issues:
                    logger.warning(f"- {issue}")
                return False
            
            logger.info("Database consistency check passed")
            return True
            
    except Exception as e:
        logger.error(f"Error checking database consistency: {str(e)}")
        traceback.print_exc()
        return False

def check_missing_sales():
    """Check for missing 2024 sales in reports."""
    logger.info("Checking for missing 2024 sales in reports")
    
    try:
        # First check if reports exist
        year_csv = EXPORT_DIR / "2024.csv"
        fifo_txt = EXPORT_DIR / "fifo_nachweis_2024.txt"
        
        if not year_csv.exists() or not fifo_txt.exists():
            logger.warning("Report files for 2024 do not exist")
            return True  # Reports missing, which is a problem
        
        # Check if sales are in the reports
        with open(fifo_txt, 'r') as f:
            content = f.read()
            sales_present = "Veräußerung #" in content
        
        # Extract sales from database
        sales_count = 0
        year_start = int(datetime(2024, 1, 1).timestamp())
        year_end = int(datetime(2024, 12, 31, 23, 59, 59).timestamp())
        
        with get_db_connection() as conn:
            cursor = conn.cursor()
            
            cursor.execute(
                """SELECT COUNT(*) FROM trades 
                WHERE timestamp >= ? AND timestamp <= ? 
                AND json_extract(data_json, '$.type') = 'sell'""",
                (year_start, year_end)
            )
            trades_count = cursor.fetchone()[0]
            
            cursor.execute(
                """SELECT COUNT(*) FROM ledger 
                WHERE timestamp >= ? AND timestamp <= ? 
                AND json_extract(data_json, '$.type') = 'trade'""",
                (year_start, year_end)
            )
            ledger_count = cursor.fetchone()[0]
            
            sales_count = trades_count + ledger_count
        
        if sales_count > 0 and not sales_present:
            logger.warning(f"Found {sales_count} sales in database but none in reports")
            return True  # Missing sales problem
        
        logger.info(f"Found {sales_count} sales in database, reports {'do' if sales_present else 'do not'} include sales")
        return not sales_present  # Problem if no sales in reports
        
    except Exception as e:
        logger.error(f"Error checking for missing sales: {str(e)}")
        traceback.print_exc()
        return True  # Error indicates a problem

def main():
    """Main function to run all diagnostics and fixes."""
    parser = argparse.ArgumentParser(description="Debug and fix issues in crypto tax calculator")
    parser.add_argument("--db-check", action="store_true", help="Check database consistency")
    parser.add_argument("--fifo-check", action="store_true", help="Check for FIFO calculation issues")
    parser.add_argument("--sales-check", action="store_true", help="Check for missing 2024 sales")
    parser.add_argument("--generate-fixes", action="store_true", help="Generate fix suggestions")
    parser.add_argument("--all", action="store_true", help="Run all checks and generate fixes")
    
    args = parser.parse_args()
    
    # If no arguments, run all checks
    if not any([args.db_check, args.fifo_check, args.sales_check, args.generate_fixes]):
        args.all = True
    
    try:
        logger.info("Starting debugging and fix process")
        
        if not MODULES_AVAILABLE:
            logger.error("Required modules are not available")
            print("ERROR: Required modules are not available. Please check your installation.")
            return 1
        
        # Database check
        if args.db_check or args.all:
            logger.info("--- Database Consistency Check ---")
            db_ok = check_database_consistency()
            if not db_ok:
                logger.warning("Database consistency issues found")
        
        # FIFO check
        fifo_debugger = None
        problematic_assets = None
        missing_purchases = None
        if args.fifo_check or args.all:
            logger.info("--- FIFO Calculation Check ---")
            fifo_debugger = FIFODebugger()
            
            # Check 2024 transactions
            logger.info("Checking 2024 transactions")
            fifo_debugger.load_transactions(year=2024)
            
            # Also check 2023 transactions
            logger.info("Checking 2023 transactions")
            fifo_debugger.load_transactions(year=2023)
            
            problematic_assets, missing_purchases = fifo_debugger.inspect_fifo_errors()
        
        # Missing sales check
        if args.sales_check or args.all:
            logger.info("--- Missing 2024 Sales Check ---")
            sales_missing = check_missing_sales()
            if sales_missing:
                logger.warning("Missing 2024 sales detected")
        
        # Generate fixes
        if (args.generate_fixes or args.all) and fifo_debugger and problematic_assets:
            logger.info("--- Generating Fix Suggestions ---")
            fifo_debugger.generate_fix_suggestions(problematic_assets, missing_purchases)
        
        logger.info("Debugging and fix process completed")
        print("\nDebugging and fix process completed. Check the logs and export directory for results.")
        
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        traceback.print_exc()
        return 1
    
    return 0

if __name__ == "__main__":
    sys.exit(main())
