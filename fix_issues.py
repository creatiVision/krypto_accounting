#!/usr/bin/env python3
"""
Script to fix various issues with the crypto tax calculator:
1. Unify CSV delimiters (some use semicolons, others use commas)
2. Check for missing 2024 sales in reports
3. Add error handling and logging for all operations
"""

import os
import sys
import json
import argparse
from datetime import datetime
from pathlib import Path
import traceback

# Add the current directory to the path so we can import the modules
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Import logging utilities
from src.crypto_tax_calculator.logging_utils import log_event, log_error, log_warning

# Import modules with graceful dependency handling
try:
    from src.crypto_tax_calculator.reporting import unify_all_csv_files, check_missing_sales_2024
    from src.crypto_tax_calculator.models import AggregatedTaxSummary, TaxReportEntry, MatchedLotInfo
    REPORTING_MODULES_AVAILABLE = True
except ImportError as e:
    REPORTING_MODULES_AVAILABLE = False
    print(f"Warning: Some functionality may be limited due to missing dependencies: {e}")

def setup_directories():
    """Create necessary directories if they don't exist."""
    directories = ["logs", "export", "data"]
    for directory in directories:
        os.makedirs(directory, exist_ok=True)
        log_event("Setup", f"Ensured directory exists: {directory}")

def load_sales_data(file_path):
    """Load sales data from a JSON file."""
    try:
        with open(file_path, 'r') as f:
            return json.load(f)
    except Exception as e:
        log_error("DataLoading", "SalesDataError", f"Failed to load sales data from {file_path}", exception=e)
        return []

def load_report_data(file_path):
    """Load existing tax report data."""
    try:
        with open(file_path, 'r') as f:
            data = json.load(f)
            
        # Create a basic summary object from the report data
        summary = AggregatedTaxSummary(
            tax_report_entries=[],
            total_profit_loss=data.get('total_profit_loss', 0),
            total_private_sale_gains=data.get('total_private_sale_gains', 0),
            total_private_sale_losses=data.get('total_private_sale_losses', 0),
            total_other_income=data.get('total_other_income', 0),
            total_tax_liability=data.get('total_tax_liability', 0),
            private_sales_taxable=data.get('private_sales_taxable', False),
            other_income_taxable=data.get('other_income_taxable', False),
            freigrenze_private_sales=data.get('freigrenze_private_sales', 600),
            freigrenze_other_income=data.get('freigrenze_other_income', 256),
            warnings=data.get('warnings', [])
        )
        
        # Convert report entries to TaxReportEntry objects
        for entry_data in data.get('report_entries', []):
            entry = TaxReportEntry(
                timestamp=datetime.strptime(entry_data.get('date', ''), "%Y-%m-%d %H:%M:%S").timestamp() if 'date' in entry_data else 0,
                asset=entry_data.get('asset', ''),
                amount=float(entry_data.get('amount', 0)),
                cost_or_proceeds=float(entry_data.get('proceeds_eur', 0)),
                disposal_proceeds_eur=float(entry_data.get('proceeds_eur', 0)),
                disposal_cost_basis_eur=float(entry_data.get('cost_basis_eur', 0)),
                disposal_gain_loss_eur=float(entry_data.get('gain_loss_eur', 0)),
                tax_liability=float(entry_data.get('tax_liability_eur', 0)),
                holding_period_days_avg=int(entry_data.get('holding_period_days', 0)),
                is_long_term=entry_data.get('is_long_term', False),
                is_taxable=entry_data.get('is_taxable', False),
                refid=entry_data.get('refid', ''),
                matched_lots=[]
            )
            
            # Add matched lots if available
            if 'matched_lots' in entry_data:
                for lot_data in entry_data['matched_lots']:
                    lot = MatchedLotInfo(
                        original_lot_purchase_date=datetime.strptime(lot_data.get('acquisition_date', ''), "%Y-%m-%d %H:%M:%S"),
                        original_lot_refid=lot_data.get('acquisition_refid', ''),
                        amount_used=float(lot_data.get('amount_used', 0)),
                        original_lot_purchase_price_eur=float(lot_data.get('acquisition_price_eur', 0)),
                        cost_basis_eur=float(lot_data.get('cost_basis_eur', 0)),
                        holding_period_days=int(lot_data.get('holding_period_days', 0))
                    )
                    entry.matched_lots.append(lot)
            
            summary.tax_report_entries.append(entry)
        
        log_event("DataLoading", f"Loaded tax report data from {file_path} with {len(summary.tax_report_entries)} entries")
        return summary
    except Exception as e:
        log_error("DataLoading", "ReportDataError", f"Failed to load report data from {file_path}", exception=e)
        return None

def unify_csv_files(directories):
    """Unify delimiter in all CSV files in the specified directories."""
    processed_count = 0
    for directory in directories:
        if os.path.exists(directory):
            log_event("CSVUnification", f"Processing directory: {directory}")
            processed_files = unify_all_csv_files(directory, target_delimiter=',', recursive=True)
            processed_count += len(processed_files)
        else:
            log_warning("CSVUnification", "DirectoryNotFound", f"Directory not found: {directory}")
    
    return processed_count

def check_for_missing_sales(report_file, sales_file):
    """Check for missing 2024 sales in the report."""
    if not os.path.exists(report_file):
        log_error("SalesCheck", "FileNotFound", f"Report file not found: {report_file}")
        return []
    
    if not os.path.exists(sales_file):
        log_error("SalesCheck", "FileNotFound", f"Sales data file not found: {sales_file}")
        return []
    
    summary = load_report_data(report_file)
    if not summary:
        return []
    
    sales_data = load_sales_data(sales_file)
    if not sales_data:
        return []
    
    missing_sales = check_missing_sales_2024(summary, sales_data)
    
    if missing_sales:
        log_warning("SalesCheck", "MissingSales", 
                  f"Found {len(missing_sales)} sales from 2024 that are missing from the report",
                  details={"missing_count": len(missing_sales)})
        
        # Save missing sales to a file for reference
        missing_file = "missing_sales_2024.json"
        try:
            with open(missing_file, 'w') as f:
                json.dump(missing_sales, f, indent=2)
            log_event("SalesCheck", f"Saved missing sales data to {missing_file}")
        except Exception as e:
            log_error("SalesCheck", "SaveError", 
                     f"Failed to save missing sales data to {missing_file}", 
                     exception=e)
    
    return missing_sales

def main():
    """Main function to fix various issues."""
    parser = argparse.ArgumentParser(description="Fix issues with the crypto tax calculator")
    parser.add_argument("--csv-dirs", nargs="+", default=["export", "data"], 
                        help="Directories containing CSV files to unify (default: export data)")
    parser.add_argument("--report-file", default="export/tax_report_2024.json",
                        help="Path to the tax report file (default: export/tax_report_2024.json)")
    parser.add_argument("--sales-file", default="data/trades.json",
                        help="Path to the sales data file (default: data/trades.json)")
    parser.add_argument("--skip-csv", action="store_true", 
                        help="Skip CSV delimiter unification")
    parser.add_argument("--skip-sales", action="store_true", 
                        help="Skip checking for missing sales")
    
    args = parser.parse_args()
    
    try:
        # Set up necessary directories
        setup_directories()
        
        log_event("Start", "Starting issue fixing process")
        
        if not REPORTING_MODULES_AVAILABLE:
            log_error("Initialization", "MissingDependencies", 
                     "Required modules for CSV and sales operations are not available")
            print("ERROR: Required modules are not available. Please check your installation.")
            return 1
        
        # Process CSV files
        if not args.skip_csv:
            processed_count = unify_csv_files(args.csv_dirs)
            log_event("CSVUnification", f"Processed {processed_count} CSV files")
        else:
            log_event("CSVUnification", "Skipped CSV delimiter unification")
        
        # Check for missing sales
        if not args.skip_sales:
            missing_sales = check_for_missing_sales(args.report_file, args.sales_file)
            if missing_sales:
                print(f"WARNING: Found {len(missing_sales)} sales from 2024 that are missing from the report.")
                print(f"         Details have been saved to missing_sales_2024.json")
                print(f"         You may need to run the tax calculator again to include these sales.")
            else:
                log_event("SalesCheck", "No missing sales found for 2024")
        else:
            log_event("SalesCheck", "Skipped checking for missing sales")
        
        log_event("Complete", "Issue fixing process completed successfully")
        print("Issue fixing process completed. See logs for details.")
        
    except Exception as e:
        log_error("General", "UnexpectedError", "An unexpected error occurred", exception=e)
        traceback.print_exc()
        return 1
    
    return 0

if __name__ == "__main__":
    sys.exit(main())
