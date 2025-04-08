# Crypto Tax Calculator Debugging Guide

This document provides a comprehensive guide to debugging and fixing issues identified in the Crypto Tax Calculator application.

## Identified Issues

From analyzing the logs and source code, we've identified the following issues:

1. **Variable Errors in Reporting Module**: 
   - In `reporting.py`, the `sale_price_per_unit` variable was used before being defined
   - Tax status variables were also missing in the `export_as_year_csv` function

2. **FIFO Calculation Errors**:
   - Log shows errors like: "Cannot match lots for 0.4516100000 ETH (Ref: LJC3TF-S3X4H-G6MWWN) - No holdings available"
   - This indicates missing purchase records for assets that have sales

3. **Missing 2024 Sales in Reports**:
   - Reports are generated but sales from 2024 aren't included
   - The database has trades but they aren't reflected in the tax reports

4. **Database Consistency Issues**:
   - Potential database integrity problems
   - Possible malformed JSON data in the database

## Fixed Issues

1. **Variable Error in Reporting Module**: 
   - Fixed the `sale_price_per_unit` variable initialization in `reporting.py`
   - Added missing tax status variables

## Debug Tools

We've created a comprehensive debugging script (`debug_plan.py`) to identify and suggest fixes for the remaining issues.

### Running the Debug Script

```bash
# Run all checks
python debug_plan.py --all

# Run specific checks
python debug_plan.py --db-check      # Check database consistency
python debug_plan.py --fifo-check    # Check for FIFO calculation issues 
python debug_plan.py --sales-check   # Check for missing 2024 sales
python debug_plan.py --generate-fixes # Generate fix suggestions
```

### What the Debug Script Does

1. **Database Consistency Check**
   - Verifies tables exist and have data
   - Checks for malformed JSON entries
   - Runs the SQLite PRAGMA integrity check

2. **FIFO Calculation Analysis**
   - Identifies assets with missing purchase records
   - Calculates the total shortfall in purchase quantities
   - Generates reports on problematic assets

3. **Missing Sales Check**
   - Checks if 2024 sales from the database appear in the reports
   - Compares database records with report content

4. **Fix Generation**
   - Creates SQL scripts to add missing purchase records
   - Suggests parameter values for manual entries
   - Calculates appropriate timestamps for the entries

## How to Fix Issues

### 1. Fix Variable Errors
- Already fixed in `reporting.py`

### 2. Fix FIFO Calculation Errors
- Run `debug_plan.py --fifo-check --generate-fixes`
- Review the generated `fifo_fix_suggestions.json` file
- Apply the SQL fixes using the generated `add_manual_entries.sql` script:
  ```bash
  sqlite3 data/kraken_cache.db < export/add_manual_entries.sql
  ```

### 3. Fix Missing 2024 Sales
- After fixing the FIFO issues, re-run the tax calculator
- Check if sales now appear in the reports
- If problems persist, review the database entries for proper classification

### 4. Fix Database Consistency
- Run `debug_plan.py --db-check` to identify database issues
- Fix any malformed JSON entries using the diagnostic output

## Validation

After applying fixes:

1. Run the tax calculator again
2. Check the logs for any remaining errors
3. Verify reports include all 2024 sales
4. Confirm that FIFO calculations no longer show "No holdings available" errors

## Debug Logs

All debugging output is saved to `logs/debug_fix.log` for reference. Additional diagnostics are saved to the `export/` directory.
