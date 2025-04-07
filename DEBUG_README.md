# Debugging and Error Handling Guide

This guide provides information about the debugging tools, error handling, and logging capabilities of the Crypto Tax Calculator application.

## Logging System

A comprehensive logging system has been implemented to track events, errors, and warnings throughout the application. Logs are stored in the `logs/` directory.

### Log Types

1. **Event Logs**: Track normal application operations
2. **Error Logs**: Capture exceptions and error conditions
3. **Warning Logs**: Record potential issues that don't halt execution
4. **API Call Logs**: Monitor interactions with external APIs
5. **Transaction Logs**: Document cryptocurrency transactions

### Log Format

Logs include the following information:
- Timestamp
- Module/Component 
- Event/Error type
- Message
- Details (if applicable)
- Traceback (for errors)

## Diagnostic Tool

The `diagnostic.py` script helps identify issues with the system:

```bash
python3 diagnostic.py [options]
```

### Options

- `--skip-api`: Skip API connection tests
- `--skip-db`: Skip database integrity checks
- `--skip-data`: Skip data consistency validation
- `--assets ASSETS`: Specify assets to test (default: BTC ETH ADA AVAX ARB DOT SOL XRP)
- `--historical`: Include historical price API tests
- `-h, --help`: Show help message

### Features

1. **API Connectivity Tests**: Checks connectivity to price APIs
2. **Database Integrity Checks**: Validates the SQLite database structure and contents
3. **Data Consistency Validation**: Ensures cached price data is valid and within reasonable ranges
4. **Report Generation**: Creates JSON reports with the test results

### Example Usage

```bash
# Run all diagnostics
python3 diagnostic.py

# Skip API tests and only check database and data
python3 diagnostic.py --skip-api

# Test specific assets
python3 diagnostic.py --assets BTC ETH

# Include historical price API tests
python3 diagnostic.py --historical
```

## Issue Fixer Tool

The `fix_issues.py` script automatically addresses common issues:

```bash
python3 fix_issues.py [options]
```

### Options

- `--csv-dirs CSV_DIRS`: Directories containing CSV files to unify (default: export data)
- `--report-file REPORT_FILE`: Path to the tax report file (default: export/tax_report_2024.json)
- `--sales-file SALES_FILE`: Path to the sales data file (default: data/trades.json)
- `--skip-csv`: Skip CSV delimiter unification
- `--skip-sales`: Skip checking for missing sales
- `-h, --help`: Show help message

### Features

1. **CSV Delimiter Unification**: Standardizes all CSV files to use a consistent delimiter (comma)
2. **Missing Sales Detection**: Identifies 2024 sales that might be missing from reports
3. **Directory Setup**: Creates necessary directories if they don't exist

### Example Usage

```bash
# Fix all issues using default settings
python3 fix_issues.py

# Only unify CSV delimiters in specific directories
python3 fix_issues.py --skip-sales --csv-dirs export data/exports

# Only check for missing 2024 sales
python3 fix_issues.py --skip-csv
```

## Error Handling

The application now includes comprehensive error handling across all major components:

1. **API Calls**: All API calls include error handling with retries and fallbacks
2. **Database Operations**: Database errors are properly caught and logged
3. **File Operations**: File read/write operations include proper exception handling
4. **Data Validation**: Input data is validated before processing
5. **CSV Processing**: CSV files are checked for correct format and delimiters

## Debugging Common Issues

### Missing 2024 Sales in Reports

If you notice 2024 sales are missing from your reports:

1. Run the issue fixer with specific focus on sales:
   ```bash
   python3 fix_issues.py --skip-csv
   ```
2. Check the generated `missing_sales_2024.json` file
3. Run the tax calculator again with the correct date range

### CSV Delimiter Issues

If you're experiencing issues with CSV files:

1. Run the issue fixer to standardize delimiters:
   ```bash
   python3 fix_issues.py --skip-sales
   ```
2. Verify the CSV files open correctly in your spreadsheet application

### Database Issues

If the database seems corrupted or incomplete:

1. Run the diagnostic tool with focus on database:
   ```bash
   python3 diagnostic.py --skip-api --skip-data
   ```
2. Check the diagnostic report for specific database issues
3. Consider backing up and rebuilding the database if serious issues are found

## Log Files

### Main Log File

The main application log is stored at `logs/crypto_tax_{date}.log`

### Diagnostic Reports

Diagnostic reports are stored at `logs/diagnostic_report_{timestamp}.json`

### Missing Sales Reports

Missing sales reports are stored at `missing_sales_2024.json`
