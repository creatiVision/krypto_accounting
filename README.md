# German Crypto Tax Calculator (Kraken)

This Python application calculates cryptocurrency capital gains and income according to German tax law (§23 EStG and §22 Nr. 3 EStG), specifically using transaction data fetched from the Kraken exchange API.

## Purpose

The goal is to automate the process of generating tax-relevant reports for cryptocurrency activities on Kraken, suitable for inclusion in a German tax declaration. It handles:

*   **Private Sales (§23 EStG):** Calculates gains/losses from selling or spending crypto using the First-In, First-Out (FIFO) method. It correctly applies the 1-year holding period for tax exemption and the relevant `Freigrenze` (€600 until 2023, €1000 from 2024 onwards).
*   **Other Income (§22 Nr. 3 EStG):** Identifies income from staking, lending, or potentially other reward mechanisms, valuing them at the time of receipt. It considers the separate `Freigrenze` of €256 for this income type.
*   **Fees:** Accounts for transaction fees, treating fees paid in crypto as taxable disposal events.

## Calculation Logic

1.  **Data Fetching:** Retrieves complete trade history and ledger entries from the Kraken API for the user's account up to the end of the specified tax year.
2.  **Transaction Standardization:** Converts raw Kraken data into a standardized `Transaction` format, normalizing asset names (e.g., XXBT -> BTC).
3.  **Type Mapping:** Maps Kraken's transaction types (e.g., `buy`, `sell`, `trade`, `staking`, `withdrawal`) to internal categories (`BUY`, `SELL`, `STAKING_REWARD`, `FEE_PAYMENT`, etc.) defined in `tax_rules.py`.
4.  **Valuation:** Determines the EUR value of each transaction and associated fee at the time it occurred.
    *   Uses direct EUR pair data from Kraken if available.
    *   Otherwise, fetches the historical daily price in EUR from the CoinGecko API for the relevant asset(s).
    *   **Important:** If a price cannot be determined, an error is logged, and the transaction might be skipped or flagged for manual review in the report.
5.  **FIFO Processing:**
    *   Maintains a record of all acquired crypto lots (purchases, rewards) sorted by date (`fifo.py`).
    *   When a disposal event occurs (sell, spend, fee paid in crypto):
        *   Matches the disposed amount against the oldest available acquisition lots (FIFO).
        *   Calculates the cost basis based on the purchase price of the consumed lots.
        *   Determines the holding period for each consumed portion.
        *   Calculates the capital gain or loss (Proceeds - Cost Basis - Fees).
        *   Updates the remaining amounts in the holding lots.
6.  **Tax Classification:** For each relevant transaction within the target tax year:
    *   Determines if it falls under §23 EStG (Private Sales) or §22 Nr. 3 EStG (Other Income).
    *   For §23 disposals, checks if the gain is taxable based on the holding period (>1 year = tax-free).
7.  **Aggregation:** Sums up total taxable gains and losses under §23, and total income under §22 for the tax year. Compares these totals against the respective `Freigrenze`.
8.  **Reporting:** Generates output files detailing the calculations and summary.

## Program Workflow (Steps)

The `main.py` script orchestrates the following steps:

1.  **Parse Arguments:** Reads the target `tax_year` from the command line (defaults to the previous year).
2.  **Load Configuration:** Reads API keys and other settings from environment variables (`.env` file) and optionally `config.json` (`config.py`). Validates essential settings.
3.  **Fetch Data:** Calls `kraken_api.py` to get all trades and ledger entries up to the end of the `tax_year`.
4.  **Process Transactions:** Iterates through the fetched data:
    *   Standardizes transactions (`models.py`).
    *   Maps types (`tax_rules.py`).
    *   Gets EUR values (`price_api.py`).
    *   Adds acquisitions to the FIFO calculator (`fifo.py`).
    *   Processes disposals using the FIFO calculator (`fifo.py`).
    *   Generates a `TaxReportEntry` (`models.py`) for each relevant event in the `tax_year`.
5.  **Aggregate Results:** Calculates the overall gains, losses, and income totals for the year (`main.py`, using `AggregatedTaxSummary` from `models.py`).
6.  **Generate Reports:** Calls `reporting.py` to create:
    *   `krypto_steuer_YYYY.csv`: Detailed transaction list for the tax year.
    *   `fifo_nachweis_YYYY.txt`: Text file documenting the FIFO calculations for disposals.
    *   `log_YYYY.csv`: Log of script execution events.
    *   Console output summarizing the results.
    *   (Optional) Google Sheets export.

## Installation and Setup

1.  **Prerequisites:**
    *   Python 3.8 or higher recommended.
    *   `pip` (Python package installer).
    *   `git` (optional, for cloning).

2.  **Clone Repository (Optional):**
    ```bash
    # git clone <repository_url>
    cd skripts-py/accounting
    ```
    (Or navigate to the `skripts-py/accounting` directory if you already have it).

3.  **Create Virtual Environment (Recommended):**
    ```bash
    cd skripts-py/accounting
    python3 -m venv venv
    source venv/bin/activate  # Linux/macOS
    # venv\Scripts\activate    # Windows
    ```

4.  **Install Dependencies:**
    ```bash
    pip install -r requirements.txt
    ```

5.  **Configure API Keys:**
    *   Create a file named `.env` in the `skripts/skripts-py/accounting_kryptos/` directory.
    *   You can copy the provided template file first:
        ```bash
        cp .env.template .env
        ```
    *   Then edit the `.env` file to add your Kraken API key and secret:
        ```dotenv
        KRAKEN_API_KEY=YOUR_KRAKEN_API_KEY_HERE
        KRAKEN_API_SECRET=YOUR_KRAKEN_API_SECRET_HERE
        ```
    *   **Security:** The `.env` file is already included in `.gitignore` to ensure your credentials remain private.

6.  **Configure Google Sheets (Optional):**
    *   If you want to export to Google Sheets:
        *   Follow Google Cloud instructions to create a service account and download its JSON credentials file.
        *   Place the credentials JSON file in the project root directory (the same directory as the `start.sh` script).
        *   Add the following to your `.env` file:
            ```dotenv
            GOOGLE_SHEET_ID=YOUR_TARGET_GOOGLE_SHEET_ID
            GOOGLE_CREDENTIALS_FILE=your-credentials-filename.json
            ```
        *   Make sure the service account has edit permissions on the target Google Sheet.
        *   **Security Note:** Credential files are automatically excluded from Git via `.gitignore` to prevent accidental commits of sensitive information.

## Usage

### Quick Start with Interactive Script

The easiest way to run the application is using the provided `start.sh` script, which offers an interactive, menu-driven interface:

```bash
# Navigate to the project directory
cd skripts/skripts-py/accounting_kryptos

# Make sure the script is executable
chmod +x start.sh

# Run the interactive script
./start.sh
```

The script will guide you through:
1. Choosing between Online mode (using real Kraken API data) or Offline Demo mode (using mock data)
2. Entering the tax year to analyze
3. Selecting the export format (CSV, Excel, or JSON)
4. Choosing the output directory
5. Enabling/disabling verbose logging
6. Additional options for mock data (if using Offline Demo mode)

### Manual Execution

Alternatively, you can run the main script directly from within the activated virtual environment:

```bash
# Navigate to the project directory if not already there
cd skripts/skripts-py/accounting_kryptos

# Activate the virtual environment
source venv/bin/activate

# Run for tax year 2023
python -m src.crypto_tax_calculator.main --tax-year 2023

# Run for the previous year (default)
python -m src.crypto_tax_calculator.main
```

The output files will be generated in the selected output directory (default: `export/`). A summary will also be printed to the console.

## Export Formats

The application supports multiple export formats to accommodate different use cases:

### CSV Export (Default)

The default export format generates CSV files that are compatible with most spreadsheet applications:
- `krypto_steuer_YYYY.csv`: Detailed transaction list for the tax year
- `fifo_nachweis_YYYY.txt`: Text file documenting the FIFO calculations for disposals

### JSON Export

For programmatic access or integration with other systems, the application can export data in JSON format:

```bash
# Using the export_as_json.py utility
python export_as_json.py --tax-year 2024 --output-dir export/
```

The JSON export includes:
- Complete tax summary (gains, losses, income)
- Detailed transaction entries
- FIFO calculation details (matched lots)
- Holding period information

This format is particularly useful for:
- Importing into custom analysis tools
- Integration with tax software
- Data visualization applications
- Long-term archiving in a structured format

## Maintenance Utilities

The application includes several utility scripts to help maintain the system:

### Cache Management

```bash
# Flush the price cache only
python flush_cache.py

# Flush all caches and database
./flush_all.sh

# Remove test data
./remove_test_data.sh
```

These utilities are useful for:
- Clearing cached price data to force fresh API requests
- Resetting the Kraken API cache database
- Removing test data before production use
- Troubleshooting data inconsistencies

## Diagnostic Tools

The application includes a comprehensive diagnostic system to help identify and resolve issues:

```bash
# Run full diagnostics
python diagnostic.py

# Run specific diagnostic tests
python diagnostic.py --skip-api --assets BTC ETH
```

The diagnostic tool performs:
1. **API Connection Tests**: Verifies connectivity to price APIs
2. **Database Integrity Checks**: Validates the structure and integrity of the cache database
3. **Data Consistency Validation**: Checks for inconsistencies in cached price data
4. **Performance Monitoring**: Measures and reports on API response times

A detailed diagnostic report is generated in JSON format in the `logs/` directory, providing a comprehensive overview of the system's health.

## Disclaimer

This tool is provided for informational purposes only and does not constitute tax advice. Tax laws are complex and subject to change. Always consult with a qualified tax professional for advice specific to your situation. The accuracy of the calculations depends on the completeness and correctness of the data provided by the Kraken API and the CoinGecko API.
