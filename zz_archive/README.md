# German Crypto Tax Calculator

A Python script for calculating taxable gains and losses from cryptocurrency transactions according to German tax law (§23 EStG).

## Features

- Retrieves transaction data from Kraken API
- Calculates gains/losses using FIFO method
- Generates tax reports in CSV format
- Exports data to Google Sheets (if configured)
- Creates detailed FIFO documentation for tax authorities
- Supports German tax rules with holding period exemptions

## Requirements

- Python 3.6+
- Required Python packages:
  - requests
  - google-auth
  - google-auth-oauthlib
  - google-auth-httplib2
  - google-api-python-client

## Installation

1. Clone or download this repository
2. Install required packages:

```bash
pip install requests google-auth google-auth-oauthlib google-auth-httplib2 google-api-python-client
```

3. Configure your API keys in `config.json`

## Configuration

Edit the `config.json` file with your API credentials and preferences:

```json
{
  "API_KEY": "your_kraken_api_key_here",
  "API_SECRET": "your_kraken_api_secret_here",
  "SHEET_ID": "your_google_sheets_id_here",
  "start_date": "2024-01-01",
  "end_date": "2024-12-31",
  "google_sheets": {
    "credentials_file": "path_to_your_credentials_file.json"
  }
}
```

### Kraken API Access

To obtain your Kraken API keys:

1. Log in to your Kraken account
2. Go to Security > API
3. Create a new API key with the following permissions:
   - Query Funds
   - Query Ledger Entries
   - Query Closed Orders & Trades

### Google Sheets Integration

For Google Sheets export:

1. Create a Google Cloud Platform project
2. Enable the Google Sheets API
3. Create a service account and download the JSON credentials file
4. Share your Google Sheets document with the service account email
5. Specify the Sheet ID and path to credentials in config.json

## Usage

Run the script with an optional tax year parameter:

```bash
python crypto_tax_calculator.py [TAX_YEAR]
```

If no tax year is specified, the script will use the previous year.

## Output Files

The script generates several output files in the `export` directory:

- `krypto_steuer_YEAR.csv`: Main tax report with all transactions
- `fifo_nachweis_YEAR.txt`: Detailed FIFO documentation for tax authorities
- `log_YEAR.csv`: Log file with detailed operations performed

## Tax Calculation Logic

The script implements the German tax rules for private sales transactions:

- Gains from assets held less than 1 year (365 days) are taxable
- Gains from assets held more than 1 year are tax-free
- Losses can be offset against taxable gains
- Annual exemption limit of €600 (until 2023) or €1000 (from 2024)

## Disclaimer

This tool is provided for informational purposes only and does not constitute tax advice. Users should consult with a qualified tax professional for specific tax guidance. The authors are not responsible for any errors or omissions, or any actions taken based on the information provided by this script.
