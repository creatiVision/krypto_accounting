# crypto_tax_calculator/main.py

"""
Main entry point for the German Crypto Tax Calculator.

Orchestrates the process:
1. Parses command-line arguments (tax year).
2. Loads configuration.
3. Fetches transaction data from Kraken.
4. Processes transactions (maps types, gets prices, applies FIFO).
5. Aggregates results.
6. Generates output reports (CSV, FIFO proof, console, Google Sheets).
"""

import argparse
import csv
import sys
import traceback
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import List, Dict, Any

# Import modules from the package
from . import config
from . import kraken_api
from . import price_api
from . import fifo
from . import tax_rules
from . import reporting
from .models import Transaction, TaxReportEntry, AggregatedTaxSummary, MatchedLotInfo # Import necessary models

# --- Global Variables / Setup ---
# Setup basic logging (can be replaced with a more robust logging setup later)
LOG_DATA: List[List[str]] = []

def log_event(event: str, details: str) -> None:
    """Basic logging function."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    LOG_DATA.append([timestamp, event, details])
    print(f"[{timestamp}] {event}: {details}")

# --- Helper Functions ---
def parse_arguments() -> argparse.Namespace:
    """Parses command-line arguments."""
    parser = argparse.ArgumentParser(description="German Crypto Tax Calculator for Kraken data.")
    parser.add_argument(
        "tax_year",
        type=int,
        nargs='?', # Makes the argument optional
        default=datetime.now().year - 1, # Default to previous year
        help="The tax year to process (e.g., 2023). Defaults to the previous year."
    )
    # Add other arguments if needed (e.g., --config-file, --output-dir)
    args = parser.parse_args()

    # Validate tax year
    current_year = datetime.now().year
    if args.tax_year < 2010 or args.tax_year > current_year:
         print(f"[WARN] Tax year {args.tax_year} seems unusual. Please verify.")
         # Allow proceeding but warn the user

    return args

def normalize_kraken_asset(kraken_asset: str) -> str:
    """Converts Kraken's asset representation (e.g., XXBT, ZEUR) to a more standard form (e.g., BTC, EUR)."""
    # Simple normalization, might need refinement based on all possible Kraken assets
    asset_upper = kraken_asset.upper()
    if asset_upper.startswith('X') and len(asset_upper) > 3:
        return asset_upper[1:]
    if asset_upper.startswith('Z') and len(asset_upper) > 3:
        return asset_upper[1:]
    # Handle specific cases like XBT -> BTC if needed
    if asset_upper == 'XBT':
        return 'BTC'
    return asset_upper # Return as is if no rule matches

def is_fiat_currency(asset: str) -> bool:
    """Determines if an asset is a fiat currency."""
    # Add more fiat currencies as needed
    return asset.upper() in ['EUR', 'USD', 'GBP', 'JPY', 'CAD', 'AUD', 'CHF']

def process_transactions(kraken_trades: List[Dict[str, Any]], kraken_ledger: List[Dict[str, Any]], tax_year: int, cfg: Dict[str, Any]) -> List[TaxReportEntry]:
    """
    Processes raw Kraken data, applies FIFO, and generates detailed tax report entries.
    """
    log_event("Processing", f"Starting transaction processing for tax year {tax_year}...")
    all_raw_transactions = kraken_trades + kraken_ledger
    if not all_raw_transactions:
        log_event("Processing Warning", "No raw transactions found from Kraken API.")
        return []

    # Sort all transactions chronologically
    all_raw_transactions.sort(key=lambda x: x.get("time", 0))
    log_event("Processing", f"Sorted {len(all_raw_transactions)} raw transactions.")

    fifo_calc = fifo.FifoCalculator()
    report_entries: List[TaxReportEntry] = []
    processed_refids = set() # Avoid double processing ledger/trade overlap if any
    line_num = 1

    # --- Stage 1: Convert raw data to standardized Transaction objects and get EUR values ---
    standardized_txs: List[Transaction] = []
    # Group related transactions by pair and close timestamp
    # This helps to identify pairs like ETHZ/ETH that represent the same logical transaction
    grouped_txs = {}
    
    for raw_tx in all_raw_transactions:
        refid = raw_tx.get("refid")
        if not refid or refid in processed_refids:
            continue
        
        # Special handling for different transaction types
        asset = raw_tx.get("asset", "")
        kraken_type = raw_tx.get("type", "unknown").lower()
        
        # Filter out non-taxable transactions - only skip EUR transactions and wrapper token conversions
        # Do NOT skip crypto purchases, sales, or any crypto-related transactions
        
        # Special case 1: EUR/fiat currency spend/withdrawals are not relevant for crypto tax
        if is_fiat_currency(asset) and kraken_type.lower() in ["spend", "withdrawal"]:
            processed_refids.add(refid)
            log_event("Processing", f"Skipping fiat payment transaction: {refid} ({kraken_type} {asset})")
            continue
            
        # Special case 2: Skip all EUR transactions that are in the ledger only 
        # (crypto purchases should appear in trades data)
        if is_fiat_currency(asset):
            processed_refids.add(refid)
            log_event("Processing", f"Skipping fiat currency transaction: {refid} ({kraken_type} {asset})")
            continue
            
        # Special case 2: Skip wrapper token conversions (handled separately)
        if asset.endswith("Z") and asset != "XTZ":
            processed_refids.add(refid)
            log_event("Processing", f"Skipping wrapper token transaction: {refid} ({kraken_type} {asset})")
            continue

        timestamp = int(float(raw_tx.get("time", 0)))
        kraken_type = raw_tx.get("type", "unknown")
        asset_kraken = raw_tx.get("asset", raw_tx.get("pair", "").split('/')[0] if '/' in raw_tx.get("pair", "") else "UNKNOWN")
        amount_str = raw_tx.get("amount", raw_tx.get("vol")) # vol for trades, amount for ledger
        fee_amount_str = raw_tx.get("fee", "0")
        price_str = raw_tx.get("price") # Price per unit in quote currency (trades)
        cost_str = raw_tx.get("cost") # Total cost in quote currency (trades)

        # --- Basic Data Cleaning & Conversion ---
        try:
            amount = Decimal(amount_str) if amount_str is not None else Decimal(0)
            fee_amount = Decimal(fee_amount_str) if fee_amount_str is not None else Decimal(0)
            price = Decimal(price_str) if price_str is not None else None
            cost_or_proceeds = Decimal(cost_str) if cost_str is not None else None
        except Exception as e:
            log_event("Data Error", f"Could not convert amount/fee/price for tx {refid}: {e}. Skipping.")
            continue

        # Determine primary asset and quote asset (if applicable)
        pair = raw_tx.get("pair")
        quote_asset_kraken = None
        if pair and '/' in pair:
            # Assuming format BASE/QUOTE like XXBT/ZEUR
            base_k, quote_k = pair.split('/')
            asset_kraken = base_k
            quote_asset_kraken = quote_k
        elif pair:
             # Assuming format like XXBTZEUR
             # This requires more robust parsing based on known assets
             # Simple guess: last 3 chars are quote? Risky.
             if pair.endswith("EUR"): asset_kraken, quote_asset_kraken = pair[:-3], "ZEUR"
             elif pair.endswith("USD"): asset_kraken, quote_asset_kraken = pair[:-3], "ZUSD"
             # Add more pairs...
             else: asset_kraken = pair # Fallback

        # Normalize asset names
        asset = normalize_kraken_asset(asset_kraken)
        quote_asset = normalize_kraken_asset(quote_asset_kraken) if quote_asset_kraken else None
        # Fee asset needs determination (often the quote currency for trades)
        fee_asset_kraken = raw_tx.get("fee_currency", quote_asset_kraken if kraken_type in ['buy', 'sell'] else asset_kraken) # Guess fee currency
        fee_asset = normalize_kraken_asset(fee_asset_kraken) if fee_asset_kraken else None

        # Create Transaction object (mapping types comes later)
        tx = Transaction(
            refid=refid,
            timestamp=timestamp,
            kraken_type=kraken_type,
            kraken_subtype=raw_tx.get("subtype"), # Ledger specific
            asset=asset,
            amount=abs(amount), # Store absolute amount, sign determined by type
            fee_asset=fee_asset,
            fee_amount=abs(fee_amount),
            pair=pair,
            quote_asset=quote_asset,
            price_per_unit=price,
            cost_or_proceeds=abs(cost_or_proceeds) if cost_or_proceeds else (abs(amount) * price if price else None),
            balance_change=amount if kraken_type not in ['buy', 'sell'] else None # Ledger specific
        )

        # --- Map internal type and tax category ---
        # Special handling for EUR/fiat transactions - they're not taxable when received
        is_fiat = is_fiat_currency(asset)
        
        # Refined mapping based on context
        tx.internal_type = tax_rules.map_kraken_type(tx.kraken_type, tx.kraken_subtype)
        
        # Adjust transaction type and category for fiat currency transactions
        if is_fiat:
            if tx.internal_type == tax_rules.InternalTransactionType.TRADE:
                # EUR received from selling crypto should be NON_TAXABLE
                tx.internal_type = tax_rules.InternalTransactionType.NON_TAXABLE_FIAT
            elif tx.kraken_type.lower() == "deposit":
                # Explicitly mark deposits as non-taxable
                tx.internal_type = tax_rules.InternalTransactionType.DEPOSIT
        
        # Determine tax category based on possibly adjusted internal type
        tx.tax_category = tax_rules.determine_tax_category(tx.internal_type)

        # Adjust amount sign based on type (important!)
        if tx.internal_type in [tax_rules.InternalTransactionType.SELL, tax_rules.InternalTransactionType.WITHDRAWAL, tax_rules.InternalTransactionType.SPEND, tax_rules.InternalTransactionType.FEE_PAYMENT, tax_rules.InternalTransactionType.GIFT_SENT]:
            tx.amount = -abs(tx.amount) # Negative for disposals/outflows
        elif tx.internal_type in [tax_rules.InternalTransactionType.BUY, tax_rules.InternalTransactionType.DEPOSIT, tax_rules.InternalTransactionType.STAKING_REWARD, tax_rules.InternalTransactionType.LENDING_REWARD, tax_rules.InternalTransactionType.MINING_REWARD, tax_rules.InternalTransactionType.AIRDROP, tax_rules.InternalTransactionType.GIFT_RECEIVED, tax_rules.InternalTransactionType.NON_TAXABLE_FIAT]:
            tx.amount = abs(tx.amount) # Positive for acquisitions/inflows
        # Trades need special handling (sell quote, buy base or vice versa) - handled below

        # --- Get EUR values ---
        tx_dt = datetime.fromtimestamp(timestamp, timezone.utc)
        current_price_eur = None
        fee_price_eur = None

        # Value of the main asset transaction
        if tx.quote_asset == 'EUR':
            tx.value_eur = tx.cost_or_proceeds
            current_price_eur = tx.price_per_unit # Price was already in EUR
        elif tx.internal_type in [tax_rules.InternalTransactionType.STAKING_REWARD, tax_rules.InternalTransactionType.LENDING_REWARD, tax_rules.InternalTransactionType.MINING_REWARD, tax_rules.InternalTransactionType.AIRDROP]:
             # Value rewards/income at time of receipt
             current_price_eur = price_api.get_historical_price_eur(tx.asset, timestamp)
             if current_price_eur is not None:
                  tx.value_eur = abs(tx.amount) * Decimal(str(current_price_eur))
             else:
                  tx.notes.append(f"FEHLER: Konnte EUR-Preis für {tx.asset} am {tx_dt.strftime('%Y-%m-%d')} nicht ermitteln (Ref: {refid}). Manuelle Prüfung erforderlich.")
                  log_event("Price Error", f"Failed to get price for reward/income {tx.asset} (Ref: {refid})")
        elif tx.internal_type in [tax_rules.InternalTransactionType.BUY, tax_rules.InternalTransactionType.SELL, tax_rules.InternalTransactionType.TRADE, tax_rules.InternalTransactionType.SPEND]:
             # Value trades/spends based on quote currency value if possible, or fetch base asset price
             if tx.quote_asset:
                  quote_price_eur = price_api.get_historical_price_eur(tx.quote_asset, timestamp)
                  if quote_price_eur is not None and tx.cost_or_proceeds is not None:
                       tx.value_eur = tx.cost_or_proceeds * quote_price_eur
                       # Estimate base price if not directly available
                       if tx.amount != 0:
                            current_price_eur = tx.value_eur / abs(tx.amount)
                  else:
                       # Fallback: try getting base asset price directly
                       current_price_eur = price_api.get_historical_price_eur(tx.asset, timestamp)
                       if current_price_eur is not None:
                            tx.value_eur = abs(tx.amount) * current_price_eur
                       else:
                            tx.notes.append(f"FEHLER: Konnte EUR-Preis weder für {tx.asset} noch für {tx.quote_asset} am {tx_dt.strftime('%Y-%m-%d')} ermitteln (Ref: {refid}). Manuelle Prüfung erforderlich.")
                            log_event("Price Error", f"Failed to value trade/spend {tx.asset}/{tx.quote_asset} (Ref: {refid})")
             else: # Should not happen for trades, maybe ledger 'spend'?
                  current_price_eur = price_api.get_historical_price_eur(tx.asset, timestamp)
                  if current_price_eur is not None:
                       tx.value_eur = abs(tx.amount) * Decimal(str(current_price_eur))
                  else:
                       tx.notes.append(f"FEHLER: Konnte EUR-Preis für {tx.asset} am {tx_dt.strftime('%Y-%m-%d')} nicht ermitteln (Ref: {refid}). Manuelle Prüfung erforderlich.")
                       log_event("Price Error", f"Failed to value ledger entry {tx.asset} (Ref: {refid})")

        # Value of the fee
        if tx.fee_amount > 0 and tx.fee_asset:
            fee_price_eur = price_api.get_historical_price_eur(tx.fee_asset, timestamp)
            if fee_price_eur is not None:
                tx.fee_value_eur = tx.fee_amount * Decimal(str(fee_price_eur))
            else:
                tx.notes.append(f"FEHLER: Konnte EUR-Preis für Gebühr ({tx.fee_asset}) am {tx_dt.strftime('%Y-%m-%d')} nicht ermitteln (Ref: {refid}). Manuelle Prüfung erforderlich.")
                log_event("Price Error", f"Failed to value fee {tx.fee_asset} (Ref: {refid})")

        # Store price used for main asset value calculation if found
        if current_price_eur is not None:
             # Store the price per unit used for the main asset valuation
             tx.price_per_unit = current_price_eur # Overwrite if fetched

        standardized_txs.append(tx)
        processed_refids.add(refid)

    log_event("Processing", f"Standardized {len(standardized_txs)} transactions and attempted EUR valuation.")

    # --- Stage 2: Process transactions chronologically using FIFO ---
    # Filter transactions for the specific tax year *after* standardization and valuation
    # We need prior years' data to establish correct cost basis.
    # The FIFO calculator handles this internally by storing all lots.

    # We generate report entries only for events *within* the tax year.
    for tx in standardized_txs:
        # Skip if value could not be determined (critical error)
        if tx.value_eur is None and tx.internal_type not in [tax_rules.InternalTransactionType.DEPOSIT, tax_rules.InternalTransactionType.WITHDRAWAL, tax_rules.InternalTransactionType.TRANSFER_INTERNAL]:
             log_event("Processing Skip", f"Skipping tx {tx.refid} due to missing EUR value.")
             # TODO: Optionally create a report entry marking the error?
             continue

        # --- Handle Acquisitions ---
        if tax_rules.is_acquisition(tx.internal_type):
            price_eur = tx.value_eur / abs(tx.amount) if tx.amount != 0 else Decimal(0)
            is_income = tx.tax_category == tax_rules.TransactionCategory.OTHER_INCOME
            fifo_calc.add_purchase(
                asset=tx.asset,
                amount=abs(tx.amount),
                price_eur=price_eur,
                timestamp=tx.timestamp,
                refid=tx.refid,
                source=tx.kraken_type,
                # is_income=is_income # Add this to HoldingLot later
            )
            # Create report entry if tx is within the target tax year
            if tx.datetime_utc.year == tax_year:
                 report_entry = TaxReportEntry( # Use the dedicated model
                      line_num=line_num,
                      tx_refid=tx.refid,
                      tx_datetime=tx.datetime_utc,
                      tx_type=tx.internal_type.name, # Or map to German string
                      tax_category=tx.tax_category.value,
                      asset=tx.asset,
                      amount=abs(tx.amount),
                      acquisition_price_eur_per_unit=price_eur,
                      acquisition_value_eur=tx.value_eur,
                      disposal_fee_eur=tx.fee_value_eur, # Fees can occur on acquisition too
                      notes=" | ".join(tx.notes)
                 )
                 # Mark income specifically
                 if is_income:
                      report_entry.is_taxable = True # Income is generally taxable in year received
                      report_entry.tax_reason = "Sonstige Einkünfte (§22 Nr. 3 EStG)"
                      report_entry.disposal_gain_loss_eur = tx.value_eur # Report income as "gain"

                 report_entries.append(report_entry)
                 line_num += 1

        # --- Handle Disposals ---
        elif tax_rules.is_disposal(tx.internal_type):
            # Disposal price is based on proceeds / amount
            sale_price_eur = tx.value_eur / abs(tx.amount) if tx.amount != 0 else Decimal(0)
            fee_eur = tx.fee_value_eur if tx.fee_value_eur is not None else Decimal(0)

            # Process using FIFO calculator
            disposal_result = fifo_calc.process_disposal(
                asset=tx.asset,
                amount=abs(tx.amount),
                sale_price_eur=sale_price_eur,
                timestamp=tx.timestamp,
                refid=tx.refid,
                fee_eur=fee_eur
            )

            # Create report entry if tx is within the target tax year
            if tx.datetime_utc.year == tax_year:
                 matched_lot_info_list = []
                 fifo_details_parts = []
                 for lot, amount_used in disposal_result.matched_lots:
                      holding_days = (tx.datetime_utc - lot.purchase_datetime).days
                      cost_basis_part = amount_used * lot.purchase_price_eur
                      matched_lot_info_list.append(
                           MatchedLotInfo( # Use the model imported from models.py
                                original_lot_refid=lot.purchase_tx_refid,
                                original_lot_purchase_date=lot.purchase_date_str,
                                original_lot_purchase_price_eur=lot.purchase_price_eur,
                                amount_used=amount_used,
                                cost_basis_eur=cost_basis_part,
                                holding_period_days=holding_days
                           )
                      )
                      # Build detail string part
                      fifo_details_parts.append(
                           f"Lot Ref:{lot.purchase_tx_refid} ({amount_used:.8f} @ {lot.purchase_price_eur:.4f}€ from {lot.purchase_date_str}, Held:{holding_days}d)"
                      )

                 # Determine tax reason string
                 tax_reason = ""
                 if disposal_result.taxable_status:
                      if disposal_result.gain_loss_eur >= 0:
                           tax_reason = "Haltedauer <= 1 Jahr, steuerpflichtig"
                      else:
                           tax_reason = "Verlust Haltedauer <= 1 Jahr, verrechenbar"
                 else:
                      if disposal_result.gain_loss_eur >= 0:
                           tax_reason = "Haltedauer > 1 Jahr, steuerfrei"
                      else:
                           tax_reason = "Verlust Haltedauer > 1 Jahr, nicht verrechenbar" # Check German rules for long-term losses

                 report_entry = TaxReportEntry(
                      line_num=line_num,
                      tx_refid=tx.refid,
                      tx_datetime=tx.datetime_utc,
                      tx_type=tx.internal_type.name, # Or map to German string
                      tax_category=tx.tax_category.value,
                      asset=tx.asset,
                      amount=abs(tx.amount), # Report positive amount disposed
                      disposal_proceeds_eur=disposal_result.total_proceeds_eur,
                      disposal_cost_basis_eur=disposal_result.total_cost_basis_eur,
                      disposal_fee_eur=disposal_result.fee_eur,
                      disposal_gain_loss_eur=disposal_result.gain_loss_eur,
                      matched_lots_info=matched_lot_info_list,
                      holding_period_days_avg=disposal_result.holding_period_days_avg,
                      is_long_term=(disposal_result.holding_period_days_avg > tax_rules.HOLDING_PERIOD_DAYS if disposal_result.holding_period_days_avg is not None else None),
                      is_taxable=disposal_result.taxable_status if disposal_result.gain_loss_eur > 0 else False, # Only gains are taxable
                      tax_reason=tax_reason,
                      # fifo_details_text=" | ".join(fifo_details_parts), # Use property instead
                      notes=" | ".join(tx.notes + disposal_result.notes)
                 )
                 report_entries.append(report_entry)
                 line_num += 1

        # --- Handle Non-Taxable / Other ---
        # elif tx.internal_type in [tax_rules.InternalTransactionType.DEPOSIT, tax_rules.InternalTransactionType.WITHDRAWAL, tax_rules.InternalTransactionType.TRANSFER_INTERNAL]:
             # Optionally create report entries for these if needed for tracking, but mark as non-taxable
             # if tx.datetime_utc.year == tax_year:
             #      report_entry = TaxReportEntry(...)
             #      report_entries.append(report_entry)
             #      line_num += 1
             # pass # Usually not directly reported unless fees involved

    log_event("Processing", f"Generated {len(report_entries)} report entries for tax year {tax_year}.")
    return report_entries


def aggregate_results(report_entries: List[TaxReportEntry], tax_year: int) -> AggregatedTaxSummary:
    """Aggregates results from report entries for the final summary."""
    summary = AggregatedTaxSummary(
        tax_year=tax_year,
        freigrenze_private_sales=tax_rules.get_freigrenze_private_sales(tax_year),
        freigrenze_other_income=tax_rules.get_freigrenze_other_income(tax_year)
    )

    for entry in report_entries:
        # Aggregate §23 Private Sales
        if entry.tax_category == tax_rules.TransactionCategory.PRIVATE_SALE.value and entry.is_taxable:
            if entry.disposal_gain_loss_eur is not None:
                if entry.disposal_gain_loss_eur > 0:
                    summary.total_private_sale_gains += entry.disposal_gain_loss_eur
                else:
                    summary.total_private_sale_losses += entry.disposal_gain_loss_eur # Add negative loss

        # Aggregate §22 Other Income
        elif entry.tax_category == tax_rules.TransactionCategory.OTHER_INCOME.value:
             # Income is reported as gain in our current structure
             if entry.disposal_gain_loss_eur is not None and entry.disposal_gain_loss_eur > 0:
                  summary.total_other_income += entry.disposal_gain_loss_eur

        # Collect warnings
        if "FEHLER" in entry.notes or "WARNUNG" in entry.notes:
             summary.warnings.append(f"Tx {entry.tx_refid}: {entry.notes}")

    # Recalculate taxable status based on aggregated amounts and Freigrenze
    summary.__post_init__() # Call post_init again to update taxable flags

    log_event("Aggregation", f"Aggregated results: Net Sales(§23)={summary.net_private_sales:.2f}€, Other Income(§22)={summary.total_other_income:.2f}€")
    return summary

# --- Main Execution ---
def run():
    """Main execution function."""
    args = parse_arguments()
    tax_year = args.tax_year
    log_event("Start", f"Crypto Tax Calculator starting for tax year {tax_year}")

    try:
        # 1. Load Config
        cfg = config.load_configuration()
        api_key = cfg.get("KRAKEN_API_KEY")
        api_secret = cfg.get("KRAKEN_API_SECRET")

        # Determine date range for API fetch (needs to cover potential purchase history)
        # Fetch from the beginning of time? Or configurable start?
        # For now, fetch *everything* up to the end of the tax year.
        # WARNING: This can be very slow and data-intensive for long histories!
        # Consider adding a configurable earliest_date or fetching year-by-year iteratively.
        fetch_start_time = int(datetime(2010, 1, 1).timestamp()) # Arbitrary early start
        fetch_end_time = int(datetime(tax_year, 12, 31, 23, 59, 59, tzinfo=timezone.utc).timestamp())
        log_event("API Fetch", f"Fetching data up to {datetime.fromtimestamp(fetch_end_time, timezone.utc)}")

        # 2. Fetch Data
        kraken_trades = kraken_api.get_trades(api_key, api_secret, fetch_start_time, fetch_end_time)
        kraken_ledger = kraken_api.get_ledger(api_key, api_secret, fetch_start_time, fetch_end_time)

        # 3. Process Transactions
        report_entries = process_transactions(kraken_trades, kraken_ledger, tax_year, cfg)

        # 4. Aggregate Results
        summary = aggregate_results(report_entries, tax_year)

        # 5. Generate Reports
        output_dir = Path(__file__).resolve().parent.parent.parent / "export" # export dir in skripts-py/accounting/
        output_files = []

        csv_file = reporting.export_to_csv(report_entries, summary, tax_year, output_dir)
        output_files.append(csv_file)

        fifo_file = reporting.export_fifo_proof(report_entries, summary, tax_year, output_dir)
        output_files.append(fifo_file)

        # Optional Google Sheets Export
        if cfg.get("google_sheets", {}).get("sheet_id"):
             sheets_result = reporting.export_to_google_sheets(report_entries, summary, tax_year, cfg)
             output_files.append(sheets_result) # Append status message

        # Print Console Summary
        reporting.print_console_summary(summary)

        # Export Log Data
        log_file = output_dir / f"log_{tax_year}.csv"
        try:
            log_header = [["Timestamp", "Event", "Details"]]
            with open(log_file, "w", newline="", encoding="utf-8") as csvfile:
                writer = csv.writer(csvfile, delimiter=";", quotechar='"')
                writer.writerows(log_header + LOG_DATA)
            output_files.append(str(log_file))
            log_event("End", f"Log data saved to {log_file}")
        except Exception as e:
            print(f"[ERROR] Failed to export log data: {e}")

        print("\nTax calculation completed.")
        print("Generated files/outputs:")
        for item in output_files:
            print(f"- {item}")

        if summary.warnings:
             print("\n--- Warnings/Errors Encountered During Processing ---")
             for warning in summary.warnings:
                  print(f"- {warning}")
             print("--- Please review warnings carefully! ---")

    except Exception as e:
        log_event("Fatal Error", f"An unhandled exception occurred: {e}")
        print(f"\n[FATAL ERROR] {e}")
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    # This allows running the main script directly, e.g., python -m crypto_tax_calculator.main 2023
    # Assumes the package is installed or PYTHONPATH is set correctly.
    # For development, you might run from the project root: python src/crypto_tax_calculator/main.py 2023
    run()
