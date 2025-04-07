import argparse
import datetime
from decimal import Decimal
import logging
from typing import List, Dict, Any

from .models import Transaction, TaxReportEntry, AggregatedTaxSummary, MatchedLotInfo
from .fifo import FifoCalculator
from .kraken_cache import get_trades, get_ledger
from .price_api import get_historical_price_eur
from .tax_rules import calculate_tax_liability
from .tx_classifier import is_sale_transaction

# Set up logging
logging.basicConfig(level=logging.INFO)
log_event = logging.info

def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Process Kraken trades and ledger for tax reporting.")
    parser.add_argument(
        "--tax-year", type=int, default=datetime.datetime.now().year - 1,
        help="The tax year to process (e.g., 2023). Defaults to the previous year."
    )
    parser.add_argument(
        "--export-format", choices=["csv", "excel", "json"], default="csv",
        help="Export format for tax report. Defaults to CSV."
    )
    parser.add_argument(
        "--output-dir", type=str, default="export",
        help="Directory to save output files. Defaults to 'export'."
    )
    parser.add_argument(
        "--verbose", action="store_true",
        help="Enable verbose logging"
    )
    args = parser.parse_args()

    # Validate tax year
    current_year = datetime.datetime.now().year
    if args.tax_year < 2010 or args.tax_year > current_year:
        log_event(f"[WARN] Tax year {args.tax_year} seems unusual. Please verify.")
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
    return asset_upper  # Return as is if no rule matches

def is_fiat_currency(asset: str) -> bool:
    """Determines if an asset is a fiat currency."""
    # Add more fiat currencies as needed
    return asset.upper() in ['EUR', 'USD', 'GBP', 'JPY', 'CAD', 'AUD', 'CHF']

def process_transactions(kraken_trades: List[Dict[str, Any]], kraken_ledger: List[Dict[str, Any]], tax_year: int) -> List[TaxReportEntry]:
    """
    Processes raw Kraken data, applies FIFO, and generates detailed tax report entries.
    """
    log_event(f"Processing: Starting transaction processing for tax year {tax_year}...")
    all_raw_transactions = kraken_trades + kraken_ledger
    if not all_raw_transactions:
        log_event("Processing Warning: No raw transactions found from Kraken API.")
        return []

    # Sort all transactions chronologically
    all_raw_transactions.sort(key=lambda x: x.get("time", 0))
    log_event(f"Processing: Sorted {len(all_raw_transactions)} raw transactions.")

    fifo_calc = FifoCalculator()
    report_entries: List[TaxReportEntry] = []
    processed_refids = set()  # Avoid double processing ledger/trade overlap if any
    line_num = 1

    # --- Stage 1: Convert raw data to standardized Transaction objects and get EUR values ---
    standardized_txs: List[Transaction] = []
    # Group related transactions by pair and close timestamp
    # This helps to identify pairs like ETHZ/ETH that represent the same logical transaction
    grouped_txs: Dict[str, List[Dict[str, Any]]] = {}

    for raw_tx in all_raw_transactions:
        refid = raw_tx.get("refid")
        if not refid or refid in processed_refids:
            continue

        # Special handling for different transaction types
        asset = raw_tx.get("asset", "")
        kraken_type = raw_tx.get("type", "unknown").lower()

        # Identify and process crypto sales (including 'spend' transactions in Kraken ledger)
        if not is_fiat_currency(asset) and is_sale_transaction(raw_tx):
            # This is a crypto sale - keep processing
            log_event("Processing", f"Processing crypto sale transaction: {refid} ({kraken_type} {asset})")
        # Filter out non-taxable transactions
        elif is_fiat_currency(asset) and kraken_type.lower() in ["spend", "withdrawal"]:
            # Special case 1: EUR/fiat currency spend/withdrawals are not relevant for crypto tax
            processed_refids.add(refid)
            log_event("Processing", f"Skipping fiat payment transaction: {refid} ({kraken_type} {asset})")
            continue
        elif is_fiat_currency(asset):
            # Special case 2: Skip all other EUR/fiat transactions in the ledger only
            # (crypto purchases should appear in trades data)
            processed_refids.add(refid)
            log_event("Processing", f"Skipping fiat currency transaction: {refid} ({kraken_type} {asset})")
            continue
        elif asset.endswith("Z") and asset != "XTZ":
            # Special case 3: Skip wrapper token conversions (handled separately)
            processed_refids.add(refid)
            log_event("Processing", f"Skipping wrapper token transaction: {refid} ({kraken_type} {asset})")
            continue

        timestamp = int(float(raw_tx.get("time", 0)))
        kraken_type = raw_tx.get("type", "unknown")
        asset_kraken = raw_tx.get("asset", raw_tx.get("pair", "").split('/')[0] if '/' in raw_tx.get("pair", "") else "UNKNOWN")
        amount_str = raw_tx.get("amount", raw_tx.get("vol"))  # vol for trades, amount for ledger
        fee_amount_str = raw_tx.get("fee", "0")
        price_str = raw_tx.get("price")  # Price per unit in quote currency (trades)
        cost_str = raw_tx.get("cost")  # Total cost in quote currency (trades)

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
            else: asset_kraken = pair  # Fallback

        # Normalize asset names
        asset = normalize_kraken_asset(asset_kraken)
        quote_asset = normalize_kraken_asset(quote_asset_kraken) if quote_asset_kraken else None
        # Fee asset needs determination (often the quote currency for trades)
        fee_asset_kraken = raw_tx.get("fee_currency", quote_asset_kraken if kraken_type in ['buy', 'sell'] else asset_kraken)  # Guess fee currency
        fee_asset = normalize_kraken_asset(fee_asset_kraken) if fee_asset_kraken else None

        # Create Transaction object (mapping types comes later)
        tx = Transaction(
            refid=refid,
            timestamp=timestamp,
            kraken_type=kraken_type,
            kraken_subtype=raw_tx.get("subtype", ""),
            asset=asset,
            amount=amount,
            fee_amount=fee_amount,
            price=price if price is not None else Decimal(0),
            cost_or_proceeds=cost_or_proceeds if cost_or_proceeds is not None else Decimal(0),
            quote_asset=quote_asset if quote_asset is not None else "",
            fee_asset=fee_asset if fee_asset is not None else ""
        )

        standardized_txs.append(tx)

    # --- Stage 2: Apply FIFO and generate tax report entries ---
    for tx in standardized_txs:
        # Include both traditional 'sell' and Kraken ledger 'spend' entries as sales
        if tx.kraken_type in ['buy', 'sell'] or (tx.kraken_type == 'spend' and not is_fiat_currency(tx.asset)):
            matched_lots_raw = fifo_calc.match_lots(
                tx.asset,
                tx.amount,
                tx.timestamp,
                tx.refid
            )
            # Convert matched lots to MatchedLotInfo objects
            matched_lots = []
            for lot, amount_used in matched_lots_raw:
                holding_period_days = (datetime.datetime.utcfromtimestamp(tx.timestamp) - lot.purchase_datetime).days
                matched_lots.append(MatchedLotInfo(
                    refid=tx.refid,
                    timestamp=tx.timestamp,
                    asset=tx.asset,
                    amount=amount_used,
                    cost=amount_used * lot.purchase_price_eur,
                    original_lot_refid=lot.purchase_tx_refid,
                    original_lot_purchase_date=lot.purchase_datetime,
                    original_lot_purchase_price_eur=lot.purchase_price_eur,
                    amount_used=amount_used,
                    cost_basis_eur=amount_used * lot.purchase_price_eur,
                    holding_period_days=holding_period_days
                ))

            tax_liability = calculate_tax_liability(tx, matched_lots)
            report_entry = TaxReportEntry(
                refid=tx.refid,
                timestamp=tx.timestamp,
                asset=tx.asset,
                amount=tx.amount,
                cost_or_proceeds=tx.cost_or_proceeds,
                tax_liability=tax_liability,
                matched_lots=matched_lots
            )
            report_entries.append(report_entry)

    return report_entries

import os
from dotenv import load_dotenv

def main() -> None:
    load_dotenv()  # Load environment variables from .env file
    args = parse_arguments()
    tax_year = args.tax_year
    start_time = int(datetime.datetime(tax_year, 1, 1).timestamp())
    end_time = int(datetime.datetime(tax_year + 1, 1, 1).timestamp())

    api_key = os.getenv("KRAKEN_API_KEY")
    api_secret = os.getenv("KRAKEN_API_SECRET")

    # Check for API keys
    if not api_key or not api_secret:
        log_event("Error: Missing Kraken API keys. Set KRAKEN_API_KEY and KRAKEN_API_SECRET in .env file.")
        return

    kraken_trades = get_trades(api_key, api_secret, start_time, end_time)
    kraken_ledger = get_ledger(api_key, api_secret, start_time, end_time)

    # Process transactions and generate tax report entries
    report_entries = process_transactions(kraken_trades, kraken_ledger, tax_year)

    # Generate and print the aggregated tax summary
    total_tax = Decimal(sum(entry.tax_liability for entry in report_entries))
    total_pl = Decimal(sum(entry.cost_or_proceeds for entry in report_entries))
    
    # Calculate totals by category
    priv_sale_gains = Decimal(sum(entry.disposal_gain_loss_eur for entry in report_entries 
                               if entry.disposal_gain_loss_eur > 0))
    priv_sale_losses = Decimal(sum(entry.disposal_gain_loss_eur for entry in report_entries 
                                if entry.disposal_gain_loss_eur < 0))
    
    aggregated_summary = AggregatedTaxSummary(
        total_tax_liability=total_tax,
        total_profit_loss=total_pl,
        tax_report_entries=report_entries,
        total_private_sale_gains=priv_sale_gains,
        total_private_sale_losses=priv_sale_losses,
        tax_year=tax_year
    )
    
    # Apply Freigrenze rules
    aggregated_summary.update_tax_status()

    # Export report to the selected format
    from .reporting import export_tax_report
    
    output_dir = args.output_dir
    export_format = args.export_format
    
    created_files = export_tax_report(
        aggregated_summary,
        tax_year, 
        output_dir=output_dir, 
        format=export_format
    )
    
    log_event(f"Summary: Total tax liability for {tax_year}: {aggregated_summary.total_tax_liability} EUR")
    log_event(f"Summary: Total profit/loss for {tax_year}: {aggregated_summary.total_profit_loss} EUR")
    log_event(f"Export: Tax report exported to: {', '.join(created_files.values())}")

if __name__ == "__main__":
    main()
