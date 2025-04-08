import argparse
import datetime
from decimal import Decimal
import logging
import os
from typing import List, Dict, Any

from .models import Transaction, TaxReportEntry, AggregatedTaxSummary, MatchedLotInfo
from .fifo import FifoCalculator
from .kraken_cache import get_trades, get_ledger
from .price_api import get_historical_price_eur
from .tax_rules import calculate_tax_liability
from .tx_classifier import is_sale_transaction
from .logging_utils import log_warning, log_error

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

def is_fiat_currency(asset: str) -> bool:
    """Determines if an asset is a fiat currency."""
    # Kraken prefixes fiat currencies with 'Z'
    return asset.upper() in ['ZEUR', 'ZUSD', 'ZGBP', 'ZJPY', 'ZCAD', 'ZAUD', 'ZCHF']

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
            log_event("Processing: Processing crypto sale transaction: %s (%s %s)", refid, kraken_type, asset)
        # Filter out non-taxable transactions
        elif is_fiat_currency(asset) and kraken_type.lower() in ["spend", "withdrawal"]:
            # Special case 1: EUR/fiat currency spend/withdrawals are not relevant for crypto tax
            processed_refids.add(refid)
            log_event("Skipping fiat payment transaction: %s (%s %s)", refid, kraken_type, asset)
            continue
        elif is_fiat_currency(asset):
            # Special case 2: Skip all other EUR/fiat transactions in the ledger only
            # (crypto purchases should appear in trades data)
            processed_refids.add(refid)
            log_event("Skipping fiat currency transaction: %s (%s %s)", refid, kraken_type, asset)
            continue
        elif asset.endswith("Z") and asset != "XTZ":
            # Special case 3: Skip wrapper token conversions (handled separately)
            processed_refids.add(refid)
            log_event("Skipping wrapper token transaction: %s (%s %s)", refid, kraken_type, asset)
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

        # Use original Kraken asset IDs
        asset = asset_kraken
        quote_asset = quote_asset_kraken
        # Fee asset needs determination (often the quote currency for trades)
        fee_asset_kraken = raw_tx.get("fee_currency", quote_asset_kraken if kraken_type in ['buy', 'sell'] else asset_kraken)  # Guess fee currency
        fee_asset = fee_asset_kraken

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

    # --- Stage 2: First pass - Process purchases and add to FIFO calculator ---
    log_event("Processing: First pass - Adding purchases to FIFO calculator")
    purchase_count = 0
    for tx in standardized_txs:
            # Handle purchases - when amount is positive and it's a buy or deposit transaction
            if tx.amount > 0 and tx.kraken_type in ['buy', 'receive', 'deposit']:
                # Special handling for EUR - always use price of 1.0
                if tx.asset.upper() == 'ZEUR':
                    price_eur = Decimal('1.0')
                else:
                    # Get the price in EUR for non-EUR assets
                    price_eur = tx.price if tx.price and tx.quote_asset == 'ZEUR' else get_historical_price_eur(tx.asset, tx.timestamp)
                
                # Add purchase to FIFO calculator
                fifo_calc.add_purchase(
                    asset=tx.asset,
                    amount=tx.amount,
                    price_eur=price_eur,
                    timestamp=tx.timestamp,
                    refid=tx.refid,
                    source="kraken"
                )
                purchase_count += 1
    
    log_event(f"Processing: Added {purchase_count} purchases to FIFO calculator")
    
    # --- Stage 3: Second pass - Process sales and calculate tax info ---
    log_event("Processing: Second pass - Processing sales")
    for tx in standardized_txs:
        # Process sales - negative amount for sell/spend transactions
        if (tx.amount < 0 and (tx.kraken_type in ['sell', 'spend'] or 
                is_sale_transaction({"type": tx.kraken_type, "amount": str(tx.amount)}))):
            # Try to match lots
            matched_lots_raw = fifo_calc.match_lots(
                tx.asset,
                abs(tx.amount),  # Use absolute value for matching
                tx.timestamp,
                tx.refid
            )
            
            # If no lots were matched, try to fetch more data for this asset
            if not matched_lots_raw:
                log_event(f"FIFO Error: Cannot match lots for {abs(tx.amount)} {tx.asset} (Ref: {tx.refid}) - Attempting to fetch more data")
                
                # Make explicit calls to fetch all possible buys for this asset
                # Get the earliest possible date (e.g., 2010-01-01)
                earliest_date = int(datetime.datetime(2010, 1, 1).timestamp())
                
                # Get the API key and secret
                api_key = os.getenv("KRAKEN_API_KEY")
                api_secret = os.getenv("KRAKEN_API_SECRET")
                
                if api_key and api_secret:
                    # Fetch all trades for this asset
                    log_event(f"Fetching all trades for {tx.asset} from 2010-01-01")
                    additional_trades = get_trades(api_key, api_secret, earliest_date, tx.timestamp, is_recovery_call=True)
                    
                    # Fetch all ledger entries for this asset
                    log_event(f"Fetching all ledger entries for {tx.asset} from 2010-01-01")
                    additional_ledger = get_ledger(api_key, api_secret, earliest_date, tx.timestamp, is_recovery_call=True)
                    
                    # Process additional data
                    additional_txs = []
                    for raw_tx in additional_trades + additional_ledger:
                        # Skip already processed transactions
                        refid = raw_tx.get("refid")
                        if not refid or refid in processed_refids:
                            continue
                        
                        # Only process transactions for this asset
                        asset_kraken = raw_tx.get("asset", raw_tx.get("pair", "").split('/')[0] if '/' in raw_tx.get("pair", "") else "UNKNOWN")
                        if asset_kraken != tx.asset:
                            continue
                        
                        # Process the transaction as before
                        timestamp = int(float(raw_tx.get("time", 0)))
                        kraken_type = raw_tx.get("type", "unknown")
                        amount_str = raw_tx.get("amount", raw_tx.get("vol"))
                        fee_amount_str = raw_tx.get("fee", "0")
                        price_str = raw_tx.get("price")
                        cost_str = raw_tx.get("cost")
                        
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
                            base_k, quote_k = pair.split('/')
                            asset_kraken = base_k
                            quote_asset_kraken = quote_k
                        elif pair:
                            if pair.endswith("EUR"): asset_kraken, quote_asset_kraken = pair[:-3], "ZEUR"
                            elif pair.endswith("USD"): asset_kraken, quote_asset_kraken = pair[:-3], "ZUSD"
                            else: asset_kraken = pair
                        
                        # Use original Kraken asset IDs
                        asset = asset_kraken
                        quote_asset = quote_asset_kraken
                        fee_asset_kraken = raw_tx.get("fee_currency", quote_asset_kraken if kraken_type in ['buy', 'sell'] else asset_kraken)
                        fee_asset = fee_asset_kraken
                        
                        # Create Transaction object
                        additional_tx = Transaction(
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
                        
                        additional_txs.append(additional_tx)
                        processed_refids.add(refid)
                    
                    # Add purchases to FIFO calculator
                    for additional_tx in additional_txs:
                        if additional_tx.amount > 0 and additional_tx.kraken_type in ['buy', 'receive', 'deposit']:
                            # Special handling for EUR - always use price of 1.0
                            if additional_tx.asset.upper() == 'ZEUR':
                                price_eur = Decimal('1.0')
                            else:
                                # Get the price in EUR for non-EUR assets
                                price_eur = additional_tx.price if additional_tx.price and additional_tx.quote_asset == 'ZEUR' else get_historical_price_eur(additional_tx.asset, additional_tx.timestamp)
                            
                            # Add purchase to FIFO calculator
                            fifo_calc.add_purchase(
                                asset=additional_tx.asset,
                                amount=additional_tx.amount,
                                price_eur=price_eur,
                                timestamp=additional_tx.timestamp,
                                refid=additional_tx.refid,
                                source="kraken"
                            )
                            log_event(f"Added additional purchase: {additional_tx.amount} {additional_tx.asset} at {price_eur} EUR")
                    
                    # Try to match lots again
                    matched_lots_raw = fifo_calc.match_lots(
                        tx.asset,
                        abs(tx.amount),
                        tx.timestamp,
                        tx.refid
                    )
            # Convert matched lots to MatchedLotInfo objects
            matched_lots = []
            
            # Get the sale price in EUR
            sale_price_eur = Decimal(0)
            if tx.price is not None and tx.price > 0 and tx.quote_asset == 'ZEUR':
                # Use the price from the transaction if it's in EUR
                sale_price_eur = tx.price
            elif tx.cost_or_proceeds is not None and tx.cost_or_proceeds > 0 and abs(tx.amount) > 0:
                # Calculate the price from the total cost/proceeds
                sale_price_eur = tx.cost_or_proceeds / abs(tx.amount)
            else:
                # Get the historical price from the price API
                sale_price_eur = get_historical_price_eur(tx.asset, tx.timestamp)
                if sale_price_eur is None:
                    # If we can't get the price, use 0
                    sale_price_eur = Decimal(0)
                    log_warning("Price", "MissingSalePriceNotRecovered", 
                               f"Could not recover sale price for {tx.asset} at {tx.timestamp}. Using 0.")
            
            for lot, amount_used in matched_lots_raw:
                disposal_proceeds_eur = amount_used * sale_price_eur
                
                # Handle case when purchase_price_eur is None
                if lot.purchase_price_eur is None:
                    # Try to get the price from another source
                    fallback_price = get_historical_price_eur(tx.asset, lot.purchase_timestamp)
                    if fallback_price is not None:
                        log_warning("Price", "MissingPurchasePriceRecovered", 
                                   f"Recovered missing purchase price for {tx.asset} in lot {lot.purchase_tx_refid} using historical data: {fallback_price} EUR")
                        lot.purchase_price_eur = fallback_price
                    else:
                        # Raise an error if we can't find a price
                        error_msg = f"Missing purchase price for {tx.asset} in lot {lot.purchase_tx_refid} and could not recover from historical data"
                        log_error("Price", "MissingPurchasePrice", error_msg)
                        raise ValueError(error_msg)
                    
                disposal_cost_basis_eur = amount_used * lot.purchase_price_eur
                # Calculate fee in EUR
                fee_in_eur = Decimal(0)
                if tx.fee_amount > 0:
                    if tx.fee_asset == 'ZEUR':
                        fee_in_eur = tx.fee_amount
                    else:
                        # Try to get fee asset price in EUR
                        fee_asset_price = get_historical_price_eur(tx.fee_asset, tx.timestamp)
                        if fee_asset_price is not None:
                            fee_in_eur = tx.fee_amount * fee_asset_price
                
                # Calculate proportional fee for this lot
                lot_fee_proportion = amount_used / abs(tx.amount) if tx.amount != 0 else Decimal(0)
                lot_fee_in_eur = fee_in_eur * lot_fee_proportion
                
                # Calculate gain/loss including fees
                disposal_gain_loss_eur = disposal_proceeds_eur - disposal_cost_basis_eur - lot_fee_in_eur
                
                holding_period_days = (datetime.datetime.fromtimestamp(tx.timestamp, datetime.timezone.utc) - lot.purchase_datetime).days
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
                    holding_period_days=holding_period_days,
                    disposal_proceeds_eur=disposal_proceeds_eur,
                    disposal_cost_basis_eur=disposal_cost_basis_eur,
                    disposal_gain_loss_eur=disposal_gain_loss_eur,
                    disposal_fee_eur=lot_fee_in_eur
                ))

            tax_liability = calculate_tax_liability(tx, matched_lots)
            
            # Calculate total proceeds, cost basis, and gain/loss from all matched lots
            total_proceeds = sum(lot.disposal_proceeds_eur for lot in matched_lots) if matched_lots else tx.cost_or_proceeds
            total_cost_basis = sum(lot.disposal_cost_basis_eur for lot in matched_lots) if matched_lots else Decimal(0)
            total_gain_loss = sum(lot.disposal_gain_loss_eur for lot in matched_lots) if matched_lots else (total_proceeds - total_cost_basis)
            total_fee = sum(lot.disposal_fee_eur for lot in matched_lots) if matched_lots else tx.fee_amount
            
            # Calculate average holding period
            if matched_lots:
                total_weighted_days = sum(lot.holding_period_days * lot.amount_used for lot in matched_lots)
                total_amount = sum(lot.amount_used for lot in matched_lots)
                avg_holding_period = int(total_weighted_days / total_amount) if total_amount > 0 else 0
                is_long_term = all(lot.holding_period_days > 365 for lot in matched_lots)
            else:
                avg_holding_period = 0
                is_long_term = False
            
            report_entry = TaxReportEntry(
                refid=tx.refid,
                timestamp=tx.timestamp,
                asset=tx.asset,
                amount=tx.amount,
                cost_or_proceeds=tx.cost_or_proceeds,
                tax_liability=tax_liability,
                matched_lots=matched_lots,
                disposal_proceeds_eur=total_proceeds,
                disposal_cost_basis_eur=total_cost_basis,
                disposal_gain_loss_eur=total_gain_loss,
                disposal_fee_eur=total_fee,
                holding_period_days_avg=avg_holding_period,
                is_long_term=is_long_term
            )
            report_entries.append(report_entry)

    return report_entries

import os
from dotenv import load_dotenv

def main() -> None:
    load_dotenv()  # Load environment variables from .env file
    args = parse_arguments()
    tax_year = args.tax_year
    
    # Always start looking for data from 2020 onwards
    start_year = 2020
    start_time = int(datetime.datetime(start_year, 1, 1).timestamp())
    end_time = int(datetime.datetime(tax_year + 1, 1, 1).timestamp())
    
    log_event(f"Data Retrieval: Fetching data from {start_year}-01-01 to {tax_year+1}-01-01")

    api_key = os.getenv("KRAKEN_API_KEY")
    api_secret = os.getenv("KRAKEN_API_SECRET")

    # Check for API keys
    if not api_key or not api_secret:
        log_event("Error: Missing Kraken API keys. Set KRAKEN_API_KEY and KRAKEN_API_SECRET in .env file.")
        return

    kraken_trades = get_trades(api_key, api_secret, start_time, end_time)
    kraken_ledger = get_ledger(api_key, api_secret, start_time, end_time)

    log_event(f"Data Retrieval: Retrieved {len(kraken_trades)} trades and {len(kraken_ledger)} ledger entries")

    # Process transactions and generate tax report entries
    report_entries = process_transactions(kraken_trades, kraken_ledger, tax_year)

    # Generate and print the aggregated tax summary
    total_tax = Decimal(sum(entry.tax_liability for entry in report_entries))
    
    # Calculate totals by category
    priv_sale_gains = Decimal(sum(entry.disposal_gain_loss_eur for entry in report_entries 
                               if entry.disposal_gain_loss_eur > 0))
    priv_sale_losses = Decimal(sum(entry.disposal_gain_loss_eur for entry in report_entries 
                                if entry.disposal_gain_loss_eur < 0))
    
    # Calculate total profit/loss correctly
    total_pl = priv_sale_gains + priv_sale_losses
    
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
    log_event(f"Export: Tax report exported to: {created_files.get('year_csv', '')}")

if __name__ == "__main__":
    main()
