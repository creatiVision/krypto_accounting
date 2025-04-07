# crypto_tax_calculator/fifo.py

"""
Implements the FIFO (First-In, First-Out) accounting method for crypto assets.
Manages holdings and calculates cost basis, gains/losses for disposals.
"""

from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal, getcontext
from typing import List, Dict, Tuple, Optional

# Set precision for Decimal calculations (important for crypto amounts)
getcontext().prec = 18 # Sufficient for most crypto assets

# Placeholder for logging function
def log_event(event: str, details: str):
    print(f"[LOG] {event}: {details}")

# Placeholder for price fetching function (will be imported from price_api)
def get_historical_price_eur(asset: str, timestamp: int) -> Optional[Decimal]:
    # This is a placeholder - the actual function will be imported
    log_event("FIFO Placeholder", f"Placeholder price fetch for {asset} at {timestamp}")
    # Return a dummy value for structure, replace with actual import later
    if asset == "BTC": return Decimal("25000.0")
    if asset == "ETH": return Decimal("1800.0")
    return Decimal("1.0") # Fallback placeholder ONLY

@dataclass
class HoldingLot:
    """Represents a single lot (purchase) of a crypto asset."""
    asset: str
    amount: Decimal
    purchase_price_eur: Decimal # Price per unit in EUR at time of purchase
    purchase_timestamp: int
    purchase_tx_refid: str # Reference ID of the purchase transaction
    source: str = "kraken" # Origin (e.g., 'kraken', 'manual', 'reward')

    # Add a property for purchase date string if needed often
    @property
    def purchase_date_str(self) -> str:
        return datetime.fromtimestamp(self.purchase_timestamp, timezone.utc).strftime("%Y-%m-%d")
        
    @property
    def purchase_datetime(self) -> datetime:
        return datetime.fromtimestamp(self.purchase_timestamp, timezone.utc)

    @property
    def cost_basis_eur(self) -> Decimal:
        return self.amount * self.purchase_price_eur

@dataclass
class DisposalResult:
    """Details about the FIFO calculation for a single disposal."""
    asset: str
    disposed_amount: Decimal
    sale_price_eur: Decimal # Price per unit in EUR at time of sale
    sale_timestamp: int
    sale_tx_refid: str
    fee_eur: Decimal # Fee associated with this disposal
    total_proceeds_eur: Decimal
    total_cost_basis_eur: Decimal
    gain_loss_eur: Decimal
    matched_lots: List[Tuple[HoldingLot, Decimal]] # List of (original_lot, amount_used_from_lot)
    taxable_status: bool = False # True if any part held <= 1 year
    holding_period_days_avg: int = 0 # Average holding period (weighted?)
    notes: List[str] = field(default_factory=list) # Notes, warnings, errors

class FifoCalculator:
    """Manages asset holdings and calculates disposals using FIFO."""

    def __init__(self):
        # Holdings structure: {'ASSET_SYMBOL': [HoldingLot, HoldingLot, ...]}
        self.holdings: Dict[str, List[HoldingLot]] = {}
        log_event("FIFO Init", "FifoCalculator initialized.")

    def add_purchase(self, asset: str, amount: Decimal, price_eur: Decimal, timestamp: int, refid: str, source: str = "kraken"):
        """Adds a new purchase lot to the holdings."""
        asset_upper = asset.upper()
        if amount <= 0:
            log_event("FIFO Warning", f"Attempted to add purchase with non-positive amount for {asset_upper}: {amount}")
            return

        lot = HoldingLot(
            asset=asset_upper,
            amount=amount,
            purchase_price_eur=price_eur,
            purchase_timestamp=timestamp,
            purchase_tx_refid=refid,
            source=source
        )

        if asset_upper not in self.holdings:
            self.holdings[asset_upper] = []

        self.holdings[asset_upper].append(lot)
        # Keep holdings sorted by purchase time for FIFO
        self.holdings[asset_upper].sort(key=lambda x: x.purchase_timestamp)
        # log_event("FIFO Purchase", f"Added {amount} {asset_upper} @ {price_eur:.4f} EUR (Ref: {refid})") # Verbose

    def process_disposal(self, asset: str, amount: Decimal, sale_price_eur: Decimal, timestamp: int, refid: str, fee_eur: Decimal) -> DisposalResult:
        """Processes a disposal (sale, fee payment, etc.) using FIFO."""
        asset_upper = asset.upper()
        disposal_datetime = datetime.fromtimestamp(timestamp, timezone.utc)
        notes = []

        if amount <= 0:
            notes.append(f"Disposal amount is zero or negative ({amount}). Skipping calculation.")
            log_event("FIFO Warning", f"Skipping disposal for {asset_upper} due to non-positive amount: {amount} (Ref: {refid})")
            return DisposalResult(
                asset=asset_upper, disposed_amount=amount, sale_price_eur=sale_price_eur,
                sale_timestamp=timestamp, sale_tx_refid=refid, fee_eur=fee_eur,
                total_proceeds_eur=Decimal(0), total_cost_basis_eur=Decimal(0), gain_loss_eur=Decimal(0),
                matched_lots=[], notes=notes
            )

        total_proceeds = amount * sale_price_eur

        if asset_upper not in self.holdings or not self.holdings[asset_upper]:
            notes.append(f"ERROR: No prior holdings found for {asset_upper} to cover disposal.")
            log_event("FIFO Error", f"Cannot process disposal of {amount} {asset_upper} (Ref: {refid}) - No holdings available.")
            return DisposalResult(
                asset=asset_upper, disposed_amount=amount, sale_price_eur=sale_price_eur,
                sale_timestamp=timestamp, sale_tx_refid=refid, fee_eur=fee_eur,
                total_proceeds_eur=total_proceeds, total_cost_basis_eur=Decimal(0), gain_loss_eur=total_proceeds - fee_eur, # Treat cost basis as 0? Or mark as error?
                matched_lots=[], notes=notes
            )

        remaining_to_dispose = amount
        total_cost_basis = Decimal(0)
        matched_lots_details: List[Tuple[HoldingLot, Decimal]] = []
        lots_to_remove_indices: List[int] = []
        partial_lot_update: Optional[Tuple[int, Decimal]] = None # (index, remaining_amount_in_lot)
        holding_periods_weighted: List[Tuple[int, Decimal]] = [] # (days, amount)

        # Iterate through sorted holdings (FIFO)
        for idx, lot in enumerate(self.holdings[asset_upper]):
            if remaining_to_dispose <= 0:
                break

            amount_from_this_lot = min(remaining_to_dispose, lot.amount)
            cost_basis_from_this_lot = amount_from_this_lot * lot.purchase_price_eur
            total_cost_basis += cost_basis_from_this_lot

            # Store details of the match
            matched_lots_details.append((lot, amount_from_this_lot))

            # Calculate holding period for this portion
            purchase_datetime = datetime.fromtimestamp(lot.purchase_timestamp, timezone.utc)
            holding_days = (disposal_datetime - purchase_datetime).days
            holding_periods_weighted.append((holding_days, amount_from_this_lot))

            # Update remaining amounts
            remaining_to_dispose -= amount_from_this_lot
            remaining_in_lot = lot.amount - amount_from_this_lot

            if remaining_in_lot <= Decimal('1e-12'): # Use tolerance for float precision
                lots_to_remove_indices.append(idx)
            else:
                # This lot was partially used, store update info
                partial_lot_update = (idx, remaining_in_lot)
                # Important: Break here if partially used, as we only update one lot per disposal pass
                # Or handle multiple partials? Simpler to update one and let next disposal handle rest.
                # For now, assume we fully consume or partially consume ONE lot at the end.
                break # Stop matching further lots if this one was partially used

        # Check if the full disposal amount was covered
        if remaining_to_dispose > Decimal('1e-12'): # Tolerance
            notes.append(f"WARNING: Insufficient holdings to cover full disposal of {amount} {asset_upper}. Short by {remaining_to_dispose:.8f}.")
            log_event("FIFO Warning", f"Insufficient holdings for disposal {refid}. Short by {remaining_to_dispose} {asset_upper}")
            # Adjust disposed amount to what was actually covered?
            # amount = amount - remaining_to_dispose # This changes proceeds, maybe not right.
            # Report based on original amount, but note the shortfall.

        # Update the holdings list
        # 1. Apply partial update if any
        if partial_lot_update:
            idx, remaining_amount = partial_lot_update
            self.holdings[asset_upper][idx].amount = remaining_amount
            # log_event("FIFO Update", f"Partially used lot {self.holdings[asset_upper][idx].purchase_tx_refid}: {remaining_amount} {asset_upper} remaining.") # Verbose

        # 2. Remove fully consumed lots (in reverse order to avoid index issues)
        for idx in sorted(lots_to_remove_indices, reverse=True):
            removed_lot = self.holdings[asset_upper].pop(idx)
            # log_event("FIFO Update", f"Fully consumed lot {removed_lot.purchase_tx_refid} ({removed_lot.amount} {asset_upper}).") # Verbose


        # Calculate final results
        gain_loss = total_proceeds - total_cost_basis - fee_eur

        # Determine taxability (based on German 1-year rule)
        # TODO: Import HOLDING_PERIOD_DAYS from tax_rules module
        holding_period_limit = 365
        is_taxable = any(days <= holding_period_limit for days, _ in holding_periods_weighted)

        # Calculate average holding period (weighted by amount)
        total_days_weighted = sum(days * amt for days, amt in holding_periods_weighted)
        total_amount_matched = sum(amt for _, amt in holding_periods_weighted)
        avg_holding_days = int(total_days_weighted / total_amount_matched) if total_amount_matched > 0 else 0

        return DisposalResult(
            asset=asset_upper,
            disposed_amount=amount, # Report original intended amount
            sale_price_eur=sale_price_eur,
            sale_timestamp=timestamp,
            sale_tx_refid=refid,
            fee_eur=fee_eur,
            total_proceeds_eur=total_proceeds,
            total_cost_basis_eur=total_cost_basis,
            gain_loss_eur=gain_loss,
            matched_lots=matched_lots_details,
            taxable_status=is_taxable,
            holding_period_days_avg=avg_holding_days,
            notes=notes
        )

    def get_holdings_summary(self) -> Dict[str, Dict[str, Decimal]]:
        """Returns a summary of current holdings."""
        summary = {}
        for asset, lots in self.holdings.items():
            total_amount = sum(lot.amount for lot in lots)
            if total_amount > Decimal('1e-12'): # Only report if holding exists
                avg_cost = sum(lot.cost_basis_eur for lot in lots) / total_amount if total_amount else Decimal(0)
                summary[asset] = {"amount": total_amount, "average_cost_eur": avg_cost}
        return summary

# Example usage (for testing this module directly)
if __name__ == "__main__":
    print("Testing FIFO Calculator module...")
    calc = FifoCalculator()

    # Scenario: Buy BTC, Buy more BTC, Sell some BTC
    ts1 = int(datetime(2022, 1, 10).timestamp())
    ts2 = int(datetime(2022, 5, 15).timestamp())
    ts3 = int(datetime(2023, 3, 20).timestamp()) # More than 1 year after ts1, less than 1 year after ts2

    print("\nAdding Purchases...")
    calc.add_purchase("BTC", Decimal("0.5"), Decimal("35000.0"), ts1, "BUY001")
    calc.add_purchase("BTC", Decimal("0.3"), Decimal("40000.0"), ts2, "BUY002")

    print("\nCurrent Holdings:")
    print(calc.get_holdings_summary())

    print("\nProcessing Sale...")
    # Sell 0.6 BTC (0.5 from first lot, 0.1 from second lot)
    sale_price = Decimal("45000.0")
    sale_fee = Decimal("25.0")
    disposal_result = calc.process_disposal("BTC", Decimal("0.6"), sale_price, ts3, "SELL001", sale_fee)

    print("\nDisposal Result:")
    print(f"  Asset: {disposal_result.asset}")
    print(f"  Amount Sold: {disposal_result.disposed_amount}")
    print(f"  Sale Price: {disposal_result.sale_price_eur:.2f} EUR")
    print(f"  Proceeds: {disposal_result.total_proceeds_eur:.2f} EUR")
    print(f"  Cost Basis: {disposal_result.total_cost_basis_eur:.2f} EUR")
    print(f"  Fee: {disposal_result.fee_eur:.2f} EUR")
    print(f"  Gain/Loss: {disposal_result.gain_loss_eur:.2f} EUR")
    print(f"  Avg Holding (days): {disposal_result.holding_period_days_avg}")
    print(f"  Taxable: {disposal_result.taxable_status}")
    print(f"  Notes: {disposal_result.notes}")
    print("  Matched Lots:")
    for lot, amount_used in disposal_result.matched_lots:
        holding_days = (datetime.fromtimestamp(ts3, timezone.utc) - datetime.fromtimestamp(lot.purchase_timestamp, timezone.utc)).days
        print(f"    - Used {amount_used:.8f} from Lot {lot.purchase_tx_refid} (Purchased {lot.purchase_date_str} @ {lot.purchase_price_eur:.2f} EUR, Held {holding_days} days)")

    print("\nHoldings After Sale:")
    print(calc.get_holdings_summary())

    # Scenario: Sell more than available
    print("\nProcessing Sale (Insufficient Holdings)...")
    disposal_result_err = calc.process_disposal("BTC", Decimal("1.0"), sale_price, ts3 + 86400, "SELL002", sale_fee)
    print("\nDisposal Result (Error):")
    print(f"  Gain/Loss: {disposal_result_err.gain_loss_eur:.2f} EUR")
    print(f"  Notes: {disposal_result_err.notes}")
    print("\nHoldings After Error Sale:")
    print(calc.get_holdings_summary()) # Should be unchanged from previous summary
