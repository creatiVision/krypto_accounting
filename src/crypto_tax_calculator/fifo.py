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

# Import the real price function
from .price_api import get_historical_price_eur

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

    def match_lots(self, asset: str, amount: Decimal, timestamp: int, refid: str) -> List[Tuple[HoldingLot, Decimal]]:
        """Matches lots using FIFO for a given disposal amount."""
        asset_upper = asset.upper()
        remaining_to_dispose = amount
        matched_lots: List[Tuple[HoldingLot, Decimal]] = []
        lots_to_remove_indices: List[int] = []
        partial_lot_update: Optional[Tuple[int, Decimal]] = None

        if asset_upper not in self.holdings or not self.holdings[asset_upper]:
            log_event("FIFO Error", f"Cannot match lots for {amount} {asset_upper} (Ref: {refid}) - No holdings available.")
            return []

        for idx, lot in enumerate(self.holdings[asset_upper]):
            if remaining_to_dispose <= 0:
                break

            amount_from_this_lot = min(remaining_to_dispose, lot.amount)
            matched_lots.append((lot, amount_from_this_lot))

            remaining_to_dispose -= amount_from_this_lot
            remaining_in_lot = lot.amount - amount_from_this_lot

            if remaining_in_lot <= Decimal('1e-12'):
                lots_to_remove_indices.append(idx)
            else:
                partial_lot_update = (idx, remaining_in_lot)
                break

        if partial_lot_update:
            idx, remaining_amount = partial_lot_update
            self.holdings[asset_upper][idx].amount = remaining_amount

        for idx in sorted(lots_to_remove_indices, reverse=True):
            self.holdings[asset_upper].pop(idx)

        return matched_lots

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
            
            # Calculate holding period
            holding_period_days = (disposal_datetime - lot.purchase_datetime).days
            holding_periods_weighted.append((holding_period_days, amount_from_this_lot))
            
            matched_lots_details.append((lot, amount_from_this_lot))
            
            remaining_to_dispose -= amount_from_this_lot
            remaining_in_lot = lot.amount - amount_from_this_lot
            
            if remaining_in_lot <= Decimal('1e-12'):  # Effectively zero
                lots_to_remove_indices.append(idx)
            else:
                partial_lot_update = (idx, remaining_in_lot)
                break
        
        # Update holdings: Apply partial lot update if any
        if partial_lot_update:
            idx, remaining_amount = partial_lot_update
            self.holdings[asset_upper][idx].amount = remaining_amount
        
        # Remove fully used lots (in reverse order to not affect indices)
        for idx in sorted(lots_to_remove_indices, reverse=True):
            self.holdings[asset_upper].pop(idx)
        
        # Verify we've matched all the disposal amount
        if remaining_to_dispose > Decimal('1e-12'):  # More than epsilon
            shortage = remaining_to_dispose
            notes.append(f"WARNING: Insufficient holdings to cover full disposal. Short by {shortage} {asset_upper}")
            log_event("FIFO Warning", f"Disposal of {amount} {asset_upper} (Ref: {refid}) exceeds available holdings by {shortage}")
        
        # Calculate weighted average holding period
        total_weighted_days = Decimal(0)
        total_weight = Decimal(0)
        for days, weight in holding_periods_weighted:
            total_weighted_days += Decimal(days) * weight
            total_weight += weight
        
        avg_holding_period = int(total_weighted_days / total_weight) if total_weight > 0 else 0
        
        # Check if any matched lot was held ≤ 1 year (365 days)
        taxable_status = any(days <= 365 for days, _ in holding_periods_weighted)
        
        # Calculate final values
        total_proceeds_adjusted = total_proceeds - fee_eur  # Adjust for fees
        gain_loss = total_proceeds_adjusted - total_cost_basis
        
        # Create and return the result
        return DisposalResult(
            asset=asset_upper,
            disposed_amount=amount,
            sale_price_eur=sale_price_eur,
            sale_timestamp=timestamp,
            sale_tx_refid=refid,
            fee_eur=fee_eur,
            total_proceeds_eur=total_proceeds,
            total_cost_basis_eur=total_cost_basis,
            gain_loss_eur=gain_loss,
            matched_lots=matched_lots_details,
            taxable_status=taxable_status,
            holding_period_days_avg=avg_holding_period,
            notes=notes
        )
