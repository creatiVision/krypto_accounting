# crypto_tax_calculator/fifo.py

"""
Implements the FIFO (First-In, First-Out) accounting method for crypto assets.
Manages holdings and calculates cost basis, gains/losses for disposals.
"""

from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal, getcontext
from typing import List, Dict, Tuple, Optional, Any

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
    matched_lots: List[Dict[str, Any]] # List of detailed matched lots
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

        # Ensure timestamp is not in the future
        current_time = int(datetime.now(timezone.utc).timestamp())
        if timestamp > current_time:
            log_event("FIFO Warning", f"Future timestamp detected for {asset_upper}: {timestamp} > {current_time}, adjusting to current time")
            timestamp = current_time

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

    def _is_fiat_currency(self, asset: str) -> bool:
        """Helper method to determine if an asset is a fiat currency."""
        return asset.upper() in ['EUR', 'USD', 'GBP', 'JPY', 'CAD', 'AUD', 'CHF', 'ZEUR', 'ZUSD', 'ZGBP', 'ZJPY', 'ZCAD', 'ZAUD', 'ZCHF']

    def _get_canonical_asset_name(self, asset: str) -> str:
        """Determines the canonical asset name to use for storage."""
        asset_upper = asset.upper()
        
        # Handle fiat currencies
        if self._is_fiat_currency(asset_upper):
            if asset_upper.startswith('Z'):
                return asset_upper  # Keep Kraken format for fiat
            return asset_upper  # Use as-is
        
        # Handle crypto assets
        if asset_upper == 'XBT':
            return 'BTC'  # Special case for Bitcoin
        
        if asset_upper.startswith('X') and len(asset_upper) > 1:
            return asset_upper  # Keep Kraken format for crypto
        
        return asset_upper  # Use as-is

    def match_lots(self, asset: str, amount: Decimal, timestamp: int, refid: str) -> List[Tuple[HoldingLot, Decimal]]:
        """Matches lots using FIFO for a given disposal amount."""
        asset_upper = asset.upper()
        remaining_to_dispose = amount
        matched_lots: List[Tuple[HoldingLot, Decimal]] = []
        
        # Create a list of possible asset names to check
        possible_asset_names = [asset_upper]
        
        # Add X-prefixed version if not already present (for crypto assets)
        if not asset_upper.startswith('X') and not self._is_fiat_currency(asset_upper):
            possible_asset_names.append('X' + asset_upper)
        
        # Add version without X prefix if it has one
        if asset_upper.startswith('X') and len(asset_upper) > 1:
            possible_asset_names.append(asset_upper[1:])
        
        # Special case for BTC/XBT
        if asset_upper == 'BTC':
            possible_asset_names.append('XBT')
        elif asset_upper == 'XBT':
            possible_asset_names.append('BTC')
        
        # For fiat currencies, check with Z prefix
        if self._is_fiat_currency(asset_upper):
            if not asset_upper.startswith('Z'):
                possible_asset_names.append('Z' + asset_upper)
            elif asset_upper.startswith('Z'):
                possible_asset_names.append(asset_upper[1:])
        
        # Try to find holdings under any of the possible asset names
        found_holdings = False
        used_asset_name = None
        
        for asset_name in possible_asset_names:
            if asset_name in self.holdings and self.holdings[asset_name]:
                found_holdings = True
                used_asset_name = asset_name
                break
        
        if not found_holdings:
            # Enhanced logging
            holdings_keys = list(self.holdings.keys())
            log_event("FIFO Error", f"Cannot match lots for {amount} {asset_upper} (Ref: {refid}) - No holdings available. Tried: {possible_asset_names}. Available holdings keys: {holdings_keys}")
            return []
        
        # Log which asset name was used
        log_event("FIFO Match", f"Matched {amount} {asset_upper} using holdings under '{used_asset_name}' (Ref: {refid})")
        
        lots_to_remove_indices: List[int] = []
        partial_lot_update: Optional[Tuple[int, Decimal]] = None

        # Only consider lots purchased on or before the disposal timestamp
        for idx, lot in enumerate(self.holdings[used_asset_name]):
            # Skip any lots purchased after this disposal timestamp
            if lot.purchase_timestamp > timestamp:
                continue
            if lot.purchase_timestamp > timestamp:
                # Skip lots not yet purchased at time of sale
                continue
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
            self.holdings[used_asset_name][idx].amount = remaining_amount

        for idx in sorted(lots_to_remove_indices, reverse=True):
            self.holdings[used_asset_name].pop(idx)

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

        # Create a list of possible asset names to check
        possible_asset_names = [asset_upper]
        
        # Add X-prefixed version if not already present (for crypto assets)
        if not asset_upper.startswith('X') and not self._is_fiat_currency(asset_upper):
            possible_asset_names.append('X' + asset_upper)
        
        # Add version without X prefix if it has one
        if asset_upper.startswith('X') and len(asset_upper) > 1:
            possible_asset_names.append(asset_upper[1:])
        
        # Special case for BTC/XBT
        if asset_upper == 'BTC':
            possible_asset_names.append('XBT')
        elif asset_upper == 'XBT':
            possible_asset_names.append('BTC')
        
        # For fiat currencies, check with Z prefix
        if self._is_fiat_currency(asset_upper):
            if not asset_upper.startswith('Z'):
                possible_asset_names.append('Z' + asset_upper)
            elif asset_upper.startswith('Z'):
                possible_asset_names.append(asset_upper[1:])
        
        # Try to find holdings under any of the possible asset names
        found_holdings = False
        used_asset_name = None
        
        for asset_name in possible_asset_names:
            if asset_name in self.holdings and self.holdings[asset_name]:
                found_holdings = True
                used_asset_name = asset_name
                break
        
        if not found_holdings:
            # Enhanced logging
            holdings_keys = list(self.holdings.keys())
            notes.append(f"ERROR: No prior holdings found for {asset_upper} to cover disposal. Tried: {possible_asset_names}")
            log_event("FIFO Error", f"Cannot process disposal of {amount} {asset_upper} (Ref: {refid}) - No holdings available. Tried: {possible_asset_names}. Available holdings keys: {holdings_keys}")
            return DisposalResult(
                asset=asset_upper, 
                disposed_amount=amount, 
                sale_price_eur=sale_price_eur,
                sale_timestamp=timestamp, 
                sale_tx_refid=refid, 
                fee_eur=fee_eur,
                total_proceeds_eur=total_proceeds, 
                total_cost_basis_eur=Decimal(0), 
                gain_loss_eur=total_proceeds - fee_eur,
                matched_lots=[], 
                notes=notes,
                holding_period_days_avg=0  # No real holding period
            )

        # Log which asset name was used
        log_event("FIFO Disposal", f"Processing disposal of {amount} {asset_upper} using holdings under '{used_asset_name}' (Ref: {refid})")

        remaining_to_dispose = amount
        total_cost_basis = Decimal(0)
        matched_lots_details: List[Tuple[HoldingLot, Decimal]] = []
        lots_to_remove_indices: List[int] = []
        partial_lot_update: Optional[Tuple[int, Decimal]] = None # (index, remaining_amount_in_lot)
        holding_periods_weighted: List[Tuple[int, Decimal]] = [] # (days, amount)

        # Iterate through sorted holdings (FIFO)
        for idx, lot in enumerate(self.holdings[used_asset_name]):
            if remaining_to_dispose <= 0:
                break

            amount_from_this_lot = min(remaining_to_dispose, lot.amount)
            cost_basis_from_this_lot = amount_from_this_lot * lot.purchase_price_eur
            total_cost_basis += cost_basis_from_this_lot
            
            # Calculate holding period
            holding_period_days = (disposal_datetime - lot.purchase_datetime).days
            holding_periods_weighted.append((holding_period_days, amount_from_this_lot))
            
            matched_lots_details.append({
                "lot": lot,
                "amount_used": amount_from_this_lot,
                "kaufdatum": lot.purchase_date_str,
                "kaufpreis": lot.purchase_price_eur
            })
            
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
            self.holdings[used_asset_name][idx].amount = remaining_amount
        
        # Remove fully used lots (in reverse order to not affect indices)
        for idx in sorted(lots_to_remove_indices, reverse=True):
            self.holdings[used_asset_name].pop(idx)
        
        # Verify we've matched all the disposal amount
        if remaining_to_dispose > Decimal('1e-12'):  # More than epsilon
            shortage = remaining_to_dispose
            notes.append(f"WARNING: Insufficient holdings to cover full disposal. Short by {shortage} {asset_upper}")
            log_event("FIFO Warning", f"Disposal of {amount} {asset_upper} (Ref: {refid}) exceeds available holdings by {shortage}")
        
        # Check if any matched lot was held ≤ 1 year (365 days)
        taxable_status = any(days <= 365 for days, _ in holding_periods_weighted)
        
        # Calculate weighted average holding period
        total_weighted_days = Decimal(0)
        total_weight = Decimal(0)
        for days, weight in holding_periods_weighted:
            total_weighted_days += Decimal(days) * weight
            total_weight += weight
        
        avg_holding_period = int(total_weighted_days / total_weight) if total_weight > 0 else 0
        
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
