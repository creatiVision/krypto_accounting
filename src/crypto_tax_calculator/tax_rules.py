# crypto_tax_calculator/tax_rules.py

"""
Encapsulates German-specific tax rules for cryptocurrency transactions.
Includes holding periods, tax exemption limits (Freigrenze), and
classification of transaction types.
"""

from enum import Enum, auto
from typing import Dict, Optional # Ensure correct import
from decimal import Decimal

# --- Constants ---
HOLDING_PERIOD_DAYS = 365  # §23 Abs. 1 Satz 1 Nr. 2 EStG

FREIGRENZE_PRIVATE_SALES_UNTIL_2023 = Decimal("600.00") # §23 Abs. 3 Satz 5 EStG a.F.
FREIGRENZE_PRIVATE_SALES_2024_ONWARDS = Decimal("1000.00") # §23 Abs. 3 Satz 5 EStG n.F.

# Note: There's also a Freigrenze for "Sonstige Einkünfte" (§22 Nr. 3 EStG)
# applicable to staking, lending, mining etc. which is currently 256 EUR.
# This needs careful consideration if mixing income types.
FREIGRENZE_OTHER_INCOME = Decimal("256.00") # §22 Nr. 3 Satz 2 EStG

# --- Enums for Classification ---
class TransactionCategory(Enum):
    """Broad classification for tax purposes."""
    PRIVATE_SALE = "Privates Veräußerungsgeschäft (§23 EStG)"
    OTHER_INCOME = "Sonstige Einkünfte (§22 Nr. 3 EStG)"
    NON_TAXABLE = "Nicht steuerbar / Nicht relevant"
    COST = "Anschaffungsnebenkosten / Werbungskosten" # e.g., Fees

class InternalTransactionType(Enum):
    """Standardized internal representation of transaction types."""
    BUY = auto()
    SELL = auto()
    TRADE = auto() # Crypto-to-crypto trade (treated as sell + buy)
    STAKING_REWARD = auto()
    LENDING_REWARD = auto()
    MINING_REWARD = auto()
    AIRDROP = auto()
    FORK = auto()
    GIFT_RECEIVED = auto()
    GIFT_SENT = auto()
    SPEND = auto() # Paying for goods/services with crypto (treated as sell)
    FEE_PAYMENT = auto() # Paying transaction fees in crypto (treated as sell)
    DEPOSIT = auto() # Transfer into Kraken from external
    WITHDRAWAL = auto() # Transfer out of Kraken to external
    TRANSFER_INTERNAL = auto() # Movement within Kraken (usually not taxable)
    MARGIN_TRADE = auto() # Needs specific handling - potentially complex
    MARGIN_FEE = auto()
    SETTLED = auto() # Often related to margin positions closing
    NON_TAXABLE_FIAT = auto() # Receiving fiat from crypto sale (not taxable itself)
    UNKNOWN = auto()

# --- Mappings ---
# Map Kraken ledger/trade types to InternalTransactionType
# This requires careful analysis of Kraken's API documentation and potential ledger types.
# This is a starting point and likely needs expansion.
KRAKEN_TYPE_MAP: Dict[str, InternalTransactionType] = {
    # Trade History Types
    "buy": InternalTransactionType.BUY,
    "sell": InternalTransactionType.SELL,
    # Ledger Types (Common examples - CHECK KRAKEN DOCS FOR FULL LIST)
    "trade": InternalTransactionType.TRADE, # Represents the result of a buy/sell pair in ledger
    "spend": InternalTransactionType.SPEND, # Likely a disposal
    "receive": InternalTransactionType.DEPOSIT, # Or could be reward/airdrop - needs context
    "deposit": InternalTransactionType.DEPOSIT,
    "withdrawal": InternalTransactionType.WITHDRAWAL,
    "transfer": InternalTransactionType.TRANSFER_INTERNAL, # Usually between Kraken sub-accounts/wallets
    "margin": InternalTransactionType.MARGIN_TRADE, # Needs careful handling
    "settled": InternalTransactionType.SETTLED, # Often related to margin
    "staking": InternalTransactionType.STAKING_REWARD,
    "reward": InternalTransactionType.LENDING_REWARD, # Or other rewards - needs context
    # Add more mappings based on actual data encountered
}

def map_kraken_type(kraken_type: str, kraken_subtype: Optional[str] = None) -> InternalTransactionType:
    """Maps a Kraken transaction type (from trade history or ledger) to an internal type."""
    kraken_type_lower = kraken_type.lower()
    # Simple direct mapping first
    if kraken_type_lower in KRAKEN_TYPE_MAP:
        # TODO: Add logic for subtypes if necessary (e.g., 'transfer' subtype 'spottowallet')
        return KRAKEN_TYPE_MAP[kraken_type_lower]

    # Add more sophisticated mapping logic if needed based on type/subtype/asset etc.
    # For example, a 'transfer' might be a reward if the asset is specific.

    print(f"[WARN] Unknown Kraken transaction type encountered: '{kraken_type}' (Subtype: {kraken_subtype}). Mapping to UNKNOWN.")
    return InternalTransactionType.UNKNOWN

# --- Functions ---
def get_freigrenze_private_sales(tax_year: int) -> Decimal:
    """Returns the applicable Freigrenze for private sales (§23 EStG) for the given tax year."""
    if tax_year >= 2024:
        return FREIGRENZE_PRIVATE_SALES_2024_ONWARDS
    else:
        # Assuming the 600 limit applied up to and including 2023
        return FREIGRENZE_PRIVATE_SALES_UNTIL_2023

def get_freigrenze_other_income(tax_year: int) -> Decimal:
     """Returns the applicable Freigrenze for other income (§22 Nr. 3 EStG) for the given tax year."""
     # This limit hasn't changed recently, but good practice to have function
     return FREIGRENZE_OTHER_INCOME

def determine_tax_category(internal_tx_type: InternalTransactionType) -> TransactionCategory:
    """Determines the German tax category based on the internal transaction type."""
    if internal_tx_type in [InternalTransactionType.SELL, InternalTransactionType.TRADE, InternalTransactionType.SPEND, InternalTransactionType.FEE_PAYMENT]:
        # These are disposals potentially falling under §23 EStG
        return TransactionCategory.PRIVATE_SALE
    elif internal_tx_type in [InternalTransactionType.STAKING_REWARD, InternalTransactionType.LENDING_REWARD, InternalTransactionType.MINING_REWARD]:
        # These are generally considered "Sonstige Einkünfte" §22 Nr. 3 EStG
        return TransactionCategory.OTHER_INCOME
    elif internal_tx_type == InternalTransactionType.BUY:
        # Purchases are relevant for cost basis but not income events themselves
        return TransactionCategory.NON_TAXABLE # Or maybe COST if fees included?
    elif internal_tx_type in [InternalTransactionType.DEPOSIT, InternalTransactionType.WITHDRAWAL, InternalTransactionType.TRANSFER_INTERNAL, InternalTransactionType.NON_TAXABLE_FIAT]:
        # Transfers and receiving fiat from crypto sales are generally not taxable events
        return TransactionCategory.NON_TAXABLE
    elif internal_tx_type == InternalTransactionType.AIRDROP:
        # Tax treatment of airdrops can vary (BMF guidance). Often §22 Nr. 3 if actively acquired,
        # or potentially 0 cost basis acquisition if passively received. Needs careful handling.
        # Defaulting to OTHER_INCOME for now, might need refinement.
        return TransactionCategory.OTHER_INCOME
    # --- Cases needing more complex handling ---
    elif internal_tx_type in [InternalTransactionType.MARGIN_TRADE, InternalTransactionType.MARGIN_FEE, InternalTransactionType.SETTLED]:
         # Margin trading might fall under different rules (Kapitalvermögen?) - needs expert review.
         print("[WARN] Margin trading detected - tax classification requires specific review.")
         return TransactionCategory.PRIVATE_SALE # Tentative - NEEDS VERIFICATION
    elif internal_tx_type == InternalTransactionType.GIFT_RECEIVED:
         # Receiving a gift: Inherit cost basis and holding period from giver (§23 Abs. 1 Satz 3 EStG)
         # This script likely cannot determine this automatically without manual input.
         return TransactionCategory.NON_TAXABLE # Acquisition itself isn't taxable event
    elif internal_tx_type == InternalTransactionType.GIFT_SENT:
         # Sending a gift is generally treated like a sale at market value (potential §23 event)
         return TransactionCategory.PRIVATE_SALE
    else: # UNKNOWN etc.
        return TransactionCategory.NON_TAXABLE # Default for unknown/unhandled

def is_disposal(internal_tx_type: InternalTransactionType) -> bool:
    """Checks if the transaction type represents a disposal of an asset."""
    return internal_tx_type in [
        InternalTransactionType.SELL,
        InternalTransactionType.TRADE, # Sell leg
        InternalTransactionType.SPEND,
        InternalTransactionType.FEE_PAYMENT,
        InternalTransactionType.GIFT_SENT,
        # Margin close/settlement might also be disposal
        InternalTransactionType.MARGIN_TRADE, # If closing a position
        InternalTransactionType.SETTLED,
    ]

def is_acquisition(internal_tx_type: InternalTransactionType) -> bool:
    """Checks if the transaction type represents an acquisition of an asset."""
    return internal_tx_type in [
        InternalTransactionType.BUY,
        InternalTransactionType.TRADE, # Buy leg
        InternalTransactionType.STAKING_REWARD,
        InternalTransactionType.LENDING_REWARD,
        InternalTransactionType.MINING_REWARD,
        InternalTransactionType.AIRDROP,
        InternalTransactionType.FORK,
        InternalTransactionType.GIFT_RECEIVED,
        InternalTransactionType.NON_TAXABLE_FIAT, # Track fiat received, but not as taxable
        # Margin open might be acquisition
        InternalTransactionType.MARGIN_TRADE, # If opening a position
    ]


# Example usage
if __name__ == "__main__":
    print("Testing Tax Rules module...")
    test_year = 2024
    print(f"Freigrenze (§23) for {test_year}: {get_freigrenze_private_sales(test_year)} EUR")
    print(f"Freigrenze (§22) for {test_year}: {get_freigrenze_other_income(test_year)} EUR")

    print("\nMapping Kraken Types:")
    print(f"Kraken 'buy' -> {map_kraken_type('buy')}")
    print(f"Kraken 'sell' -> {map_kraken_type('sell')}")
    print(f"Kraken 'staking' -> {map_kraken_type('staking')}")
    print(f"Kraken 'trade' (ledger) -> {map_kraken_type('trade')}")
    print(f"Kraken 'withdrawal' -> {map_kraken_type('withdrawal')}")
    print(f"Kraken 'UNKNOWN_TYPE' -> {map_kraken_type('UNKNOWN_TYPE')}")

    print("\nDetermining Tax Categories:")
    print(f"Internal BUY -> {determine_tax_category(InternalTransactionType.BUY).value}")
    print(f"Internal SELL -> {determine_tax_category(InternalTransactionType.SELL).value}")
    print(f"Internal STAKING_REWARD -> {determine_tax_category(InternalTransactionType.STAKING_REWARD).value}")
    print(f"Internal FEE_PAYMENT -> {determine_tax_category(InternalTransactionType.FEE_PAYMENT).value}")
    print(f"Internal DEPOSIT -> {determine_tax_category(InternalTransactionType.DEPOSIT).value}")


def calculate_tax_liability(tx, matched_lots) -> Decimal:
    """
    Calculates raw gain/loss for a transaction based on matched lots.
    Adds warnings for margin, airdrop, gift transactions.
    Does NOT apply Freigrenze or holding period exemptions.
    """
    warnings = []

    # Detect special cases and add warnings
    internal_type = getattr(tx, 'internal_type', '').lower()
    if 'margin' in internal_type:
        warnings.append("Margin trade detected - manual review required.")
    if 'airdrop' in internal_type:
        warnings.append("Airdrop detected - manual review required.")
    if 'gift' in internal_type:
        warnings.append("Gift detected - manual review required.")

    # Attach warnings to tx if possible
    if hasattr(tx, 'warnings'):
        tx.warnings.extend(warnings)
    elif hasattr(tx, 'notes'):
        # Check if notes is a string or a list
        if isinstance(tx.notes, str):
            if warnings:
                tx.notes += "; " + "; ".join(warnings)
        else:
            # Assume it's a list-like object
            tx.notes.extend(warnings)

    proceeds = tx.cost_or_proceeds
    total_cost_basis = sum(lot.cost_basis_eur for lot in matched_lots)

    gain = proceeds - total_cost_basis

    return gain
