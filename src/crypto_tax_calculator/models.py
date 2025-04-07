# crypto_tax_calculator/models.py

"""
Defines the core data structures (models) used throughout the application,
such as Transaction, HoldingLot, TaxReportEntry, and AggregatedTaxSummary.
"""

from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from typing import List, Dict, Optional, Tuple, Any

# Assuming InternalTransactionType and TransactionCategory will be imported from tax_rules
from .tax_rules import InternalTransactionType, TransactionCategory

# --- Core Data Models ---

@dataclass
class Transaction:
    """Represents a single transaction entry from Kraken (Trade or Ledger)."""
    refid: str # Original Kraken reference ID (trade ID or ledger ID)
    timestamp: int # Unix timestamp (UTC)
    datetime_utc: datetime = field(init=False) # Calculated datetime object
    kraken_type: str # Original type string from Kraken API (e.g., 'buy', 'sell', 'trade', 'staking')
   
    internal_type: InternalTransactionType = field(init=False) # Mapped internal type
    tax_category: TransactionCategory = field(init=False) # Determined tax category

    asset: str # Primary asset involved (e.g., 'BTC', 'ETH') - Kraken format (XXBT, XETH)
    amount: Decimal # Amount of the primary asset (positive for acquisition, negative for disposal)
    kraken_subtype: Optional[str] = None # Optional subtype if available from Kraken
    fee_asset: Optional[str] = None # Asset used for the fee (e.g., 'EUR', 'BTC')
    fee_amount: Decimal = Decimal(0) # Amount of the fee

    # Fields primarily relevant for trades/disposals
    pair: Optional[str] = None # Trading pair (e.g., 'XXBTZEUR')
    quote_asset: Optional[str] = None # The other asset in the pair (e.g., 'EUR')
    price_per_unit: Optional[Decimal] = None # Price per unit of 'asset' in 'quote_asset'
    cost_or_proceeds: Optional[Decimal] = None # Total value of the transaction in 'quote_asset'

    # Fields primarily relevant for ledger entries (rewards, transfers)
    balance_change: Optional[Decimal] = None # Direct change to balance if not a trade

    # Calculated fields (filled during processing)
    value_eur: Optional[Decimal] = None # Value of the transaction/reward/fee in EUR at transaction time
    fee_value_eur: Optional[Decimal] = None # Value of the fee in EUR at transaction time

    notes: List[str] = field(default_factory=list) # Processing notes, warnings, errors

    def __post_init__(self):
        """Calculate datetime and potentially map types after initialization."""
        self.datetime_utc = datetime.fromtimestamp(self.timestamp, tz=timezone.utc)
        # Initial mapping - might be refined later based on context
        # self.internal_type = map_kraken_type(self.kraken_type, self.kraken_subtype) # Requires map_kraken_type import
        # self.tax_category = determine_tax_category(self.internal_type) # Requires determine_tax_category import
        # Defer mapping until processing stage where more context might be available

    @property
    def date_str(self) -> str:
        return self.datetime_utc.strftime("%Y-%m-%d")


@dataclass
class HoldingLot:
    """Represents a single lot (purchase or acquisition) of a crypto asset."""
    asset: str # Asset symbol (standardized, e.g., BTC)
    amount: Decimal # Amount remaining in this lot
    purchase_price_eur: Decimal # Price per unit in EUR at time of acquisition
    purchase_timestamp: int
    purchase_tx_refid: str # Ref ID of the transaction that created this lot
    source: str = "kraken" # Origin (e.g., 'kraken', 'reward', 'airdrop', 'manual')
    is_income: bool = False # Was this lot acquired as income (staking, mining)?

    @property
    def purchase_datetime(self) -> datetime:
         return datetime.fromtimestamp(self.purchase_timestamp, tz=timezone.utc)

    @property
    def purchase_date_str(self) -> str:
        return self.purchase_datetime.strftime("%Y-%m-%d")

    @property
    def cost_basis_eur(self) -> Decimal:
        """Calculates the original cost basis of the amount currently in the lot."""
        return self.amount * self.purchase_price_eur


@dataclass
class MatchedLotInfo:
    """Information about how much of a specific HoldingLot was used in a disposal."""
    original_lot_refid: str
    original_lot_purchase_date: str
    original_lot_purchase_price_eur: Decimal
    amount_used: Decimal
    cost_basis_eur: Decimal
    holding_period_days: int


@dataclass
class TaxReportEntry:
    """Represents a single row in the final tax report CSV."""
    line_num: int
    tx_refid: str # Original transaction refid
    tx_datetime: datetime
    tx_type: str # User-friendly type (e.g., "Kauf", "Verkauf", "Staking", "Gebühr")
    tax_category: str # User-friendly category (e.g., "Privates Veräußerungsgeschäft (§23 EStG)")

    asset: str
    amount: Decimal # Amount of the primary asset involved

    # Acquisition details (relevant for buys, rewards)
    acquisition_price_eur_per_unit: Optional[Decimal] = None
    acquisition_value_eur: Optional[Decimal] = None # Total value of acquisition

    # Disposal details (relevant for sells, spends, fees paid in crypto)
    disposal_proceeds_eur: Optional[Decimal] = None # Total proceeds from disposal (before fees)
    disposal_cost_basis_eur: Optional[Decimal] = None # Calculated FIFO cost basis
    disposal_fee_eur: Optional[Decimal] = None
    disposal_gain_loss_eur: Optional[Decimal] = None

    # FIFO details for disposals
    matched_lots_info: List[MatchedLotInfo] = field(default_factory=list)
    holding_period_days_avg: Optional[int] = None
    is_long_term: Optional[bool] = None # Held > 1 year (based on matched lots)
    is_taxable: Optional[bool] = None # Based on holding period and gain/loss/income type
    tax_reason: str = "" # e.g., "Haltedauer <= 1 Jahr", "Steuerfrei > 1 Jahr", "Sonstige Einkünfte"

    notes: str = "" # Warnings or other info

    # --- Properties for easier reporting ---
    @property
    def tx_date_str(self) -> str:
        return self.tx_datetime.strftime("%Y-%m-%d")

    @property
    def fifo_details_text(self) -> str:
        """Generates the multi-line FIFO detail string for reports."""
        if not self.matched_lots_info:
            return "N/A"
        details = []
        for i, match in enumerate(self.matched_lots_info):
            details.append(
                f"Lot {i+1}: {match.amount_used:.8f} von Ref {match.original_lot_refid} "
                f"(Kauf {match.original_lot_purchase_date} @ {match.original_lot_purchase_price_eur:.4f} €/Stk, "
                f"Haltedauer: {match.holding_period_days} Tage)"
            )
        return " | ".join(details)

    # Add properties to map to the specific CSV columns if needed, e.g.:
    @property
    def csv_purchase_date(self) -> str:
        # For disposals, find the earliest purchase date from matched lots
        if self.matched_lots_info:
            return min(m.original_lot_purchase_date for m in self.matched_lots_info)
        # For acquisitions, use the transaction date
        elif self.acquisition_value_eur is not None:
             return self.tx_date_str
        return ""

    @property
    def csv_purchase_price(self) -> Optional[Decimal]:
         # For disposals, calculate average purchase price from matched lots
         if self.matched_lots_info and self.amount != 0:
              total_cost = sum(m.cost_basis_eur for m in self.matched_lots_info)
              total_amount = sum(m.amount_used for m in self.matched_lots_info)
              return total_cost / total_amount if total_amount else None
         # For acquisitions
         elif self.acquisition_price_eur_per_unit is not None:
              return self.acquisition_price_eur_per_unit
         return None

    @property
    def csv_sale_date(self) -> str:
        # Only relevant for disposals
        return self.tx_date_str if self.disposal_proceeds_eur is not None else ""

    @property
    def csv_sale_price(self) -> Optional[Decimal]:
         # Only relevant for disposals
         if self.disposal_proceeds_eur is not None and self.amount != 0:
              # Calculate price per unit from proceeds
              return self.disposal_proceeds_eur / self.amount if self.amount else None
         return None


@dataclass
class AggregatedTaxSummary:
    """Holds the final aggregated results for the tax year."""
    tax_year: int
    # §23 Private Sales
    total_private_sale_gains: Decimal = Decimal(0)
    total_private_sale_losses: Decimal = Decimal(0)
    net_private_sales: Decimal = field(init=False)
    freigrenze_private_sales: Decimal = Decimal(0)
    private_sales_taxable: bool = field(init=False)
    # §22 Other Income
    total_other_income: Decimal = Decimal(0) # Staking, Lending etc.
    freigrenze_other_income: Decimal = Decimal(0)
    other_income_taxable: bool = field(init=False)
    # Add other categories if needed (e.g., margin trading if handled separately)
    warnings: List[str] = field(default_factory=list)

    def __post_init__(self):
        self.net_private_sales = self.total_private_sale_gains + self.total_private_sale_losses # Losses are negative
        self.private_sales_taxable = self.net_private_sales > self.freigrenze_private_sales
        self.other_income_taxable = self.total_other_income > self.freigrenze_other_income
