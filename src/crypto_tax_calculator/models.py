from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from typing import List, Optional, Dict, Any

@dataclass
class Transaction:
    refid: str
    timestamp: int
    kraken_type: str
    kraken_subtype: str
    asset: str
    amount: Decimal
    fee_amount: Decimal
    price: Decimal
    cost_or_proceeds: Decimal
    quote_asset: str
    fee_asset: str
    internal_type: str = ""
    tax_category: str = ""
    value_eur: Decimal = Decimal(0)
    price_per_unit: Decimal = Decimal(0)
    balance_change: Decimal = Decimal(0)
    fee_value_eur: Decimal = Decimal(0)
    datetime_utc: datetime = datetime.utcnow()
    notes: str = ""
    
    @property
    def formatted_datetime(self) -> str:
        """Returns a formatted date string."""
        return self.datetime_utc.strftime("%Y-%m-%d %H:%M:%S")

@dataclass
class MatchedLotInfo:
    refid: str
    timestamp: int
    asset: str
    amount: Decimal
    cost: Decimal
    original_lot_refid: str = ""
    original_lot_purchase_date: datetime = datetime.utcnow()
    original_lot_purchase_price_eur: Decimal = Decimal(0)
    amount_used: Decimal = Decimal(0)
    cost_basis_eur: Decimal = Decimal(0)
    holding_period_days: int = 0
    
    @property
    def formatted_purchase_date(self) -> str:
        """Returns a formatted purchase date string."""
        return self.original_lot_purchase_date.strftime("%Y-%m-%d")

@dataclass
class TaxReportEntry:
    refid: str
    timestamp: int
    asset: str
    amount: Decimal
    cost_or_proceeds: Decimal
    tax_liability: Decimal
    matched_lots: List[MatchedLotInfo]
    is_taxable: bool = False
    tax_reason: str = ""
    disposal_gain_loss_eur: Decimal = Decimal(0)
    disposal_proceeds_eur: Decimal = Decimal(0)
    disposal_cost_basis_eur: Decimal = Decimal(0)
    disposal_fee_eur: Decimal = Decimal(0)
    matched_lots_info: List[MatchedLotInfo] = field(default_factory=list)
    holding_period_days_avg: int = 0
    is_long_term: bool = False
    warnings: List[str] = field(default_factory=list)
    notes: str = ""
    line_num: int = 0
    tx_type: str = ""
    tx_datetime: datetime = field(default_factory=datetime.utcnow)
    
    @property
    def tx_date_str(self) -> str:
        """Returns a formatted transaction date string."""
        return datetime.fromtimestamp(self.timestamp).strftime("%Y-%m-%d")
    
    @property
    def fifo_details_text(self) -> str:
        """Generates a formatted string with details about the matched lots."""
        if not self.matched_lots:
            return "No FIFO details available"
        
        details = []
        for lot in self.matched_lots:
            purchase_date = lot.original_lot_purchase_date.strftime("%Y-%m-%d")
            details.append(
                f"{lot.amount_used} {self.asset} bought on {purchase_date} "
                f"at {lot.original_lot_purchase_price_eur} EUR "
                f"(held for {lot.holding_period_days} days)"
            )
        
        return "\n".join(details)

@dataclass
class AggregatedTaxSummary:
    total_tax_liability: Decimal
    total_profit_loss: Decimal
    tax_report_entries: List[TaxReportEntry]
    total_private_sale_gains: Decimal = Decimal(0)
    total_private_sale_losses: Decimal = Decimal(0)
    total_other_income: Decimal = Decimal(0)
    warnings: List[str] = field(default_factory=list)
    tax_year: int = 0
    
    # Additional fields for German tax calculations
    net_private_sales: Decimal = Decimal(0)
    freigrenze_private_sales: Decimal = Decimal(600)  # Default for pre-2024
    private_sales_taxable: bool = False
    freigrenze_other_income: Decimal = Decimal(256)
    other_income_taxable: bool = False
    
    def update_tax_status(self) -> None:
        """Updates taxable status based on Freigrenze rules."""
        # Calculate net private sales
        self.net_private_sales = self.total_private_sale_gains + self.total_private_sale_losses
        
        # Apply Freigrenze rules
        self.private_sales_taxable = self.net_private_sales > self.freigrenze_private_sales
        self.other_income_taxable = self.total_other_income > self.freigrenze_other_income
        
        # If not taxable, zero out the liability
        if not self.private_sales_taxable and not self.other_income_taxable:
            self.total_tax_liability = Decimal(0)
            self.warnings.append(f"Tax liability set to 0 due to Freigrenze rules (net gains below thresholds).")
