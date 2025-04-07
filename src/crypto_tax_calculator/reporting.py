# crypto_tax_calculator/reporting.py

"""
Handles the generation of output reports:
- Detailed CSV tax report
- FIFO proof documentation text file
- Google Sheets export (optional)
- Console summary
"""

import csv
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any, Optional
from decimal import Decimal

# Import the necessary models
from .models import TaxReportEntry, AggregatedTaxSummary, MatchedLotInfo

# Placeholder for logging function
def log_event(event: str, details: str):
    print(f"[LOG] {event}: {details}")

# --- Constants ---
CSV_HEADERS = [
    "Zeile", "Typ", "Steuer-Kategorie", "Transaktions-Datum", "Asset", "Anzahl",
    "Kaufdatum", "Kaufpreis (€)/Stk", "Verkaufsdatum", "Verkaufspreis (€)/Stk",
    "Gesamtkosten (€)", "Gesamterlös (€)", "Gebühr (€)", "Gewinn / Verlust (€)", "Haltedauer (Tage)",
    "Haltedauer > 1 Jahr", "Steuerpflichtig", "Steuergrund", "FIFO-Details", "Notizen"
]

# --- Helper Functions ---
def _format_decimal(value: Optional[Decimal], precision: int = 2) -> str:
    """Formats Decimal for CSV/report output, handling None."""
    if value is None:
        return ""
    # Format with specified precision, using dot as decimal separator
    return f"{value:.{precision}f}"

def _format_datetime(dt: Optional[datetime]) -> str:
    """Formats datetime object to YYYY-MM-DD string, handling None."""
    if dt is None:
        return ""
    return dt.strftime("%Y-%m-%d")

def _format_bool(b: Optional[bool]) -> str:
    """Formats boolean for report output."""
    if b is None:
        return "Unbekannt"
    return "Ja" if b else "Nein"

# --- CSV Export ---
def export_to_csv(report_entries: List[TaxReportEntry], summary: AggregatedTaxSummary, tax_year: int, output_dir: Path) -> str:
    """Exports the detailed tax report entries to a CSV file."""
    output_dir.mkdir(parents=True, exist_ok=True)
    output_file = output_dir / f"krypto_steuer_{tax_year}.csv"

    try:
        with open(output_file, "w", newline="", encoding="utf-8") as csvfile:
            writer = csv.writer(csvfile, delimiter=";", quotechar='"', quoting=csv.QUOTE_MINIMAL)

            # Write Header
            writer.writerow(CSV_HEADERS)

            # Filter out EUR transactions - we only want crypto transactions in the report
            filtered_entries = [entry for entry in report_entries if entry.asset != "EUR"]
            
            # Write Data Rows
            for entry in filtered_entries:
                # Format dates using the tx_date_str property
                purchase_date = entry.tx_date_str if entry.acquisition_value_eur is not None else ""
                purchase_price = entry.acquisition_price_eur_per_unit
                sale_date = entry.tx_date_str if entry.disposal_proceeds_eur is not None else ""
                sale_price_per_unit = None
                
                # Calculate sale price per unit if available
                if entry.disposal_proceeds_eur is not None and entry.amount and entry.amount != 0:
                    sale_price_per_unit = entry.disposal_proceeds_eur / abs(entry.amount)
                
                row = [
                    entry.line_num,
                    entry.tx_type,
                    entry.tax_category,
                    _format_datetime(entry.tx_datetime),
                    entry.asset,
                    _format_decimal(entry.amount, 8),  # Higher precision for amount
                    purchase_date,
                    _format_decimal(purchase_price, 4),
                    sale_date,
                    _format_decimal(sale_price_per_unit, 4),
                    _format_decimal(entry.disposal_cost_basis_eur, 2),
                    _format_decimal(entry.disposal_proceeds_eur, 2),
                    _format_decimal(entry.disposal_fee_eur, 2),
                    _format_decimal(entry.disposal_gain_loss_eur, 2),
                    entry.holding_period_days_avg if entry.holding_period_days_avg is not None else "",
                    _format_bool(entry.is_long_term),
                    _format_bool(entry.is_taxable),
                    entry.tax_reason,
                    entry.fifo_details_text,
                    entry.notes
                ]
                writer.writerow(row)

            # Append Summary Block
            writer.writerow([])  # Empty row separator
            writer.writerow(["--- Steuerliche Zusammenfassung ---"])
            writer.writerow(["Steuerjahr:", tax_year])
            writer.writerow([])
            writer.writerow(["Private Veräußerungsgeschäfte (§23 EStG):"])
            writer.writerow(["Gesamtgewinne:", _format_decimal(summary.total_private_sale_gains)])
            writer.writerow(["Gesamtverluste:", _format_decimal(summary.total_private_sale_losses)])
            writer.writerow(["Nettobetrag (§23):", _format_decimal(summary.net_private_sales)])
            writer.writerow(["Freigrenze (§23):", _format_decimal(summary.freigrenze_private_sales)])
            writer.writerow(["Steuerpflichtig (§23):", _format_bool(summary.private_sales_taxable)])
            writer.writerow([])
            writer.writerow(["Sonstige Einkünfte (§22 Nr. 3 EStG):"])
            writer.writerow(["Gesamteinkünfte (z.B. Staking):", _format_decimal(summary.total_other_income)])
            writer.writerow(["Freigrenze (§22):", _format_decimal(summary.freigrenze_other_income)])
            writer.writerow(["Steuerpflichtig (§22):", _format_bool(summary.other_income_taxable)])

        log_event("CSV Export", f"Tax report exported successfully to {output_file}")
        return str(output_file)
    
    except Exception as e:
        log_event("CSV Export Error", f"Failed to export CSV: {e}")
        print(f"[ERROR] Failed to export CSV: {e}")
        return f"Error exporting CSV: {e}"


# --- FIFO Proof Export ---
def export_fifo_proof(report_entries: List[TaxReportEntry], summary: AggregatedTaxSummary, tax_year: int, output_dir: Path) -> str:
    """Exports detailed FIFO calculations to a text file."""
    output_dir.mkdir(parents=True, exist_ok=True)
    output_file = output_dir / f"fifo_nachweis_{tax_year}.txt"

    try:
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(f"FIFO Nachweis für Steuerjahr {tax_year}\n")
            f.write("=" * 80 + "\n\n")
            f.write("Gemäß BMF-Schreiben zur steuerlichen Behandlung von Kryptowährungen\n")
            f.write("werden Veräußerungen nach dem FIFO-Prinzip (First In - First Out) behandelt.\n\n")
            f.write("Detailaufstellung der Veräußerungen:\n")
            f.write("-" * 80 + "\n\n")

            # Iterate through report entries, focusing on disposals (filter out EUR)
            disposals = [e for e in report_entries if e.disposal_gain_loss_eur is not None and e.asset != "EUR"]

            if not disposals:
                f.write("Keine Veräußerungen im Berichtszeitraum gefunden.\n\n")
            else:
                for entry in disposals:
                    # Calculate sale price per unit if available
                    sale_price_per_unit = None
                    if entry.disposal_proceeds_eur is not None and entry.amount and entry.amount != 0:
                        sale_price_per_unit = entry.disposal_proceeds_eur / abs(entry.amount)
                        
                    f.write(f"Veräußerung #{entry.line_num}:\n")
                    f.write(f"  Datum: {_format_datetime(entry.tx_datetime)}\n")
                    f.write(f"  Asset: {entry.asset}\n")
                    f.write(f"  Verkaufte Menge: {_format_decimal(entry.amount, 8)}\n")
                    f.write(f"  Verkaufspreis/Stk: {_format_decimal(sale_price_per_unit, 4)} €\n")
                    f.write(f"  Gesamterlös: {_format_decimal(entry.disposal_proceeds_eur, 2)} €\n")
                    f.write(f"  Gebühr: {_format_decimal(entry.disposal_fee_eur, 2)} €\n")
                    f.write(f"  FIFO-Zuordnung:\n")
                    
                    if entry.matched_lots_info:
                        for i, match in enumerate(entry.matched_lots_info):
                            f.write(f"    - Lot {i+1}: {_format_decimal(match.amount_used, 8)} von Kauf am "
                                   f"{match.original_lot_purchase_date} "
                                   f"@ {_format_decimal(match.original_lot_purchase_price_eur, 4)} €/Stk, "
                                   f"Haltedauer: {match.holding_period_days} Tage\n")
                    elif entry.fifo_details_text:
                        # Fallback to fifo_details_text if available
                        lines = entry.fifo_details_text.split(' | ')  # Based on old script format
                        for line in lines:
                            f.write(f"    - {line}\n")
                    else:
                        f.write("    - Keine FIFO-Details verfügbar.\n")

                    f.write(f"  Gesamtkosten (FIFO): {_format_decimal(entry.disposal_cost_basis_eur, 2)} €\n")
                    f.write(f"  Gewinn/Verlust: {_format_decimal(entry.disposal_gain_loss_eur, 2)} €\n")
                    f.write(f"  Haltedauer (Durchschnitt): {entry.holding_period_days_avg} Tage\n")
                    f.write(f"  Steuerpflichtig: {_format_bool(entry.is_taxable)} ({entry.tax_reason})\n")
                    if entry.notes:
                        f.write(f"  Notizen: {entry.notes}\n")
                    f.write("-" * 40 + "\n\n")

            # Append Summary Block
            f.write("\n" + "=" * 80 + "\n")
            f.write("Steuerliche Zusammenfassung\n")
            f.write("=" * 80 + "\n\n")
            f.write(f"Steuerjahr: {tax_year}\n\n")

            f.write("Private Veräußerungsgeschäfte (§23 EStG):\n")
            f.write(f"  Gesamtgewinne: {_format_decimal(summary.total_private_sale_gains)} €\n")
            f.write(f"  Gesamtverluste: {_format_decimal(summary.total_private_sale_losses)} €\n")
            f.write(f"  Nettobetrag (§23): {_format_decimal(summary.net_private_sales)} €\n")
            f.write(f"  Freigrenze (§23): {_format_decimal(summary.freigrenze_private_sales)} €\n")
            f.write(f"  Steuerpflichtig (§23): {_format_bool(summary.private_sales_taxable)}\n\n")

            f.write("Sonstige Einkünfte (§22 Nr. 3 EStG):\n")
            f.write(f"  Gesamteinkünfte (z.B. Staking): {_format_decimal(summary.total_other_income)} €\n")
            f.write(f"  Freigrenze (§22): {_format_decimal(summary.freigrenze_other_income)} €\n")
            f.write(f"  Steuerpflichtig (§22): {_format_bool(summary.other_income_taxable)}\n")

        log_event("FIFO Proof Export", f"FIFO proof exported successfully to {output_file}")
        return str(output_file)
    
    except Exception as e:
        log_event("FIFO Proof Export Error", f"Failed to export FIFO proof: {e}")
        print(f"[ERROR] Failed to export FIFO proof: {e}")
        return f"Error exporting FIFO proof: {e}"


# --- Google Sheets Export ---
def export_to_google_sheets(report_entries: List[TaxReportEntry], summary: AggregatedTaxSummary, tax_year: int, config: Dict[str, Any]) -> str:
    """Exports the tax report data to Google Sheets."""
    # TODO: Implement Google Sheets export using googleapiclient
    # Needs proper handling of credentials, sheet creation/clearing, writing data.
    sheet_id = config.get("google_sheets", {}).get("sheet_id")
    credentials_path = config.get("google_sheets", {}).get("credentials_file")

    if not sheet_id:
        msg = "Google Sheets export skipped - SHEET_ID not configured."
        log_event("Google Sheets", msg)
        return msg

    if not credentials_path or not Path(credentials_path).exists():
        msg = f"Google Sheets export skipped - Credentials file not found or not configured: {credentials_path}"
        log_event("Google Sheets Error", msg)
        print(f"[WARN] {msg}")
        return msg

    log_event("Google Sheets", "Google Sheets export is not yet fully implemented in refactored code.")
    print("[INFO] Google Sheets export needs implementation.")
    # --- Implementation Steps ---
    # 1. Import google libraries
    # 2. Load credentials using service_account.Credentials.from_service_account_file
    # 3. Build sheets service: service = build("sheets", "v4", credentials=credentials)
    # 4. Define sheet name: sheet_name = f"Steuer {tax_year}"
    # 5. Check if sheet exists, create or clear it
    # 6. Format data: Convert report_entries + summary into a list of rows
    # 7. Write data using service.spreadsheets().values().update(...)
    # 8. Add error handling

    return "Google Sheets export not implemented yet."


# --- Console Summary ---
def print_console_summary(summary: AggregatedTaxSummary):
    """Prints the aggregated tax summary to the console."""
    print("\n" + "=" * 40)
    print(f"Steuerliche Zusammenfassung für {summary.tax_year}")
    print("=" * 40)

    print("\nPrivate Veräußerungsgeschäfte (§23 EStG):")
    print(f"  Gesamtgewinne: {_format_decimal(summary.total_private_sale_gains)} €")
    print(f"  Gesamtverluste: {_format_decimal(summary.total_private_sale_losses)} €")
    print(f"  Nettobetrag (§23): {_format_decimal(summary.net_private_sales)} €")
    print(f"  Freigrenze (§23): {_format_decimal(summary.freigrenze_private_sales)} €")
    if summary.private_sales_taxable:
        print(f"  Status: Steuerpflichtig (Nettobetrag > Freigrenze)")
    else:
        print(f"  Status: Steuerfrei (Nettobetrag <= Freigrenze)")

    print("\nSonstige Einkünfte (§22 Nr. 3 EStG):")
    print(f"  Gesamteinkünfte (z.B. Staking): {_format_decimal(summary.total_other_income)} €")
    print(f"  Freigrenze (§22): {_format_decimal(summary.freigrenze_other_income)} €")
    if summary.other_income_taxable:
        print(f"  Status: Steuerpflichtig (Einkünfte > Freigrenze)")
    else:
        print(f"  Status: Steuerfrei (Einkünfte <= Freigrenze)")
    print("=" * 40)
