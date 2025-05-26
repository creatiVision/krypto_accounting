"""
Handles tax report generation and export functionality.
Supports various export formats including CSV, Excel and JSON.
CSV files use comma as the standard separator, with an option to use semicolons.
"""

import os
import json
import csv
from datetime import datetime
from decimal import Decimal
from pathlib import Path
import traceback
from typing import List, Dict, Any, Optional, Union, Callable

# Optional Excel support - gracefully handle if not installed
# Excel export functionality has been disabled
# try:
#     import openpyxl
#     from openpyxl import Workbook
#     from openpyxl.styles import Font, PatternFill, Alignment
#     EXCEL_SUPPORT = True
# except ImportError:
#     EXCEL_SUPPORT = False
#     print("Excel export disabled. Install openpyxl for Excel support: pip install openpyxl")
EXCEL_SUPPORT = False

from .models import TaxReportEntry, AggregatedTaxSummary, MatchedLotInfo
from .logging_utils import log_event, log_error

# Ensure the logs directory exists
logs_dir = Path("logs")
logs_dir.mkdir(exist_ok=True)

# JSON export functionality has been disabled
# class DecimalEncoder(json.JSONEncoder):
#     """Custom JSON encoder that handles Decimal types."""
#     def default(self, obj):
#         if isinstance(obj, Decimal):
#             return float(obj)
#         if isinstance(obj, datetime):
#             return obj.isoformat()
#         return super(DecimalEncoder, self).default(obj)

def format_timestamp(timestamp: int) -> str:
    """Format a Unix timestamp as a human-readable date string."""
    dt = datetime.fromtimestamp(timestamp)
    return dt.strftime("%Y-%m-%d %H:%M:%S")

def ensure_output_dir(output_dir: str) -> Path:
    """Ensure the output directory exists."""
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    return output_path

def create_filename(base_name: str, tax_year: int, extension: str) -> str:
    """Create a filename based on report type and tax year."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return f"{base_name}_{tax_year}_{timestamp}.{extension}"

def unify_csv_delimiter(input_path: str, output_path: Optional[str] = None, target_delimiter: str = ',') -> str:
    """
    Read a CSV file and unify the delimiter to the specified character.
    Automatically detects if the file uses semicolons or commas.
    
    Parameters:
        input_path: Path to the input CSV file
        output_path: Path to write the output (if None, modifies in place)
        target_delimiter: The delimiter to standardize to (default: comma)
        
    Returns:
        The path to the converted file
    """
    try:
        if output_path is None:
            output_path = input_path
            # Create a temporary file for intermediary operations
            temp_path = f"{input_path}.temp"
        else:
            temp_path = output_path
            
        with open(input_path, 'r', newline='') as f:
            sample = f.read(4096)
        comma_count = sample.count(',')
        semicolon_count = sample.count(';')
        source_delimiter = ';' if semicolon_count > comma_count else ','
        if source_delimiter == target_delimiter:
            log_event("CSV", f"File {input_path} already uses the target delimiter: {target_delimiter}")
            return input_path
        log_event("CSV", f"Converting {input_path} from delimiter '{source_delimiter}' to '{target_delimiter}'")
        rows = []
        with open(input_path, 'r', newline='') as infile:
            reader = csv.reader(infile, delimiter=source_delimiter)
            for row in reader:
                rows.append(row)
        with open(temp_path, 'w', newline='') as outfile:
            writer = csv.writer(outfile, delimiter=target_delimiter)
            writer.writerows(rows)
        if output_path == input_path:
            os.replace(temp_path, input_path)
        log_event("CSV", f"Successfully converted delimiter in {input_path}")
        return output_path
    except Exception as e:
        error_msg = f"Failed to unify CSV delimiter: {str(e)}"
        log_error("reporting", "CSVDelimiterError", error_msg, details={"input_path": input_path, "output_path": output_path}, exception=e)
        return input_path

def unify_all_csv_files(directory: str, target_delimiter: str = ',', recursive: bool = True) -> List[str]:
    try:
        processed_files = []
        dir_path = Path(directory)
        def process_directory(path):
            nonlocal processed_files
            for item in path.iterdir():
                if item.is_file() and item.suffix.lower() == '.csv':
                    processed_file = unify_csv_delimiter(str(item), target_delimiter=target_delimiter)
                    processed_files.append(processed_file)
                elif recursive and item.is_dir():
                    process_directory(item)
        process_directory(dir_path)
        log_event("CSV", f"Processed {len(processed_files)} CSV files in {directory}")
        return processed_files
    except Exception as e:
        error_msg = f"Failed to unify CSV files in directory: {str(e)}"
        log_error("reporting", "CSVBatchError", error_msg, details={"directory": directory}, exception=e)
        return []

def check_missing_sales_2024(summary: AggregatedTaxSummary, known_sales_data: List[Dict]) -> List[Dict]:
    try:
        included_sales = {}
        for entry in summary.tax_report_entries:
            entry_date = datetime.fromtimestamp(entry.timestamp)
            if entry_date.year == 2024:
                key = f"{entry.asset}_{entry.timestamp}_{entry.refid}"
                included_sales[key] = entry
        missing_sales = []
        for sale in known_sales_data:
            if not 'timestamp' in sale or not 'asset' in sale or not 'refid' in sale:
                continue
            sale_date = datetime.fromtimestamp(sale['timestamp'])
            if sale_date.year == 2024:
                key = f"{sale['asset']}_{sale['timestamp']}_{sale['refid']}"
                if key not in included_sales:
                    missing_sales.append(sale)
        if missing_sales:
            log_event("Report", f"Found {len(missing_sales)} missing sales from 2024")
        return missing_sales
    except Exception as e:
        error_msg = f"Failed to check for missing 2024 sales: {str(e)}"
        log_error("reporting", "MissingSalesError", error_msg, exception=e)
        return []

def export_tax_report(summary: AggregatedTaxSummary, tax_year: int, output_dir: str = "export", format: str = "csv", include_lot_details: bool = True, csv_delimiter: str = ";") -> Dict[str, str]:
    try:
        output_path = ensure_output_dir(output_dir)
        created_files = {}
        german_file = export_as_year_csv(summary, tax_year, output_path, delimiter=";")
        if german_file:
            created_files['year_csv'] = german_file
        log_event("Export", f"Successfully exported tax report in German format for year {tax_year}")
        return created_files
    except Exception as e:
        error_msg = f"Failed to export tax report: {str(e)}"
        log_error("reporting", "ExportError", error_msg, details={"tax_year": tax_year, "format": format}, exception=e)
        return {}

def export_as_csv(summary: AggregatedTaxSummary, tax_year: int, output_path: Path, include_lot_details: bool = True, delimiter: str = ";") -> Dict[str, str]:
    try:
        created_files = {}
        report_filename = create_filename("tax_report", tax_year, "csv")
        report_path = output_path / report_filename
        with open(report_path, 'w', newline='') as csvfile:
            fieldnames = [
                'Datum', 'Asset', 'Menge', 'Erlös (EUR)', 'Anschaffungskosten (EUR)', 
                'Gewinn/Verlust (EUR)', 'Steuerpflicht (EUR)', 'Haltedauer (Tage)', 
                'Langfristig', 'Steuerpflichtig', 'Referenz-ID'
            ]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames, delimiter=delimiter if delimiter else ';')
            writer.writeheader()
            for entry in summary.tax_report_entries:
                disposal_proceeds = entry.disposal_proceeds_eur if entry.disposal_proceeds_eur is not None else (entry.cost_or_proceeds if entry.cost_or_proceeds is not None else Decimal('0'))
                disposal_cost_basis = entry.disposal_cost_basis_eur if entry.disposal_cost_basis_eur is not None else Decimal('0')
                disposal_gain_loss = entry.disposal_gain_loss_eur if entry.disposal_gain_loss_eur is not None else Decimal('0')
                holding_period = entry.holding_period_days_avg if entry.holding_period_days_avg is not None else 0
                erloes = disposal_proceeds if disposal_proceeds is not None and disposal_proceeds != 0 else entry.cost_or_proceeds
                kosten = disposal_cost_basis if disposal_cost_basis is not None and disposal_cost_basis != 0 else Decimal('0')
                gewinn_verlust = disposal_gain_loss if disposal_gain_loss is not None else (erloes - kosten)
                writer.writerow({
                    'Datum': format_timestamp(entry.timestamp),
                    'Asset': entry.asset,
                    'Menge': str(entry.amount),
                    'Erlös (EUR)': str(erloes),
                    'Anschaffungskosten (EUR)': str(kosten),
                    'Gewinn/Verlust (EUR)': str(gewinn_verlust),
                    'Steuerpflicht (EUR)': str(entry.tax_liability if entry.tax_liability is not None else Decimal('0')),
                    'Haltedauer (Tage)': str(holding_period),
                    'Langfristig': 'Ja' if entry.is_long_term else 'Nein',
                    'Steuerpflichtig': 'Ja' if entry.is_taxable else 'Nein',
                    'Referenz-ID': entry.refid,
                })
        created_files['report'] = str(report_path)
        log_event("Export", f"Created CSV tax reports in {output_path}")
        return created_files
    except Exception as e:
        error_msg = f"Failed to export CSV reports: {str(e)}"
        log_error("reporting", "CSVExportError", error_msg, details={"tax_year": tax_year}, exception=e)
        return {}

def export_as_year_csv(summary: AggregatedTaxSummary, tax_year: int, output_path: Path, delimiter: str = ";") -> str:
    try:
        year_filename = f"krypto_steuer_{tax_year}.csv"
        year_path = output_path / year_filename
        with open(year_path, 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = [
                'Bezeichnung des Wirtschaftsguts',
                'Anschaffungsdatum',
                'Anschaffungskosten',
                'Veräußerungsdatum',
                'Veräußerungspreis oder Wert bei Abgabe',
                'Werbungskosten (Veräußerungskosten)',
                'Gewinn/Verlust'
            ]
            writer = csv.writer(csvfile, delimiter=delimiter)
            writer.writerow(fieldnames)

            for entry in summary.tax_report_entries:
                entry_date = datetime.fromtimestamp(entry.timestamp)
                if entry_date.year != tax_year:
                    continue
                transaction_date = entry_date.strftime("%d.%m.%Y")
                
                # Calculate sale_price_per_unit safely
                sale_price_per_unit = Decimal('0')
                if entry.amount and entry.amount != Decimal('0'): # Check for non-zero amount
                    proceeds = entry.disposal_proceeds_eur if entry.disposal_proceeds_eur is not None else entry.cost_or_proceeds
                    if proceeds is not None: # Ensure proceeds is not None
                         sale_price_per_unit = Decimal(proceeds) / Decimal(abs(entry.amount))

                if not entry.matched_lots:
                    row = [
                        entry.asset,
                        "Unbekannt", # Anschaffungsdatum
                        "0.00",      # Anschaffungskosten
                        transaction_date, # Veräußerungsdatum
                        f"{(entry.disposal_proceeds_eur if entry.disposal_proceeds_eur is not None else entry.cost_or_proceeds):.2f}", # Veräußerungspreis
                        f"{(entry.disposal_fee_eur if entry.disposal_fee_eur is not None else Decimal('0')):.2f}", # Werbungskosten
                        f"{(entry.disposal_gain_loss_eur if entry.disposal_gain_loss_eur is not None else Decimal('0')):.2f}"  # Gewinn/Verlust
                    ]
                    writer.writerow(row)
                    continue

                for matched_lot in entry.matched_lots:
                    lot_purchase_date = matched_lot.original_lot_purchase_date.strftime("%d.%m.%Y")
                    
                    # Anschaffungskosten for this portion of the lot
                    acquisition_cost_lot_portion = Decimal(matched_lot.original_lot_purchase_price_eur) * Decimal(matched_lot.amount_used)
                    
                    # Veräußerungspreis for this portion of the lot
                    proceeds_lot_portion = Decimal(sale_price_per_unit) * Decimal(matched_lot.amount_used)
                    
                    # Gebühren for this portion of the lot (assuming disposal_fee_eur on matched_lot is for that portion)
                    fees_lot_portion = Decimal(matched_lot.disposal_fee_eur if matched_lot.disposal_fee_eur is not None else Decimal('0'))
                    
                    # Gewinn/Verlust for this portion of the lot
                    gain_loss_lot_portion = proceeds_lot_portion - acquisition_cost_lot_portion - fees_lot_portion
                    
                    row = [
                        entry.asset,                                      # Bezeichnung des Wirtschaftsguts
                        lot_purchase_date,                                # Anschaffungsdatum
                        f"{acquisition_cost_lot_portion:.2f}",            # Anschaffungskosten
                        transaction_date,                                 # Veräußerungsdatum
                        f"{proceeds_lot_portion:.2f}",                    # Veräußerungspreis oder Wert bei Abgabe
                        f"{fees_lot_portion:.2f}",                        # Werbungskosten (Veräußerungskosten)
                        f"{gain_loss_lot_portion:.2f}"                    # Gewinn/Verlust
                    ]
                    writer.writerow(row)
        
        # Summary removed from CSV output as per requirements. It remains in fifo_nachweis_YEAR.txt
        
        fifo_txt_filename = f"fifo_nachweis_{tax_year}.txt"
        fifo_txt_path = ensure_output_dir("export") / fifo_txt_filename
        with open(fifo_txt_path, 'w', encoding='utf-8') as f:
            f.write(f"FIFO Nachweis für Steuerjahr {tax_year}\n")
            f.write("="*80 + "\n\n")
            f.write("Gemäß BMF-Schreiben zur steuerlichen Behandlung von Kryptowährungen\n")
            f.write("werden Veräußerungen nach dem FIFO-Prinzip (First In - First Out) behandelt.\n\n")
            f.write("Detailaufstellung der Veräußerungen:\n")
            f.write("-"*80 + "\n\n")
            entry_count = 0
            for entry in summary.tax_report_entries:
                entry_date = datetime.fromtimestamp(entry.timestamp)
                if entry_date.year != tax_year:
                    continue
                entry_count += 1
                transaction_date = entry_date.strftime("%Y-%m-%d")
                sale_price_per_unit = (float(entry.disposal_proceeds_eur) if entry.disposal_proceeds_eur is not None 
                                       else float(entry.cost_or_proceeds)) / float(abs(entry.amount)) if float(entry.amount) != 0 else 0
                f.write(f"Veräußerung #{entry_count}:\n")
                f.write(f"  Datum: {transaction_date}\n")
                f.write(f"  Asset: {entry.asset}\n")
                f.write(f"  Verkaufte Menge: {abs(float(entry.amount)):.8f}\n")
                f.write(f"  Verkaufspreis/Stk: {sale_price_per_unit:.4f} €\n")
                f.write(f"  Gesamterlös: {float(entry.disposal_proceeds_eur) if entry.disposal_proceeds_eur is not None else float(entry.cost_or_proceeds):.2f} €\n")
                f.write(f"  Gebühren: {float(entry.disposal_fee_eur):.2f} €\n")
                if entry.matched_lots:
                    f.write("  FIFO-Zuordnung:\n")
                    for j, lot in enumerate(entry.matched_lots, 1):
                        purchase_date = lot.original_lot_purchase_date.strftime("%Y-%m-%d")
                        f.write(f"    - Lot {j}: Kauf von {float(lot.amount_used):.8f} {entry.asset} am {purchase_date} @ Einkaufspreis: {float(lot.original_lot_purchase_price_eur):.4f} €/{entry.asset}, Haltedauer: {lot.holding_period_days} Tage\n")
                        f.write(f"      Verkaufserlös: {float(lot.amount_used) * sale_price_per_unit:.2f} €\n")
                        gain_loss = (sale_price_per_unit - float(lot.original_lot_purchase_price_eur)) * float(lot.amount_used) - float(lot.disposal_fee_eur)
                        f.write(f"      Gewinn/Verlust: {gain_loss:.2f} €\n")
                        lot_taxable = "Ja" if lot.holding_period_days <= 365 else "Nein"
                        lot_tax_reason = "Haltedauer <= 1 Jahr, steuerpflichtig" if lot.holding_period_days <= 365 else "Haltedauer > 1 Jahr, steuerfrei"
                        f.write(f"      Steuerpflichtig: {lot_taxable} ({lot_tax_reason})\n")
                else:
                    f.write("  FIFO-Zuordnung:\n")
                    f.write("    - Keine Kaufdaten gefunden\n")
                if entry.matched_lots:
                    total_gl = 0
                    for lot in entry.matched_lots:
                        total_gl += (sale_price_per_unit - float(lot.original_lot_purchase_price_eur)) * float(lot.amount_used) - float(lot.disposal_fee_eur)
                    f.write(f"  Gewinn/Verlust: {total_gl:.2f} €\n")
                else:
                    f.write(f"  Gewinn/Verlust: {float(entry.disposal_gain_loss_eur):.2f} €\n")
                # Removed summary-level Steuerpflichtig; tax info is now reported per lot.
                f.write("-"*40 + "\n\n")
            f.write("\n" + "="*80 + "\n")
            f.write("Steuerliche Zusammenfassung\n")
            f.write("="*80 + "\n\n")
            f.write(f"Steuerjahr: {tax_year}\n\n")
            total_proceeds = Decimal(0)
            total_costs = Decimal(0)
            total_fees = Decimal(0)
            for entry in summary.tax_report_entries:
                entry_date = datetime.fromtimestamp(entry.timestamp)
                if entry_date.year == tax_year:
                    total_proceeds += entry.disposal_proceeds_eur if entry.disposal_proceeds_eur is not None else Decimal('0')
                    total_costs += entry.disposal_cost_basis_eur if entry.disposal_cost_basis_eur is not None else Decimal('0')
                    total_fees += entry.disposal_fee_eur if entry.disposal_fee_eur is not None else Decimal('0')
            net_amount = float(total_proceeds) - float(total_costs) - float(total_fees)
            f.write("Private Veräußerungsgeschäfte (§23 EStG):\n")
            f.write(f"  Verkaufserlös: {float(total_proceeds):.2f} €\n")
            f.write(f"  Einkaufskosten: {float(total_costs):.2f} €\n")
            f.write(f"  Gebühren: {float(total_fees):.2f} €\n")
            if net_amount >= 0:
                f.write(f"  Gesamtgewinn (§23): {net_amount:.2f} €\n")
            else:
                f.write(f"  Gesamtverlust (§23): {net_amount:.2f} €\n")
            f.write(f"  Freigrenze (§23): {1000.00 if tax_year >= 2023 else 600.00:.2f} €\n")
            is_taxable_23 = "Ja" if summary.private_sales_taxable else "Nein"
            f.write(f"  Steuerpflichtig (§23): {is_taxable_23}\n\n")
            if summary.total_other_income and summary.total_other_income > Decimal('0'):
                f.write("Sonstige Einkünfte (§22 Nr. 3 EStG):\n")
                f.write(f"  Gesamteinkünfte (z.B. Staking): {float(summary.total_other_income):.2f} €\n")
                f.write(f"  Freigrenze (§22): {float(summary.freigrenze_other_income):.2f} €\n")
                is_taxable_22 = "Ja" if summary.other_income_taxable else "Nein"
                f.write(f"  Steuerpflichtig (§22): {is_taxable_22}\n")
        
        log_event("Export", f"Created German format tax report: {year_path}")
        log_event("Export", f"Created FIFO Nachweis text report: {fifo_txt_path}")
        
        return str(year_path)
    except Exception as e:
        error_msg = f"Failed to export year CSV report: {str(e)}"
        log_error("reporting", "YearCSVExportError", error_msg, details={"tax_year": tax_year}, exception=e)
        return ""
