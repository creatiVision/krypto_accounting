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

from .models import TaxReportEntry, AggregatedTaxSummary, MatchedLotInfo, Transaction
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
                
                sale_price_per_unit = Decimal('0')
                if entry.amount and entry.amount != Decimal('0'): 
                    proceeds = entry.disposal_proceeds_eur if entry.disposal_proceeds_eur is not None else entry.cost_or_proceeds
                    if proceeds is not None: 
                         sale_price_per_unit = Decimal(proceeds) / Decimal(abs(entry.amount))

                if not entry.matched_lots:
                    bezeichnung_unmatched = f"Verkauf {entry.asset} am {transaction_date} - Details zu Anschaffung unbekannt"
                    row = [
                        bezeichnung_unmatched,
                        "Unbekannt", 
                        "0.00",      
                        transaction_date, 
                        f"{(entry.disposal_proceeds_eur if entry.disposal_proceeds_eur is not None else entry.cost_or_proceeds):.2f}", 
                        f"{(entry.disposal_fee_eur if entry.disposal_fee_eur is not None else Decimal('0')):.2f}", 
                        f"{(entry.disposal_gain_loss_eur if entry.disposal_gain_loss_eur is not None else Decimal('0')):.2f}"
                    ]
                    writer.writerow(row)
                    continue

                for matched_lot in entry.matched_lots:
                    lot_purchase_date = matched_lot.original_lot_purchase_date.strftime("%d.%m.%Y")
                    
                    acquisition_cost_lot_portion = Decimal(matched_lot.original_lot_purchase_price_eur) * Decimal(matched_lot.amount_used)
                    proceeds_lot_portion = Decimal(sale_price_per_unit) * Decimal(matched_lot.amount_used)
                    fees_lot_portion = Decimal(matched_lot.disposal_fee_eur if matched_lot.disposal_fee_eur is not None else Decimal('0'))
                    gain_loss_lot_portion = proceeds_lot_portion - acquisition_cost_lot_portion - fees_lot_portion
                    
                    if entry_date.tzinfo is not None and matched_lot.original_lot_purchase_date.tzinfo is None:
                        entry_date_naive = entry_date.replace(tzinfo=None)
                        holding_period_days = (entry_date_naive - matched_lot.original_lot_purchase_date).days
                    elif entry_date.tzinfo is None and matched_lot.original_lot_purchase_date.tzinfo is not None:
                        lot_date_naive = matched_lot.original_lot_purchase_date.replace(tzinfo=None)
                        holding_period_days = (entry_date - lot_date_naive).days
                    else:
                        holding_period_days = (entry_date - matched_lot.original_lot_purchase_date).days
                    matched_lot.holding_period_days = holding_period_days

                    bezeichnung_matched = f"Verkauf {entry.asset} am {transaction_date} - Lot: Kauf {Decimal(matched_lot.amount_used):.8f} {entry.asset} am {lot_purchase_date} @ {Decimal(matched_lot.original_lot_purchase_price_eur):.4f} EUR/{entry.asset}, Haltedauer {holding_period_days} Tage"
                    row = [
                        bezeichnung_matched,
                        lot_purchase_date,
                        f"{acquisition_cost_lot_portion:.2f}",
                        transaction_date,
                        f"{proceeds_lot_portion:.2f}",
                        f"{fees_lot_portion:.2f}",
                        f"{gain_loss_lot_portion:.2f}"
                    ]
                    writer.writerow(row)
        
        fifo_txt_filename = f"fifo_nachweis_{tax_year}.txt"
        fifo_txt_path = ensure_output_dir("export") / fifo_txt_filename
        with open(fifo_txt_path, 'w', encoding='utf-8') as f:
            f.write(f"FIFO Nachweis für Steuerjahr {tax_year}\n")
            f.write("="*80 + "\n\n")
            f.write("Gemäß BMF-Schreiben zur steuerlichen Behandlung von Kryptowährungen\n")
            f.write("werden Veräußerungen nach dem FIFO-Prinzip (First In - First Out) behandelt.\n\n")
            f.write("Detailaufstellung der Veräußerungen:\n")
            f.write("-"*80 + "\n\n")
            
            for report_entry_txt in summary.tax_report_entries:
                entry_date_obj_txt = datetime.fromtimestamp(report_entry_txt.timestamp)
                if entry_date_obj_txt.year != tax_year:
                    continue

                transaction_date_ddmmyyyy_txt = entry_date_obj_txt.strftime("%d.%m.%Y")

                sale_price_per_unit_txt_entry = Decimal('0')
                if report_entry_txt.amount and \
                   isinstance(report_entry_txt.amount, Decimal) and \
                   report_entry_txt.amount.is_normal() and \
                   Decimal(abs(report_entry_txt.amount)) != Decimal('0'):
                    entry_proceeds_raw_txt = report_entry_txt.disposal_proceeds_eur if report_entry_txt.disposal_proceeds_eur is not None else report_entry_txt.cost_or_proceeds
                    entry_proceeds_txt = Decimal(entry_proceeds_raw_txt if entry_proceeds_raw_txt is not None else '0')
                    sale_price_per_unit_txt_entry = entry_proceeds_txt / Decimal(abs(report_entry_txt.amount))
                
                f.write("--------------------------------------------------------------------------------\n")
                f.write(f"Veräußerung von {report_entry_txt.asset} am {transaction_date_ddmmyyyy_txt}\n")
                f.write(f"Referenz-ID der Veräußerung: {report_entry_txt.refid}\n")
                f.write("--------------------------------------------------------------------------------\n")

                if report_entry_txt.matched_lots:
                    for lot_index, matched_lot_txt in enumerate(report_entry_txt.matched_lots):
                        purchase_date_ddmmyyyy_txt = matched_lot_txt.original_lot_purchase_date.strftime("%d.%m.%Y")
                        holding_period_days_val_txt = matched_lot_txt.holding_period_days if isinstance(matched_lot_txt.holding_period_days, int) else 0

                        bezeichnung_lot_txt = f"Verkauf {report_entry_txt.asset} am {transaction_date_ddmmyyyy_txt} - Lot (RefID: {matched_lot_txt.original_lot_refid}): Kauf {Decimal(matched_lot_txt.amount_used):.8f} {report_entry_txt.asset} am {purchase_date_ddmmyyyy_txt} @ {Decimal(matched_lot_txt.original_lot_purchase_price_eur):.4f} EUR/{report_entry_txt.asset}, Haltedauer: {holding_period_days_val_txt} Tage"
                        f.write(f"Zeile 42: Bezeichnung: {bezeichnung_lot_txt}\n")
                        
                        f.write(f"Zeile 43: Zeitpunkt der Anschaffung: {purchase_date_ddmmyyyy_txt}\n")
                        f.write(f"Zeile 43: Zeitpunkt der Veräußerung: {transaction_date_ddmmyyyy_txt}\n")

                        proceeds_lot_portion_txt = sale_price_per_unit_txt_entry * Decimal(matched_lot_txt.amount_used)
                        f.write(f"Zeile 44: Veräußerungspreis: {proceeds_lot_portion_txt:.2f} EUR\n")
                        
                        acquisition_cost_lot_portion_txt = Decimal(matched_lot_txt.original_lot_purchase_price_eur) * Decimal(matched_lot_txt.amount_used)
                        f.write(f"Zeile 45: Anschaffungskosten: {acquisition_cost_lot_portion_txt:.2f} EUR\n")
                        
                        fees_lot_portion_txt = Decimal(matched_lot_txt.disposal_fee_eur if matched_lot_txt.disposal_fee_eur is not None else '0')
                        f.write(f"Zeile 46: Werbungskosten (Veräußerungsgebühren): {fees_lot_portion_txt:.2f} EUR\n")
                        
                        gain_loss_lot_portion_txt = proceeds_lot_portion_txt - acquisition_cost_lot_portion_txt - fees_lot_portion_txt
                        f.write(f"Zeile 47: Gewinn / Verlust: {gain_loss_lot_portion_txt:.2f} EUR\n")
                        
                        tax_status_lot_txt = 'Ja' if holding_period_days_val_txt <= 365 else 'Nein'
                        holding_comparison_lot_txt = '<=' if holding_period_days_val_txt <= 365 else '>'
                        f.write(f"  Steuerpflichtig: {tax_status_lot_txt} (Haltedauer {holding_comparison_lot_txt} 1 Jahr)\n")
                        f.write("    --- (Ende Lot) ---\n\n")
                else: 
                    f.write(f"Zeile 42: Bezeichnung: Verkauf {report_entry_txt.asset} am {transaction_date_ddmmyyyy_txt} - Details zu Anschaffung unbekannt.\n")
                    f.write(f"Zeile 43: Zeitpunkt der Anschaffung: Unbekannt\n")
                    f.write(f"Zeile 43: Zeitpunkt der Veräußerung: {transaction_date_ddmmyyyy_txt}\n")

                    unmatched_proceeds_raw_txt = report_entry_txt.disposal_proceeds_eur if report_entry_txt.disposal_proceeds_eur is not None else report_entry_txt.cost_or_proceeds
                    unmatched_proceeds_val_txt = Decimal(unmatched_proceeds_raw_txt if unmatched_proceeds_raw_txt is not None else '0')
                    f.write(f"Zeile 44: Veräußerungspreis: {unmatched_proceeds_val_txt:.2f} EUR\n")
                    
                    f.write(f"Zeile 45: Anschaffungskosten: 0.00 EUR\n")
                    
                    unmatched_fees_val_txt = Decimal(report_entry_txt.disposal_fee_eur if report_entry_txt.disposal_fee_eur is not None else '0')
                    f.write(f"Zeile 46: Werbungskosten (Veräußerungsgebühren): {unmatched_fees_val_txt:.2f} EUR\n")
                    
                    gain_loss_unmatched_raw_txt = report_entry_txt.disposal_gain_loss_eur
                    if gain_loss_unmatched_raw_txt is not None:
                        gain_loss_unmatched_final_txt = Decimal(gain_loss_unmatched_raw_txt)
                    else: 
                        gain_loss_unmatched_final_txt = unmatched_proceeds_val_txt - unmatched_fees_val_txt
                    
                    f.write(f"Zeile 47: Gewinn / Verlust: {gain_loss_unmatched_final_txt:.2f} EUR\n")
                    f.write(f"  Steuerpflichtig: Ja (Details zu Anschaffung unbekannt, Haltedauer kann nicht ermittelt werden)\n\n")
            
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

def export_raw_transactions_csv(all_transactions: List[Transaction], output_dir: str = "export") -> str:
    """Exports all raw transactions to a CSV file."""
    try:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"Crypto-Steueraufstellung_Raw_Transactions_{timestamp}.csv"
        
        output_path_obj = ensure_output_dir(output_dir)
        file_path = output_path_obj / filename

        header = [
            'Transaction ID', 'Time (UTC)', 'Type', 'Subtype', 'Asset', 'Wallet',
            'Amount', 'Currency of Amount', 'Price per Unit (Original)', 'Currency of Price (Original)',
            'Total Value (Original)', 'Currency of Total Value (Original)', 'Fee (Original)', 'Currency of Fee (Original)',
            'Value (EUR)', 'Fee (EUR)', 'Notes'
        ]

        with open(file_path, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile, delimiter=';')
            writer.writerow(header)

            for tx in all_transactions:
                # Helper to convert None to empty string, and Decimal to string
                def fmt(value):
                    if value is None:
                        return ""
                    if isinstance(value, Decimal):
                        return str(value) # Decimals use '.' by default
                    return str(value)

                row = [
                    fmt(tx.refid),
                    tx.datetime_utc.strftime("%Y-%m-%d %H:%M:%S") if tx.datetime_utc else "",
                    fmt(tx.internal_type if tx.internal_type else tx.kraken_type),
                    fmt(tx.kraken_subtype),
                    fmt(tx.asset),
                    "N/A",  # Wallet
                    fmt(tx.amount),
                    fmt(tx.asset), # Currency of Amount
                    fmt(tx.price),
                    fmt(tx.quote_asset), # Currency of Price (Original)
                    fmt(tx.cost_or_proceeds),
                    fmt(tx.quote_asset), # Currency of Total Value (Original)
                    fmt(tx.fee_amount),
                    fmt(tx.fee_asset), # Currency of Fee (Original)
                    fmt(tx.value_eur),
                    fmt(tx.fee_value_eur),
                    fmt(tx.notes)
                ]
                writer.writerow(row)
        
        log_event("Export", f"Successfully exported raw transactions to {file_path}")
        return str(file_path)

    except Exception as e:
        error_msg = f"Failed to export raw transactions CSV: {str(e)}"
        log_error("reporting", "RawTransactionExportError", error_msg, details={}, exception=e)
        return ""
