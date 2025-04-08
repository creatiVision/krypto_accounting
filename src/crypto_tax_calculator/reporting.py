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
            
        # Try to detect the delimiter
        with open(input_path, 'r', newline='') as f:
            sample = f.read(4096)  # Read a sample to detect delimiter
            
        # Count occurrences of potential delimiters
        comma_count = sample.count(',')
        semicolon_count = sample.count(';')
        
        # Determine the source delimiter
        source_delimiter = ';' if semicolon_count > comma_count else ','
        
        if source_delimiter == target_delimiter:
            log_event("CSV", f"File {input_path} already uses the target delimiter: {target_delimiter}")
            return input_path
            
        log_event("CSV", f"Converting {input_path} from delimiter '{source_delimiter}' to '{target_delimiter}'")
        
        # Read the source file with the detected delimiter and write with the target delimiter
        rows = []
        with open(input_path, 'r', newline='') as infile:
            reader = csv.reader(infile, delimiter=source_delimiter)
            for row in reader:
                rows.append(row)
        
        with open(temp_path, 'w', newline='') as outfile:
            writer = csv.writer(outfile, delimiter=target_delimiter)
            writer.writerows(rows)
            
        # If we're modifying in place, replace the original file
        if output_path == input_path:
            os.replace(temp_path, input_path)
            
        log_event("CSV", f"Successfully converted delimiter in {input_path}")
        return output_path
    except Exception as e:
        error_msg = f"Failed to unify CSV delimiter: {str(e)}"
        log_error("reporting", "CSVDelimiterError", error_msg, 
                 details={"input_path": input_path, "output_path": output_path}, 
                 exception=e)
        # Return the input path, indicating no changes were made
        return input_path

def unify_all_csv_files(directory: str, target_delimiter: str = ',', recursive: bool = True) -> List[str]:
    """
    Process all CSV files in a directory to use a standardized delimiter.
    
    Parameters:
        directory: Directory containing CSV files
        target_delimiter: The delimiter to standardize to (default: comma)
        recursive: Whether to process subdirectories
        
    Returns:
        List of paths to processed files
    """
    try:
        processed_files = []
        dir_path = Path(directory)
        
        # Function to process a single directory
        def process_directory(path):
            nonlocal processed_files
            for item in path.iterdir():
                if item.is_file() and item.suffix.lower() == '.csv':
                    processed_file = unify_csv_delimiter(str(item), target_delimiter=target_delimiter)
                    processed_files.append(processed_file)
                elif recursive and item.is_dir():
                    process_directory(item)
        
        # Start processing from the root directory
        process_directory(dir_path)
        log_event("CSV", f"Processed {len(processed_files)} CSV files in {directory}")
        return processed_files
    except Exception as e:
        error_msg = f"Failed to unify CSV files in directory: {str(e)}"
        log_error("reporting", "CSVBatchError", error_msg, 
                 details={"directory": directory}, 
                 exception=e)
        return []

def check_missing_sales_2024(summary: AggregatedTaxSummary, known_sales_data: List[Dict]) -> List[Dict]:
    """
    Check for sales from 2024 that might be missing from the provided reports.
    
    Parameters:
        summary: The tax summary containing processed entries
        known_sales_data: List of known sales transactions
        
    Returns:
        List of potentially missing sale entries
    """
    try:
        # Extract 2024 entries from the summary
        included_sales = {}
        for entry in summary.tax_report_entries:
            entry_date = datetime.fromtimestamp(entry.timestamp)
            if entry_date.year == 2024:
                key = f"{entry.asset}_{entry.timestamp}_{entry.refid}"
                included_sales[key] = entry
        
        # Find entries in known_sales that don't appear in the summary
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

def export_tax_report(
    summary: AggregatedTaxSummary,
    tax_year: int,
    output_dir: str = "export",
    format: str = "csv",
    include_lot_details: bool = True,
    csv_delimiter: str = ";"  # Changed default to semicolon for German format
) -> Dict[str, str]:
    """
    Export tax report in the specified format.
    Returns a dictionary with the paths of all created files.
    """
    try:
        output_path = ensure_output_dir(output_dir)
        created_files = {}

        # Only generate German FIFO Nachweis format for tax year reports
        german_file = export_as_year_csv(summary, tax_year, output_path, delimiter=";")
        if german_file:
            created_files['year_csv'] = german_file
        
        log_event("Export", f"Successfully exported tax report in German format for year {tax_year}")
        return created_files
    except Exception as e:
        error_msg = f"Failed to export tax report: {str(e)}"
        log_error("reporting", "ExportError", error_msg, 
                 details={"tax_year": tax_year, "format": format}, 
                 exception=e)
        return {}

def export_as_csv(
    summary: AggregatedTaxSummary,
    tax_year: int,
    output_path: Path,
    include_lot_details: bool = True,
    delimiter: str = ";"  # Changed default to semicolon for German format
) -> Dict[str, str]:
    """Export tax report as CSV files."""
    try:
        created_files = {}
        
        # Main tax report
        report_filename = create_filename("tax_report", tax_year, "csv")
        report_path = output_path / report_filename
        
        with open(report_path, 'w', newline='') as csvfile:
            # German headers for tax authorities
            fieldnames = [
                'Datum', 'Asset', 'Menge', 'Erlös (EUR)', 'Anschaffungskosten (EUR)', 
                'Gewinn/Verlust (EUR)', 'Steuerpflicht (EUR)', 'Haltedauer (Tage)', 
                'Langfristig', 'Steuerpflichtig', 'Referenz-ID'
            ]
            # Ensure delimiter is properly set (default to semicolon for German format)
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames, delimiter=delimiter if delimiter else ';')
            writer.writeheader()
            
            for entry in summary.tax_report_entries:
                # Ensure all values are properly included, using 0 as fallback for None values
                disposal_proceeds = entry.disposal_proceeds_eur if entry.disposal_proceeds_eur is not None else (entry.cost_or_proceeds if entry.cost_or_proceeds is not None else Decimal('0'))
                disposal_cost_basis = entry.disposal_cost_basis_eur if entry.disposal_cost_basis_eur is not None else Decimal('0')
                disposal_gain_loss = entry.disposal_gain_loss_eur if entry.disposal_gain_loss_eur is not None else Decimal('0')
                holding_period = entry.holding_period_days_avg if entry.holding_period_days_avg is not None else 0
                
                # Ensure we have valid values for all fields
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
        log_error("reporting", "CSVExportError", error_msg, 
                 details={"tax_year": tax_year}, 
                 exception=e)
        return {}

def export_as_year_csv(
    summary: AggregatedTaxSummary,
    tax_year: int,
    output_path: Path,
    delimiter: str = ";"
) -> str:
    """
    Export tax report in German FIFO Nachweis format.
    
    Parameters:
        summary: The tax summary to export
        tax_year: The year of the tax report
        output_path: Directory to save output files
        delimiter: CSV delimiter character (default: semicolon for German convention)
        
    Returns:
        The path to the generated CSV file or empty string on failure
    """
    try:
        # Generate a readable and specific filename for the year file
        year_filename = f"krypto_steuer_{tax_year}.csv"
        year_path = output_path / year_filename
        
        # Write the CSV file
        with open(year_path, 'w', newline='') as csvfile:
            fieldnames = [
                'Zeile', 'Typ', 'Steuer-Kategorie', 'Transaktions-Datum', 'Asset', 'Anzahl',
                'Kaufdatum', 'Einkaufspreis (EUR)/Stk', 'Verkaufsdatum', 'Verkaufspreis (EUR)/Stk',
                'Gesamtkosten (EUR)', 'Verkaufserlös (EUR)', 'Gebühren (EUR)', 'Gewinn / Verlust (EUR)',
                'Haltedauer (Tage)', 'Haltedauer > 1 Jahr', 'Steuerpflichtig', 'Steuergrund',
                'FIFO-Details', 'Notizen'
            ]
            # CSV writer with specified delimiter (default is semicolon, as per German convention)
            writer = csv.writer(csvfile, delimiter=delimiter)
            writer.writerow(fieldnames)
            
            # Add each transaction with numbered rows
            row_counter = 1
            for entry in summary.tax_report_entries:
                # Skip entries that don't match the tax year
                entry_date = datetime.fromtimestamp(entry.timestamp)
                if entry_date.year != tax_year:
                    continue
                    
                # Format the transaction date from timestamp
                transaction_date = entry_date.strftime("%Y-%m-%d")
                
                # Determine transaction type - using 'SPEND' for crypto sales
                tx_type = "SPEND"
                
                # Tax category - always "Privates Veräußerungsgeschäft (§23 EStG)" for crypto sales
                tax_category = "Privates Veräußerungsgeschäft (§23 EStG)"
                
                # Calculate sale price per unit for each entry
                sale_price_per_unit = float(entry.disposal_proceeds_eur or entry.cost_or_proceeds) / float(abs(entry.amount)) if float(entry.amount) != 0 else 0
                
                # If no matched lots, still create a row with available information
                if not entry.matched_lots:
                    # Define tax status variables
                    is_taxable_text = "Ja" if entry.is_taxable else "Nein"
                    tax_reason = "Haltedauer <= 1 Jahr" if entry.is_taxable else "Haltedauer > 1 Jahr"
                    
                    # Row data for this entry without matched lots
                    row = [
                        str(row_counter),  # Zeile
                        tx_type,  # Typ
                        tax_category,  # Steuer-Kategorie
                        transaction_date,  # Transaktions-Datum
                        entry.asset,  # Asset
                        str(abs(entry.amount)),  # Anzahl
                        "Unbekannt",  # Kaufdatum
                        "0.0000",  # Kaufpreis (€)/Stk
                        transaction_date,  # Verkaufsdatum
                        f"{sale_price_per_unit:.4f}",  # Verkaufspreis (€)/Stk
                        "0.00",  # Gesamtkosten (€)
                        str(entry.disposal_proceeds_eur or entry.cost_or_proceeds),  # Gesamterlös (€)
                        str(entry.disposal_fee_eur or 0),  # Gebühr (€)
                        str(entry.disposal_gain_loss_eur or 0),  # Gewinn / Verlust (€)
                        str(entry.holding_period_days_avg or 0),  # Haltedauer (Tage)
                        "Nein",  # Haltedauer > 1 Jahr
                        is_taxable_text,  # Steuerpflichtig
                        tax_reason,  # Steuergrund
                        "Keine FIFO-Details verfügbar",  # FIFO-Details
                        "Warnung: Keine Kaufdaten gefunden"  # Notizen
                    ]
                    writer.writerow(row)
                    row_counter += 1
                    continue
                
                # Format the FIFO details information for entries with matched lots
                for matched_lot in entry.matched_lots:
                    lot_date = matched_lot.original_lot_purchase_date.strftime("%Y-%m-%d")
                    lot_price = matched_lot.original_lot_purchase_price_eur
                    
                    # Define tax status variables
                    is_taxable_text = "Ja" if entry.is_taxable else "Nein"
                    tax_reason = "Haltedauer <= 1 Jahr" if entry.is_taxable else "Haltedauer > 1 Jahr"
                    
                    # Calculate holding period for this specific lot
                    # Convert both dates to naive or aware to avoid "can't subtract offset-naive and offset-aware datetimes" error
                    if entry_date.tzinfo is not None and matched_lot.original_lot_purchase_date.tzinfo is None:
                        # entry_date is aware, matched_lot date is naive
                        entry_date_naive = entry_date.replace(tzinfo=None)
                        holding_period_days = (entry_date_naive - matched_lot.original_lot_purchase_date).days
                    elif entry_date.tzinfo is None and matched_lot.original_lot_purchase_date.tzinfo is not None:
                        # entry_date is naive, matched_lot date is aware
                        lot_date_naive = matched_lot.original_lot_purchase_date.replace(tzinfo=None)
                        holding_period_days = (entry_date - lot_date_naive).days
                    else:
                        # Both dates are either naive or aware
                        holding_period_days = (entry_date - matched_lot.original_lot_purchase_date).days
                    
                    # Calculate gain/loss for this specific lot
                    lot_gain_loss = (sale_price_per_unit - float(lot_price)) * float(matched_lot.amount_used)
                    
                    # Row data for this entry
                    row = [
                        str(row_counter),  # Zeile
                        tx_type,  # Typ
                        tax_category,  # Steuer-Kategorie
                        transaction_date,  # Transaktions-Datum
                        entry.asset,  # Asset
                        str(matched_lot.amount_used),  # Anzahl (using amount from matched lot)
                        lot_date,  # Kaufdatum
                        f"{lot_price:.4f}",  # Einkaufspreis (€)/Stk
                        transaction_date,  # Verkaufsdatum
                        f"{sale_price_per_unit:.4f}",  # Verkaufspreis (€)/Stk
                        f"{float(matched_lot.amount_used) * float(lot_price):.2f}",  # Gesamtkosten (€)
                        f"{float(matched_lot.amount_used) * sale_price_per_unit:.2f}",  # Verkaufserlös (€)
                        f"{float(matched_lot.disposal_fee_eur or 0):.2f}",  # Gebühren (€) - from matched lot
                        f"{lot_gain_loss:.2f}",  # Gewinn / Verlust (€)
                        str(holding_period_days),  # Haltedauer (Tage)
                        "Ja" if holding_period_days > 365 else "Nein",  # Haltedauer > 1 Jahr
                        is_taxable_text,  # Steuerpflichtig
                        tax_reason,  # Steuergrund
                        f"Lot {matched_lot.original_lot_refid}",  # FIFO-Details
                        "; ".join(entry.notes) if hasattr(entry, 'notes') and entry.notes else ""  # Notizen
                    ]
                    writer.writerow(row)
                    row_counter += 1
            
            # Add summary section with a blank row separator
            writer.writerow([])
            writer.writerow(["--- Steuerliche Zusammenfassung ---"])
            writer.writerow(["Steuerjahr:", str(tax_year)])
            writer.writerow([])
            
            # Calculate total proceeds, costs, and fees
            total_proceeds = Decimal(0)
            total_costs = Decimal(0)
            total_fees = Decimal(0)
            
            for entry in summary.tax_report_entries:
                entry_date = datetime.fromtimestamp(entry.timestamp)
                if entry_date.year == tax_year:
                    total_proceeds += entry.disposal_proceeds_eur or Decimal(0)
                    total_costs += entry.disposal_cost_basis_eur or Decimal(0)
                    total_fees += entry.disposal_fee_eur or Decimal(0)
            
            # Calculate net profit/loss
            net_amount = total_proceeds - total_costs - total_fees
            
            writer.writerow(["Private Veräußerungsgeschäfte (§23 EStG):"])
            writer.writerow(["Verkaufserlös:", str(total_proceeds)])
            writer.writerow(["Einkaufskosten:", str(total_costs)])
            writer.writerow(["Gebühren:", str(total_fees)])
            if net_amount >= 0:
                writer.writerow(["Gesamtgewinn (§23):", str(net_amount)])
            else:
                writer.writerow(["Gesamtverlust (§23):", str(net_amount)])
            writer.writerow(["Freigrenze (§23):", "1000.00" if tax_year >= 2023 else "600.00"])
            writer.writerow(["Steuerpflichtig (§23):", "Ja" if summary.private_sales_taxable else "Nein"])
            # Only include the "Sonstige Einkünfte" section if there's actually some other income
            if summary.total_other_income > Decimal(0):
                writer.writerow([])
                
                writer.writerow(["Sonstige Einkünfte (§22 Nr. 3 EStG):"])
                writer.writerow(["Gesamteinkünfte (z.B. Staking):", str(summary.total_other_income)])
                writer.writerow(["Freigrenze (§22):", str(summary.freigrenze_other_income)])
                writer.writerow(["Steuerpflichtig (§22):", "Ja" if summary.other_income_taxable else "Nein"])
        
        # Also create a "fifo_nachweis_{tax_year}.txt" file in plain text format 
        # with the FIFO Nachweis information
        fifo_txt_filename = f"fifo_nachweis_{tax_year}.txt"
        fifo_txt_path = output_path / fifo_txt_filename
        
        with open(fifo_txt_path, 'w') as f:
            f.write(f"FIFO Nachweis für Steuerjahr {tax_year}\n")
            f.write("="*80 + "\n\n")
            f.write("Gemäß BMF-Schreiben zur steuerlichen Behandlung von Kryptowährungen\n")
            f.write("werden Veräußerungen nach dem FIFO-Prinzip (First In - First Out) behandelt.\n\n")
            
            f.write("Detailaufstellung der Veräußerungen:\n")
            f.write("-"*80 + "\n\n")
            
            entry_count = 0
            for entry in summary.tax_report_entries:
                # Skip entries that don't match the tax year
                entry_date = datetime.fromtimestamp(entry.timestamp)
                if entry_date.year != tax_year:
                    continue
                
                entry_count += 1
                transaction_date = entry_date.strftime("%Y-%m-%d")
                sale_price_per_unit = float(entry.disposal_proceeds_eur or entry.cost_or_proceeds) / float(abs(entry.amount)) if float(entry.amount) != 0 else 0
                
                f.write(f"Veräußerung #{entry_count}:\n")
                f.write(f"  Datum: {transaction_date}\n")
                f.write(f"  Asset: {entry.asset}\n")
                f.write(f"  Verkaufte Menge: {abs(float(entry.amount)):.8f}\n")
                f.write(f"  Verkaufspreis/Stk: {sale_price_per_unit:.4f} €\n")
                f.write(f"  Gesamterlös: {float(entry.disposal_proceeds_eur or entry.cost_or_proceeds):.2f} €\n")
                f.write(f"  Gebühren: {float(entry.disposal_fee_eur or 0):.2f} €\n")
                
                if entry.matched_lots:
                    f.write("  FIFO-Zuordnung:\n")
                    for j, lot in enumerate(entry.matched_lots, 1):
                        purchase_date = lot.original_lot_purchase_date.strftime("%Y-%m-%d")
                        f.write(f"    - Lot {j}: Kauf von {float(lot.amount_used):.8f} {entry.asset} am {purchase_date} @ Einkaufspreis: {float(lot.original_lot_purchase_price_eur):.4f} €/{entry.asset}, Haltedauer: {lot.holding_period_days} Tage\n")
                        f.write(f"      Gesamtkosten: {float(lot.cost_basis_eur):.2f} €, Verkauf von {float(lot.amount_used):.8f} {entry.asset} am {transaction_date} @ Verkaufspreis: {sale_price_per_unit:.4f} €/{entry.asset}, Verkaufserlös: {float(lot.amount_used) * sale_price_per_unit:.2f} €, ")
                        gain_loss = (sale_price_per_unit - float(lot.original_lot_purchase_price_eur)) * float(lot.amount_used)
                        if gain_loss >= 0:
                            f.write(f"Gewinn: +{gain_loss:.2f} €\n")
                        else:
                            f.write(f"Verlust: {gain_loss:.2f} €\n")
                else:
                    f.write("  FIFO-Zuordnung:\n")
                    f.write("    - Keine Kaufdaten gefunden\n")
                    
                f.write(f"  Gesamtkosten (FIFO): {float(entry.disposal_cost_basis_eur or 0):.2f} €\n")
                f.write(f"  Gewinn/Verlust: {float(entry.disposal_gain_loss_eur or 0):.2f} €\n")
                f.write(f"  Haltedauer (Durchschnitt): {entry.holding_period_days_avg} Tage\n")
                
                # Tax status with reason
                taxable_status = "Ja" if entry.is_taxable else "Nein"
                tax_reason = "Haltedauer <= 1 Jahr, steuerpflichtig" if entry.is_taxable else "Haltedauer > 1 Jahr, steuerfrei"
                f.write(f"  Steuerpflichtig: {taxable_status} ({tax_reason})\n")
                
                # Add any notes or warnings
                if hasattr(entry, 'notes') and entry.notes:
                    f.write(f"  Notizen: {'; '.join(entry.notes)}\n")
                    
                f.write("-"*40 + "\n\n")
            
            # Add summary section
            f.write("\n" + "="*80 + "\n")
            f.write("Steuerliche Zusammenfassung\n")
            f.write("="*80 + "\n\n")
            
            f.write(f"Steuerjahr: {tax_year}\n\n")
            
            # Calculate total proceeds, costs, and fees
            total_proceeds = Decimal(0)
            total_costs = Decimal(0)
            total_fees = Decimal(0)
            
            for entry in summary.tax_report_entries:
                entry_date = datetime.fromtimestamp(entry.timestamp)
                if entry_date.year == tax_year:
                    total_proceeds += entry.disposal_proceeds_eur or Decimal(0)
                    total_costs += entry.disposal_cost_basis_eur or Decimal(0)
                    total_fees += entry.disposal_fee_eur or Decimal(0)
            
            # Calculate net profit/loss
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
            
            # Only include the "Sonstige Einkünfte" section if there's actually some other income
            if summary.total_other_income > Decimal(0):
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
        log_error("reporting", "YearCSVExportError", error_msg, 
                 details={"tax_year": tax_year}, 
                 exception=e)
        # Return empty string to indicate failure
        return ""
