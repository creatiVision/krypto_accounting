#!/usr/bin/env python3
"""
This script enhances the German crypto tax reporting with:
1. Extended headers with more detailed tax reasoning
2. Comprehensive FIFO documentation for tax authority review
3. Enhanced sale processing with detailed reasoning for taxability
"""

import re
from pathlib import Path

def enhance_tax_reporting():
    """Apply all tax reporting enhancements to the main module"""
    # Path to the module
    module_path = Path(__file__).parent / "krypto-accounting_german_tax.py"
    
    # Read the file content
    with open(module_path, 'r') as f:
        content = f.read()
    
    # Apply all enhancements
    content = enhance_headers(content)
    content = enhance_fifo_documentation(content)
    content = enhance_sale_processing(content)
    
    # Write the updated content back to the file
    with open(module_path, 'w') as f:
        f.write(content)
    
    print(f"Successfully enhanced tax reporting in {module_path}")
    return True

def enhance_headers(content):
    """Enhance the HEADERS definition with more detailed columns"""
    # Find the HEADERS definition
    headers_pattern = r'HEADERS = \[(.*?)\]'
    headers_match = re.search(headers_pattern, content, re.DOTALL)
    
    if not headers_match:
        print("Could not find HEADERS definition")
        return content
    
    # Define the new enhanced headers
    new_headers = '''HEADERS = [
    "Zeile", "Typ", "Steuer-Kategorie", "Transaktionsdatum", "Asset", "Anzahl", 
    "Kaufdatum", "Kaufpreis (€)/Stk", "Verkaufsdatum", "Verkaufspreis (€)/Stk", 
    "Kosten (€)", "Erlös (€)", "Gebühr (€)", "Gewinn / Verlust (€)", "Haltedauer (Tage)", 
    "Haltedauer > 1 Jahr", "Steuerpflichtig", "Steuergrund", "FIFO-Details", "Notizen"
]'''
    
    # Replace the headers
    updated_content = content.replace(headers_match.group(0), new_headers)
    
    return updated_content

def enhance_fifo_documentation(content):
    """Enhance the FIFO documentation export function"""
    # Find the export_detailed_fifo_documentation function
    function_pattern = r'def export_detailed_fifo_documentation\(year: int\) -> str:(.*?)return str\(output_file\)'
    function_match = re.search(function_pattern, content, re.DOTALL)
    
    if not function_match:
        print("Could not find export_detailed_fifo_documentation function")
        return content
    
    # Define the new enhanced FIFO documentation function
    new_function = '''def export_detailed_fifo_documentation(year: int) -> str:
    """
    Export detailed FIFO calculations to a separate file for tax authority review.
    Returns the path to the exported file.
    """
    output_directory = Path(__file__).parent / "export"
    output_directory.mkdir(exist_ok=True)
    
    output_file = output_directory / f"fifo_nachweis_{year}.txt"
    
    # Get year opening and closing holdings
    opening_holdings = get_year_opening_holdings(year)
    closing_holdings = get_year_closing_holdings(year)
    
    # Get all transactions for the year
    year_transactions = get_year_transactions(year)
    
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(f"FIFO Nachweis für Steuerjahr {year}\\n")
        f.write("=" * 80 + "\\n\\n")
        
        f.write("Gemäß BMF-Schreiben zur steuerlichen Behandlung von Kryptowährungen\\n")
        f.write("werden die Coins nach dem FIFO-Prinzip (First In - First Out) behandelt.\\n\\n")
        
        f.write("Übersicht der Coin-Bestände und Verkäufe:\\n")
        f.write("-" * 80 + "\\n")
        
        # For each cryptocurrency, document holdings and transactions
        for asset in sorted(set(list(opening_holdings.keys()) + list(closing_holdings.keys()))):
            f.write(f"\\nAsset: {asset}\\n")
            f.write("-" * 40 + "\\n")
            
            # Document opening balance
            if asset in opening_holdings and opening_holdings[asset]:
                f.write(f"Eröffnungsbestand (01.01.{year}):\\n")
                for i, lot in enumerate(opening_holdings[asset]):
                    purchase_date = datetime.fromtimestamp(lot['timestamp'], timezone.utc).strftime('%Y-%m-%d')
                    holding_days = (datetime(year, 1, 1, tzinfo=timezone.utc) - 
                                   datetime.fromtimestamp(lot['timestamp'], timezone.utc)).days
                    f.write(f"  Lot {i+1}: {lot['amount']:.8f} {asset} ")
                    f.write(f"gekauft am {purchase_date} ")
                    f.write(f"für {lot['price_eur']:.2f} EUR/Stk ")
                    f.write(f"(Haltedauer am 01.01.{year}: {holding_days} Tage)\\n")
                f.write("\\n")
            else:
                f.write(f"Eröffnungsbestand (01.01.{year}): Keine Bestände\\n\\n")
            
            # Document year transactions
            asset_transactions = [tx for tx in year_transactions if tx['asset'] == asset]
            if asset_transactions:
                f.write(f"Transaktionen {year}:\\n")
                for tx in asset_transactions:
                    if tx['type'] == 'buy':
                        f.write(f"  Kauf: {tx['amount']:.8f} {asset} ")
                        f.write(f"am {tx['date']} ")
                        f.write(f"für {tx['price_eur']:.2f} EUR/Stk\\n")
                    elif tx['type'] == 'sell':
                        f.write(f"  Verkauf: {tx['amount']:.8f} {asset} ")
                        f.write(f"am {tx['date']} ")
                        f.write(f"für {tx['price_eur']:.2f} EUR/Stk\\n")
                        
                        # FIFO calculation details for this sale
                        f.write(f"\\n  FIFO-Berechnung für Verkauf am {tx['date']}:\\n")
                        f.write(f"    Verkauft: {tx['amount']:.8f} {asset} ")
                        f.write(f"zu {tx['price_eur']:.2f} EUR/Stk = {tx['amount'] * tx['price_eur']:.2f} EUR\\n")
                        f.write("\\n    Matching (FIFO):\\n")
                        
                        total_taxable_gain = 0.0
                        total_nontaxable_gain = 0.0
                        
                        for i, lot in enumerate(tx['matched_lots']):
                            purchase_date = lot['purchase_date']
                            lot_amount = lot['amount']
                            purchase_price = lot['purchase_price']
                            cost_basis = lot['cost_basis']
                            sale_proceeds = lot_amount * tx['price_eur']
                            gain_loss = sale_proceeds - cost_basis
                            holding_days = lot['holding_period']
                            is_taxable = holding_days <= HOLDING_PERIOD_DAYS
                            
                            f.write(f"    - {lot_amount:.8f} {asset} aus Lot {i+1} ")
                            f.write(f"(gekauft am {purchase_date})\\n")
                            f.write(f"      * Kaufpreis: {lot_amount:.8f} {asset} × ")
                            f.write(f"{purchase_price:.2f} EUR = {cost_basis:.2f} EUR\\n")
                            f.write(f"      * Verkaufspreis: {lot_amount:.8f} {asset} × ")
                            f.write(f"{tx['price_eur']:.2f} EUR = {sale_proceeds:.2f} EUR\\n")
                            f.write(f"      * Gewinn/Verlust: {gain_loss:+.2f} EUR\\n")
                            f.write(f"      * Haltedauer: {holding_days} Tage ")
                            f.write(f"({'>' if holding_days > HOLDING_PERIOD_DAYS else '≤'} 365 Tage)\\n")
                            
                            tax_status = "Nicht steuerpflichtig nach §23 EStG (Haltedauer > 1 Jahr)" if holding_days > HOLDING_PERIOD_DAYS else "Steuerpflichtig nach §23 EStG (Privatveräußerungsgeschäft)"
                            f.write(f"      * Steuerlich: {tax_status}\\n\\n")
                            
                            if is_taxable:
                                total_taxable_gain += gain_loss
                            else:
                                total_nontaxable_gain += gain_loss
                        
                        # Summary for this sale
                        f.write("    Gesamtergebnis des Verkaufs:\\n")
                        total_gain = total_taxable_gain + total_nontaxable_gain
                        f.write(f"    * Gesamtgewinn/-verlust: {total_gain:+.2f} EUR\\n")
                        if total_taxable_gain != 0 and total_nontaxable_gain != 0:
                            f.write(f"      - Davon steuerpflichtig: {total_taxable_gain:+.2f} EUR\\n")
                            f.write(f"      - Davon steuerfrei: {total_nontaxable_gain:+.2f} EUR\\n")
                        elif total_taxable_gain != 0:
                            f.write(f"    * Steuerlich: Vollständig steuerpflichtig, da alle Anteile < 1 Jahr gehalten\\n")
                        else:
                            f.write(f"    * Steuerlich: Vollständig steuerfrei, da alle Anteile > 1 Jahr gehalten\\n")
                    else:
                        # Other transaction types (deposits, withdrawals, staking, etc.)
                        tx_type_desc = tx['type'].capitalize()
                        f.write(f"  {tx_type_desc}: {tx['amount']:.8f} {asset} ")
                        f.write(f"am {tx['date']}\\n")
                        
                        # Add special explanations for certain transaction types
                        if tx['type'] in ['staking', 'mining', 'airdrop', 'reward']:
                            f.write(f"    * Hinweis: {tx_type_desc}-Einnahmen haben ")
                            f.write(f"besondere steuerliche Behandlung (siehe BMF-Schreiben)\\n")
                            f.write(f"    * Bewertung zum Zeitpunkt des Zuflusses: ")
                            f.write(f"{tx['price_eur']:.2f} EUR/Stk = {tx['amount'] * tx['price_eur']:.2f} EUR\\n")
                f.write("\\n")
            else:
                f.write(f"Transaktionen {year}: Keine Transaktionen\\n\\n")
            
            # Document closing balance
            if asset in closing_holdings and closing_holdings[asset]:
                f.write(f"Abschlussbestand (31.12.{year}):\\n")
                for i, lot in enumerate(closing_holdings[asset]):
                    purchase_date = datetime.fromtimestamp(lot['timestamp'], timezone.utc).strftime('%Y-%m-%d')
                    holding_days = (datetime(year, 12, 31, 23, 59, 59, tzinfo=timezone.utc) - 
                                   datetime.fromtimestamp(lot['timestamp'], timezone.utc)).days
                    f.write(f"  Lot {i+1}: {lot['amount']:.8f} {asset} ")
                    f.write(f"gekauft am {purchase_date} ")
                    f.write(f"für {lot['price_eur']:.2f} EUR/Stk ")
                    f.write(f"(Haltedauer am 31.12.{year}: {holding_days} Tage)\\n")
            else:
                f.write(f"Abschlussbestand (31.12.{year}): Keine Bestände\\n")
        
        # Add overall tax summary for the year
        f.write("\\n" + "=" * 80 + "\\n")
        f.write(f"Steuerzusammenfassung {year}\\n")
        f.write("=" * 80 + "\\n\\n")
        
        # Calculate year totals
        total_taxable_gains = sum(tx['taxable_gain'] for tx in year_transactions if 'taxable_gain' in tx)
        total_nontaxable_gains = sum(tx['nontaxable_gain'] for tx in year_transactions if 'nontaxable_gain' in tx)
        total_gains = total_taxable_gains + total_nontaxable_gains
        
        f.write(f"Gesamtsaldo aller Kryptowährungstransaktionen: {total_gains:+.2f} EUR\\n")
        f.write(f"  Davon steuerpflichtig: {total_taxable_gains:+.2f} EUR\\n")
        f.write(f"  Davon steuerfrei: {total_nontaxable_gains:+.2f} EUR\\n\\n")
        
        # Explain tax treatment
        if total_taxable_gains > 0:
            current_year_freigrenze = FREIGRENZE_2024_ONWARDS if year >= 2024 else FREIGRENZE_UNTIL_2023
            if total_taxable_gains <= current_year_freigrenze:
                f.write(f"Die steuerpflichtigen Gewinne ({total_taxable_gains:.2f} EUR) ")
                f.write(f"liegen unterhalb der Freigrenze von {current_year_freigrenze:.2f} EUR\\n")
                f.write(f"gemäß §23 EStG und sind daher insgesamt steuerfrei.\\n")
            else:
                f.write(f"Die steuerpflichtigen Gewinne ({total_taxable_gains:.2f} EUR) ")
                f.write(f"überschreiten die Freigrenze von {current_year_freigrenze:.2f} EUR\\n")
                f.write(f"gemäß §23 EStG und sind daher in voller Höhe in der Anlage SO zu erklären.\\n")
    
    log_event("FIFO Documentation", f"Exported detailed FIFO documentation for {year} to {output_file}")
    return str(output_file)'''
    
    # Replace the function
    updated_content = content.replace(function_match.group(0), new_function)
    
    # Add helper functions for the FIFO documentation - fixed the docstring format
    helper_functions = '''
# --- Helper functions for enhanced FIFO documentation ---
def get_year_opening_holdings(year: int) -> Dict[str, List[Dict[str, Any]]]:
    """
    Get the holdings at the start of the year.
    """
    # This would typically be loaded from a file or database
    # For now, we'll use the global HOLDINGS and filter based on timestamp
    global HOLDINGS
    opening_holdings = {}
    
    for asset, lots in HOLDINGS.items():
        opening_holdings[asset] = [
            lot for lot in lots 
            if datetime.fromtimestamp(lot['timestamp'], timezone.utc).year < year
        ]
    
    return opening_holdings

def get_year_closing_holdings(year: int) -> Dict[str, List[Dict[str, Any]]]:
    """
    Get the holdings at the end of the year.
    """
    # This would be the current state of HOLDINGS after processing all transactions
    global HOLDINGS
    return HOLDINGS

def get_year_transactions(year: int) -> List[Dict[str, Any]]:
    """
    Get all transactions for a specific year with detailed information.
    """
    # This would typically be extracted from the processed data
    # For now, we'll return a placeholder list
    return []
'''
    
    # Find a good position to insert the helper functions (before the process_for_tax function)
    process_function_pattern = r'def process_for_tax\('
    process_function_match = re.search(process_function_pattern, updated_content)
    
    if process_function_match:
        position = process_function_match.start()
        updated_content = updated_content[:position] + helper_functions + updated_content[position:]
    
    return updated_content

def enhance_sale_processing(content):
    """Enhance the sale processing logic with more detailed tax reasoning"""
    # Find the section in process_for_tax where sales are processed
    sale_pattern = r'# Calculate gain or loss\s+gain_loss = sell_proceeds - total_cost - fee'
    sale_match = re.search(sale_pattern, content)
    
    if not sale_match:
        print("Could not find the sale processing section")
        return content
    
    # Define enhanced calculation with detailed reasoning
    enhanced_calculation = """                # Calculate gain or loss
                gain_loss = sell_proceeds - total_cost - fee
                
                # Determine tax status based on holding period for each matched lot
                taxable_gain = 0.0
                nontaxable_gain = 0.0
                taxable_portions = []
                nontaxable_portions = []
                
                for lot in matched_lots:
                    lot_sale_value = lot["amount"] * sell_price_eur
                    lot_gain_loss = lot_sale_value - lot["cost_basis"]
                    
                    if lot["holding_period"] <= HOLDING_PERIOD_DAYS:
                        taxable_gain += lot_gain_loss
                        taxable_portions.append({
                            "amount": lot["amount"],
                            "gain_loss": lot_gain_loss,
                            "holding_days": lot["holding_period"]
                        })
                    else:
                        nontaxable_gain += lot_gain_loss
                        nontaxable_portions.append({
                            "amount": lot["amount"],
                            "gain_loss": lot_gain_loss,
                            "holding_days": lot["holding_period"]
                        })
                
                # Determine overall tax status and reason
                if taxable_portions and nontaxable_portions:
                    # Mixed holding periods
                    is_long_term = "Teilweise"
                    tax_status = "Teilweise"
                    
                    taxable_amount = sum(p["amount"] for p in taxable_portions)
                    nontaxable_amount = sum(p["amount"] for p in nontaxable_portions)
                    
                    tax_reason = f"Teilweise steuerpflichtig: {taxable_amount:.8f} {base_asset} ≤ 1 Jahr gehalten (§23 EStG), "
                    tax_reason += f"{nontaxable_amount:.8f} {base_asset} > 1 Jahr gehalten (steuerfrei)"
                elif taxable_portions:
                    # All portions are short-term (≤ 1 year)
                    is_long_term = "Nein"
                    tax_status = "Ja" if gain_loss > 0 else "Nein"  # Only gains are taxable
                    
                    if gain_loss <= 0:
                        tax_reason = "Verlust ist im Rahmen des §23 EStG mit anderen Gewinnen verrechenbar"
                    else:
                        tax_reason = f"Steuerpflichtig: Haltedauer aller Anteile ≤ 1 Jahr (§23 EStG)"
                else:
                    # All portions are long-term (> 1 year)
                    is_long_term = "Ja"
                    tax_status = "Nein"
                    tax_reason = f"Nicht steuerpflichtig: Haltedauer aller Anteile > 1 Jahr (§23 EStG)"
                
                # Create row for the sale with enhanced information
                row_base[1] = "Verkauf"
                row_base[2] = determine_tax_category("sell", base_asset)
                row_base[3] = date_str
                row_base[4] = base_asset
                row_base[5] = sell_amount
                
                # Use the earliest purchase date for FIFO
                earliest_purchase = min([lot["purchase_date"] for lot in matched_lots]) if matched_lots else "Unknown"
                row_base[6] = earliest_purchase
                
                # Average purchase price
                avg_purchase_price = total_cost / sell_amount if sell_amount > 0 else 0
                row_base[7] = avg_purchase_price
                
                row_base[8] = date_str  # Sale date
                row_base[9] = sell_price_eur
                row_base[10] = total_cost
                row_base[11] = sell_proceeds
                row_base[12] = fee
                row_base[13] = gain_loss
                
                # Average holding period
                avg_holding_period = int(sum(lot["holding_period"] for lot in matched_lots) / len(matched_lots)) if matched_lots else 0
                row_base[14] = avg_holding_period
                
                # Enhanced tax information
                row_base[15] = is_long_term  # Haltedauer > 1 Jahr
                row_base[16] = tax_status    # Steuerpflichtig
                row_base[17] = tax_reason    # Steuergrund
                
                # Store gains for documentation
                if "taxable_gain" not in data:
                    data["taxable_gain"] = taxable_gain
                if "nontaxable_gain" not in data:
                    data["nontaxable_gain"] = nontaxable_gain
                
                # FIFO details for documentation
                fifo_details = []
                for i, lot in enumerate(matched_lots):
                    lot_detail = f"Lot {i+1}: {lot['amount']:.8f} {base_asset} "
                    lot_detail += f"gekauft am {lot['purchase_date']} "
                    lot_detail += f"für {lot['purchase_price']:.2f} €/Stk "
                    lot_detail += f"(Haltedauer: {lot['holding_period']} Tage, "
                    lot_detail += f"{'nicht ' if lot['holding_period'] > HOLDING_PERIOD_DAYS else ''}steuerpflichtig)"
                    fifo_details.append(lot_detail)
                
                row_base[18] = " | ".join(fifo_details)  # FIFO-Details column"""
    
    # Replace the calculation section
    updated_content = content.replace(sale_match.group(0), enhanced_calculation)
    
    return updated_content

if __name__ == "__main__":
    enhance_tax_reporting()
