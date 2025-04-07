#!/usr/bin/env python3
"""
This script enhances the HEADERS array in krypto-accounting_german_tax.py with 
additional columns for more detailed tax reasoning.
"""

from pathlib import Path

def enhance_headers():
    # Path to the module
    module_path = Path(__file__).parent / "krypto-accounting_german_tax.py"
    
    # Read the file content
    with open(module_path, 'r') as f:
        content = f.read()
    
    # Original headers pattern
    original_headers = """HEADERS = [
    "Zeile", "Typ", "Steuer-Kategorie", "Transaktionsdatum", "Asset", "Anzahl", 
    "Kaufdatum", "Kaufpreis (€)/Stk", "Verkaufsdatum", "Verkaufspreis (€)/Stk", 
    "Kosten (€)", "Erlös (€)", "Gebühr (€)", "Gewinn / Verlust (€)", "Haltedauer (Tage)", 
    "Steuerpflichtig", "Besondere Hinweise", "Notizen / FIFO-Details"
]"""
    
    # New enhanced headers
    new_headers = """HEADERS = [
    "Zeile", "Typ", "Steuer-Kategorie", "Transaktionsdatum", "Asset", "Anzahl", 
    "Kaufdatum", "Kaufpreis (€)/Stk", "Verkaufsdatum", "Verkaufspreis (€)/Stk", 
    "Kosten (€)", "Erlös (€)", "Gebühr (€)", "Gewinn / Verlust (€)", "Haltedauer (Tage)", 
    "Haltedauer > 1 Jahr", "Steuerpflichtig", "Steuergrund", "FIFO-Details", "Notizen"
]"""
    
    # Replace the headers
    updated_content = content.replace(original_headers, new_headers)
    
    # Write the updated content back to the file
    with open(module_path, 'w') as f:
        f.write(updated_content)
    
    print(f"Successfully enhanced tax headers in {module_path}")
    return True

if __name__ == "__main__":
    enhance_headers()
