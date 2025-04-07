#!/usr/bin/env python3
"""
This script manually fixes specific syntax errors in krypto-accounting_german_tax.py
"""

from pathlib import Path

def fix_critical_syntax_errors():
    # Path to the module
    module_path = Path(__file__).parent / "krypto-accounting_german_tax.py"
    
    # Read the file content
    with open(module_path, 'r') as f:
        content = f.read()
    
    # Fix HOLDINGS.setdefault missing comma
    content = content.replace(
        'HOLDINGS.setdefault(base_asset [])',
        'HOLDINGS.setdefault(base_asset, [])'
    )
    
    # Fix dictionary missing commas
    content = content.replace(
        '"amount": abs(amount)\n                    "price_eur"', 
        '"amount": abs(amount),\n                    "price_eur"'
    )
    
    content = content.replace(
        '"price_eur": price if quote_asset == "EUR" else get_market_price(base_asset timestamp)\n                    "timestamp"', 
        '"price_eur": price if quote_asset == "EUR" else get_market_price(base_asset, timestamp),\n                    "timestamp"'
    )
    
    content = content.replace(
        '"timestamp": timestamp\n                    "refid"', 
        '"timestamp": timestamp,\n                    "refid"'
    )
    
    content = content.replace(
        '"refid": refid\n                    "year"', 
        '"refid": refid,\n                    "year"'
    )
    
    # Write the fixed content back to the file
    with open(module_path, 'w') as f:
        f.write(content)
    
    print(f"Fixed critical syntax errors in {module_path}")
    return True

if __name__ == "__main__":
    fix_critical_syntax_errors()
