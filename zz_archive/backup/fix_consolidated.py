#!/usr/bin/env python3
"""
Consolidated script to fix all syntax errors in the tax reporter module
"""
import re
import os

def fix_all():
    # Get the file path
    file_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "krypto-accounting_german_tax.py")
    
    # Read the content
    with open(file_path, 'r', encoding='utf-8') as file:
        content = file.read()
    
    # 1. Fix function parameter syntax
    content = content.replace("def log_event(event: str details: str)", 
                              "def log_event(event: str, details: str)")
    
    # 2. Fix LOG_DATA.append call with missing commas
    content = content.replace("LOG_DATA.append([timestamp event details])", 
                              "LOG_DATA.append([timestamp, event, details])")
    
    # 3. Fix HOLDINGS.setdefault missing commas
    holdings_pattern = r'HOLDINGS\.setdefault\(([^,\)]+)\s+(\[[^\]]*\])'
    content = re.sub(holdings_pattern, r'HOLDINGS.setdefault(\1, \2', content)
    
    # 4. Fix missing commas in dictionary entries
    dict_fixes = [
        ('"amount": abs(amount)\n', '"amount": abs(amount),\n'),
        ('"price_eur": price if quote_asset == "EUR" else get_market_price(base_asset timestamp)', 
         '"price_eur": price if quote_asset == "EUR" else get_market_price(base_asset, timestamp)'),
        ('"timestamp": timestamp\n', '"timestamp": timestamp,\n'),
        ('"refid": refid\n', '"refid": refid,\n'),
        ('"year": year\n', '"year": year,\n'),
        ('gridProperties": {"rowCount": 2000 "columnCount"', 'gridProperties": {"rowCount": 2000, "columnCount"')
    ]
    
    for old, new in dict_fixes:
        content = content.replace(old, new)
    
    # 5. More general fixes for missing commas in function calls
    func_pattern = r'(\w+\()([^,\(\)]+)\s+([^,\(\)]+)'
    while re.search(func_pattern, content):
        content = re.sub(func_pattern, r'\1\2, \3', content)
    
    # 6. More general fixes for missing commas in method calls
    method_pattern = r'(\.\w+\()([^,\(\)]+)\s+([^,\(\)]+)'
    while re.search(method_pattern, content):
        content = re.sub(method_pattern, r'\1\2, \3', content)
    
    # 7. Write modified content back to file
    with open(file_path, 'w', encoding='utf-8') as file:
        file.write(content)
    
    print("All syntax errors have been fixed.")

if __name__ == "__main__":
    fix_all()
