#!/usr/bin/env python3
"""
This script fixes all syntax errors in krypto-accounting_german_tax.py including:
1. Missing commas in function parameters
2. Missing commas in dictionaries
3. Missing commas in lists and method calls
4. Other syntax-related issues
"""

import re
from pathlib import Path
import fileinput
import sys

def fix_all_syntax_errors():
    # Path to the module
    module_path = Path(__file__).parent / "krypto-accounting_german_tax.py"
    
    print(f"Fixing syntax errors in {module_path}")
    
    # Read the file content
    with open(module_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Fix 1: Function parameter missing commas
    # Find all function definitions and add missing commas between parameters
    func_param_pattern = r'def\s+\w+\(([^)]+)\)'
    
    # Process each function definition
    for match in re.finditer(func_param_pattern, content):
        params_str = match.group(1)
        if ':' in params_str and ',' not in params_str:
            # Missing comma between type-annotated parameters
            fixed_params = re.sub(r'(\w+\s*:\s*\w+)\s+(\w+\s*:)', r'\1, \2', params_str)
            content = content.replace(params_str, fixed_params)
    
    # Fix 2: Missing commas in dictionary creation
    dict_pattern = r'(?<![=,:{\[])\s*"(\w+)"\s*:\s*([^,{}\n]+)\s+("[\w_]+"\s*:)'
    while re.search(dict_pattern, content):
        content = re.sub(dict_pattern, r' "\1": \2, \3', content)
    
    # Fix 3: HOLDINGS.setdefault missing comma
    holdings_pattern = r'HOLDINGS\.setdefault\(([^,\)]+)\s+(\[[^\]]*\])'
    content = re.sub(holdings_pattern, r'HOLDINGS.setdefault(\1, \2', content)
    
    # Fix 4: Missing commas in method arguments
    method_args_pattern = r'(\.\w+\()([^,\(\)]+)\s+([^,\(\)]+)'
    while re.search(method_args_pattern, content):
        content = re.sub(method_args_pattern, r'\1\2, \3', content)
    
    # Fix 5: Missing commas in function arguments
    func_args_pattern = r'(\w+\()([^,\(\)]+)\s+([^,\(\)]+)'
    while re.search(func_args_pattern, content):
        content = re.sub(func_args_pattern, r'\1\2, \3', content)
    
    # Fix 6: Missing commas in dictionaries within lists/arrays
    dict_in_list_pattern = r'(\{[^\}]*"[^"]+"\s*:\s*[^,\{\}\[\]]+)\s+("[\w_]+"\s*:)'
    while re.search(dict_in_list_pattern, content):
        content = re.sub(dict_in_list_pattern, r'\1, \2', content)
    
    # Fix 7: Direct fixes for specific syntax errors
    specific_fixes = [
        # Fix log_event function definition
        ('def log_event(event: str details: str) -> None:', 'def log_event(event: str, details: str) -> None:'),
        
        # Fix missing commas in dictionary entries
        ('"amount": abs(amount)\n                    "price_eur"', '"amount": abs(amount),\n                    "price_eur"'),
        ('"price_eur": price if quote_asset == "EUR" else get_market_price(base_asset timestamp)', 
         '"price_eur": price if quote_asset == "EUR" else get_market_price(base_asset, timestamp)'),
        ('"timestamp": timestamp\n                    "refid"', '"timestamp": timestamp,\n                    "refid"'),
        ('"refid": refid\n                    "year"', '"refid": refid,\n                    "year"'),
        
        # Fix any JSON syntax issues
        ('gridProperties": {"rowCount": 2000 "columnCount"', 'gridProperties": {"rowCount": 2000, "columnCount"'),
        
        # Fix any array/list syntax issues
        ('LOG_DATA.append([timestamp event details])', 'LOG_DATA.append([timestamp, event, details])'),
    ]
    
    for old, new in specific_fixes:
        content = content.replace(old, new)
    
    # Write the fixed content back to the file
    with open(module_path, 'w', encoding='utf-8') as f:
        f.write(content)
    
    print(f"Syntax fixes have been applied to {module_path}")
    return True

if __name__ == "__main__":
    fix_all_syntax_errors()
