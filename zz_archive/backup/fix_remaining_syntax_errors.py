#!/usr/bin/env python3
"""
This script fixes the remaining syntax errors in krypto-accounting_german_tax.py 
particularly missing commas in dictionaries and function arguments.
"""

import re
from pathlib import Path

def fix_syntax_errors():
    # Path to the module
    module_path = Path(__file__).parent / "krypto-accounting_german_tax.py"
    
    # Read the file content
    with open(module_path, 'r') as f:
        content = f.read()
    
    # Fix missing commas in dictionary creation
    # Pattern: "key": value "key2": value
    # Replace with: "key": value, "key2": value
    dict_pattern = r'("[\w_]+"\s*:\s*[^,{}\n]+)\s+("[\w_]+"\s*:)'
    fixed_content = re.sub(dict_pattern, r'\1, \2', content)
    
    # Apply multiple times to catch nested dictionaries
    for _ in range(10):
        fixed_content = re.sub(dict_pattern, r'\1, \2', fixed_content)
    
    # Fix missing commas in function calls
    # Pattern: function_name(arg1 arg2)
    # Replace with: function_name(arg1, arg2)
    func_pattern = r'([a-zA-Z_]+\()([^,\(\)]+)\s+([^,\(\)]+)'
    fixed_content = re.sub(func_pattern, r'\1\2, \3', fixed_content)
    
    # Apply multiple times for nested calls
    for _ in range(10):
        fixed_content = re.sub(func_pattern, r'\1\2, \3', fixed_content)
    
    # Fix specific issues around HOLDINGS.setdefault
    holdings_pattern = r'HOLDINGS\.setdefault\(([^,\)]+)\s+(\[[^\]]*\])'
    fixed_content = re.sub(holdings_pattern, r'HOLDINGS.setdefault(\1, \2', fixed_content)
    
    # Fix missing commas in array/list access
    array_pattern = r'\[([^,\[\]]+)\s+([^,\[\]]+)\]'
    fixed_content = re.sub(array_pattern, r'[\1, \2]', fixed_content)
    
    # Fix method chaining without commas
    method_pattern = r'(\.[\w_]+\()([^,\(\)]+)\s+([^,\(\)]+)'
    fixed_content = re.sub(method_pattern, r'\1\2, \3', fixed_content)
    
    # Write the fixed content back to the file
    with open(module_path, 'w') as f:
        f.write(fixed_content)
    
    print(f"Fixed remaining syntax errors in {module_path}")
    return True

if __name__ == "__main__":
    fix_syntax_errors()
