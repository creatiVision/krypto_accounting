#!/usr/bin/env python3
"""
This script fixes the syntax errors in krypto-accounting_german_tax.py 
by adding missing commas in function definitions and dictionary literals.
"""

import re
from pathlib import Path

def fix_syntax_errors():
    # Path to the module
    module_path = Path(__file__).parent / "krypto-accounting_german_tax.py"
    
    # Read the file content
    with open(module_path, 'r') as f:
        content = f.read()
    
    # Fix function definitions
    # Pattern: def name(param: type param: type) -> return_type:
    # Replace with: def name(param: type, param: type) -> return_type:
    def_pattern = r'def\s+(\w+)\(([^)]*?)\s*(\w+):\s*(\w+)([^,)]*?\s*)(\w+):\s*(\w+)'
    fixed_content = re.sub(def_pattern, r'def \1(\2\3: \4\5, \6: \7', content)
    
    # Apply multiple times to catch multiple parameters
    for _ in range(10):  # Apply up to 10 times to catch all instances
        fixed_content = re.sub(def_pattern, r'def \1(\2\3: \4\5, \6: \7', fixed_content)
    
    # Fix dictionary literals in list comprehensions
    # Pattern: {"key": "value" "key2": "value2"}
    # Replace with: {"key": "value", "key2": "value2"}
    dict_pattern = r'{"(\w+)":\s*([^{},"]+|"[^"]*")\s+"(\w+)":'
    fixed_content = re.sub(dict_pattern, r'{"\\1": \2, "\\3":', fixed_content)
    
    # Apply multiple times
    for _ in range(10):
        fixed_content = re.sub(dict_pattern, r'{"\\1": \2, "\\3":', fixed_content)
    
    # Fix specific other patterns
    # Lists with missing commas
    fixed_content = re.sub(r'\]\s+key=lambda', r'], key=lambda', fixed_content)
    fixed_content = re.sub(r'timestamp timezone', r'timestamp, timezone', fixed_content)
    fixed_content = re.sub(r'data.get\("(\w+)"\s+(\w+)', r'data.get("\1", \2', fixed_content)
    
    # Fix missing commas in list brackets
    brackets_pattern = r'\["(\w+)"\s+"(\w+)"\]'
    fixed_content = re.sub(brackets_pattern, r'["\1", "\2"]', fixed_content)
    
    # Fix the process_for_tax function definition specifically
    process_pattern = r'def process_for_tax\(trades: List\[Dict\[str Any\]\] ledger: List\[Dict\[str Any\]\] year: int\)'
    fixed_content = re.sub(process_pattern, 
                          r'def process_for_tax(trades: List[Dict[str, Any]], ledger: List[Dict[str, Any]], year: int)', 
                          fixed_content)
    
    # Fix dictionary references in event list comprehensions
    event_pattern = r'{"type": "(trade|ledger)" "data": (\w+) "time": float\((\w+).get\("time" 0\)\)}'
    fixed_content = re.sub(event_pattern, r'{"type": "\1", "data": \2, "time": float(\3.get("time", 0))}', fixed_content)
    
    # Write the fixed content back to the file
    with open(module_path, 'w') as f:
        f.write(fixed_content)
    
    print(f"Fixed syntax errors in {module_path}")
    return True

if __name__ == "__main__":
    fix_syntax_errors()
