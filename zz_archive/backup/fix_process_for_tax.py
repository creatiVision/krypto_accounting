#!/usr/bin/env python3
"""
Hotfix to add return statement to process_for_tax function.
This will modify the krypto-accounting_german_tax.py file to add a return statement.
"""

import re
import os
from pathlib import Path

def fix_tax_module():
    # Path to the module
    module_path = Path(__file__).parent / "krypto-accounting_german_tax.py"
    
    # Read the file content
    with open(module_path, 'r') as f:
        content = f.read()
    
    # Find the process_for_tax function 
    process_function_pattern = r'def process_for_tax\([^)]*\).*?processed_refids\.add\(refid\)'
    match = re.search(process_function_pattern, content, re.DOTALL)
    
    if match:
        # Add return statement for tax_data at the end of the function
        modified_content = content[:match.end()] + "\n\n    # Return the tax data for further processing\n    return tax_data" + content[match.end():]
        
        # Write back to the file
        with open(module_path, 'w') as f:
            f.write(modified_content)
        
        print(f"Successfully added return statement to process_for_tax in {module_path}")
        return True
    else:
        print("Could not find the process_for_tax function. No changes made.")
        return False

if __name__ == "__main__":
    fix_tax_module()
