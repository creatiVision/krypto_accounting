#!/usr/bin/env python3
"""
This script combines all the fixes made to the krypto-accounting_german_tax.py file.
It takes the base file and adds the complete process_for_tax function implementation.
"""

import os
from pathlib import Path

def combine_fixes():
    # Paths to all the files we need
    base_dir = Path(__file__).parent
    base_file = base_dir / "krypto-accounting_german_tax.py.new"
    complete_process_code = base_dir / "krypto-accounting_german_tax_complete.py"
    output_file = base_dir / "krypto-accounting_german_tax.py"
    
    if not base_file.exists():
        print(f"Error: Base file not found at {base_file}")
        return False
        
    if not complete_process_code.exists():
        print(f"Error: Complete process function code not found at {complete_process_code}")
        return False
    
    # Read the files
    with open(base_file, 'r') as f:
        base_content = f.read()
    
    with open(complete_process_code, 'r') as f:
        process_content = f.read()
    
    # Find where we need to insert the process function code
    insert_point = base_content.find("# Determine base and quote asset from")
    
    if insert_point == -1:
        print("Could not find insertion point in the base file")
        return False
    
    # Create the final content
    final_content = base_content[:insert_point] + process_content
    
    # Write the output file
    with open(output_file, 'w') as f:
        f.write(final_content)
    
    print(f"Successfully created complete file at {output_file}")
    return True

if __name__ == "__main__":
    combine_fixes()
