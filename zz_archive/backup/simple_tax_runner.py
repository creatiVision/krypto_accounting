#!/usr/bin/env python3
"""
Simple wrapper script for the German crypto tax reporter.
This script directly imports the tax module and runs it.
"""
import sys
import importlib.util
from pathlib import Path

def main():
    """Main function to run the tax calculator."""
    # Get the tax module path
    script_dir = Path(__file__).parent.absolute()
    tax_module_path = script_dir / "krypto-accounting_german_tax.py"
    
    if not tax_module_path.exists():
        print(f"Error: Tax module not found at {tax_module_path}")
        sys.exit(1)
    
    try:
        # Import the module directly using importlib (handles filename with hyphens)
        spec = importlib.util.spec_from_file_location("tax_module", tax_module_path)
        tax_module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(tax_module)
        
        # Get the tax year
        if len(sys.argv) > 1:
            try:
                tax_year = int(sys.argv[1])
            except ValueError:
                print(f"Error: Invalid year format: {sys.argv[1]}. Please provide a valid year (e.g., 2024).")
                sys.exit(1)
        else:
            tax_year = int(input("Enter tax year to generate report for (e.g. 2022): "))
        
        # Run the main function directly with the specified year
        print(f"Running tax calculation for year {tax_year}...")
        tax_module.main(tax_year)
        
    except ImportError as e:
        print(f"Error importing tax module: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"Error running tax calculation: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
