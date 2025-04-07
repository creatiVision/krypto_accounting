#!/usr/bin/env python3
"""
Script to fix syntax errors in run_tax_report.py
"""

from pathlib import Path
import re

def fix_run_tax_report():
    """Fix syntax errors in run_tax_report.py"""
    file_path = Path(__file__).parent / "run_tax_report.py"
    backup_path = file_path.with_suffix(".py.bak")
    
    # Create a backup
    if not backup_path.exists():
        with open(file_path, 'r', encoding='utf-8') as src:
            with open(backup_path, 'w', encoding='utf-8') as dst:
                dst.write(src.read())
        print(f"Created backup at {backup_path}")
    
    # Read the file
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Fix missing commas
    fixes = [
        # Missing shebang line
        (re.compile(r'^usr/bin/env python3'), '#!/usr/bin/env python3'),
        
        # Fix missing commas in function parameters and arguments
        (r'with open(config_path \'r\')', 'with open(config_path, \'r\')'),
        (r'"API_KEY" "API_SECRET" "SHEET_ID"', '"API_KEY", "API_SECRET", "SHEET_ID"'),
        (r'"requests" "google-auth" "google-auth-oauthlib" "google-auth-httplib2" "google-api-python-client"', 
         '"requests", "google-auth", "google-auth-oauthlib", "google-auth-httplib2", "google-api-python-client"'),
        
        # Fix missing commas in dictionary assignments
        (r"'__file__': str(tax_module_path)  # Add __file__ to the namespace\n", 
         "'__file__': str(tax_module_path),  # Add __file__ to the namespace\n"),
        
        # Fix missing commas in function calls
        (r'with open(tax_module_path \'r\')', 'with open(tax_module_path, \'r\')'),
        
        # Check for other common Python syntax errors
        (r'exec\(file\.read\(\) namespace\)', 'exec(file.read(), namespace)'),
    ]
    
    # Apply all fixes
    for pattern, replacement in fixes:
        if isinstance(pattern, str):
            content = content.replace(pattern, replacement)
        else:  # It's a regex pattern
            content = pattern.sub(replacement, content)
    
    # Write the fixed content back to the file
    with open(file_path, 'w', encoding='utf-8') as f:
        f.write(content)
    
    print(f"Fixed syntax errors in {file_path}")

if __name__ == "__main__":
    fix_run_tax_report()
