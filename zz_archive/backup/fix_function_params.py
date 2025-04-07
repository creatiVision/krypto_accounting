#!/usr/bin/env python3
import os

def fix_params():
    # Get the file path
    file_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "krypto-accounting_german_tax.py")

    # Read the content
    with open(file_path, 'r') as file:
        content = file.read()

    # Make specific direct replacements
    content = content.replace("def log_event(event: str details: str)", "def log_event(event: str, details: str)")
    content = content.replace("LOG_DATA.append([timestamp event details])", "LOG_DATA.append([timestamp, event, details])")

    # Write content back
    with open(file_path, 'w') as file:
        file.write(content)
    
    print("Direct parameter fixes applied")

if __name__ == "__main__":
    fix_params()
