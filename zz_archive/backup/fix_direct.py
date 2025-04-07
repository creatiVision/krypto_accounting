#!/usr/bin/env python3
with open("krypto-accounting_german_tax.py", "r", encoding="utf-8") as f:
    content = f.read()

# Direct fixes with exact string replacements
content = content.replace(
    'def log_event(event: str details: str) -> None:',
    'def log_event(event: str, details: str) -> None:'
)
content = content.replace(
    'LOG_DATA.append([timestamp event details])',
    'LOG_DATA.append([timestamp, event, details])'
)

with open("krypto-accounting_german_tax.py", "w", encoding="utf-8") as f:
    f.write(content)

print("Direct string replacements completed")
