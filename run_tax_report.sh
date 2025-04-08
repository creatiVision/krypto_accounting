#!/bin/bash

# Run the application with the tax year and export format already specified
python -m src.crypto_tax_calculator.main --tax-year 2024 --export-format csv --output-dir export
