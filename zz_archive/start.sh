#!/bin/bash
# Simple startup script for the Crypto Tax Calculator

# Default to previous year if no year specified
YEAR=${1:-$(date -d "last year" +%Y)}

echo "Starting Crypto Tax Calculator for tax year: $YEAR"
echo "================================================"

# Run the calculator with the specified or default year
python crypto_tax_calculator.py $YEAR

echo "================================================"
echo "Process complete"
