#!/bin/bash

# Activate the virtual environment
source venv/bin/activate

# Check if a tax year is provided as a command-line argument
if [ -z "$1" ]; then
  # If no argument is provided, prompt the user for the tax year
  read -p "Enter the tax year: " TAX_YEAR
else
  # If an argument is provided, use it as the tax year
  TAX_YEAR="$1"
fi

# Check if TAX_YEAR is a number
if ! [[ "$TAX_YEAR" =~ ^[0-9]+$ ]]; then
  echo "Invalid tax year. Please enter a number."
  exit 1
fi

# Run the crypto tax calculator with the specified tax year
echo "Starting the crypto tax calculator for tax year $TAX_YEAR..."
export PYTHONPATH="$PWD/src:$PYTHONPATH"
python -m src.crypto_tax_calculator.main "$TAX_YEAR"

# Deactivate the virtual environment (optional)
deactivate
