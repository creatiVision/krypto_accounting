#!/bin/bash

# Script to remove all test data and mock data generation code

echo "Removing test data and mock data generation code..."

# Remove mock_data.py
if [ -f "src/crypto_tax_calculator/mock_data.py" ]; then
    echo "Removing src/crypto_tax_calculator/mock_data.py"
    rm src/crypto_tax_calculator/mock_data.py
fi

# Remove offline_mode.py
if [ -f "src/crypto_tax_calculator/offline_mode.py" ]; then
    echo "Removing src/crypto_tax_calculator/offline_mode.py"
    rm src/crypto_tax_calculator/offline_mode.py
fi

# Remove run_offline_demo.py
if [ -f "run_offline_demo.py" ]; then
    echo "Removing run_offline_demo.py"
    rm run_offline_demo.py
fi

# Remove mock data directory
if [ -d "data/mock_data" ]; then
    echo "Removing data/mock_data directory"
    rm -rf data/mock_data
fi

echo "All test data and mock data generation code has been removed."
