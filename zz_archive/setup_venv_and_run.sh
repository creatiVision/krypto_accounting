#!/bin/bash
# This script creates a virtual environment, installs required packages, 
# and runs the tax reporting tool within the environment.

# Directory for the virtual environment
VENV_DIR="./venv"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Required packages
REQUIRED_PACKAGES=(
  "requests"
  "google-auth"
  "google-auth-oauthlib"
  "google-auth-httplib2"
  "google-api-python-client"
)

# Create virtual environment if it doesn't exist
if [ ! -d "$VENV_DIR" ]; then
  echo "Creating virtual environment..."
  python3 -m venv "$VENV_DIR"
  if [ $? -ne 0 ]; then
    echo "Error: Failed to create virtual environment. Please install venv:"
    echo "sudo apt-get install python3-venv"
    exit 1
  fi
  echo "Virtual environment created successfully."
else
  echo "Using existing virtual environment."
fi

# Activate the virtual environment
source "$VENV_DIR/bin/activate"
if [ $? -ne 0 ]; then
  echo "Error: Failed to activate virtual environment."
  exit 1
fi
echo "Virtual environment activated."

# Install required packages
echo "Installing required packages..."
for package in "${REQUIRED_PACKAGES[@]}"; do
  pip install "$package"
  if [ $? -ne 0 ]; then
    echo "Error: Failed to install $package."
    exit 1
  fi
done

echo "All packages installed successfully."
echo ""

# Run the tax reporting tool
echo "=== Deutsches Krypto-Steuer Berechnungstool ==="
echo ""
echo "Dieses Tool berechnet die steuerrelevanten Gewinne und Verluste"
echo "aus Kryptowährungstransaktionen gemäß §23 EStG für deutsche Steuererklärungen."
echo ""

# Check if the year argument is provided as a parameter to this script
if [ -z "$1" ]; then
  # No year provided, ask the user
  echo "Bitte geben Sie das Steuerjahr ein (z.B. 2022):"
  read year
else
  # Use the provided year
  year=$1
fi

# Validate year input
if [[ ! "$year" =~ ^[0-9]{4}$ ]]; then
  echo "Fehler: Bitte geben Sie ein gültiges Jahr ein (z.B. 2022)"
  deactivate
  exit 1
fi

echo ""
echo "Steuer-Bericht wird für das Jahr $year erstellt..."
echo ""

# Add debug information
echo "Debugging environment setup:"
echo "Python version: $(python3 --version)"
echo "PYTHONPATH: $PYTHONPATH"
echo "Virtual env python: $(which python3)"
echo "Installed packages:"
pip list | grep -E "requests|google"

# This should show that all packages are installed
echo "Checking for packages in Python:"
python3 -c "
try:
    import requests
    print('✓ requests is installed')
except ImportError:
    print('✗ requests is not installed')

try:
    import google.auth
    print('✓ google-auth is installed')
except ImportError:
    print('✗ google-auth is not installed')

try:
    from google.oauth2.service_account import Credentials
    print('✓ google.oauth2.service_account is installed')
except ImportError:
    print('✗ google.oauth2.service_account is not installed')

try:
    from googleapiclient.discovery import build
    print('✓ googleapiclient.discovery is installed')
except ImportError:
    print('✗ googleapiclient.discovery is not installed')
"

# Export Python path to ensure we're using the virtual environment
export PYTHONPATH="${VENV_DIR}/lib/python3.13/site-packages:${PYTHONPATH}"
echo "Updated PYTHONPATH: $PYTHONPATH"

# Run the tax calculator script with the provided year in the virtual environment using the virtual env's Python
"$VENV_DIR/bin/python3" "$SCRIPT_DIR/crypto_tax_calculator.py" $year

# Exit status
exit_status=$?
if [ $exit_status -ne 0 ]; then
  echo ""
  echo "Es ist ein Fehler aufgetreten. Bitte prüfen Sie die Fehlermeldungen oben."
fi

# Deactivate the virtual environment
deactivate
echo "Virtual environment deactivated."

exit $exit_status
