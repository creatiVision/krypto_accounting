#!/bin/bash
# This script runs the crypto_tax_calculator.py within the virtual environment

# Directory for the virtual environment
VENV_DIR="./venv"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Check if virtual environment exists
if [ ! -d "$VENV_DIR" ]; then
  echo "Virtual environment not found. Creating one..."
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

# Make sure required packages are installed
REQUIRED_PACKAGES=(
  "requests"
  "google-auth"
  "google-auth-oauthlib"
  "google-auth-httplib2"
  "google-api-python-client"
)

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

# Run the tax calculator with the tax year if provided
if [ -z "$1" ]; then
  # No year provided, just run the script
  python3 "$SCRIPT_DIR/crypto_tax_calculator.py"
else
  # Use the provided year
  python3 "$SCRIPT_DIR/crypto_tax_calculator.py" "$1"
fi

# Capture exit status
exit_status=$?

# Deactivate the virtual environment
deactivate
echo "Virtual environment deactivated."

exit $exit_status
