#!/bin/bash

# Colors for better readability
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Function to display information with a header
function display_info() {
    local header=$1
    local message=$2
    echo -e "\n${BLUE}==================== $header ====================${NC}"
    echo -e "$message"
    echo
}

echo -e "${BLUE}=======================================${NC}"
echo -e "${GREEN}Crypto Tax Calculator${NC}"
echo -e "${BLUE}=======================================${NC}"

# Check if virtual environment exists, create if not
if [ ! -d "venv" ]; then
    echo -e "${YELLOW}Virtual environment not found. Creating one...${NC}"
    python3 -m venv venv
    source venv/bin/activate
    pip install -r requirements.txt
else
    # Activate the virtual environment
    source venv/bin/activate
fi

# Set up parameters
OUTPUT_DIR="export"
echo -e "${GREEN}Running with Kraken API${NC}"
mkdir -p "$OUTPUT_DIR"

# 1. Choose tax year from options
display_info "Tax Year" "This is the calendar year for which the tax calculations will be made.\nAll transactions from January 1st to December 31st of this year will be included."

CURRENT_YEAR=$(date +%Y)

# Display year options directly
echo -e "Available tax years:"
echo -e "  1. $CURRENT_YEAR"
echo -e "  2. $(($CURRENT_YEAR-1))"
echo -e "  3. $(($CURRENT_YEAR-2))"
echo -e "  4. $(($CURRENT_YEAR-3))"
echo -e "  5. $(($CURRENT_YEAR-4))"
echo -e ""
echo -e "Please enter a number from 1 to 5:"
read -p "Your choice [1-5]: " choice

case $choice in
    1) TAX_YEAR=$CURRENT_YEAR ;;
    2) TAX_YEAR=$(($CURRENT_YEAR-1)) ;;
    3) TAX_YEAR=$(($CURRENT_YEAR-2)) ;;
    4) TAX_YEAR=$(($CURRENT_YEAR-3)) ;;
    5) TAX_YEAR=$(($CURRENT_YEAR-4)) ;;
    *) 
        echo -e "Invalid choice. Defaulting to $CURRENT_YEAR."
        TAX_YEAR=$CURRENT_YEAR 
        ;;
esac

echo -e "Tax year set to: ${YELLOW}$TAX_YEAR${NC}"

# 2. Choose Export Format
display_info "Export Format" "Choose the format for your tax reports."

echo -e "Available formats:"
echo -e "  1. CSV (Comma-Separated Values)"
echo -e "  2. JSON (JavaScript Object Notation)"
echo -e "  3. Excel (requires openpyxl installed)"
echo -e ""
echo -e "Please enter a number from 1 to 3:"
read -p "Your choice [1-3]: " choice

case $choice in
    1) 
        EXPORT_FORMAT="CSV (Comma-Separated Values)"
        EXPORT_FORMAT_LOWER="csv"
        ;;
    2) 
        EXPORT_FORMAT="JSON (JavaScript Object Notation)"
        EXPORT_FORMAT_LOWER="json"
        ;;
    3) 
        EXPORT_FORMAT="Excel (requires openpyxl installed)"
        EXPORT_FORMAT_LOWER="excel"
        ;;
    *) 
        echo -e "Invalid choice. Defaulting to CSV."
        EXPORT_FORMAT="CSV (Comma-Separated Values)"
        EXPORT_FORMAT_LOWER="csv"
        ;;
esac

echo -e "Export format set to: ${YELLOW}$EXPORT_FORMAT${NC}"

echo -e "\n${BLUE}==================== Output Location ====================${NC}"
echo -e "Reports will be saved to the ${YELLOW}$OUTPUT_DIR${NC} directory.\n"

# Create output directory if it doesn't exist
mkdir -p "$OUTPUT_DIR"

# Load credentials from .env file if it exists
if [ -f ".env" ]; then
    source .env
fi

# Set up Python path
export PYTHONPATH="$PWD/src:$PYTHONPATH"

# Run the application
echo -e "${BLUE}=======================================${NC}"
echo -e "${GREEN}Starting Crypto Tax Calculator with:${NC}"
echo -e "  Tax Year: ${YELLOW}$TAX_YEAR${NC}"
echo -e "  Export Format: ${YELLOW}$EXPORT_FORMAT${NC}"
echo -e "  Output Directory: ${YELLOW}$OUTPUT_DIR${NC}"
echo -e "${BLUE}=======================================${NC}"

# Check Kraken API credentials
echo -e "${GREEN}Checking Kraken API credentials...${NC}"

# Verify that credentials are loaded
if [ -z "$KRAKEN_API_KEY" ] || [ -z "$KRAKEN_API_SECRET" ]; then
    echo -e "${RED}Error: Kraken API credentials not found!${NC}"
    echo -e "${YELLOW}Please check your .env file contains the following:${NC}"
    echo -e "  KRAKEN_API_KEY=your_api_key_here"
    echo -e "  KRAKEN_API_SECRET=your_api_secret_here"
    echo -e "\nIf you haven't created the .env file yet, follow these steps:"
    echo -e "  1. Copy the template: cp .env.template .env"
    echo -e "  2. Edit the new .env file and add your Kraken API credentials"
    echo -e "\nExample:"
    echo -e "  nano .env  # Use your preferred text editor"
    echo -e "\nAfter setting up your credentials, run this script again."
    exit 1
fi
echo -e "${GREEN}Kraken API credentials loaded successfully.${NC}"

# Run the calculator
./venv/bin/python -m src.crypto_tax_calculator.main --tax-year "$TAX_YEAR" --export-format "$EXPORT_FORMAT_LOWER" --output-dir "$OUTPUT_DIR"

# Print completion message
echo -e "${GREEN}Tax calculation complete! Reports available in the ${YELLOW}$OUTPUT_DIR${GREEN} directory.${NC}"

# Deactivate the virtual environment
deactivate
