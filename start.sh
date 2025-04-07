#!/bin/bash

# Colors for better readability
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${BLUE}=======================================${NC}"
echo -e "${GREEN}Crypto Tax Calculator${NC}"
echo -e "${BLUE}=======================================${NC}"

# Function to display multiple choice options and get user selection
function choose_option() {
    local prompt=$1
    shift
    local options=("$@")
    local selected=0
    
    echo -e "${YELLOW}$prompt${NC}"
    echo
    # Display options with numbers before the description
    for i in "${!options[@]}"; do
        echo -e "  ${GREEN}$(($i+1)).${NC} ${options[$i]}"
    done
    echo
    
    while true; do
        read -p "Enter your choice [1-${#options[@]}]: " choice
        
        if [[ "$choice" =~ ^[0-9]+$ ]] && [ "$choice" -ge 1 ] && [ "$choice" -le "${#options[@]}" ]; then
            selected=$((choice-1))
            break
        else
            echo -e "${RED}Invalid selection. Please try again.${NC}"
        fi
    done
    
    echo "${options[$selected]}"
}

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

# 1. Choose the mode
echo -e "\n${BLUE}==================== Operation Mode ====================${NC}"
echo -e "Choose how you want to run the tax calculator:\n"
echo -e "  ${GREEN}1.${NC} ${YELLOW}Online (Kraken API)${NC}: Uses your real Kraken account data via API"
echo -e "  ${GREEN}2.${NC} ${YELLOW}Offline Demo${NC}: Uses mock transaction data for testing/demonstration"
echo
MODE=$(choose_option "Select operation mode:" "Online (Kraken API)" "Offline Demo (Mock Data)")

# 2. Input the tax year directly (4 digits)
echo -e "\n${BLUE}==================== Tax Year ====================${NC}"
echo -e "This is the calendar year for which the tax calculations will be made."
echo -e "All transactions from January 1st to December 31st of this year will be included."
echo

CURRENT_YEAR=$(date +%Y)
while true; do
    read -p "Enter tax year (4 digits, 2010-$CURRENT_YEAR): " TAX_YEAR
    
    # Validate input is a 4-digit year within acceptable range
    if [[ "$TAX_YEAR" =~ ^[0-9]{4}$ ]] && [ "$TAX_YEAR" -ge 2010 ] && [ "$TAX_YEAR" -le "$CURRENT_YEAR" ]; then
        break
    else
        echo -e "${RED}Invalid year. Please enter a 4-digit year between 2010 and $CURRENT_YEAR.${NC}"
    fi
done
echo -e "Tax year set to: ${YELLOW}$TAX_YEAR${NC}"

# 3. Choose the export format
echo -e "\n${BLUE}==================== Report Format ====================${NC}"
echo -e "Choose the format for your tax report:\n"
echo -e "  ${GREEN}1.${NC} ${YELLOW}CSV${NC}: Comma-separated values format (can be opened in Excel, Google Sheets, etc.)"
echo -e "  ${GREEN}2.${NC} ${YELLOW}Excel${NC}: Native Excel spreadsheet format (.xlsx)"
echo -e "  ${GREEN}3.${NC} ${YELLOW}JSON${NC}: JSON format (machine-readable, good for data processing)"
echo
EXPORT_FORMAT=$(choose_option "Select export format:" "CSV" "Excel" "JSON")
EXPORT_FORMAT_LOWER=$(echo "$EXPORT_FORMAT" | tr '[:upper:]' '[:lower:]')

# 4. Choose the output directory
echo -e "\n${BLUE}==================== Output Location ====================${NC}"
echo -e "Choose where to save the generated reports:\n"
echo -e "  ${GREEN}1.${NC} ${YELLOW}export${NC}: Standard directory for reports (recommended)"
echo -e "  ${GREEN}2.${NC} ${YELLOW}demo_export${NC}: Directory for demo/test reports"
echo -e "  ${GREEN}3.${NC} ${YELLOW}custom${NC}: Specify your own directory path"
echo
OUTPUT_DIR=$(choose_option "Select output directory:" "export" "demo_export" "custom")
if [ "$OUTPUT_DIR" = "custom" ]; then
    read -p "Enter custom output directory path: " OUTPUT_DIR
fi

# Create output directory if it doesn't exist
mkdir -p "$OUTPUT_DIR"

# 5. Choose verbose logging
echo -e "\n${BLUE}==================== Logging Detail ====================${NC}"
echo -e "Verbose logging provides detailed information about the calculation process:\n"
echo -e "  ${GREEN}1.${NC} ${YELLOW}No${NC}: Standard logging (errors and important messages only)"
echo -e "  ${GREEN}2.${NC} ${YELLOW}Yes${NC}: Detailed logging (shows all operations and decisions)"
echo
VERBOSE=$(choose_option "Enable verbose logging?" "No" "Yes")
VERBOSE_FLAG=""
if [ "$VERBOSE" = "Yes" ]; then
    VERBOSE_FLAG="--verbose"
fi

# 6. If offline mode, choose mock data option
GENERATE_NEW_DATA_FLAG=""
if [ "$MODE" = "Offline Demo (Mock Data)" ]; then
    echo -e "\n${BLUE}==================== Mock Data Options ====================${NC}"
    echo -e "Choose whether to use existing mock data or generate new random data:\n"
    echo -e "  ${GREEN}1.${NC} ${YELLOW}No (use existing)${NC}: Use pre-defined mock transactions"
    echo -e "  ${GREEN}2.${NC} ${YELLOW}Yes (generate new)${NC}: Generate new random transactions each time"
    echo
    GENERATE_NEW_DATA=$(choose_option "Generate new mock data?" "No (use existing)" "Yes (generate new)")
    if [ "$GENERATE_NEW_DATA" = "Yes (generate new)" ]; then
        GENERATE_NEW_DATA_FLAG="--generate-new-data"
    fi
fi

# Set up Python path
export PYTHONPATH="$PWD/src:$PYTHONPATH"

# Run the application based on selected mode
echo -e "${BLUE}=======================================${NC}"
echo -e "${GREEN}Starting Crypto Tax Calculator with:${NC}"
echo -e "  Mode: ${YELLOW}$MODE${NC}"
echo -e "  Tax Year: ${YELLOW}$TAX_YEAR${NC}"
echo -e "  Export Format: ${YELLOW}$EXPORT_FORMAT${NC}"
echo -e "  Output Directory: ${YELLOW}$OUTPUT_DIR${NC}"
echo -e "  Verbose Logging: ${YELLOW}$VERBOSE${NC}"
if [ "$MODE" = "Offline Demo (Mock Data)" ] && [ "$GENERATE_NEW_DATA" = "Yes (generate new)" ]; then
    echo -e "  Generate New Mock Data: ${YELLOW}Yes${NC}"
fi
echo -e "${BLUE}=======================================${NC}"

if [ "$MODE" = "Offline Demo (Mock Data)" ]; then
    python3 run_offline_demo.py --tax-year "$TAX_YEAR" $GENERATE_NEW_DATA_FLAG
else
    # Online mode with Kraken API
    # Check for .env file and load credentials
    if [ -f ".env" ]; then
        echo -e "${GREEN}Loading Kraken API credentials from .env file...${NC}"
        # Source the .env file to load environment variables
        source .env
    else
        echo -e "${RED}Error: .env file not found!${NC}"
        echo -e "${YELLOW}Please create a .env file with your Kraken API credentials:${NC}"
        echo -e "  1. Copy .env.template to .env"
        echo -e "  2. Edit .env and add your Kraken API key and secret"
        echo -e "\nExample:"
        echo -e "  cp .env.template .env"
        echo -e "  nano .env   # or use any text editor\n"
        exit 1
    fi
    
    # Verify that credentials are loaded
    if [ -z "$KRAKEN_API_KEY" ] || [ -z "$KRAKEN_API_SECRET" ]; then
        echo -e "${RED}Error: Kraken API credentials not found in .env file!${NC}"
        echo -e "${YELLOW}Please ensure your .env file contains:${NC}"
        echo -e "  KRAKEN_API_KEY=your_api_key_here"
        echo -e "  KRAKEN_API_SECRET=your_api_secret_here"
        exit 1
    fi
    
    python3 -m src.crypto_tax_calculator.main --tax-year "$TAX_YEAR" --export-format "$EXPORT_FORMAT_LOWER" --output-dir "$OUTPUT_DIR" $VERBOSE_FLAG
fi

# Print completion message
echo -e "${GREEN}Tax calculation complete! Reports available in the ${YELLOW}$OUTPUT_DIR${GREEN} directory.${NC}"

# Deactivate the virtual environment
deactivate
