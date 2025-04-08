#!/bin/bash
# Script to run the debugging tools and apply fixes for the crypto tax calculator

echo "========================================"
echo "Crypto Tax Calculator Debugging Utility"
echo "========================================"
echo ""

# Make the script executable
chmod +x debug_plan.py

# Check if a specific command was provided
if [ "$1" == "--help" ]; then
  echo "Usage: ./run_debug_fixes.sh [OPTIONS]"
  echo ""
  echo "Options:"
  echo "  --check-only     Only perform checks without generating fixes"
  echo "  --apply-fixes    Generate and apply fixes automatically"
  echo "  --help           Show this help message"
  echo ""
  echo "If no options are provided, all checks will be performed and fixes will be suggested but not applied."
  exit 0
fi

# Create necessary directories
mkdir -p export logs

echo "Step 1: Running database consistency check..."
python3 debug_plan.py --db-check
echo ""

echo "Step 2: Checking for FIFO calculation issues..."
python3 debug_plan.py --fifo-check
echo ""

echo "Step 3: Checking for missing 2024 sales..."
python3 debug_plan.py --sales-check
echo ""

# Generate fixes if requested
if [ "$1" == "--apply-fixes" ]; then
  echo "Step 4: Generating and applying fixes..."
  python3 debug_plan.py --generate-fixes

  # Apply SQL fixes if they exist
  if [ -f "export/add_manual_entries.sql" ]; then
    echo "Applying SQL fixes to the database..."
    sqlite3 data/kraken_cache.db < export/add_manual_entries.sql
    echo "Database updates applied."
  else
    echo "No SQL fixes needed to be applied."
  fi
elif [ "$1" != "--check-only" ]; then
  echo "Step 4: Generating fix suggestions..."
  python3 debug_plan.py --generate-fixes
  echo ""
  echo "Fixes have been generated but not applied."
  echo "To apply the fixes, run: ./run_debug_fixes.sh --apply-fixes"
fi

echo ""
echo "========================================"
echo "Debugging process complete!"
echo "========================================"
echo ""
echo "Debug logs have been saved to logs/debug_fix.log"
echo "Fix suggestions and reports have been saved to the export/ directory."
echo ""

# Show recommendations
echo "Next steps:"
echo "1. Review the debug logs and fix suggestions"
echo "2. If fixes were applied, run the tax calculator again"
echo "3. Verify that the reports now include all 2024 sales"
echo "4. Check for any remaining errors in the logs"
echo ""
