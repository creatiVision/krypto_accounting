#!/usr/bin/env python3

"""
Utility script to flush the price cache.
"""

import os
import shutil
from pathlib import Path

# Define cache directory relative to this file's location
CACHE_DIR = Path(__file__).parent / "data" / "price_cache"

def flush_cache():
    """Flush the price cache by removing all files in the cache directory."""
    if CACHE_DIR.exists():
        print(f"Flushing price cache at {CACHE_DIR}...")
        # Remove all files in the cache directory
        for file in CACHE_DIR.glob("*"):
            if file.is_file():
                try:
                    file.unlink()
                    print(f"Deleted {file.name}")
                except Exception as e:
                    print(f"Error deleting {file.name}: {e}")
        print("Cache flush complete.")
    else:
        print(f"Cache directory {CACHE_DIR} does not exist. Nothing to flush.")

if __name__ == "__main__":
    flush_cache()
