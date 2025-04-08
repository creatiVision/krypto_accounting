#!/bin/bash

# Script to flush all caches and database

echo "Flushing all caches and database..."

# Flush price cache
echo "Flushing price cache..."
./flush_cache.py

# Flush database
echo "Flushing database..."
rm -f data/kraken_cache.db

echo "All caches and database have been flushed."
