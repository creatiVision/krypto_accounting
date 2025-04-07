import sqlite3
import json
import os
import time
from datetime import datetime
from typing import List, Dict, Any, Optional, Tuple
from contextlib import contextmanager

from .kraken_api import get_trades as api_get_trades, get_ledger as api_get_ledger
from .logging_utils import log_event, log_error, log_warning

# Ensure the data directory exists
DATA_DIR = os.path.join(os.path.dirname(__file__), '../../data')
os.makedirs(DATA_DIR, exist_ok=True)

DB_PATH = os.path.join(DATA_DIR, 'kraken_cache.db')

def init_db():
    """Create database tables if they don't exist."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Create trades table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS trades (
        refid TEXT PRIMARY KEY,
        data_json TEXT NOT NULL,
        timestamp INTEGER NOT NULL
    )
    ''')
    
    # Create ledger table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS ledger (
        refid TEXT PRIMARY KEY,
        data_json TEXT NOT NULL,
        timestamp INTEGER NOT NULL
    )
    ''')
    
    # Create indexes for faster querying
    cursor.execute('CREATE INDEX IF NOT EXISTS trades_timestamp_idx ON trades (timestamp)')
    cursor.execute('CREATE INDEX IF NOT EXISTS ledger_timestamp_idx ON ledger (timestamp)')
    
    conn.commit()
    conn.close()
    print(f"Database initialized at {DB_PATH}")

@contextmanager
def get_db_connection():
    """
    Context manager for database connections.
    Ensures connections are properly closed even if an exception occurs.
    
    Usage:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            # perform operations
    """
    conn = None
    try:
        # Ensure database is initialized
        if not os.path.exists(DB_PATH):
            init_db()
        
        # Connect with timeout and enable foreign keys
        conn = sqlite3.connect(DB_PATH, timeout=30.0)
        conn.execute("PRAGMA foreign_keys = ON")
        
        # Log connection
        log_event("Database", "Connection opened to Kraken cache database")
        yield conn
    except sqlite3.Error as e:
        log_error("Database", "ConnectionError", "Failed to connect to database", 
                 exception=e, details={"path": DB_PATH})
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            conn.close()
            log_event("Database", "Connection closed to Kraken cache database")

def load_cached_entries(table: str, start_time: int, end_time: int) -> List[Dict[str, Any]]:
    """
    Load cached entries from the database for the specified time range.
    
    Args:
        table: The table to query (trades or ledger)
        start_time: Start timestamp (Unix time)
        end_time: End timestamp (Unix time)
        
    Returns:
        A list of dictionary objects representing the entries
    """
    result = []
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            query = f"SELECT data_json FROM {table} WHERE timestamp BETWEEN ? AND ?"
            cursor.execute(query, (start_time, end_time))
            rows = cursor.fetchall()
            result = [json.loads(row[0]) for row in rows]
            
        log_event("Database", f"Retrieved {len(result)} entries from {table}",
                 details={"start_time": start_time, "end_time": end_time})
    except sqlite3.Error as e:
        log_error("Database", "QueryError", f"Failed to load entries from {table}",
                 exception=e, details={"start_time": start_time, "end_time": end_time})
        # Return empty list on error
        result = []
    except json.JSONDecodeError as e:
        log_error("Database", "DataError", f"Invalid JSON data in {table}",
                 exception=e, details={"start_time": start_time, "end_time": end_time})
        # Return valid entries only
        pass
        
    return result

def save_entries(table: str, entries: List[Dict[str, Any]]) -> int:
    """
    Save entries to the database cache.
    
    Args:
        table: The table to insert into (trades or ledger)
        entries: List of entry dictionaries to save
        
    Returns:
        Number of entries successfully saved
    """
    if not entries:
        log_event("Database", f"No entries to save to {table}")
        return 0
        
    success_count = 0
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            
            for entry in entries:
                refid = entry.get('refid')
                if not refid:
                    log_warning("Database", "InvalidData", f"Entry missing refid in {table}")
                    continue
                    
                try:
                    timestamp = int(float(entry.get('time', 0)))
                    if timestamp <= 0:
                        log_warning("Database", "InvalidData", f"Invalid timestamp in {table} entry {refid}")
                        continue
                        
                    data_json = json.dumps(entry)
                    cursor.execute(
                        f"INSERT OR IGNORE INTO {table} (refid, data_json, timestamp) VALUES (?, ?, ?)",
                        (refid, data_json, timestamp)
                    )
                    if cursor.rowcount > 0:
                        success_count += 1
                        
                except (ValueError, TypeError) as e:
                    log_error("Database", "DataError", f"Invalid data format in {table} entry", 
                             exception=e, details={"refid": refid})
                except sqlite3.Error as e:
                    log_error("Database", "InsertError", f"Failed to insert into {table}", 
                             exception=e, details={"refid": refid})
            
            conn.commit()
            
        log_event("Database", f"Saved {success_count} entries to {table}",
                details={"total_entries": len(entries)})
    except Exception as e:
        log_error("Database", "SaveError", f"Unexpected error saving entries to {table}", exception=e)
        
    return success_count

def get_trades(api_key: str, api_secret: str, start_time: int, end_time: int) -> List[Dict[str, Any]]:
    """
    Get trade history from cache and/or Kraken API.
    Combines cached entries with newly fetched data.
    
    Args:
        api_key: Kraken API key
        api_secret: Kraken API secret
        start_time: Start timestamp (Unix time)
        end_time: End timestamp (Unix time)
        
    Returns:
        List of trade entries
    """
    log_event("Kraken", "Retrieving trades", 
             details={"start_date": datetime.fromtimestamp(start_time).strftime("%Y-%m-%d"),
                     "end_date": datetime.fromtimestamp(end_time).strftime("%Y-%m-%d")})
    
    try:
        # First try to load cached data
        cached = load_cached_entries('trades', start_time, end_time)
        log_event("Kraken", f"Retrieved {len(cached)} cached trades")
        
        # Find the latest timestamp in cached data to avoid refetching
        latest_cached = 0
        if cached:
            latest_cached = max(int(float(entry.get('time', 0))) for entry in cached)
            # Add a little buffer to avoid timestamp precision issues
            if latest_cached > 0:
                latest_cached += 1
                
        # Only fetch from latest cached timestamp onwards (or from start_time if no cache)
        fetch_start = max(start_time, latest_cached) if latest_cached > 0 else start_time
        
        if fetch_start < end_time:
            # Need to fetch additional data
            try:
                log_event("Kraken API", "Fetching trades",
                         details={"from": datetime.fromtimestamp(fetch_start).strftime("%Y-%m-%d")})
                         
                fetched = api_get_trades(api_key, api_secret, fetch_start, end_time)
                log_event("Kraken API", f"Fetched {len(fetched)} new trades")
                
                # Save new data to cache
                saved_count = save_entries('trades', fetched)
                if saved_count < len(fetched):
                    log_warning("Database", "PartialSave", 
                              f"Only saved {saved_count} of {len(fetched)} trades")
                
                # Combine results, avoiding duplicates by using a dictionary with refid as key
                all_trades = {trade.get('refid'): trade for trade in cached}
                for trade in fetched:
                    all_trades[trade.get('refid')] = trade
                
                # Convert back to list and return
                return list(all_trades.values())
            except Exception as e:
                log_error("Kraken API", "FetchError", "Failed to fetch trades", exception=e)
                # Return cached data if API fails
                return cached
        else:
            # No new data needed, return cache only
            log_event("Kraken", "Using cached trades only (cache is up to date)")
            return cached
            
    except Exception as e:
        log_error("Kraken", "TradeError", "Unexpected error retrieving trades", exception=e)
        # Return empty list as fallback
        return []

def get_ledger(api_key: str, api_secret: str, start_time: int, end_time: int) -> List[Dict[str, Any]]:
    """
    Get ledger history from cache and/or Kraken API.
    Combines cached entries with newly fetched data.
    
    Args:
        api_key: Kraken API key
        api_secret: Kraken API secret
        start_time: Start timestamp (Unix time)
        end_time: End timestamp (Unix time)
        
    Returns:
        List of ledger entries
    """
    log_event("Kraken", "Retrieving ledger entries", 
             details={"start_date": datetime.fromtimestamp(start_time).strftime("%Y-%m-%d"),
                     "end_date": datetime.fromtimestamp(end_time).strftime("%Y-%m-%d")})
    
    try:
        # First try to load cached data
        cached = load_cached_entries('ledger', start_time, end_time)
        log_event("Kraken", f"Retrieved {len(cached)} cached ledger entries")
        
        # Find the latest timestamp in cached data to avoid refetching
        latest_cached = 0
        if cached:
            latest_cached = max(int(float(entry.get('time', 0))) for entry in cached)
            # Add a little buffer to avoid timestamp precision issues
            if latest_cached > 0:
                latest_cached += 1
                
        # Only fetch from latest cached timestamp onwards (or from start_time if no cache)
        fetch_start = max(start_time, latest_cached) if latest_cached > 0 else start_time
        
        if fetch_start < end_time:
            # Need to fetch additional data
            try:
                log_event("Kraken API", "Fetching ledger entries",
                         details={"from": datetime.fromtimestamp(fetch_start).strftime("%Y-%m-%d")})
                         
                fetched = api_get_ledger(api_key, api_secret, fetch_start, end_time)
                log_event("Kraken API", f"Fetched {len(fetched)} new ledger entries")
                
                # Save new data to cache
                saved_count = save_entries('ledger', fetched)
                if saved_count < len(fetched):
                    log_warning("Database", "PartialSave", 
                              f"Only saved {saved_count} of {len(fetched)} ledger entries")
                
                # Combine results, avoiding duplicates by using a dictionary with refid as key
                all_entries = {entry.get('refid'): entry for entry in cached}
                for entry in fetched:
                    all_entries[entry.get('refid')] = entry
                
                # Convert back to list and return
                return list(all_entries.values())
            except Exception as e:
                log_error("Kraken API", "FetchError", "Failed to fetch ledger entries", exception=e)
                # Return cached data if API fails
                return cached
        else:
            # No new data needed, return cache only
            log_event("Kraken", "Using cached ledger entries only (cache is up to date)")
            return cached
            
    except Exception as e:
        log_error("Kraken", "LedgerError", "Unexpected error retrieving ledger entries", exception=e)
        # Return empty list as fallback
        return []
