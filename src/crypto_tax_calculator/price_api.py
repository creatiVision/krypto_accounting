# crypto_tax_calculator/price_api.py

"""
Handles fetching historical cryptocurrency prices.
Uses Kraken API as the primary source for historical data.
Falls back to CoinGecko API for recent data when needed.
Falls back to Yahoo Finance as a final option.
Includes basic file-based caching.
"""

import time
import json
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional, Dict, List, Any, Tuple

from pycoingecko import CoinGeckoAPI
from crypto_tax_calculator.kraken_api import get_kraken_ohlc

# Import yfinance conditionally to prevent installation errors
try:
    import yfinance as yf
    YFINANCE_AVAILABLE = True
except ImportError:
    YFINANCE_AVAILABLE = False
    print("[WARNING] yfinance not available. Install it for additional price sources.")

# Placeholder for logging function
def log_event(event: str, details: str):
    print(f"[LOG] {event}: {details}")

# --- Constants ---
# Define cache directory relative to this file's location
CACHE_DIR = Path(__file__).parent.parent.parent / "data" / "price_cache"
CACHE_DURATION_SECONDS = 24 * 60 * 60  # Cache prices for 1 day

# --- Initialize CoinGecko API ---
cg = CoinGeckoAPI()

# --- Caching Functions ---
def _get_cache_filepath(asset_id: str, date_str: str) -> Path:
    """Constructs the filepath for a cached price."""
    CACHE_DIR.mkdir(parents=True, exist_ok=True) # Ensure cache dir exists
    # Sanitize asset_id for filename if needed (though coingecko IDs are usually safe)
    safe_asset_id = asset_id.replace('/', '_')
    return CACHE_DIR / f"{safe_asset_id}_{date_str}.json"

def _read_from_cache(asset_id: str, date_str: str) -> Optional[float]:
    """Reads price from cache if valid."""
    cache_file = _get_cache_filepath(asset_id, date_str)
    if cache_file.exists():
        try:
            with open(cache_file, 'r') as f:
                data = json.load(f)
            cache_timestamp = data.get("timestamp", 0)
            price = data.get("price_eur")

            # Check if cache is still valid
            if time.time() - cache_timestamp < CACHE_DURATION_SECONDS and price is not None:
                # print(f"Cache hit for {asset_id} on {date_str}") # Debug
                return float(price)
            else:
                # print(f"Cache expired or invalid for {asset_id} on {date_str}") # Debug
                pass # Cache expired or invalid
        except (json.JSONDecodeError, IOError, KeyError, ValueError) as e:
            log_event("Cache Read Error", f"Error reading cache file {cache_file}: {e}")
            # Attempt to delete corrupted cache file
            try:
                cache_file.unlink()
            except OSError:
                pass
    return None

def _write_to_cache(asset_id: str, date_str: str, price_eur: float) -> None:
    """Writes fetched price to cache."""
    cache_file = _get_cache_filepath(asset_id, date_str)
    try:
        with open(cache_file, 'w') as f:
            json.dump({"timestamp": time.time(), "price_eur": price_eur}, f)
        # print(f"Cached price for {asset_id} on {date_str}") # Debug
    except IOError as e:
        log_event("Cache Write Error", f"Error writing cache file {cache_file}: {e}")

# --- Asset ID Mapping ---
# Kraken uses non-standard tickers (e.g., XBT, XETH). Map them to CoinGecko IDs.
KRAKEN_TO_CG_MAP = {
    # Bitcoin & Ethereum
    "XXBT": "bitcoin", 
    "XBT": "bitcoin",  # Common alternative
    "BTC": "bitcoin",
    "XETH": "ethereum",
    "ETH": "ethereum",

    # Other major cryptos
    "XXDG": "dogecoin",
    "XDG": "dogecoin",
    "DOGE": "dogecoin",
    "ADA": "cardano",
    "DOT": "polkadot",
    "SOL": "solana",
    "MATIC": "polygon",
    "XLTC": "litecoin",
    "LTC": "litecoin",
    "XXRP": "ripple",
    "XRP": "ripple",
    "XTZ": "tezos",
    "XXLM": "stellar",
    "XLM": "stellar",
    "ATOM": "cosmos",
    "XLINK": "chainlink",
    "LINK": "chainlink",
    
    # DeFi tokens
    "UNI": "uniswap",
    "AAVE": "aave",
    "SNX": "synthetix-network-token",
    "YFI": "yearn-finance",
    "COMP": "compound-governance-token",
    "1INCH": "1inch",
    "BAL": "balancer",
    "CRV": "curve-dao-token",
    "GRT": "the-graph",
    
    # Layer 1s & Layer 2s
    "AVAX": "avalanche-2",
    "FTM": "fantom",
    "ALGO": "algorand",
    "NEAR": "near",
    "ONE": "harmony",
    "FIL": "filecoin",
    "FLOW": "flow",
    
    # Others
    "MANA": "decentraland",
    "SAND": "the-sandbox",
    "AXS": "axie-infinity",
    "ENJ": "enjincoin",
    "CHZ": "chiliz",
    "GALA": "gala",
    "APE": "apecoin",
    "SHIB": "shiba-inu",
    "LUNA": "terra-luna-2",
    "LUNA1": "terra-luna",
    "XREP": "augur",
    "REP": "augur",
    "KSM": "kusama",
    "ZEC": "zcash",
    "XZEC": "zcash",
    "DASH": "dash",
    "XMR": "monero",
    "XXMR": "monero",
    "BCH": "bitcoin-cash",
    "XDASH": "dash",
    "XETC": "ethereum-classic",
    "ETC": "ethereum-classic",
    "TRX": "tron",
    "OCEAN": "ocean-protocol",
    "QTUM": "qtum",
    "ICX": "icon",
    "OXT": "orchid-protocol",
    "OMG": "omisego",
    "ZRX": "0x",
    "BAT": "basic-attention-token",
    
    # Stablecoins
    "USDT": "tether",
    "USDC": "usd-coin",
    "DAI": "dai",
    "BUSD": "binance-usd",
    "UST": "terrausd",
    
    # Fiat currencies (for completeness)
    "EUR": "eur", 
    "ZEUR": "eur",
    "USD": "usd",
    "ZUSD": "usd",
    "GBP": "gbp",
    "ZGBP": "gbp",
    "JPY": "jpy",
    "ZJPY": "jpy",
    "CAD": "cad",
    "ZCAD": "cad",
    "AUD": "aud",
    "ZAUD": "aud",
    "CHF": "chf",
    "ZCHF": "chf",
}

def get_coingecko_id(kraken_asset: str) -> Optional[str]:
    """Map Kraken asset ticker to CoinGecko ID."""
    # Normalize Kraken asset (remove leading X/Z if common)
    normalized_asset = kraken_asset.upper()
    if normalized_asset.startswith('X') and len(normalized_asset) > 3:
         normalized_asset = normalized_asset[1:]
    if normalized_asset.startswith('Z') and len(normalized_asset) > 3:
         normalized_asset = normalized_asset[1:]

    # Check direct map first
    if kraken_asset.upper() in KRAKEN_TO_CG_MAP:
        return KRAKEN_TO_CG_MAP[kraken_asset.upper()]
    # Check normalized map
    if normalized_asset in KRAKEN_TO_CG_MAP:
         return KRAKEN_TO_CG_MAP[normalized_asset]

    # Fallback: try lowercase symbol directly (common case)
    # This requires fetching the coin list, could be slow initially
    # Consider caching the coin list if this is used often
    try:
        # print(f"Attempting direct symbol lookup for {kraken_asset.lower()}...") # Debug
        coins_list = cg.get_coins_list(include_platform=False)
        for coin in coins_list:
            if coin['symbol'].lower() == kraken_asset.lower():
                log_event("CoinGecko Mapping", f"Mapped Kraken '{kraken_asset}' to CoinGecko ID '{coin['id']}' via symbol lookup.")
                # Add to map for future use? Could grow large.
                # KRAKEN_TO_CG_MAP[kraken_asset.upper()] = coin['id']
                return coin['id']
    except Exception as e:
        log_event("CoinGecko API Error", f"Failed to fetch coin list for mapping: {e}")

    log_event("Price API Warning", f"Could not map Kraken asset '{kraken_asset}' to CoinGecko ID.")
    return None


# --- Kraken Symbol Mapping ---
def get_kraken_pair_symbol(asset: str) -> str:
    """Convert a standard asset symbol to Kraken's pair format for the OHLC API."""
    # Normalize the asset name first
    normalized = asset.upper()
    
    # Map of common crypto assets to their Kraken pair format
    # List based on common Kraken trading pairs
    kraken_pair_map = {
        # Assets with X prefix in Kraken
        "BTC": "XXBTZEUR",
        "ETH": "XETHZEUR",
        "LTC": "XLTCZEUR",
        "XMR": "XXMRZEUR",
        "XRP": "XXRPZEUR",
        "ZEC": "XZECZEUR",
        "REP": "XREPZEUR",
        "XTZ": "XTZEUR",
        "XLM": "XXLMZEUR",
        "DASH": "DASHEUR",
        "EOS": "EOSEUR",
        "ETC": "XETCZEUR",
        
        # Assets with standard format
        "ADA": "ADAEUR",
        "ALGO": "ALGOEUR",
        "ATOM": "ATOMEUR",
        "AVAX": "AVAXEUR",
        "DOT": "DOTEUR",
        "LINK": "LINKEUR",
        "SOL": "SOLEUR",
        "MATIC": "MATICEUR",
        "UNI": "UNIEUR",
        "AAVE": "AAVEEUR",
        "SNX": "SNXEUR",
        "YFI": "YFIEUR",
        "GRT": "GRTEUR",
        "BCH": "BCHEUR",
        "FIL": "FILEUR",
        "TRX": "TRXEUR",
        "1INCH": "1INCHEUR",
        "COMP": "COMPEUR",
        "DOGE": "XDGEUR",
        "KSM": "KSMEUR",
        "LUNA": "LUNAEUR",
        "MANA": "MANAEUR",
        "NEAR": "NEAREUR",
        "OCEAN": "OCEANEUR",
        "SAND": "SANDEUR",
        "SHIB": "SHIBEUR",
    }
    
    # Check if we have a direct mapping for this asset
    if normalized in kraken_pair_map:
        return kraken_pair_map[normalized]
    
    # Special case handling for assets with alternate names
    if normalized == "XBT":
        return "XXBTZEUR"  # Bitcoin
    elif normalized == "XDG":
        return "XDGEUR"    # Dogecoin
    
    # For X-prefixed assets from Kraken that need normalization
    if normalized.startswith('X') and len(normalized) > 3:
        base = normalized[1:]  # Remove X prefix
        if base in kraken_pair_map:
            return kraken_pair_map[base]
    
    # Default fallback format
    if len(normalized) < 5:  # Most base assets are short symbols
        return f"{normalized}EUR"
    
    # Log when we're using fallback format
    log_event("Kraken Symbol", f"Using fallback format for {asset}: {normalized}EUR")
    return f"{normalized}EUR"

def get_price_from_kraken(asset: str, timestamp: int) -> Optional[float]:
    """
    Fetch historical price data from Kraken's OHLC API.
    Returns the closing price in EUR for the day containing the timestamp.
    
    Enhanced to handle more edge cases and improve reliability:
    - Tries multiple time intervals
    - Attempts alternate pair formats
    - Includes more flexible date matching
    """
    if asset.upper() in ["EUR", "ZEUR"]:
        return 1.0
        
    # Convert input timestamp to datetime for calculations
    dt = datetime.fromtimestamp(timestamp, timezone.utc)
    date_str = dt.strftime("%Y-%m-%d")
    
    # Get Kraken pair symbol 
    primary_pair_symbol = get_kraken_pair_symbol(asset)
    
    # Define alternate pair formats to try if primary fails
    alternate_pairs = []
    normalized = asset.upper()
    if normalized not in ["EUR", "ZEUR", "USD", "ZUSD"]:
        # Try some common alternative formats
        if not normalized.startswith('X'):
            alternate_pairs.append(f"X{normalized}ZEUR")
        if normalized.startswith('X') and len(normalized) > 1:
            alternate_pairs.append(f"{normalized[1:]}EUR")
        alternate_pairs.append(f"{normalized}EUR")
        
        # For 2024 data, also check USD pairs as fallback
        current_year = datetime.now().year
        if dt.year >= current_year - 1:
            alternate_pairs.append(f"{normalized}USD")
            if not normalized.startswith('X'):
                alternate_pairs.append(f"X{normalized}ZUSD")
    
    # Time intervals to try (in minutes)
    # 1440 = 1 day, 240 = 4 hours, 60 = 1 hour
    intervals = [1440, 240, 60]
    
    # Try primary pair first with all intervals
    for interval in intervals:
        # OHLC data returned by Kraken is an array of:
        # [time, open, high, low, close, vwap, volume, count]
        ohlc_data = get_kraken_ohlc(primary_pair_symbol, interval=interval)
        
        if ohlc_data:
            price = _find_closest_price(ohlc_data, dt, interval)
            if price is not None:
                log_event("Kraken Price Success", f"Found price for {primary_pair_symbol} on {date_str} with interval {interval}")
                return price
    
    # If primary pair failed, try alternate pairs
    for alt_pair in alternate_pairs:
        for interval in intervals:
            log_event("Kraken Price Attempt", f"Trying alternate pair {alt_pair} with interval {interval}")
            ohlc_data = get_kraken_ohlc(alt_pair, interval=interval)
            
            if ohlc_data:
                price = _find_closest_price(ohlc_data, dt, interval)
                if price is not None:
                    log_event("Kraken Price Success", f"Found price using alternate pair {alt_pair} on {date_str} with interval {interval}")
                    return price
    
    # If we reached here, we failed to find a price with all attempts
    log_event("Kraken Price Error", f"Failed to find price for {asset} on {date_str} after trying all formats and intervals")
    return None

def _find_closest_price(ohlc_data: List[List], dt: datetime, interval_minutes: int) -> Optional[float]:
    """Helper function to find the closest price point in OHLC data."""
    # Calculate timestamp for the start of the day
    timestamp_day_start = int(datetime(dt.year, dt.month, dt.day, tzinfo=timezone.utc).timestamp())
    
    # For higher precision with smaller intervals, include hour/minute
    if interval_minutes < 1440:  # If using intervals smaller than a day
        # Try to find closer timestamp including hours/minutes
        timestamp_target = int(dt.timestamp())
    else:
        timestamp_target = timestamp_day_start
    
    closest_data = None
    min_distance = float('inf')
    
    for data_point in ohlc_data:
        data_time = data_point[0]  # Unix timestamp of this data point
        distance = abs(data_time - timestamp_target)
        if distance < min_distance:
            min_distance = distance
            closest_data = data_point
    
    # Accept tolerance based on interval
    # For daily data: accept up to 2 days difference
    # For hourly data: accept up to 12 hours difference
    tolerance_seconds = 86400 * 2 if interval_minutes >= 1440 else interval_minutes * 60 * 12
    
    if closest_data and min_distance < tolerance_seconds:
        closing_price = float(closest_data[4])  # Close price is at index 4
        return closing_price
    
    return None

# --- Main Price Fetching Function ---
def get_historical_price_eur(kraken_asset: str, timestamp: int) -> Optional[float]:
    """
    Get the historical price of a Kraken asset in EUR for a specific timestamp.
    Always tries Kraken API first for all dates, falls back to CoinGecko for dates <= 365 days
    if Kraken data isn't available.
    Returns None if price cannot be found.
    """
    if kraken_asset.upper() in ["EUR", "ZEUR"]:
        return 1.0

    # Convert timestamp to datetime and date string
    dt_object = datetime.fromtimestamp(timestamp, timezone.utc)
    date_str = dt_object.strftime("%d-%m-%Y") # Format for cache and CoinGecko
    
    # Normalize asset name
    normalized_asset = kraken_asset.upper()
    if normalized_asset.startswith('X') and len(normalized_asset) > 3:
        normalized_asset = normalized_asset[1:]
    if normalized_asset.startswith('Z') and len(normalized_asset) > 3:
        normalized_asset = normalized_asset[1:]

    # Check cache first
    cached_price = _read_from_cache(normalized_asset, date_str)
    if cached_price is not None:
        return cached_price
    
    # Always try Kraken API first, regardless of date
    price_eur = get_price_from_kraken(normalized_asset, timestamp)
    if price_eur is not None:
        _write_to_cache(normalized_asset, date_str, price_eur)
        log_event("Price API", f"Fetched price from Kraken: {price_eur:.4f} EUR")
        return price_eur
    
    # Determine if this date is within the last 365 days (CoinGecko limit)
    one_year_ago = datetime.now(timezone.utc) - timedelta(days=365)
    is_recent = dt_object >= one_year_ago
    
    # Only try CoinGecko as a fallback for recent data (≤ 365 days)
    if is_recent:
        try:
            # Map to CoinGecko ID 
            asset_id = get_coingecko_id(kraken_asset)
            if not asset_id:
                log_event("Price Fetch Error", f"Cannot fetch price from CoinGecko for unknown asset: {kraken_asset}")
                return None  # Indicate failure
                
            log_event("Price API", f"Fallback: Fetching price for {asset_id} ({kraken_asset}) on {date_str} from CoinGecko...")
            # Note: Free CoinGecko API provides daily average price for historical data
            history = cg.get_coin_history_by_id(id=asset_id, date=date_str, localization='false')

            # Extract price in EUR
            price_data = history.get("market_data", {}).get("current_price", {})
            price_eur = price_data.get("eur")

            if price_eur is None:
                log_event("Price Fetch Warning", f"CoinGecko did not return EUR price for {asset_id} on {date_str}")
                return None
            else:
                price_float = float(price_eur)
                _write_to_cache(normalized_asset, date_str, price_float)
                log_event("Price API", f"Fetched price from CoinGecko: {price_float:.4f} EUR")
                return price_float

        except Exception as e:
            # Handle potential API errors (rate limits, network issues, invalid asset ID)
            log_event("Price Fetch Error", f"Error fetching price from CoinGecko: {e}")
            return None
    else:
        # For older data, we've already tried Kraken and it failed
        log_event("Price Fetch Error", f"Cannot fetch price for {normalized_asset} on {date_str} - No data from Kraken and too old for CoinGecko")
        
    # Try Yahoo Finance as a final fallback if it's available
    if YFINANCE_AVAILABLE:
        price_eur = get_price_from_yahoo(normalized_asset, timestamp)
        if price_eur is not None:
            _write_to_cache(normalized_asset, date_str, price_eur)
            log_event("Price API", f"Fetched price from Yahoo Finance: {price_eur:.4f} EUR")
            return price_eur
    
    # If we get here, we couldn't find a price from any source
    log_event("Price Fetch Error", f"Failed to find price for {normalized_asset} from any available source")
    return None


def get_price_from_yahoo(asset: str, timestamp: int) -> Optional[float]:
    """
    Fetch historical price data from Yahoo Finance as a fallback.
    
    This function tries multiple ticker formats and handles both EUR and USD denominated symbols.
    For USD symbols, it performs currency conversion to EUR.
    """
    if not YFINANCE_AVAILABLE:
        return None
        
    if asset.upper() in ["EUR", "ZEUR"]:
        return 1.0
        
    try:
        # Convert timestamp to datetime for calculations
        dt = datetime.fromtimestamp(timestamp, timezone.utc)
        
        # List of possible Yahoo Finance ticker formats to try
        # Most crypto tickers on Yahoo are formatted as "BTC-EUR", "ETH-USD", etc.
        possible_tickers = [
            f"{asset}-EUR",         # Standard EUR pair
            f"{asset.upper()}-EUR", # Uppercase
            f"{asset}-USD",         # USD pair (will need conversion)
            f"{asset.upper()}-USD"  # Uppercase USD pair
        ]
        
        # If asset has an X prefix (like XBT), try without it
        if asset.upper().startswith('X') and len(asset.upper()) > 3:
            possible_tickers.append(f"{asset.upper()[1:]}-EUR")
            possible_tickers.append(f"{asset.upper()[1:]}-USD")
            
        # For Bitcoin, try special Yahoo ticker
        if asset.upper() in ["XBT", "BTC", "XXBT"]:
            possible_tickers.append("BTC-EUR")
            possible_tickers.append("BTC-USD")
        
        # Define a window around the target date to ensure we get data
        start_date = dt - timedelta(days=5)  # 5 days before
        end_date = dt + timedelta(days=5)    # 5 days after
        
        # Try each possible ticker
        for ticker in possible_tickers:
            log_event("Yahoo Price Attempt", f"Trying Yahoo ticker {ticker} for {asset}")
            data = yf.download(ticker, start=start_date, end=end_date, progress=False)
            
            if not data.empty:
                # Find the closest date in the data
                closest_date = min(data.index, 
                                  key=lambda x: abs((x - dt.replace(tzinfo=None)).total_seconds()))
                
                # Get closing price for the closest date
                close_price = data.loc[closest_date, 'Close']
                
                # Convert to EUR if ticker is in USD
                if ticker.endswith("-USD"):
                    # Get EUR/USD rate for the date
                    log_event("Yahoo Price", f"Converting USD price to EUR using EURUSD exchange rate")
                    
                    try:
                        # Yahoo ticker for EUR/USD is EURUSD=X
                        # We need a slightly larger window for forex data
                        eur_usd_data = yf.download("EURUSD=X", 
                                               start=closest_date-timedelta(days=2),
                                               end=closest_date+timedelta(days=2),
                                               progress=False)
                        
                        if not eur_usd_data.empty:
                            # Find closest forex date
                            closest_fx_date = min(eur_usd_data.index,
                                               key=lambda x: abs((x - closest_date).total_seconds()))
                            
                            # EUR/USD rate (need to invert for USD → EUR)
                            eur_usd_rate = eur_usd_data.loc[closest_fx_date, 'Close']
                            usd_eur_rate = 1 / eur_usd_rate
                            
                            # Convert the price
                            close_price = close_price * usd_eur_rate
                            log_event("Yahoo Price", f"USD→EUR conversion: {close_price:.4f} EUR (rate: {usd_eur_rate:.4f})")
                        else:
                            # Fallback USD/EUR conversion if we can't get forex data
                            # This is a very rough approximation
                            log_event("Yahoo Price Warning", "Using fallback USD→EUR conversion rate of 0.85")
                            close_price = close_price * 0.85
                    except Exception as e:
                        # Fallback USD/EUR conversion if anything goes wrong
                        log_event("Yahoo FX Error", f"Error getting USD→EUR rate: {e}, using fallback 0.85")
                        close_price = close_price * 0.85
                
                log_event("Yahoo Price Success", f"Found price for {asset} using ticker {ticker}: {close_price:.4f} EUR")
                return float(close_price)
        
        # If we get here, we couldn't find a price with any of the tickers
        log_event("Yahoo Price Error", f"Could not find Yahoo Finance data for {asset} with any ticker format")
        return None
        
    except Exception as e:
        error_msg = str(e)
        if "unsupported format string" in error_msg or "YFTzMissingError" in error_msg:
            log_event("Yahoo Error", f"Yahoo Finance likely doesn't have data for this future date: {error_msg}")
        else:
            log_event("Yahoo Error", f"Error fetching price from Yahoo Finance: {error_msg}")
        return None


# Example usage (for testing this module directly)
if __name__ == "__main__":
    print("Testing Price API module...")

    # Test cases
    test_asset_kraken = "XXBT"
    test_timestamp = int(datetime(2023, 6, 15, 12, 0, 0, tzinfo=timezone.utc).timestamp()) # June 15, 2023

    print(f"\nFetching price for {test_asset_kraken} at {datetime.fromtimestamp(test_timestamp, timezone.utc)}")
    price = get_historical_price_eur(test_asset_kraken, test_timestamp)
    if price is not None:
        print(f"Result: {price:.2f} EUR")
    else:
        print("Result: Price not found.")

    # Test caching
    print(f"\nFetching price again for {test_asset_kraken} at {datetime.fromtimestamp(test_timestamp, timezone.utc)} (should use cache)")
    price = get_historical_price_eur(test_asset_kraken, test_timestamp)
    if price is not None:
        print(f"Result: {price:.2f} EUR")
    else:
        print("Result: Price not found.")

    # Test unknown asset
    print(f"\nFetching price for UNKNOWNASSET")
    price = get_historical_price_eur("UNKNOWNASSET", test_timestamp)
    if price is not None:
        print(f"Result: {price:.2f} EUR")
    else:
        print("Result: Price not found (as expected).")

    # Test EUR
    print(f"\nFetching price for EUR")
    price = get_historical_price_eur("EUR", test_timestamp)
    if price is not None:
        print(f"Result: {price:.2f} EUR")
    else:
        print("Result: Price not found.")
