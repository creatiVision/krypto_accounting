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
from decimal import Decimal, InvalidOperation # Import Decimal and InvalidOperation

from pycoingecko import CoinGeckoAPI
from .kraken_api import get_kraken_ohlc

# Import yfinance conditionally to prevent installation errors
try:
    import yfinance as yf
    YFINANCE_AVAILABLE = True
except ImportError:
    YFINANCE_AVAILABLE = False
    print("[WARNING] yfinance not available. Install it for additional price sources.")

# Import logging functions
from .logging_utils import log_event, log_error, log_warning, log_api_call

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

def _read_from_cache(asset_id: str, date_str: str) -> Optional[Decimal]:
    """Reads price from cache if valid. Returns Decimal."""
    cache_file = _get_cache_filepath(asset_id, date_str)
    if cache_file.exists():
        try:
            with open(cache_file, 'r') as f:
                data = json.load(f)
            cache_timestamp = data.get("timestamp", 0)
            price_str = data.get("price_eur") # Read as string

            # Check if cache is still valid and price exists
            if time.time() - cache_timestamp < CACHE_DURATION_SECONDS and price_str is not None:
                try:
                    # Convert cached string price to Decimal
                    price_decimal = Decimal(str(price_str))
                    # print(f"Cache hit for {asset_id} on {date_str}") # Debug
                    return price_decimal
                except InvalidOperation:
                    log_warning("Cache Read Warning", f"Invalid price format in cache file {cache_file}: {price_str}")
            else:
                # print(f"Cache expired or invalid for {asset_id} on {date_str}") # Debug
                pass # Cache expired or invalid
        except (json.JSONDecodeError, IOError, KeyError, ValueError) as e:
            log_error("Cache", "ReadError", f"Error reading cache file {cache_file}", exception=e)
            # Attempt to delete corrupted cache file
            try:
                cache_file.unlink()
            except OSError:
                pass
    return None

def _write_to_cache(asset_id: str, date_str: str, price_eur: Decimal) -> None:
    """Writes fetched price (as Decimal) to cache."""
    cache_file = _get_cache_filepath(asset_id, date_str)
    try:
        with open(cache_file, 'w') as f:
            # Store price as string for JSON compatibility
            json.dump({"timestamp": time.time(), "price_eur": str(price_eur)}, f)
        # print(f"Cached price for {asset_id} on {date_str}") # Debug
    except IOError as e:
        log_error("Cache", "WriteError", f"Error writing cache file {cache_file}", exception=e)

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
    "APT": "aptos", # Added APT

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
    "ARB": "arbitrum", # Added ARB

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
        # Try without the X prefix (common for crypto assets)
        normalized_asset_no_prefix = normalized_asset[1:]
        if normalized_asset_no_prefix in KRAKEN_TO_CG_MAP:
            return KRAKEN_TO_CG_MAP[normalized_asset_no_prefix]
    
    # Try direct lookup
    if normalized_asset in KRAKEN_TO_CG_MAP:
        return KRAKEN_TO_CG_MAP[normalized_asset]
    
    # Try without Z prefix (common for fiat currencies)
    if normalized_asset.startswith('Z') and len(normalized_asset) > 3:
        normalized_asset_no_prefix = normalized_asset[1:]
        if normalized_asset_no_prefix in KRAKEN_TO_CG_MAP:
            return KRAKEN_TO_CG_MAP[normalized_asset_no_prefix]
    
    # No match found
    log_warning("Asset Mapping", f"No CoinGecko ID mapping found for Kraken asset: {kraken_asset}")
    return None

def get_yfinance_ticker(asset: str) -> Optional[str]:
    """Map asset to Yahoo Finance ticker."""
    # Simple mapping for common cryptos
    asset = asset.upper()
    if asset in ["BTC", "XBT", "XXBT"]:
        return "BTC-EUR"
    elif asset in ["ETH", "XETH"]:
        return "ETH-EUR"
    elif asset in ["ADA"]:
        return "ADA-EUR"
    elif asset in ["DOT"]:
        return "DOT-EUR"
    elif asset in ["SOL"]:
        return "SOL-EUR"
    elif asset in ["AVAX"]:
        return "AVAX-EUR"
    elif asset in ["MATIC"]:
        return "MATIC-EUR"
    elif asset in ["LINK", "XLINK"]:
        return "LINK-EUR"
    elif asset in ["XRP", "XXRP"]:
        return "XRP-EUR"
    elif asset in ["LTC", "XLTC"]:
        return "LTC-EUR"
    elif asset in ["DOGE", "XDG", "XXDG"]:
        return "DOGE-EUR"
    elif asset in ["ARB"]:
        return "ARB-EUR"
    # Add more mappings as needed
    
    # For most assets, try a simple -EUR suffix
    if asset not in ["EUR", "ZEUR", "USD", "ZUSD"]:  # Skip fiat currencies
        return f"{asset}-EUR"
    
    return None

def _format_date_for_cache(dt: datetime) -> str:
    """Format date for cache filename."""
    return dt.strftime("%d-%m-%Y")

def _get_date_from_timestamp(timestamp: int) -> datetime:
    """Convert Unix timestamp to datetime."""
    return datetime.fromtimestamp(timestamp, tz=timezone.utc)

def _get_kraken_price(asset: str, timestamp: int) -> Optional[Decimal]:
    """
    Get historical price from Kraken OHLC API.
    Returns price in EUR as Decimal.
    """
    try:
        # Convert timestamp to datetime
        dt = _get_date_from_timestamp(timestamp)
        
        # Get start of day for the timestamp
        start_time = int(datetime(dt.year, dt.month, dt.day, tzinfo=timezone.utc).timestamp())
        
        # Get OHLC data from Kraken
        # Map asset to correct Kraken trading pair
        if asset == "XETH":
            pair = "ETHEUR"
        elif asset == "XXBT":
            pair = "XBTEUR"
        else:
            pair = f"{asset}EUR"
        
        ohlc_data = get_kraken_ohlc(pair, interval=1440, since=start_time)  # 1440 = 1 day
        
        if not ohlc_data:
            return None
        
        # Extract price from OHLC data (use closing price)
        for time_data in ohlc_data:
            ohlc_time = time_data[0]
            # Find the closest time entry
            if abs(ohlc_time - timestamp) < 86400:  # Within 1 day
                close_price = time_data[4]  # Closing price
                return Decimal(str(close_price))
        
        return None
    except Exception as e:
        log_error("Price", "KrakenError", f"Error fetching Kraken price for {asset} at {timestamp}", exception=e)
        return None

def _get_coingecko_price(asset: str, timestamp: int) -> Optional[Decimal]:
    """
    Get historical price from CoinGecko API.
    Returns price in EUR as Decimal.
    
    Note: Free CoinGecko API only allows querying data within the past 365 days.
    """
    try:
        # Convert timestamp to datetime
        dt = _get_date_from_timestamp(timestamp)
        date_str = dt.strftime("%d-%m-%Y")
        
        # Check if the requested date is within the allowed range (past 365 days)
        # for free CoinGecko API users
        days_ago = (datetime.now(timezone.utc) - dt).days
        if days_ago > 365:
            log_warning("Price", "CoinGeckoLimitation", 
                       f"Skipping CoinGecko API call for {asset} on {date_str} - exceeds 365 day limit for free API")
            return None
        
        # Get CoinGecko ID for the asset
        cg_id = get_coingecko_id(asset)
        if not cg_id:
            return None
        
        # Get historical price from CoinGecko
        price_data = cg.get_coin_history_by_id(
            id=cg_id,
            date=date_str,
            localization="false"
        )
        
        if not price_data or "market_data" not in price_data:
            return None
        
        # Extract EUR price
        price_eur = price_data["market_data"]["current_price"].get("eur")
        if price_eur is None:
            return None
        
        return Decimal(str(price_eur))
    except Exception as e:
        log_error("Price", "CoinGeckoError", f"Error fetching CoinGecko price for {asset} at {timestamp}", exception=e)
        return None

def _get_yfinance_price(asset: str, timestamp: int) -> Optional[Decimal]:
    """
    Get historical price from Yahoo Finance.
    Returns price in EUR as Decimal.
    """
    if not YFINANCE_AVAILABLE:
        return None
    
    try:
        # Convert timestamp to datetime
        dt = _get_date_from_timestamp(timestamp)
        
        # Get Yahoo Finance ticker for the asset
        yf_ticker = get_yfinance_ticker(asset)
        if not yf_ticker:
            return None
        
        # Get historical price from Yahoo Finance
        ticker = yf.Ticker(yf_ticker)
        
        # Get data for the specific date
        start_date = dt.strftime("%Y-%m-%d")
        end_date = (dt + timedelta(days=1)).strftime("%Y-%m-%d")
        hist = ticker.history(start=start_date, end=end_date)
        
        if hist.empty:
            return None
        
        # Use closing price
        price = hist["Close"].iloc[0]
        return Decimal(str(price))
    except Exception as e:
        log_error("Price", "YahooFinanceError", f"Error fetching Yahoo Finance price for {asset} at {timestamp}", exception=e)
        return None

def get_historical_price(asset: str, timestamp: int) -> Optional[Decimal]:
    """
    Get historical price for an asset at a specific timestamp.
    Returns price in EUR as Decimal.
    
    Args:
        asset: Asset ticker (e.g., XXBT, XETH, ADA)
        timestamp: Unix timestamp
        
    Returns:
        Decimal price in EUR or None if not found
    """
    # Skip price lookup for EUR (always 1:1)
    if asset in ["ZEUR"]:
        return Decimal("1.0")
    
    # Format date for cache
    dt = _get_date_from_timestamp(timestamp)
    date_str = _format_date_for_cache(dt)
    
    # Check cache first
    cached_price = _read_from_cache(asset, date_str)
    if cached_price is not None:
        log_event("Price", f"Using cached price for {asset} on {date_str}: {cached_price} EUR")
        return cached_price
    
    # Try Kraken first
    log_api_call("Kraken", f"Fetching price for {asset} on {date_str}")
    price = _get_kraken_price(asset, timestamp)
    if price is not None:
        log_event("Price", f"Got Kraken price for {asset} on {date_str}: {price} EUR")
        _write_to_cache(asset, date_str, price)
        return price
    
    # Try CoinGecko next
    log_api_call("CoinGecko", f"Fetching price for {asset} on {date_str}")
    price = _get_coingecko_price(asset, timestamp)
    if price is not None:
        log_event("Price", f"Got CoinGecko price for {asset} on {date_str}: {price} EUR")
        _write_to_cache(asset, date_str, price)
        return price
    
    # Try Yahoo Finance as a last resort
    if YFINANCE_AVAILABLE:
        log_api_call("Yahoo Finance", f"Fetching price for {asset} on {date_str}")
        price = _get_yfinance_price(asset, timestamp)
        if price is not None:
            log_event("Price", f"Got Yahoo Finance price for {asset} on {date_str}: {price} EUR")
            _write_to_cache(asset, date_str, price)
            return price
    
    # No price found
    log_warning("Price", "NotFound", f"Could not find price for {asset} on {date_str}")
    return None

def get_current_price(asset: str) -> Optional[Decimal]:
    """
    Get current price for an asset.
    Returns price in EUR as Decimal.
    
    Args:
        asset: Asset ticker (e.g., BTC, ETH, ADA)
        
    Returns:
        Decimal price in EUR or None if not found
    """
    # Use current timestamp
    return get_historical_price(asset, int(time.time()))

# Alias for backward compatibility
get_historical_price_eur = get_historical_price
