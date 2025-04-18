# crypto_tax_calculator/kraken_api.py

"""
Handles communication with the Kraken REST API.
"""

import base64
import datetime
import hashlib
import hmac
import time
import traceback
import urllib.parse
import threading
import random
from pathlib import Path
from typing import Dict, List, Optional, Any

import requests

from .logging_utils import log_event, log_error, log_api_call

# Placeholder for nonce management, will be refined
LAST_NONCE = 0

# Rate limiter for Kraken API
class RateLimiter:
    """Simple token bucket rate limiter for Kraken API."""
    def __init__(self, rate=3, per=1.0, initial_tokens=3):
        self.rate = rate  # tokens per second
        self.per = per    # seconds
        self.tokens = initial_tokens
        self.last_check = time.time()
        self.lock = threading.Lock()  # For thread safety

    def wait_for_token(self):
        """Wait until a token is available."""
        with self.lock:
            now = time.time()
            time_passed = now - self.last_check
            self.last_check = now
            
            # Add tokens based on time passed, up to max capacity
            self.tokens = min(self.rate, self.tokens + time_passed * (self.rate / self.per))
            
            if self.tokens < 1:
                # Calculate sleep time needed to get 1 token
                sleep_time = (1 - self.tokens) * (self.per / self.rate)
                time.sleep(sleep_time)
                self.tokens = 0  # We used our token
                self.last_check = time.time()
            else:
                self.tokens -= 1  # We used one token

# Create a global rate limiter instance
RATE_LIMITER = RateLimiter(rate=3, per=1.0)  # 3 requests per second

def get_safe_nonce() -> str:
    """Generate a nonce that is guaranteed to be higher than previous ones."""
    global LAST_NONCE
    current_nonce = int(time.time() * 1000000)
    if current_nonce <= LAST_NONCE:
        current_nonce = LAST_NONCE + 1000
    LAST_NONCE = current_nonce
    return str(current_nonce)

def get_kraken_signature(urlpath: str, data: Dict[str, Any], secret: str) -> str:
    """Create API signature for Kraken private API requests."""
    postdata = urllib.parse.urlencode(data)
    encoded = (str(data['nonce']) + postdata).encode()
    message = urlpath.encode() + hashlib.sha256(encoded).digest()
    mac = hmac.new(base64.b64decode(secret), message, hashlib.sha512)
    sigdigest = base64.b64encode(mac.digest())
    return sigdigest.decode()

def kraken_request(uri_path: str, data: Dict[str, Any], api_key: str, api_sec: str, public: bool = False) -> Dict[str, Any]:
    """Make a request to Kraken API with rate limiting."""
    # Log the timestamp before waiting for a token
    request_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    log_event("API", f"[{request_time}] Requesting token for {uri_path}")
    
    # Wait for a token before making the request
    start_time = time.time()
    RATE_LIMITER.wait_for_token()
    
    # Log the timestamp after getting a token
    token_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    log_event("API", f"[{token_time}] Got token for {uri_path}")
    
    headers = {}
    if not public:
        headers['API-Key'] = api_key
        headers['API-Sign'] = get_kraken_signature(uri_path, data, api_sec)

    api_domain = "https://api.kraken.com"
    url = api_domain + uri_path
    method = "GET" if public else "POST"

    try:
        # Record the time before making the request
        request_start_time = time.time()
        
        if public:
             response = requests.get(url, params=data, timeout=30)
        else:
            response = requests.post(url, headers=headers, data=data, timeout=30)
        
        # Calculate the duration of the request
        duration_ms = (time.time() - request_start_time) * 1000
        
        # Log the API call with the response time
        response_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        log_event("API", f"[{response_time}] Received response for {uri_path}")
        
        response_data = response.json()

        if response.status_code != 200:
            error_msg = f"Kraken API returned status code {response.status_code}: {response_data}"
            log_api_call(
                api_name="Kraken",
                endpoint=uri_path,
                method=method,
                params=data,
                success=False,
                response_code=response.status_code,
                duration_ms=duration_ms,
                error_message=error_msg
            )
            print(f"ERROR: {error_msg}")
            return {"error": response_data.get("error", ["Unknown API error"])}

        if "error" in response_data and response_data["error"]:
            error_msg = f"Kraken API returned error: {response_data['error']}"
            # Don't log nonce errors excessively here, handle in calling function
            if "EAPI:Invalid nonce" not in str(response_data['error']):
                log_api_call(
                    api_name="Kraken",
                    endpoint=uri_path,
                    method=method,
                    params=data,
                    success=False,
                    response_code=response.status_code,
                    duration_ms=duration_ms,
                    error_message=error_msg
                )
                print(f"ERROR: {error_msg}")
            return response_data # Return the error structure for handling

        # Log successful API call
        log_api_call(
            api_name="Kraken",
            endpoint=uri_path,
            method=method,
            params=data,
            success=True,
            response_code=response.status_code,
            duration_ms=duration_ms
        )
        
        return response_data
    except requests.exceptions.RequestException as e:
        error_msg = f"Network error during Kraken API request: {str(e)}"
        log_error("API", "NetworkError", error_msg, exception=e)
        print(f"ERROR: {error_msg}")
        traceback.print_exc()
        return {"error": [f"Network error: {str(e)}"]}
    except Exception as e:
        error_msg = f"Exception during Kraken API request: {str(e)}"
        log_error("API", "Exception", error_msg, exception=e)
        print(f"ERROR: {error_msg}")
        traceback.print_exc()
        return {"error": [str(e)]}

def fetch_kraken_data(endpoint: str, params: Dict[str, Any], api_key: str, api_secret: str, is_recovery_call: bool = False) -> List[Dict[str, Any]]:
    """
    Fetches paginated data (trades or ledger) from Kraken.
    Handles nonce errors and retries.
    
    Args:
        endpoint: The API endpoint to call
        params: Parameters for the API call
        api_key: Kraken API key
        api_secret: Kraken API secret
        is_recovery_call: Whether this is a recovery call during FIFO error handling
    """
    all_data = []
    offset = 0
    max_retries = 3
    data_key = "trades" if endpoint == '/0/private/TradesHistory' else "ledger"
    result_key = "trades" if endpoint == '/0/private/TradesHistory' else "ledger" # Key within result dict

    if not is_recovery_call:
        print(f"Fetching {data_key} from Kraken...")

    while True:
        current_retry = 0
        retry_success = False
        result = {}

        while current_retry < max_retries and not retry_success:
            try:
                request_data = {
                    "nonce": get_safe_nonce(),
                    "ofs": offset,
                    **params # Add other params like start, end, type
                }

                # print(f"Making Kraken {endpoint} request with offset {offset}...") # Reduced verbosity
                result = kraken_request(endpoint, request_data, api_key, api_secret)

                if "error" in result and result["error"]:
                    error_str = str(result["error"])
                    if "EAPI:Rate limit exceeded" in error_str:
                        # Specific handling for rate limit errors
                        if current_retry < max_retries - 1:
                            current_retry += 1
                            wait_time = (2 ** current_retry) + (random.random() * 0.5)  # Exponential backoff with jitter
                            retry_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
                            print(f"Rate limit exceeded ({data_key}). Retrying in {wait_time:.2f}s (attempt {current_retry}/{max_retries})...")
                            log_event("API", f"[{retry_time}] Rate limit exceeded for {data_key}. Retrying in {wait_time:.2f}s (attempt {current_retry}/{max_retries})")
                            time.sleep(wait_time)
                            continue  # Retry the loop
                        else:
                            error_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
                            error_msg = f"Max retry attempts reached for rate limit error ({data_key})."
                            print(f"ERROR: {error_msg}")
                            log_error("API", "RateLimitError", f"[{error_time}] {data_key} rate limit error persists after {max_retries} retries")
                            raise Exception(error_msg)  # Propagate error up
                    elif "EAPI:Invalid nonce" in error_str:
                        if current_retry < max_retries - 1:
                            current_retry += 1
                            wait_time = (2 ** current_retry) + (random.random() * 0.5)  # Exponential backoff with jitter
                            retry_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
                            print(f"Invalid nonce error ({data_key}). Retrying in {wait_time:.2f}s (attempt {current_retry}/{max_retries})...")
                            log_event("API", f"[{retry_time}] Invalid nonce error for {data_key}. Retrying in {wait_time:.2f}s (attempt {current_retry}/{max_retries})")
                            time.sleep(wait_time)
                            continue # Retry the loop
                        else:
                            error_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
                            error_msg = f"Max retry attempts reached for nonce error ({data_key})."
                            print(f"ERROR: {error_msg}")
                            log_error("API", "NonceError", f"[{error_time}] {data_key} nonce error persists after {max_retries} retries")
                            raise Exception(error_msg) # Propagate error up
                    else:
                        # Other API errors
                        error_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
                        error_msg = f"Error fetching {data_key}: {result['error']}"
                        print(f"ERROR: {error_msg}")
                        log_error("API", "APIError", f"[{error_time}] {error_msg}")
                        raise Exception(error_msg) # Propagate error up

                # If we made it here, the request was successful
                retry_success = True

            except Exception as retry_error:
                 # Don't retry generic exceptions unless specifically nonce related
                error_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
                log_error("API", "FetchError", f"[{error_time}] Unhandled exception during {data_key} fetch", exception=retry_error)
                raise # Re-raise the exception

        # Process successful batch
        batch_dict = result.get("result", {}).get(result_key, {})
        batch_list = list(batch_dict.values()) # Convert dict of trades/ledgers to list
        print(f"Received {len(batch_list)} {data_key} entries in this batch.")

        if not batch_list:
            print(f"No more {data_key} found.")
            break

        # Add refid to each item
        for item_id, item_data in batch_dict.items():
             item_data['refid'] = item_id # Ensure refid is present
             all_data.append(item_data)

        if len(batch_list) < 50:  # Kraken default limit is 50
            print(f"Received fewer {data_key} than the limit, ending pagination.")
            break

        offset += len(batch_list)
        # print(f"Increasing offset to {offset} for next batch") # Reduced verbosity
        
        # Add jitter to the delay between pages for better distribution of requests
        jitter = random.uniform(0.3, 0.7)  # 30-70% variation
        time.sleep(jitter)  # Variable delay between pages

    if not is_recovery_call:
        print(f"Total {data_key} found: {len(all_data)}")
        if not all_data:
            print(f"WARNING: No {data_key} were found for the specified parameters.")
            print("Please verify your API keys and date range.")
        print("-" * 30)
    return all_data

def get_trades(api_key: str, api_secret: str, start_time: int, end_time: int, is_recovery_call: bool = False) -> List[Dict[str, Any]]:
    """Fetch trade data from Kraken API."""
    params = {"start": start_time, "end": end_time, "trades": "true"} # trads=true might be needed? Check docs.
    return fetch_kraken_data('/0/private/TradesHistory', params, api_key, api_secret, is_recovery_call)

def get_ledger(api_key: str, api_secret: str, start_time: int, end_time: int, is_recovery_call: bool = False) -> List[Dict[str, Any]]:
    """Fetch ledger entries from Kraken API."""
    params = {"start": start_time, "end": end_time, "type": "all"}
    return fetch_kraken_data('/0/private/Ledgers', params, api_key, api_secret, is_recovery_call)

def get_kraken_ohlc(pair: str, interval: int = 1440, since: Optional[int] = None) -> List[List]:
     """Fetch OHLC data from Kraken public API."""
     # Note: Public endpoint, no API key needed for basic OHLC
     params = {"pair": pair, "interval": interval}
     if since:
         params["since"] = since

     result = kraken_request('/0/public/OHLC', params, "", "", public=True)

     if "error" in result and result["error"]:
         print(f"Error fetching OHLC for {pair}: {result['error']}")
         return []

     # The result structure is {'result': {'PAIR': [[time, open, high, low, close, vwap, volume, count]], 'last': ...}}
     ohlc_data = result.get("result", {}).get(pair, [])
     return ohlc_data

# Example usage (for testing this module directly)
if __name__ == "__main__":
    print("Testing Kraken API module...")
    # Requires config.json in the parent directory relative to this file
    # Or better, use environment variables
    from dotenv import load_dotenv
    import os
    parent_dir = Path(__file__).resolve().parent.parent.parent # Go up three levels to skripts-py/accounting/
    load_dotenv(dotenv_path=parent_dir / '.env') # Load .env from accounting dir

    API_KEY = os.getenv("KRAKEN_API_KEY")
    API_SECRET = os.getenv("KRAKEN_API_SECRET")

    if not API_KEY or not API_SECRET:
        print("Error: KRAKEN_API_KEY or KRAKEN_API_SECRET not found in .env file.")
        print("Create a .env file in skripts-py/accounting/ with your keys.")
    else:
        print("API Keys loaded from .env")
        # Example: Fetch trades for a specific period (replace with actual timestamps)
        test_start_time = int(datetime.datetime(2023, 1, 1).timestamp())
        test_end_time = int(datetime.datetime(2023, 1, 31).timestamp())

        try:
            # print("\nFetching Trades...")
            # trades = get_trades(API_KEY, API_SECRET, test_start_time, test_end_time)
            # if trades:
            #     print(f"Fetched {len(trades)} trades. First trade: {trades[0]}")

            print("\nFetching Ledger...")
            ledger = get_ledger(API_KEY, API_SECRET, test_start_time, test_end_time)
            if ledger:
                print(f"Fetched {len(ledger)} ledger entries. First entry: {ledger[0]}")

            # print("\nFetching OHLC...")
            # ohlc = get_kraken_ohlc("XXBTZEUR", interval=60) # Hourly
            # if ohlc:
            #      print(f"Fetched {len(ohlc)} OHLC data points for BTC/EUR. Last point: {ohlc[-1]}")

        except Exception as e:
            print(f"An error occurred during testing: {e}")
            traceback.print_exc()
