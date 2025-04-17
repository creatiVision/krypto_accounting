"""
Provides centralized logging functionality for the crypto tax calculator.
Logs events, errors, warnings, API calls, and transactions to help with troubleshooting
and provide an audit trail of operations.

All log files are stored in the "logs" directory.
"""

import os
import json
import logging
import traceback
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional, Union

# Setup logs directory
LOGS_DIR = Path("logs")
LOGS_DIR.mkdir(exist_ok=True)

# Configure the main application logger
logger = logging.getLogger("crypto_tax")
logger.setLevel(logging.INFO)

# Determine the log filename with current date
log_date = datetime.now().strftime("%Y%m%d")
LOG_FILENAME = f"crypto_tax_{log_date}.log"
LOG_PATH = LOGS_DIR / LOG_FILENAME

# Configure handlers if they haven't been set up yet
if not logger.handlers:
    # File handler
    file_handler = logging.FileHandler(LOG_PATH)
    file_handler.setLevel(logging.INFO)
    file_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(file_format)
    logger.addHandler(file_handler)
    
    # Console handler (optional - for development)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.WARNING)  # Only warnings and errors to console
    console_format = logging.Formatter('%(levelname)s: %(message)s')
    console_handler.setFormatter(console_format)
    logger.addHandler(console_handler)

def format_details(details: Optional[Dict[str, Any]] = None) -> str:
    """Format details dictionary as JSON string if available."""
    if details:
        try:
            return json.dumps(details)
        except Exception:
            return str(details)
    return ""

def log_event(
    component: str,
    message: str,
    details: Optional[Dict[str, Any]] = None
) -> None:
    """
    Log a normal application event.
    
    Parameters:
        component: The application component generating the event
        message: The event message
        details: Optional dictionary of additional details
    """
    detail_str = format_details(details)
    logger.info(f"[{component}] {message} {detail_str if detail_str else ''}")

def log_error(
    component: str,
    error_type: str,
    message: str,
    details: Optional[Dict[str, Any]] = None,
    exception: Optional[Exception] = None
) -> None:
    """
    Log an error with detailed information.
    
    Parameters:
        component: The application component where the error occurred
        error_type: Classification of the error
        message: The error message
        details: Optional dictionary of additional details
        exception: Optional exception object to extract traceback
    """
    detail_str = format_details(details)
    
    # Include exception details if available
    if exception:
        error_details = f" | Exception: {type(exception).__name__}: {str(exception)}"
        logger.error(f"[{component}] ERROR - {error_type}: {message}{error_details} {detail_str if detail_str else ''}")
        
        # Log the traceback as well
        tb_str = ''.join(traceback.format_exception(type(exception), exception, exception.__traceback__))
        logger.error(f"[{component}] Traceback: {tb_str}")
    else:
        logger.error(f"[{component}] ERROR - {error_type}: {message} {detail_str if detail_str else ''}")

def log_warning(
    component: str,
    warning_type: str,
    message: str,
    details: Optional[Dict[str, Any]] = None
) -> None:
    """
    Log a warning with detailed information.
    
    Parameters:
        component: The application component generating the warning
        warning_type: Classification of the warning
        message: The warning message
        details: Optional dictionary of additional details
    """
    detail_str = format_details(details)
    logger.warning(f"[{component}] WARNING - {warning_type}: {message} {detail_str if detail_str else ''}")

def log_api_call(
    api_name: str,
    endpoint: str,
    method: str = "GET",
    params: Optional[Dict[str, Any]] = None,
    success: bool = True,
    response_code: Optional[Union[int, str]] = None,
    duration_ms: Optional[float] = None,
    error_message: Optional[str] = None
) -> None:
    """
    Log an API call with request and response details.
    
    Parameters:
        api_name: Name of the API being called
        endpoint: The specific API endpoint
        method: HTTP method (GET, POST, etc.)
        params: Optional dictionary of request parameters (will be sanitized)
        success: Whether the call was successful
        response_code: HTTP status code or other response code
        duration_ms: Time taken for the call in milliseconds
        error_message: Error message if the call failed
    """
    details = {
        "method": method,
        "endpoint": endpoint,
        "success": success
    }
    
    if params:
        # Sanitize params to remove sensitive information
        sanitized_params = {**params}
        for key in sanitized_params:
            if any(sensitive in key.lower() for sensitive in ["key", "token", "secret", "pass", "auth"]):
                sanitized_params[key] = "********"
        details["params"] = sanitized_params
    
    if response_code is not None:
        details["response_code"] = response_code
    
    if duration_ms is not None:
        details["duration_ms"] = duration_ms
    
    if not success and error_message:
        details["error"] = error_message
        log_error("API", f"{api_name}Call", f"Failed API call to {endpoint}", details)
    else:
        log_event("API", f"API call to {api_name} - {endpoint}", details)

def log_transaction(
    tx_type: str,
    asset: str,
    amount: float,
    timestamp: datetime,
    price_eur: Optional[float] = None,
    fee_eur: Optional[float] = None,
    reference_id: Optional[str] = None,
    source: Optional[str] = None,
    details: Optional[Dict[str, Any]] = None
) -> None:
    """
    Log a cryptocurrency transaction.
    
    Parameters:
        tx_type: Transaction type (buy, sell, transfer, etc.)
        asset: The cryptocurrency asset
        amount: Amount of the asset involved
        timestamp: When the transaction occurred
        price_eur: Price in EUR (if applicable)
        fee_eur: Fee in EUR (if applicable)
        reference_id: Transaction ID or reference
        source: Source of the transaction (exchange, wallet, etc.)
        details: Additional transaction details
    """
    tx_details = {
        "type": tx_type,
        "asset": asset,
        "amount": amount,
        "timestamp": timestamp.isoformat()
    }
    
    if price_eur is not None:
        tx_details["price_eur"] = price_eur
    
    if fee_eur is not None:
        tx_details["fee_eur"] = fee_eur
    
    if reference_id:
        tx_details["reference_id"] = reference_id
    
    if source:
        tx_details["source"] = source
    
    if details:
        tx_details.update(details)
    
    log_event("Transaction", f"{tx_type.capitalize()} {amount} {asset}", tx_details)
