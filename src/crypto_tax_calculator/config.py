# crypto_tax_calculator/config.py

"""
Handles loading and validation of application configuration.
Prioritizes environment variables for sensitive data (API keys).
Can also load non-sensitive settings from a config file (e.g., config.json).
"""

import json
import os
from pathlib import Path
from typing import List, Dict, Optional, Any
from dotenv import load_dotenv

# Placeholder for logging function
def log_event(event: str, details: str):
    print(f"[LOG] {event}: {details}")

# --- Configuration Loading ---

def load_configuration(config_file_name: str = "config.json") -> Dict[str, Any]:
    """
    Loads configuration from environment variables and an optional JSON file.
    Environment variables override JSON file settings for specific keys.
    """
    config = {}
    project_root = Path(__file__).resolve().parent.parent.parent # Should be skripts-py/accounting/

    # 1. Load from .env file (for environment variables)
    env_path = project_root / '.env'
    if env_path.exists():
        load_dotenv(dotenv_path=env_path)
        log_event("Config", f"Loaded environment variables from {env_path}")
    else:
        log_event("Config", f".env file not found at {env_path}. Relying on system environment variables.")

    # 2. Load base configuration from JSON file (optional, for non-sensitive defaults)
    config_file_path = project_root / config_file_name
    if config_file_path.exists():
        try:
            with open(config_file_path, "r", encoding="utf-8") as f:
                config = json.load(f)
            log_event("Config", f"Loaded base configuration from {config_file_path}")
        except (json.JSONDecodeError, IOError) as e:
            log_event("Config Error", f"Failed to load or parse {config_file_path}: {e}")
            print(f"[WARN] Could not load base configuration file {config_file_path}. Proceeding without it.")
            config = {} # Start with empty config if file is invalid
    else:
        log_event("Config", f"Base configuration file {config_file_path} not found. Relying on environment variables.")

    # 3. Override/Add settings from Environment Variables (prioritized)
    # Sensitive keys
    kraken_api_key = os.getenv("KRAKEN_API_KEY")
    kraken_api_secret = os.getenv("KRAKEN_API_SECRET")
    google_creds_path_env = os.getenv("GOOGLE_CREDENTIALS_FILE") # Path to JSON creds file
    google_sheet_id_env = os.getenv("GOOGLE_SHEET_ID")

    if kraken_api_key:
        config["KRAKEN_API_KEY"] = kraken_api_key
        log_event("Config", "Loaded KRAKEN_API_KEY from environment.")
    if kraken_api_secret:
        config["KRAKEN_API_SECRET"] = kraken_api_secret
        log_event("Config", "Loaded KRAKEN_API_SECRET from environment.")

    # Google Sheets specific config structure
    if "google_sheets" not in config:
        config["google_sheets"] = {}

    if google_creds_path_env:
        # Store the absolute path if found via env var
        abs_creds_path = project_root / google_creds_path_env
        if abs_creds_path.exists():
             config["google_sheets"]["credentials_file"] = str(abs_creds_path)
             log_event("Config", f"Loaded Google credentials path from environment: {abs_creds_path}")
        else:
             log_event("Config Warning", f"GOOGLE_CREDENTIALS_FILE env var points to non-existent file: {abs_creds_path}")
             # Keep potential path from config.json if it exists
             if "credentials_file" not in config["google_sheets"]:
                  config["google_sheets"]["credentials_file"] = None # Explicitly set to None if not found
    elif "credentials_file" in config["google_sheets"]:
         # Resolve path relative to project root if loaded from config.json
         rel_path = config["google_sheets"]["credentials_file"]
         abs_path = project_root / rel_path
         if abs_path.exists():
              config["google_sheets"]["credentials_file"] = str(abs_path)
         else:
              log_event("Config Warning", f"Google credentials file from config.json not found: {abs_path}")
              config["google_sheets"]["credentials_file"] = None


    if google_sheet_id_env:
        config["google_sheets"]["sheet_id"] = google_sheet_id_env
        log_event("Config", "Loaded GOOGLE_SHEET_ID from environment.")

    # Add other environment variable overrides as needed (e.g., COINGECKO_API_KEY if using pro plan)

    # 4. Validate configuration
    is_valid, errors = validate_config(config)
    if not is_valid:
        log_event("Config Error", f"Configuration validation failed: {'; '.join(errors)}")
        # Decide whether to raise an error or allow proceeding with warnings
        # For critical items like API keys, raising an error is safer.
        raise ValueError(f"Configuration errors: {'; '.join(errors)}")
    else:
        log_event("Config", "Configuration loaded and validated successfully.")

    return config

# --- Configuration Validation ---

def validate_config(config: Dict[str, Any]) -> tuple[bool, List[str]]:
    """Validates the loaded configuration dictionary."""
    errors = []
    is_valid = True

    # Check for essential Kraken keys
    if not config.get("KRAKEN_API_KEY"):
        errors.append("Missing KRAKEN_API_KEY (set via environment variable KRAKEN_API_KEY)")
        is_valid = False
    if not config.get("KRAKEN_API_SECRET"):
        errors.append("Missing KRAKEN_API_SECRET (set via environment variable KRAKEN_API_SECRET)")
        is_valid = False

    # Check Google Sheets config if present
    gs_config = config.get("google_sheets", {})
    gs_sheet_id = gs_config.get("sheet_id")
    gs_creds_file = gs_config.get("credentials_file")

    # If either GS key is set, the other should ideally be set too (unless export is disabled)
    if gs_sheet_id or gs_creds_file:
        if not gs_sheet_id:
            # Allow proceeding without sheet ID, but log warning (export will be skipped)
             log_event("Config Warning", "Google credentials file provided, but GOOGLE_SHEET_ID is missing. Sheets export will be skipped.")
        if not gs_creds_file:
            # Allow proceeding without creds file, but log warning (export will be skipped)
             log_event("Config Warning", "GOOGLE_SHEET_ID provided, but Google credentials file is missing or not found. Sheets export will be skipped.")
        elif not Path(gs_creds_file).exists():
             errors.append(f"Google credentials file specified but not found at: {gs_creds_file}")
             is_valid = False # Credentials file missing is an error if specified

    # Add more validation rules as needed (e.g., date formats if specified in config)

    return is_valid, errors


# Example usage
if __name__ == "__main__":
    print("Testing Config module...")
    try:
        # Assumes .env and optionally config.json exist in skripts-py/accounting/
        loaded_config = load_configuration()
        print("\nLoaded Configuration:")
        # Avoid printing secrets directly
        for key, value in loaded_config.items():
            if "SECRET" in key.upper() or "KEY" in key.upper():
                print(f"  {key}: ****** (loaded)")
            elif key == "google_sheets":
                 print(f"  google_sheets:")
                 gs_conf = value or {}
                 print(f"    sheet_id: {gs_conf.get('sheet_id', 'Not Set')}")
                 print(f"    credentials_file: {gs_conf.get('credentials_file', 'Not Set')}")
            else:
                print(f"  {key}: {value}")

    except ValueError as e:
        print(f"\nConfiguration Error: {e}")
    except Exception as e:
        print(f"\nAn unexpected error occurred: {e}")
        import traceback
        traceback.print_exc()

    print("\nTest complete.")
