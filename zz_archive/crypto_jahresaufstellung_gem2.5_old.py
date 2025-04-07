# --- Main Processing Function ---
def process_for_tax(trades, ledger, year):
    # ... (setup: headers, events, sort, summaries, etc. as before) ...

    for event_index, event in enumerate(events):
        # ... (timestamp, year check, date_str, data, row_base init as before) ...

        if event["type"] == "trade":
            # ... (trade processing as before) ...
            if trade_type == "buy" and asset_quote_norm == "EUR":
                # ... (add to holdings as before) ...
                log_event("Buy Recorded (Trade)",
                          f"{date_str}, Asset: {asset}, Amount: {amount_traded:.8f}, Price: {price_per_unit:.4f}, Cost: {cost_or_proceeds:.2f}, Fee: {fee_paid:.4f}, Ref: {trade_refid}")
                # Log the added holding
                log_event("Buy Holding Added",
                          f"Asset: {asset}, Details: {json.dumps(buy_entry)}")
                # ... (populate row, add to tax_data, increment line_num, add to processed_refids) ...
                # ... (Attempt to link ledger spend - keep this logic) ...
            else:  # Log other trades
                # ... (logging as before) ...

        elif event["type"] == "ledger":
            # ... (get entry details: entry_type, asset_norm, amount_ledger, fee_paid, refid) ...
            # ... (skip processed, KFEE, Transfer/Margin as before) ...
            if entry_type == "Trade":
                log_event("Ledger Trade Detected",
                          f"Ref: {refid}, Asset: {asset_norm}, Amount: {amount_ledger}. Processing based on sign.")
            # ... (is_theft, notes as before) ...

            is_sale_or_trade_processed = False
            # Detect Sale/Trade (Spend/Trade non-EUR)
            if (entry_type == "Spend" or entry_type == "Trade") and amount_ledger < 0 and asset_norm != "EUR":
                # Log start
                log_event("Sale Detection Start",
                          f"Checking for sale/trade: Ref={refid}, Asset={asset_norm}, Amount={amount_ledger}, Time={timestamp}")
                corresponding_receive = None
                time_window_sale = 120  # Increased window
                for next_idx in range(event_index + 1, len(events)):
                    next_event = events[next_idx]
                    time_diff = next_event["time"] - timestamp
                    # --- Add Detailed Debugging for Match Attempt ---
                    log_event(
                        "Sale Detection Debug", f"  Checking against event {next_idx}: TimeDiff={time_diff:.2f}s, Type={next_event['type']}")
                    if time_diff > time_window_sale:
                        log_event(
                            "Sale Detection Debug", f"  -> Time window exceeded ({time_window_sale}s). Stopping search for {refid}.")
                        break  # Time window limit
                    if next_event["type"] == "ledger":
                        next_entry = next_event["data"]
                        next_refid = next_entry.get("refid")
                        next_type = next_entry.get("type", "").capitalize()
                        next_amount = float(next_entry.get("amount", -1))
                        log_event(
                            "Sale Detection Debug", f"  -> Ledger Candidate: Ref={next_refid}, Type={next_type}, Amount={next_amount}")
                        if next_refid == refid and next_type in ["Receive", "Trade"] and next_amount > 0:
                            corresponding_receive = next_entry
                            log_event("Sale Detection Debug",
                                      f"  -> MATCH FOUND for {refid}!")
                            break  # Found it
                    # --- End Detailed Debugging ---
                if not corresponding_receive and (entry_type == "Spend" or entry_type == "Trade"):
                    log_event("Sale Detection Result",
                              f"No corresponding receive found for spend {refid} ({asset_norm}, {amount_ledger}) within {time_window_sale}s.")

                if corresponding_receive:
                    # --- Process Sale / Trade ---
                    # ... (sale/trade setup as before: sell_asset, sell_amount, fees, proceeds, price_pu etc.) ...
                    log_event(f"FIFO Start ({'Sale' if not is_crypto_crypto else 'Trade'})",
                              # Log holdings before FIFO
                              f"Ref:{refid}, Selling {sell_amount:.8f} {sell_asset}. Holdings BEFORE: {json.dumps(HOLDINGS.get(sell_asset, []))}")

                    # --- Apply FIFO ---
                    # ... (FIFO logic as before) ...
                       # Inside the FIFO loop, after consuming a lot:
                       # Log which lot was used
                       log_event(
                           "FIFO Applied", f"Ref: {refid}, Used {amount_to_use:.8f} from lot bought {datetime.fromtimestamp(buy_time, timezone.utc).strftime('%Y-%m-%d')} (Ref:{buy_ref}), Cost Basis Part: {cost_basis_part:.2f} EUR")

                    # After the FIFO loop:
                    log_event(f"FIFO End ({'Sale' if not is_crypto_crypto else 'Trade'})",
                              # Log holdings after FIFO
                              f"Ref:{refid}, Sold {sell_amount:.8f} {sell_asset}. Holdings AFTER: {json.dumps(HOLDINGS.get(sell_asset, []))}")

                    # ... (Calculate P/L, holding period, summaries as before) ...
                    # ... (Populate Sheet Row as before) ...
                    # Ensure buy_ts (earliest_buy_timestamp) is logged or visible
                    log_event(
                        "Sale/Trade Row Added", f"Ref: {refid}, Asset: {sell_asset}, Qty: {sell_amount:.8f}, BuyDateUsed: {datetime.fromtimestamp(buy_ts, timezone.utc).strftime('%Y-%m-%d %H:%M:%S') if buy_ts < timestamp else 'N/A'}, P/L: {gain_loss:.2f}, Holding: {round(hold_days)}")
                    # ... (rest of sale processing: tax_data.append, line_num, processed_refids)

            # Unmatched Spend/Trade
            elif not is_sale_or_trade_processed and (entry_type == "Spend" or entry_type == "Trade") and amount_ledger < 0:
                # ... (EUR Spend logic as before) ...
                if asset_norm != "EUR":  # Withdrawal
                    log_event(
                        "Withdrawal Detected", f"{date_str}, Asset:{asset_norm}, Amt:{abs(amount_ledger):.8f}, Ref:{refid}. Reducing holdings.")
                    log_event("Withdrawal FIFO Start",
                              f"Ref:{refid}, Withdrawing {abs(amount_ledger):.8f} {asset_norm}. Holdings BEFORE: {json.dumps(HOLDINGS.get(asset_norm, []))}")
                    # --- Apply FIFO for withdrawal ---
                    # ... (FIFO logic for withdrawal as before) ...
                        # Inside withdrawal FIFO loop:
                        log_event(
                            "Withdrawal FIFO Applied", f"Ref: {refid}, Removed {amount_to_use:.8f} from lot bought {datetime.fromtimestamp(buy_lot['timestamp'], timezone.utc).strftime('%Y-%m-%d')} (Ref:{buy_lot.get('refid', 'N/A')})")
                    # After withdrawal FIFO loop:
                    log_event(
                        "Withdrawal FIFO End", f"Ref:{refid}, Withdrew {abs(amount_ledger):.8f} {asset_norm}. Holdings AFTER: {json.dumps(HOLDINGS.get(asset_norm, []))}")
                    # ... (Populate withdrawal row as before) ...

            # Unmatched Receive/Pos-Trade
            elif (entry_type == "Receive" or (entry_type == "Trade" and amount_ledger > 0)) and asset_norm != "EUR":
                if refid not in processed_refids:
                    log_event("Deposit/Receive/Pos-Trade Detected",
                              f"{date_str}, Asset:{asset_norm}, Amt:{amount_ledger:.8f}, Ref:{refid}. Assuming 0 cost basis.")
                    deposit_entry = {"amount": amount_ledger, "price_eur": 0,
                        "timestamp": timestamp, "fee_eur": fee_paid, "refid": refid}
                    HOLDINGS.setdefault(asset_norm, []).append(deposit_entry)
                    HOLDINGS[asset_norm].sort(key=lambda x: x["timestamp"])
                    # Log added deposit
                    log_event("Deposit Holding Added",
                              f"Asset: {asset_norm}, Details: {json.dumps(deposit_entry)}")
                    # ... (Populate deposit row as before) ...

            # ... (Rest of ledger handling: EUR Deposit, Unhandled types) ...

    # ... (Final Calcs & Summary generation as before) ...
    return tax_data

# --- Sheet Writing Functions ---
# Use the corrected write_to_sheets from the previous response


def write_to_sheets(data, year):
    """Writes the processed tax data to a Google Sheet for the given year."""
    if len(data) <= 1:
        log_event("Sheet Write Skip", f"No data rows for {year}.")
        print(f"Skipping sheet generation for {year}: No data.")
        return

    sheet_name = f"Steuer {year}"
    try:
        sheet_name, sheet_id = get_or_create_sheet(SHEET_ID, sheet_name)
    except Exception as e:
        log_event("Sheet Write Error",
                  f"Failed get/create sheet '{sheet_name}': {e}")
        print(f"ERROR: Could not get/create sheet '{sheet_name}'.")
        return  # Cannot proceed

    num_rows, num_cols = len(data), len(HEADERS)
    header_values = [str(h) for h in data[0]]
    data_rows_values = []
    for r, row_data in enumerate(data[1:], start=1):
        current_row_vals = []
        is_sum = row_data[1] == "Summe"
        is_summary = row_data[1] in [
            "--- Steuerliche Zusammenfassung ---", "GESAMT", "INFO", ""] or r >= num_rows-10
        for c, cell_val in enumerate(row_data):
            if c == 12 and not is_sum and not is_summary:  # Gewinn/Verlust Formula
                k, j, l = row_data[10], row_data[9], row_data[11]
                formula_ok = False
                try:
                    if k in [None, ""] or isinstance(k, (int, float)) or (isinstance(k, str) and k.replace('.', '', 1).replace(',', '', 1).replace('-', '', 1).isdigit()):
                        if j in [None, ""] or isinstance(j, (int, float)) or (isinstance(j, str) and j.replace('.', '', 1).replace(',', '', 1).replace('-', '', 1).isdigit()):
                            if l in [None, ""] or isinstance(l, (int, float)) or (isinstance(l, str) and l.replace('.', '', 1).replace(',', '', 1).replace('-', '', 1).isdigit()):
                                formula_ok = True
                except Exception: pass
                if formula_ok:
                    formula = f"=IF(ISBLANK(K{r+1}),\"\",ROUND(IFERROR(K{r+1},0)-IFERROR(J{r+1},0)-IFERROR(L{r+1},0),2))"
                    current_row_vals.append(formula)
                else:
                    current_row_vals.append(
                        cell_val if cell_val is not None else "")
            elif is_sum and isinstance(cell_val, str) and cell_val.startswith("=SUM("):
                current_row_vals.append(cell_val)
            else:  # Standard values
                if isinstance(cell_val, float) and (cell_val != cell_val or cell_val == float('inf') or cell_val == float('-inf')):
                    current_row_vals.append("Error")
                else: current_row_vals.append(cell_val if isinstance(cell_val, (int, float)) else (str(cell_val) if cell_val is not None else ""))
        data_rows_values.append(current_row_vals)
    try:  # Perform Sheet Updates
        log_event("Sheet Clear", f"Clearing sheet: {sheet_name}")
        service.spreadsheets().values().clear(spreadsheetId=SHEET_ID,
                             range=sheet_name).execute(); time.sleep(1)
        header_body = {"range": f"{sheet_name}!A1",
            "majorDimension": "ROWS", "values": [header_values]}
        data_body = {"range": f"{sheet_name}!A2",
            "majorDimension": "ROWS", "values": data_rows_values}
        log_event("Sheet Update", f"Updating {sheet_name} header.")
        service.spreadsheets().values().update(spreadsheetId=SHEET_ID,
                             range=header_body["range"], valueInputOption="USER_ENTERED", body=header_body).execute()
        log_event("Sheet Update",
                  f"Updating {sheet_name} data ({len(data_rows_values)} rows).")
        service.spreadsheets().values().update(spreadsheetId=SHEET_ID,
                             range=data_body["range"], valueInputOption="USER_ENTERED", body=data_body).execute()
        log_event("Sheet Update Success", f"Data updated for {sheet_name}")
        time.sleep(1)

        # --- Apply Formatting ---
        # (Formatting requests remain the same)
        formatting_requests = [
            {"repeatCell": {"range": {"sheetId": sheet_id, "startRowIndex": 0, "endRowIndex": 1}, "cell": {
                "userEnteredFormat": {"textFormat": {"bold": True}}}, "fields": "userEnteredFormat.textFormat.bold"}},
            {"repeatCell": {"range": {"sheetId": sheet_id, "startRowIndex": 1, "startColumnIndex": 6, "endColumnIndex": 7}, "cell": {
                "userEnteredFormat": {"numberFormat": {"type": "NUMBER", "pattern": "#,##0.00 €"}}}, "fields": "userEnteredFormat.numberFormat"}},
            {"repeatCell": {"range": {"sheetId": sheet_id, "startRowIndex": 1, "startColumnIndex": 8, "endColumnIndex": 9}, "cell": {
                "userEnteredFormat": {"numberFormat": {"type": "NUMBER", "pattern": "#,##0.00 €"}}}, "fields": "userEnteredFormat.numberFormat"}},
            {"repeatCell": {"range": {"sheetId": sheet_id, "startRowIndex": 1, "startColumnIndex": 9, "endColumnIndex": 13}, "cell": {
                "userEnteredFormat": {"numberFormat": {"type": "NUMBER", "pattern": "#,##0.00 €"}}}, "fields": "userEnteredFormat.numberFormat"}},
            {"repeatCell": {"range": {"sheetId": sheet_id, "startRowIndex": 1, "startColumnIndex": 4, "endColumnIndex": 5}, "cell": {
                "userEnteredFormat": {"numberFormat": {"type": "NUMBER", "pattern": "#,##0.00######"}}}, "fields": "userEnteredFormat.numberFormat"}},
            {"repeatCell": {"range": {"sheetId": sheet_id, "startRowIndex": 1, "startColumnIndex": 13, "endColumnIndex": 14}, "cell": {
                "userEnteredFormat": {"numberFormat": {"type": "NUMBER", "pattern": "0"}}}, "fields": "userEnteredFormat.numberFormat"}},
            {"repeatCell": {"range": {"sheetId": sheet_id, "startRowIndex": 1, "startColumnIndex": 2, "endColumnIndex": 3}, "cell": {
                "userEnteredFormat": {"numberFormat": {"type": "DATE_TIME", "pattern": "yyyy-mm-dd hh:mm:ss"}}}, "fields": "userEnteredFormat.numberFormat"}},
            {"repeatCell": {"range": {"sheetId": sheet_id, "startRowIndex": 1, "startColumnIndex": 5, "endColumnIndex": 6}, "cell": {
                "userEnteredFormat": {"numberFormat": {"type": "DATE_TIME", "pattern": "yyyy-mm-dd hh:mm:ss"}}}, "fields": "userEnteredFormat.numberFormat"}},
            {"repeatCell": {"range": {"sheetId": sheet_id, "startRowIndex": 1, "startColumnIndex": 7, "endColumnIndex": 8}, "cell": {
                "userEnteredFormat": {"numberFormat": {"type": "DATE_TIME", "pattern": "yyyy-mm-dd hh:mm:ss"}}}, "fields": "userEnteredFormat.numberFormat"}},
            {"addConditionalFormatRule": {"rule": {"ranges": [{"sheetId": sheet_id, "startRowIndex": 1, "endRowIndex": num_rows, "startColumnIndex": 14, "endColumnIndex": 15}], "booleanRule": {
                "condition": {"type": "TEXT_EQ", "values": [{"userEnteredValue": "Ja"}]}, "format": {"backgroundColor": {"red": 1.0, "green": 0.85, "blue": 0.85}}}}, "index": 0}},
            {"addConditionalFormatRule": {"rule": {"ranges": [{"sheetId": sheet_id, "startRowIndex": 1, "endRowIndex": num_rows, "startColumnIndex": 14, "endColumnIndex": 15}], "booleanRule": {
                "condition": {"type": "TEXT_EQ", "values": [{"userEnteredValue": "Nein"}]}, "format": {"backgroundColor": {"red": 0.85, "green": 1.0, "blue": 0.85}}}}, "index": 1}},
            {"addConditionalFormatRule": {"rule": {"ranges": [{"sheetId": sheet_id, "startRowIndex": 1, "endRowIndex": num_rows, "startColumnIndex": 12, "endColumnIndex": 13}], "booleanRule": {
                "condition": {"type": "NUMBER_GREATER", "values": [{"userEnteredValue": "0"}]}, "format": {"textFormat": {"foregroundColor": {"red": 0.0, "green": 0.5, "blue": 0.0}}}}}, "index": 2}},
            {"addConditionalFormatRule": {"rule": {"ranges": [{"sheetId": sheet_id, "startRowIndex": 1, "endRowIndex": num_rows, "startColumnIndex": 12, "endColumnIndex": 13}], "booleanRule": {
                "condition": {"type": "NUMBER_LESS", "values": [{"userEnteredValue": "0"}]}, "format": {"textFormat": {"foregroundColor": {"red": 0.7, "green": 0.0, "blue": 0.0}}}}}, "index": 3}},
            {"repeatCell": {"range": {"sheetId": sheet_id, "startColumnIndex": 15, "endColumnIndex": 16, "startRowIndex": 1},
                "cell": {"userEnteredFormat": {"wrapStrategy": "WRAP"}}, "fields": "userEnteredFormat.wrapStrategy"}},
        ]
        summe_row_idx, summary_start_idx = -1, -1
        for i, r in enumerate(data):
            if len(r) > 1 and r[1] == "Summe":
                summe_row_idx = i
            if len(r) > 1 and r[1] == "--- Steuerliche Zusammenfassung ---": summary_start_idx = i - 1; break
        if summe_row_idx != -1:
            formatting_requests.insert(1, {"repeatCell": {"range": {"sheetId": sheet_id, "startRowIndex": summe_row_idx, "endRowIndex": summe_row_idx + 1}, "cell": {
                                       "userEnteredFormat": {"textFormat": {"bold": True}}}, "fields": "userEnteredFormat.textFormat.bold"}})
        if summary_start_idx != -1: formatting_requests.insert(1, {"repeatCell": {"range": {"sheetId": sheet_id, "startRowIndex": summary_start_idx, "endRowIndex": num_rows}, "cell": {
                                                               "userEnteredFormat": {"backgroundColor": {"red": 0.92, "green": 0.92, "blue": 0.92}}}, "fields": "userEnteredFormat.backgroundColor"}})
        formatting_requests.append({"autoResizeDimensions": {"dimensions": {
                                   "sheetId": sheet_id, "dimension": "COLUMNS", "startIndex": 0, "endIndex": num_cols}}})
        if formatting_requests:
            log_event("Sheet Formatting",
                      f"Applying {len(formatting_requests)} rules to {sheet_name}")
            service.spreadsheets().batchUpdate(spreadsheetId=SHEET_ID, body={
                                 "requests": formatting_requests}).execute()
            log_event("Sheet Formatting Success",
                      f"Applied rules to {sheet_name}")
    except HttpError as e:
        error_details = str(e)
        try:
            error_details = json.loads(e.content.decode('utf-8'))
        except Exception: pass  # Keep original error string
        log_event("Sheet Write/Format Error",
                  f"Sheet: {sheet_name}, Error: {error_details}")
        print(
            f"ERROR writing/formatting sheet '{sheet_name}': {error_details}")
    except Exception as e:
        log_event("Sheet Write/Format Error",
                  f"Sheet: {sheet_name}, Unexpected Error: {e}\n{traceback.format_exc()}")
        print(
            f"ERROR writing/formatting sheet '{sheet_name}': Unexpected error {e}")

# --- Utility Functions ---


def get_or_create_sheet(spreadsheet_id, sheet_name):
    try:
        spreadsheet = service.spreadsheets().get(
            spreadsheetId=spreadsheet_id).execute()
        sheets = spreadsheet.get('sheets', [])
        existing_sheet = next(
            (s for s in sheets if s['properties']['title'] == sheet_name), None)
        if existing_sheet:
            log_event(
                "Sheet Found", f"Using sheet: '{sheet_name}' (ID: {existing_sheet['properties']['sheetId']})")
            return sheet_name, existing_sheet['properties']['sheetId']
        else:
            log_event("Sheet Creation", f"Creating sheet: '{sheet_name}'")
            req = {"addSheet": {"properties": {"title": sheet_name, "gridProperties": {
                "rowCount": 2000, "columnCount": len(HEADERS)}}}}
            res = service.spreadsheets().batchUpdate(
                spreadsheetId=spreadsheet_id, body={"requests": [req]}).execute()
            sheet_id = res['replies'][0]['addSheet']['properties']['sheetId']
            log_event("Sheet Created",
                      f"New sheet '{sheet_name}' ID: {sheet_id}")
            return sheet_name, sheet_id
    except HttpError as e:
        log_event("Sheet Discovery Error",
                  f"Failed get/create sheet '{sheet_name}'. Error: {e}")
        print(f"ERROR: Could not get or create sheet '{sheet_name}'.")
        raise


def write_log_sheet():
    sheet_name = "Log"
    log_file_path = Path(__file__).parent / "tax_script_error.log"
    if not LOG_DATA or len(LOG_DATA) <= 1:
        print("No log entries.")
        return
    try:
        sheet_id = None
        try:
            sheet_name, sheet_id = get_or_create_sheet(SHEET_ID, sheet_name)
        except Exception as sheet_err:
            print(
                f"WARNING: Log sheet error: {sheet_err}. Logging locally only.")
        if sheet_id:
            log_values = [[str(item) for item in row] for row in LOG_DATA]
            body = {"values": log_values}
            service.spreadsheets().values().clear(spreadsheetId=SHEET_ID,
                                 range=f"{sheet_name}!A1:C").execute()
            service.spreadsheets().values().update(spreadsheetId=SHEET_ID,
                                 range=f"{sheet_name}!A1", valueInputOption="USER_ENTERED", body=body).execute()
            fmt_reqs = [
                {"autoResizeDimensions": {"dimensions": {"sheetId": sheet_id,
                    "dimension": "COLUMNS", "startIndex": 0, "endIndex": 3}}},
                {"repeatCell": {"range": {"sheetId": sheet_id, "startColumnIndex": 2, "endColumnIndex": 3, "startRowIndex": 0}, "cell": {
                    "userEnteredFormat": {"wrapStrategy": "WRAP"}}, "fields": "userEnteredFormat.wrapStrategy"}}
            ]
            service.spreadsheets().batchUpdate(spreadsheetId=SHEET_ID,
                                 body={"requests": fmt_reqs}).execute()
            print(
                f"Log written to Google Sheet: https://docs.google.com/spreadsheets/d/{SHEET_ID}/edit#gid={sheet_id}")
    except Exception as e:
        print(f"CRITICAL: Error during log sheet processing: {e}")
        print(
            f"Attempting to write log to local file '{log_file_path}' as fallback.")
        try:
            with open(log_file_path, "w", encoding='utf-8') as f:
                 f.write("Timestamp\tEvent\tDetails\n")
                 for entry in LOG_DATA:
                     f.write("\t".join(map(str, entry)) + "\n")
                 f.write(f"--- ERROR DURING LOG SHEET PROCESSING: {e} ---\n")
             print(f"Log data saved to {log_file_path}")
        except Exception as log_e:
            print(f"CRITICAL: Could not write log to file: {log_e}")

# --- Main Execution Block ---

def main():
    global HOLDINGS
    run_start_time = datetime.now()
    log_event("Script Started",
              f"Execution Time: {run_start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Script started at {run_start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    date_range_msg = f" (Using range: {START_DATE_STR or 'Default'} to {END_DATE_STR or 'Present'})"
    print(f"Fetching data from Kraken{date_range_msg}...")
    all_trades, all_ledger = [], []
    try:
        print("Fetching trades...")
        all_trades = get_trades(START_TIMESTAMP, END_TIMESTAMP)
        print(f"Retrieved {len(all_trades)} trades.")
        print("Fetching ledger...")
        all_ledger = get_ledger(START_TIMESTAMP, END_TIMESTAMP)
        print(f"Retrieved {len(all_ledger)} entries.")

        # --- Write Raw Data Sheet ---
        if all_trades or all_ledger:
            print("Writing raw data sheet...")
            write_raw_data_sheet(all_trades, all_ledger)
        else:
            print("\nNo data found.")
            log_event("Abort", "No API data.")
            return  # Log writing happens in finally

        print("Grouping by year...")
        data_by_year = group_by_year(all_trades, all_ledger)
        if not data_by_year:
            print("No data found for any year.")
            log_event("Abort", "No processable years.")
            return  # Log writing happens in finally

        print(f"Found data for years: {sorted(list(data_by_year.keys()))}")
        all_years = sorted(list(data_by_year.keys()))
        HOLDINGS = {}
        log_event("Global Holdings Reset", "Resetting holdings.")

        for year in all_years:
            year_trades, year_ledger = data_by_year[year]
            print(f"\n--- Processing Year {year} ---")
            log_event(
                f"Year Start {year}", f"Input: {len(year_trades)} trades, {len(year_ledger)} entries.")
            print(f"Processing FIFO...")
            tax_data = process_for_tax(year_trades, year_ledger, year)
            meaningful_rows = [r for i, r in enumerate(tax_data) if i > 0 and len(r) >1 and r[1] not in ["Summe", "--- Steuerliche Zusammenfassung ---", "GESAMT", "INFO", ""]]
            if not meaningful_rows:
                print(f"No significant transaction rows generated for {year}.")
                log_event(f"Year Skip {year}", "No data rows generated.")
                continue
            print(f"Writing report for {year}...")
            write_to_sheets(tax_data, year)
            print(f"Sheet written for {year}.")
            time.sleep(2)  # Pause
        print("\nProcessing complete.")
    except Exception as e:
        print(f"\n--- SCRIPT ERROR ---")
        print(f"Error: {e}")
        print("Traceback:")
        traceback.print_exc()
        log_event("CRITICAL ERROR", f"{e}\n{traceback.format_exc()}")
        print("\nPlease check logs.")
    finally:
        run_end_time = datetime.now()
        duration = run_end_time - run_start_time
        log_event("Script Finished",
                  f"Execution Time: {run_end_time.strftime('%Y-%m-%d %H:%M:%S')}, Duration: {duration}")
        print(
            f"\nScript finished at {run_end_time.strftime('%Y-%m-%d %H:%M:%S')} (Duration: {duration}).")
        print("Writing final logs...")
        write_log_sheet()


if __name__ == "__main__":
    main()
