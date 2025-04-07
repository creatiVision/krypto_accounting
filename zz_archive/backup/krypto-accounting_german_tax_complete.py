# Determine base and quote asset from pair
            base_asset = trade_data.get("pair", "").split("/")[0] if "/" in trade_data.get("pair", "") else trade_data.get("asset", "")
            quote_asset = trade_data.get("pair", "").split("/")[1] if "/" in trade_data.get("pair", "") else "EUR"
            
            # Buy trades add to holdings
            if type_.lower() == "buy":
                # Add to our holdings for FIFO tracking
                HOLDINGS.setdefault(base_asset, []).append({
                    "amount": abs(amount),
                    "price_eur": price if quote_asset == "EUR" else get_market_price(base_asset, timestamp),
                    "timestamp": timestamp,
                    "refid": refid,
                    "year": year
                })
                
                row_base[1] = "Kauf"
                row_base[2] = determine_tax_category("buy", base_asset)
                row_base[3] = date_str
                row_base[4] = base_asset
                row_base[5] = abs(amount)
                row_base[6] = date_str  # Purchase date
                row_base[7] = price if quote_asset == "EUR" else get_market_price(base_asset, timestamp)
                row_base[8] = "N/A"
                row_base[9] = 0.0
                row_base[10] = cost  # Cost in EUR
                row_base[11] = 0.0
                row_base[12] = fee
                row_base[13] = 0.0  # No gain/loss for purchases
                row_base[14] = 0  # No holding period for purchases
                row_base[15] = "N/A"  # Not applicable for purchases
                row_base[16] = ""  # No special tax treatment for standard purchases
                row_base[17] = f"Trade Ref: {refid} | Kaufpreis: {price:.2f} €/Stück"
                
                # Assign transaction ID
                tx_id = assign_transaction_id("BUY", timestamp, base_asset, amount)
                row_base[17] += f" | ID: {tx_id}"
                
                tax_data.append(row_base)
                line_num += 1
                processed_refids.add(refid)
                
            # Sell trades need to be matched against holdings using FIFO
            elif type_.lower() == "sell":
                sell_amount = abs(amount)
                sell_price_eur = price if quote_asset == "EUR" else get_market_price(base_asset, timestamp)
                sell_proceeds = sell_amount * sell_price_eur
                remaining_to_sell = sell_amount
                
                # FIFO: Find matching purchase lots
                matched_lots = []
                total_cost = 0.0
                
                if base_asset in HOLDINGS and HOLDINGS[base_asset]:
                    # Sort holdings by timestamp (oldest first) for FIFO
                    HOLDINGS[base_asset].sort(key=lambda x: x["timestamp"])
                    
                    # Match against available lots
                    lots_to_remove = []
                    
                    for idx, lot in enumerate(HOLDINGS[base_asset]):
                        if remaining_to_sell <= 0:
                            break
                            
                        lot_amount = lot["amount"]
                        
                        if lot_amount <= remaining_to_sell:
                            # Use the entire lot
                            matched_amount = lot_amount
                            remaining_to_sell -= lot_amount
                            lots_to_remove.append(idx)
                        else:
                            # Use partial lot
                            matched_amount = remaining_to_sell
                            # Update the lot with remaining amount
                            HOLDINGS[base_asset][idx]["amount"] -= matched_amount
                            remaining_to_sell = 0
                        
                        # Calculate cost basis for this portion
                        lot_cost = matched_amount * lot["price_eur"]
                        total_cost += lot_cost
                        
                        # Calculate holding period
                        holding_days = (datetime.fromtimestamp(timestamp, timezone.utc) - 
                                       datetime.fromtimestamp(lot["timestamp"], timezone.utc)).days
                        
                        # Add matched lot details for documentation
                        matched_lots.append({
                            "amount": matched_amount,
                            "purchase_date": datetime.fromtimestamp(lot["timestamp"], timezone.utc).strftime("%Y-%m-%d"),
                            "purchase_price": lot["price_eur"],
                            "holding_period": holding_days,
                            "cost_basis": lot_cost,
                            "refid": lot["refid"]
                        })
                    
                    # Remove fully used lots (in reverse order to avoid index issues)
                    for idx in sorted(lots_to_remove, reverse=True):
                        del HOLDINGS[base_asset][idx]
                
                # Calculate gain or loss
                gain_loss = sell_proceeds - total_cost - fee
                
                # Determine tax status based on holding period
                is_taxable = all(lot["holding_period"] <= HOLDING_PERIOD_DAYS for lot in matched_lots) if matched_lots else True
                
                # Create row for the sale
                row_base[1] = "Verkauf"
                row_base[2] = determine_tax_category("sell", base_asset)
                row_base[3] = date_str
                row_base[4] = base_asset
                row_base[5] = sell_amount
                
                # Use the earliest purchase date for FIFO
                earliest_purchase = min([lot["purchase_date"] for lot in matched_lots]) if matched_lots else "Unknown"
                row_base[6] = earliest_purchase
                
                # Average purchase price
                avg_purchase_price = total_cost / sell_amount if sell_amount > 0 else 0
                row_base[7] = avg_purchase_price
                
                row_base[8] = date_str  # Sale date
                row_base[9] = sell_price_eur
                row_base[10] = total_cost
                row_base[11] = sell_proceeds
                row_base[12] = fee
                row_base[13] = gain_loss
                
                # Average holding period
                avg_holding_period = int(sum(lot["holding_period"] for lot in matched_lots) / len(matched_lots)) if matched_lots else 0
                row_base[14] = avg_holding_period
                
                # Tax status
                row_base[15] = "Ja" if is_taxable and gain_loss > 0 else "Nein"
                
                # Check for special tax treatment warnings
                row_base[16] = check_special_tax_treatment(row_base)
                
                # FIFO details for documentation
                fifo_details = " | ".join([
                    f"Lot {i+1}: {lot['amount']:.8f} gekauft am {lot['purchase_date']} für {lot['purchase_price']:.2f} €/Stk"
                    for i, lot in enumerate(matched_lots)
                ])
                
                row_base[17] = f"Trade Ref: {refid} | {fifo_details}"
                
                # Assign transaction ID
                tx_id = assign_transaction_id("SELL", timestamp, base_asset, amount)
                row_base[17] += f" | ID: {tx_id}"
                
                tax_data.append(row_base)
                line_num += 1
                processed_refids.add(refid)
        
        # Handle other crypto-related events from ledger entries (deposits, withdrawals, staking, etc.)
        elif event["type"] == "ledger" and data.get("asset") not in ["EUR", "ZEUR"]:
            entry_type = data.get("type", "").lower()
            asset = data.get("asset", "")
            amount = float(data.get("amount", 0))
            fee = float(data.get("fee", 0))
            refid = data.get("refid", f"ledger_{timestamp}")
            
            if refid in processed_refids:
                continue
                
            # Get current market price for the asset
            price_eur = get_market_price(asset, timestamp)
            eur_value = abs(amount) * price_eur
            
            # Determine transaction type and tax category
            if entry_type in ["deposit", "withdrawal"]:
                tx_type = "Einzahlung" if entry_type == "deposit" else "Auszahlung"
                tax_category = "Nicht steuerpflichtig"  # Generally not taxable events
                
                row_base[1] = f"{asset} {tx_type}"
                row_base[2] = tax_category
                row_base[3] = date_str
                row_base[4] = asset
                row_base[5] = abs(amount)
                row_base[6] = "N/A"
                row_base[7] = price_eur  # Current price for reference
                row_base[8] = "N/A"
                row_base[9] = 0.0
                row_base[10] = 0.0
                row_base[11] = 0.0
                row_base[12] = fee
                row_base[13] = 0.0
                row_base[14] = 0
                row_base[15] = "Nein"
                row_base[16] = ""
                row_base[17] = f"Ledger Ref: {refid} | {tx_type} von/zu externem Wallet"
                
            # Staking, rewards, and similar income
            elif entry_type in ["staking", "reward", "bonus", "payment"]:
                # Determine more specific tax category
                if "staking" in entry_type or "stake" in entry_type:
                    category = "staking"
                    tax_category = TAX_CATEGORY["STAKING"]
                elif "mining" in entry_type or "mined" in entry_type:
                    category = "mining"
                    tax_category = TAX_CATEGORY["MINING"]
                elif entry_type in ["airdrop", "drop"]:
                    category = "airdrop"
                    tax_category = TAX_CATEGORY["AIRDROP"]
                else:
                    category = "reward"
                    tax_category = TAX_CATEGORY["PRIVATE_SALE"]
                
                # For income-type events, create a "receive" entry
                row_base[1] = "Receive (0 Cost)"
                row_base[2] = tax_category
                row_base[3] = date_str
                row_base[4] = asset
                row_base[5] = abs(amount)
                row_base[6] = date_str  # Received date
                row_base[7] = 0.0  # Usually 0 cost basis for received crypto
                row_base[8] = "N/A"
                row_base[9] = 0.0
                row_base[10] = 0.0  # Zero cost basis
                row_base[11] = 0.0
                row_base[12] = fee
                row_base[13] = 0.0
                row_base[14] = 0
                row_base[15] = "N/A"
                row_base[16] = SPECIAL_TX_TYPES.get(category, "")  # Special tax treatment warning
                row_base[17] = f"Ledger Ref: {refid} | Receive (0 cost basis)"
                
                # Add to holdings for future FIFO calculations
                HOLDINGS.setdefault(asset, []).append({
                    "amount": abs(amount),
                    "price_eur": 0.0,  # Zero cost basis for received assets
                    "timestamp": timestamp,
                    "refid": refid,
                    "year": year
                })
            
            else:
                # Handle any other transaction types
                row_base[1] = f"Ledger ({entry_type.capitalize()})"
                row_base[2] = "Zu prüfen"  # Need manual verification
                row_base[3] = date_str
                row_base[4] = asset
                row_base[5] = abs(amount)
                row_base[6] = "N/A"
                row_base[7] = price_eur
                row_base[8] = "N/A"
                row_base[9] = 0.0
                row_base[10] = 0.0
                row_base[11] = 0.0
                row_base[12] = fee
                row_base[13] = 0.0
                row_base[14] = 0
                row_base[15] = "Zu prüfen"
                row_base[16] = "Manuelle Überprüfung erforderlich"
                row_base[17] = f"Ledger Ref: {refid} | Unbekannter Transaktionstyp"
            
            # Assign transaction ID
            tx_id = assign_transaction_id(entry_type.upper(), timestamp, asset, amount)
            row_base[17] += f" | ID: {tx_id}"
            
            tax_data.append(row_base)
            line_num += 1
            processed_refids.add(refid)

    # Return the tax data for further processing
    return tax_data
