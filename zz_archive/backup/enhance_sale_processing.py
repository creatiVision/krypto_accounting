#!/usr/bin/env python3
"""
This script enhances the crypto sale processing logic in krypto-accounting_german_tax.py
to provide more detailed tax reasoning and FIFO tracking for German tax authorities.
"""

import re
from pathlib import Path

def enhance_sale_processing():
    # Path to the module
    module_path = Path(__file__).parent / "krypto-accounting_german_tax.py"
    
    # Read the file content
    with open(module_path, 'r') as f:
        content = f.read()
    
    # Find the section in process_for_tax where sales are processed
    sale_pattern = r'# Calculate gain or loss\s+gain_loss = sell_proceeds - total_cost - fee'
    sale_match = re.search(sale_pattern, content)
    
    if not sale_match:
        print("Could not find the sale processing section")
        return False
    
    # Define the enhanced calculation with detailed reasoning
    enhanced_calculation = """                # Calculate gain or loss
                gain_loss = sell_proceeds - total_cost - fee
                
                # Determine tax status based on holding period for each matched lot
                taxable_gain = 0.0
                nontaxable_gain = 0.0
                taxable_portions = []
                nontaxable_portions = []
                
                for lot in matched_lots:
                    lot_sale_value = lot["amount"] * sell_price_eur
                    lot_gain_loss = lot_sale_value - lot["cost_basis"]
                    
                    if lot["holding_period"] <= HOLDING_PERIOD_DAYS:
                        taxable_gain += lot_gain_loss
                        taxable_portions.append({
                            "amount": lot["amount"],
                            "gain_loss": lot_gain_loss,
                            "holding_days": lot["holding_period"]
                        })
                    else:
                        nontaxable_gain += lot_gain_loss
                        nontaxable_portions.append({
                            "amount": lot["amount"],
                            "gain_loss": lot_gain_loss,
                            "holding_days": lot["holding_period"]
                        })
                
                # Store the matched lots for detailed FIFO documentation
                tx_info = {
                    "type": "sell",
                    "date": date_str,
                    "asset": base_asset,
                    "amount": sell_amount,
                    "price_eur": sell_price_eur,
                    "cost": total_cost,
                    "proceeds": sell_proceeds,
                    "fee": fee,
                    "gain_loss": gain_loss,
                    "matched_lots": matched_lots,
                    "taxable_gain": taxable_gain,
                    "nontaxable_gain": nontaxable_gain
                }
                
                # Store transaction info for documentation
                if "tx_info" not in data:
                    data["tx_info"] = tx_info
                
                # Determine overall tax status and reason
                if taxable_portions and nontaxable_portions:
                    # Mixed holding periods
                    is_long_term = "Teilweise"
                    tax_status = "Teilweise"
                    
                    taxable_amount = sum(p["amount"] for p in taxable_portions)
                    nontaxable_amount = sum(p["amount"] for p in nontaxable_portions)
                    
                    tax_reason = f"Teilweise steuerpflichtig: {taxable_amount:.8f} {base_asset} ≤ 1 Jahr gehalten (§23 EStG), "
                    tax_reason += f"{nontaxable_amount:.8f} {base_asset} > 1 Jahr gehalten (steuerfrei)"
                elif taxable_portions:
                    # All portions are short-term (≤ 1 year)
                    is_long_term = "Nein"
                    
                    # Only gains are taxable, losses can be offset against other private sale gains
                    if gain_loss > 0:
                        tax_status = "Ja"
                        tax_reason = f"Steuerpflichtig: Haltedauer aller Anteile ≤ 1 Jahr (§23 EStG)"
                    else:
                        tax_status = "Nein"
                        tax_reason = "Verlust ist im Rahmen des §23 EStG mit anderen Gewinnen verrechenbar"
                else:
                    # All portions are long-term (> 1 year)
                    is_long_term = "Ja"
                    tax_status = "Nein"
                    tax_reason = f"Nicht steuerpflichtig: Haltedauer aller Anteile > 1 Jahr (§23 EStG)"
                
                # Create row for the sale with enhanced information
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
                
                # Enhanced tax information
                row_base[15] = is_long_term  # Haltedauer > 1 Jahr
                row_base[16] = tax_status    # Steuerpflichtig
                row_base[17] = tax_reason    # Steuergrund
                
                # FIFO details for documentation
                fifo_details = []
                for i, lot in enumerate(matched_lots):
                    # Create a more detailed lot description that explains the tax treatment
                    lot_detail = f"Lot {i+1}: {lot['amount']:.8f} {base_asset} "
                    lot_detail += f"gekauft am {lot['purchase_date']} "
                    lot_detail += f"für {lot['purchase_price']:.2f} €/Stk, "
                    lot_detail += f"Haltedauer: {lot['holding_period']} Tage "
                    
                    # Indicate tax status for this specific lot
                    if lot['holding_period'] > HOLDING_PERIOD_DAYS:
                        lot_detail += "(steuerfrei nach §23 EStG)"
                    else:
                        lot_detail += "(steuerpflichtig nach §23 EStG)"
                    
                    fifo_details.append(lot_detail)
                
                row_base[18] = " | ".join(fifo_details)  # FIFO-Details column
                
                # Add a note about Freigrenze if applicable
                if is_taxable and gain_loss > 0:
                    current_year_freigrenze = FREIGRENZE_2024_ONWARDS if year >= 2024 else FREIGRENZE_UNTIL_2023
                    row_base[19] = f"Unterliegt der Freigrenze von {current_year_freigrenze} € (§23 EStG)"
                else:
                    row_base[19] = ""  # No additional notes"""
    
    # Replace the calculation section
    updated_content = content.replace(sale_match.group(0), enhanced_calculation)
    
    # Also need to adjust the buy processing to track transactions
    buy_pattern = r'# Buy trades add to holdings.*?processed_refids\.add\(refid\)'
    buy_match = re.search(buy_pattern, content, re.DOTALL)
    
    if buy_match:
        enhanced_buy = """                # Buy trades add to holdings
                HOLDINGS.setdefault(base_asset, []).append({
                    "amount": abs(amount),
                    "price_eur": price if quote_asset == "EUR" else get_market_price(base_asset, timestamp),
                    "timestamp": timestamp,
                    "refid": refid,
                    "year": year
                })
                
                # Store transaction info for documentation
                tx_info = {
                    "type": "buy",
                    "date": date_str,
                    "asset": base_asset,
                    "amount": abs(amount),
                    "price_eur": price if quote_asset == "EUR" else get_market_price(base_asset, timestamp),
                    "cost": cost,
                    "fee": fee
                }
                
                # Store transaction info
                if "tx_info" not in data:
                    data["tx_info"] = tx_info
                
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
                row_base[16] = "N/A"  # Not applicable for purchases
                row_base[17] = ""  # No tax reason for purchases
                row_base[18] = f"Kaufpreis: {price:.2f} €/Stück"  # FIFO-Details column
                row_base[19] = f"Trade Ref: {refid}"  # Notes column
                
                # Assign transaction ID
                tx_id = assign_transaction_id("BUY", timestamp, base_asset, amount)
                row_base[19] += f" | ID: {tx_id}"
                
                tax_data.append(row_base)
                line_num += 1
                processed_refids.add(refid)"""
        
        updated_content = updated_content.replace(buy_match.group(0), enhanced_buy)
    
    # Write the updated content back to the file
    with open(module_path, 'w') as f:
        f.write(updated_content)
    
    print(f"Successfully enhanced sale processing in {module_path}")
    return True

if __name__ == "__main__":
    enhance_sale_processing()
