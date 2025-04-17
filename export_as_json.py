def export_as_json(
    summary: AggregatedTaxSummary,
    tax_year: int,
    output_path: Path,
    include_lot_details: bool = True
) -> Dict[str, str]:
    """Export tax report as JSON files."""
    try:
        # Full report in a single JSON file
        json_filename = create_filename("tax_report", tax_year, "json")
        json_path = output_path / json_filename
        
        # Prepare summary data
        summary_data = {
            "tax_year": tax_year,
            "total_profit_loss": float(summary.total_profit_loss),
            "total_private_sale_gains": float(summary.total_private_sale_gains),
            "total_private_sale_losses": float(summary.total_private_sale_losses),
            "total_other_income": float(summary.total_other_income),
            "total_tax_liability": float(summary.total_tax_liability),
            "warnings": summary.warnings,
            "report_entries": []
        }
        
        # Prepare detailed entries
        for entry in summary.tax_report_entries:
            entry_data = {
                "date": format_timestamp(entry.timestamp),
                "asset": entry.asset,
                "amount": str(entry.amount),
                "proceeds_eur": str(entry.disposal_proceeds_eur or entry.cost_or_proceeds),
                "cost_basis_eur": str(entry.disposal_cost_basis_eur or 0),
                "gain_loss_eur": str(entry.disposal_gain_loss_eur or 0),
                "tax_liability_eur": str(entry.tax_liability),
                "holding_period_days": entry.holding_period_days_avg,
                "is_long_term": entry.is_long_term,
                "is_taxable": entry.is_taxable,
                "refid": entry.refid,
            }
            
            # Include lot details if requested
            if include_lot_details and entry.matched_lots:
                entry_data["matched_lots"] = []
                for lot in entry.matched_lots:
                    lot_data = {
                        "acquisition_date": lot.original_lot_purchase_date.strftime("%Y-%m-%d %H:%M:%S"),
                        "acquisition_refid": lot.original_lot_refid,
                        "amount_used": str(lot.amount_used),
                        "acquisition_price_eur": str(lot.original_lot_purchase_price_eur),
                        "cost_basis_eur": str(lot.cost_basis_eur),
                        "holding_period_days": lot.holding_period_days
                    }
                    entry_data["matched_lots"].append(lot_data)
            
            summary_data["report_entries"].append(entry_data)
        
        # Write to file
        with open(json_path, 'w') as json_file:
            json.dump(summary_data, json_file, cls=DecimalEncoder, indent=2)
        
        log_event("Export", f"Created JSON tax report: {json_path}")
        return {'json': str(json_path)}
    except Exception as e:
        error_msg = f"Failed to export JSON report: {str(e)}"
        log_error("reporting", "JSONExportError", error_msg, 
                details={"tax_year": tax_year}, 
                exception=e)
        return {}
