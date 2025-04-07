"""
Transaction classifier for handling different ledger entry types.
This module analyzes transaction data and classifies it according to tax relevance.
"""

from datetime import datetime
from typing import Dict, Any, List, Optional, Tuple, Set

def is_sale_transaction(transaction: Dict[str, Any]) -> bool:
    """
    Determine if a transaction represents a cryptocurrency sale.
    
    Args:
        transaction: Transaction data dictionary
        
    Returns:
        True if the transaction is a sale, False otherwise
    """
    # Check transaction type
    tx_type = transaction.get('type', '').lower()
    
    # Direct sale indicators
    if tx_type in ('sell', 'trade'):
        return True
        
    # Ledger "spend" entries are sales in Kraken's terminology
    if tx_type == 'spend':
        return True
        
    # Check for subtypes that indicate sales
    subtype = transaction.get('subtype', '').lower()
    if subtype in ('trade', 'sell'):
        return True
        
    # Check for position status closing
    if transaction.get('posstatus') == 'closed':
        return True
    
    # Check for negative amount (outgoing) with a pair or asset 
    if (transaction.get('amount', 0) 
            and float(transaction.get('amount', 0)) < 0 
            and (transaction.get('pair') or transaction.get('asset'))):
        return True
    
    return False

def classify_transactions(transactions: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    """
    Classify transactions into different categories for tax reporting.
    
    Args:
        transactions: List of transaction data dictionaries
        
    Returns:
        Dictionary with transaction lists categorized by type
    """
    result = {
        'sales': [],      # Cryptocurrency sales
        'purchases': [],  # Cryptocurrency purchases
        'income': [],     # Income (staking, rewards, etc.)
        'transfers': [],  # Transfers between wallets
        'fees': [],       # Fee transactions
        'other': []       # Other uncategorized transactions
    }
    
    for tx in transactions:
        if is_sale_transaction(tx):
            result['sales'].append(tx)
        elif tx.get('type') == 'receive' and 'staking' in tx.get('aclass', '').lower():
            result['income'].append(tx)
        elif tx.get('type') == 'receive':
            result['purchases'].append(tx)
        elif tx.get('type') in ('deposit', 'withdrawal'):
            result['transfers'].append(tx)
        elif tx.get('fee', 0) and float(tx.get('fee', 0)) > 0:
            result['fees'].append(tx)
        else:
            result['other'].append(tx)
    
    return result

def get_transaction_year(transaction: Dict[str, Any]) -> Optional[int]:
    """
    Extract the year from a transaction.
    
    Args:
        transaction: Transaction data dictionary
        
    Returns:
        Year as an integer, or None if not available
    """
    try:
        if 'time' in transaction:
            timestamp = float(transaction['time'])
            return datetime.fromtimestamp(timestamp).year
    except (ValueError, TypeError):
        pass
    return None

def filter_transactions_by_year(transactions: List[Dict[str, Any]], year: int) -> List[Dict[str, Any]]:
    """
    Filter transactions to include only those from the specified year.
    
    Args:
        transactions: List of transaction data dictionaries
        year: Year to filter for
        
    Returns:
        Filtered list of transactions
    """
    filtered = []
    for tx in transactions:
        tx_year = get_transaction_year(tx)
        if tx_year == year:
            filtered.append(tx)
    return filtered
