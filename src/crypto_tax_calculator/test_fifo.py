import unittest
from decimal import Decimal
import datetime

from .fifo import FifoCalculator, HoldingLot
from .models import Transaction # Assuming Transaction is in models.py and accessible

class TestFifoCalculator(unittest.TestCase):

    def test_simple_buy_sell(self):
        calculator = FifoCalculator()
        
        # Purchase
        purchase_timestamp = int(datetime.datetime(2023, 1, 1, 10, 0, 0).timestamp())
        calculator.add_purchase(
            asset="BTC",
            amount=Decimal("1.0"),
            price_eur=Decimal("20000.0"),
            timestamp=purchase_timestamp,
            refid="P1",
            source="test"
        )
        
        self.assertEqual(len(calculator.holdings["BTC"]), 1)
        self.assertEqual(calculator.holdings["BTC"][0].amount, Decimal("1.0"))

        # Sell
        sell_timestamp = int(datetime.datetime(2023, 6, 1, 10, 0, 0).timestamp())
        matched_lots = calculator.match_lots(
            asset="BTC",
            amount=Decimal("0.5"),
            timestamp=sell_timestamp,
            refid="S1"
        )
        
        self.assertEqual(len(matched_lots), 1)
        matched_lot, amount_used = matched_lots[0]
        
        self.assertEqual(matched_lot.purchase_tx_refid, "P1")
        self.assertEqual(amount_used, Decimal("0.5"))
        self.assertEqual(matched_lot.purchase_price_eur, Decimal("20000.0"))
        
        self.assertEqual(calculator.holdings["BTC"][0].amount, Decimal("0.5"))

    def test_sell_more_than_held(self):
        calculator = FifoCalculator()
        
        # Purchase
        purchase_timestamp = int(datetime.datetime(2023, 1, 1, 10, 0, 0).timestamp())
        calculator.add_purchase(
            asset="ETH",
            amount=Decimal("2.0"),
            price_eur=Decimal("1500.0"),
            timestamp=purchase_timestamp,
            refid="P_ETH1",
            source="test"
        )

        # Sell more than available
        sell_timestamp = int(datetime.datetime(2023, 6, 1, 10, 0, 0).timestamp())
        matched_lots = calculator.match_lots(
            asset="ETH",
            amount=Decimal("3.0"), # Trying to sell 3 ETH, only 2 held
            timestamp=sell_timestamp,
            refid="S_ETH1"
        )
        
        # Should only match the available 2 ETH
        self.assertEqual(len(matched_lots), 1) 
        matched_lot, amount_used = matched_lots[0]
        
        self.assertEqual(matched_lot.purchase_tx_refid, "P_ETH1")
        self.assertEqual(amount_used, Decimal("2.0")) # Matched amount is capped at holding
        
        # All holdings should be gone
        self.assertEqual(len(calculator.holdings["ETH"]), 0)

if __name__ == '__main__':
    unittest.main()
