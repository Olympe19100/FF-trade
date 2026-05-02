
import unittest
from unittest.mock import MagicMock
import sys
import os

# Add project root to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from core.portfolio import ibkr_portfolio_to_positions

class TestDashboardSync(unittest.TestCase):
    def test_ibkr_portfolio_mapping(self):
        # 1. Setup mock IBKR items (what ib.portfolio() returns)
        def create_mock_item(symbol, strike, exp, right, position, market_value, pnl):
            item = MagicMock()
            item.contract = MagicMock()
            item.contract.secType = "OPT"
            item.contract.symbol = symbol
            item.contract.strike = strike
            item.contract.lastTradeDateOrContractMonth = exp.replace("-", "")
            item.contract.right = right
            item.position = position
            item.marketValue = market_value
            item.unrealizedPNL = pnl
            return item

        # AAPL Calendar Spread legs
        # Combo cost = (Back - Front)
        # Market Value of Combo = (Back MV + Front MV)
        # If we have 10 contracts:
        # Front: -10 contracts, MV = -500 (value is 0.50)
        # Back: +10 contracts, MV = 800 (value is 0.80)
        # Net Combo MV = 300. 
        # Net Combo Price = 300 / (10 * 100) = 0.30
        ibkr_items = [
            create_mock_item("AAPL", 200, "2026-05-15", "C", -10, -500, -50),
            create_mock_item("AAPL", 200, "2026-06-18", "C", 10, 800, 100),
        ]

        # 2. Setup mock local portfolio (active positions)
        active_positions = [
            {
                "ticker": "AAPL",
                "strike": 200,
                "front_exp": "2026-05-15",
                "back_exp": "2026-06-18",
                "right": "C",
                "contracts": 10,
                "cost_per_share": 0.25, # entry cost
                "total_deployed": 250, # 0.25 * 100 * 10
                "spread_type": "single",
                "n_legs": 2
            }
        ]

        # 3. Run mapping
        priced, errors = ibkr_portfolio_to_positions(ibkr_items, active_positions)

        # 4. Verify
        self.assertEqual(len(priced), 1)
        self.assertEqual(len(errors), 0)
        
        pos = priced[0]
        self.assertEqual(pos["ticker"], "AAPL")
        self.assertEqual(pos["unrealized_pnl"], 50.0) # 100 - 50
        self.assertEqual(pos["current_cost"], 0.30) # 300 / 1000
        self.assertEqual(pos["return_pct"], 0.20) # 50 / 250
        
        print(f"SUCCESS: AAPL mapped correctly: P&L=${pos['unrealized_pnl']}, Cost=${pos['current_cost']}, Ret={pos['return_pct']:.1%}")

if __name__ == "__main__":
    unittest.main()
