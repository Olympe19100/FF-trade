
import asyncio
try:
    asyncio.get_event_loop()
except RuntimeError:
    asyncio.set_event_loop(asyncio.new_event_loop())

from ib_insync import IB
from core.config import DEFAULT_HOST, GW_PAPER, CLIENT_ID

async def check_details():
    ib = IB()
    await ib.connectAsync(DEFAULT_HOST, GW_PAPER, clientId=CLIENT_ID + 20)
    
    portfolio = ib.portfolio()
    print(f"{'Symbol':<8} {'Right':<2} {'Strike':<8} {'Exp':<10} {'Pos':<5} {'SecType':<7}")
    print("-" * 50)
    for item in portfolio:
        c = item.contract
        if c.secType == "OPT":
            print(f"{c.symbol:<8} {c.right:<2} {c.strike:<8} {c.lastTradeDateOrContractMonth:<10} {item.position:<5} {c.secType:<7}")
    
    ib.disconnect()

if __name__ == "__main__":
    asyncio.run(check_details())
