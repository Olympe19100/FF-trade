
import asyncio
try:
    asyncio.get_event_loop()
except RuntimeError:
    asyncio.set_event_loop(asyncio.new_event_loop())

import logging
from datetime import datetime
import pandas as pd
import numpy as np
from ib_insync import IB, Option, MarketOrder, Bag, ComboLeg

from core.config import (
    DEFAULT_HOST, GW_PAPER, CLIENT_ID
)
from core.scanner import fetch_option_chain_thetadata

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(name)s] %(levelname)s: %(message)s')
log = logging.getLogger("FinalRepair")

async def run_final_repair():
    ib = IB()
    await ib.connectAsync(DEFAULT_HOST, GW_PAPER, clientId=CLIENT_ID + 25)
    
    # 1. TSLA REBALANCE
    log.info("Rebalancing TSLA...")
    tsla_p365_f = Option("TSLA", "20260515", 365, "P", "SMART")
    tsla_c390_f = Option("TSLA", "20260515", 390, "C", "SMART")
    tsla_p365_b = Option("TSLA", "20260618", 365, "P", "SMART")
    
    # We have 3x C390_b, but 2x on others. Add 1x to the others.
    cands = await ib.qualifyContractsAsync(tsla_p365_f, tsla_c390_f, tsla_p365_b)
    if len(cands) == 3:
        # Sell 1x short legs, Buy 1x put long
        ib.placeOrder(cands[0], MarketOrder("SELL", 1))
        ib.placeOrder(cands[1], MarketOrder("SELL", 1))
        ib.placeOrder(cands[2], MarketOrder("BUY", 1))
        log.info("  TSLA orders placed (1x each for missing legs)")

    # 2. COMPLETE SINGLES
    singles = [
        {"ticker": "KO", "cts": 10, "f": "2026-05-22", "b": "2026-07-17"},
        {"ticker": "LLY", "cts": 1, "f": "2026-05-29", "b": "2026-07-17"},
        {"ticker": "MO", "cts": 10, "f": "2026-05-22", "b": "2026-07-17"},
        {"ticker": "MRK", "cts": 10, "f": "2026-05-22", "b": "2026-07-17"},
        {"ticker": "PM", "cts": 10, "f": "2026-05-15", "b": "2026-07-17"},
    ]

    from scipy.stats import norm
    def get_delta(S, K, T, iv, is_call=True):
        if T <= 0 or iv <= 0:
            return 0.5
        d1 = (np.log(S / K) + (0.5 * iv ** 2) * T) / (iv * np.sqrt(T))
        return norm.cdf(d1) if is_call else norm.cdf(d1) - 1

    for s in singles:
        ticker = s["ticker"]
        log.info(f"Completing {ticker}...")
        stock_px, chain = fetch_option_chain_thetadata(ticker)
        if stock_px <= 0 or chain.empty:
            continue
        
        chain["mid"] = (chain["bid"] + chain["ask"]) / 2
        puts = chain[chain["type"] == "put"].copy()
        
        # Find strikes existing in BOTH expirations
        f_exp = s["f"]
        b_exp = s["b"]
        f_strikes = set(puts[puts["exp_date"] == f_exp]["strike"])
        b_strikes = set(puts[puts["exp_date"] == b_exp]["strike"])
        common = f_strikes.intersection(b_strikes)
        
        if not common:
            log.warning(f"  {ticker}: No common strikes found between {f_exp} and {b_exp}")
            continue
            
        common_puts = puts[puts["strike"].isin(common) & (puts["exp_date"] == f_exp)].copy()
        pf_T = (pd.to_datetime(f_exp) - datetime.now()).days / 365.0
        iv_ref = 0.4 # default
        common_puts['delta'] = common_puts.apply(lambda r: abs(get_delta(stock_px, r['strike'], pf_T, iv_ref, False)), axis=1)
        common_puts['diff'] = (common_puts['delta'] - 0.35).abs()
        best_strike = common_puts.sort_values('diff').iloc[0]['strike']
        
        log.info(f"  Best common strike for {ticker}: {best_strike}")
        
        f_opt = Option(ticker, f_exp.replace("-", ""), best_strike, "P", "SMART")
        b_opt = Option(ticker, b_exp.replace("-", ""), best_strike, "P", "SMART")
        
        qualified = await ib.qualifyContractsAsync(f_opt, b_opt)
        if len(qualified) == 2:
            bag = Bag(symbol=ticker, exchange="SMART", currency="USD", comboLegs=[
                ComboLeg(conId=qualified[0].conId, ratio=1, action="SELL"),
                ComboLeg(conId=qualified[1].conId, ratio=1, action="BUY")
            ])
            ib.placeOrder(bag, MarketOrder("BUY", s["cts"]))
            log.info(f"  {ticker} Put Calendar ordered ({s['cts']}x)")

    await asyncio.sleep(5)
    ib.disconnect()

if __name__ == "__main__":
    asyncio.run(run_final_repair())
