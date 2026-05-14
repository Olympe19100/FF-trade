import asyncio
try:
    asyncio.get_event_loop()
except RuntimeError:
    asyncio.set_event_loop(asyncio.new_event_loop())

import json
import logging
from pathlib import Path
from datetime import datetime
import pandas as pd
import numpy as np
from ib_insync import IB, Option, MarketOrder, Bag, ComboLeg

from core.config import (
    DEFAULT_HOST, GW_PAPER, CLIENT_ID, PORTFOLIO_FILE,
    CONTRACT_MULT
)
from core.scanner import fetch_option_chain_thetadata

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(name)s] %(levelname)s: %(message)s')
log = logging.getLogger("RepairBot")

def load_portfolio():
    path = Path(PORTFOLIO_FILE)
    if not path.exists():
        return []
    with open(path, "r") as f:
        return json.load(f)

def save_portfolio(data):
    with open(PORTFOLIO_FILE, "w") as f:
        json.dump(data, f, indent=4)

async def repair_portfolio():
    ib = IB()
    try:
        await ib.connectAsync(DEFAULT_HOST, GW_PAPER, clientId=CLIENT_ID + 15)
    except Exception as e:
        log.error(f"Failed to connect to IBKR: {e}")
        return

    portfolio_data = load_portfolio()
    positions = portfolio_data.get("positions", [])
    singles = [p for p in positions if p.get("spread_type") != "double"]
    
    if not singles:
        log.info("No single positions found to repair.")
        ib.disconnect()
        return

    log.info(f"Found {len(singles)} single positions to repair: {[p['ticker'] for p in singles]}")

    for pos in singles:
        ticker = pos["ticker"]
        contracts = pos["contracts"]
        front_exp = pos["front_exp"]
        back_exp = pos["back_exp"]
        
        log.info(f"Repairing {ticker} ({contracts}x)...")
        
        try:
            stock_px, chain = fetch_option_chain_thetadata(ticker)
            if stock_px <= 0 or chain.empty:
                log.warning(f"  {ticker}: No data found for repair.")
                continue
            
            chain["mid"] = (chain["bid"] + chain["ask"]) / 2
            puts = chain[chain["type"] == "put"].copy()
            # Filter for correct expirations
            puts = puts[puts["exp_date"].isin([front_exp, back_exp])]
            
            if puts.empty:
                log.warning(f"  {ticker}: No puts found for {front_exp}/{back_exp}")
                continue
            
            # ── Delta-based strike selection (Target 35 Delta) ──
            # Replicate scanner logic: find put strike closest to 35 delta
            pf_T = (pd.to_datetime(front_exp) - datetime.now()).days / 365.0
            
            # Need a simplified delta calculation
            from scipy.stats import norm
            def get_delta(S, K, T, iv, is_call=True):
                if T <= 0 or iv <= 0:
                    return 0.5
                d1 = (np.log(S / K) + (0.5 * iv ** 2) * T) / (iv * np.sqrt(T))
                return norm.cdf(d1) if is_call else norm.cdf(d1) - 1

            # Get IVs for candidate puts
            put_cands = puts[(puts["exp_date"] == front_exp)].copy()
            # Basic ATM IV estimate if missing
            atm_put = put_cands.iloc[(put_cands['strike'] - stock_px).abs().argsort()[:1]]
            iv_ref = atm_put['iv'].iloc[0] if not atm_put.empty and atm_put['iv'].iloc[0] > 0 else 0.4
            
            put_cands['delta'] = put_cands.apply(lambda r: abs(get_delta(stock_px, r['strike'], pf_T, iv_ref, False)), axis=1)
            put_cands['delta_diff'] = (put_cands['delta'] - 0.35).abs()
            
            best_put = put_cands.sort_values('delta_diff').iloc[0]
            target_strike = float(best_put['strike'])
            
            log.info(f"  Target Put Strike: {target_strike} (Delta: {best_put['delta']:.2f})")
            
            # Check if this strike exists in back expiry
            p_back = puts[(puts["exp_date"] == back_exp) & (puts["strike"] == target_strike)]
            if p_back.empty:
                log.warning(f"  {ticker}: Put strike {target_strike} missing in back expiry {back_exp}")
                continue
                
            pb = p_back.iloc[0]
            pf = best_put
            
            log.info(f"  Found Put leg: Strike {target_strike} (Mid: ${pb['mid']-pf['mid']:.2f})")
            
            # Construct legs for IBKR
            f_opt = Option(ticker, front_exp.replace("-", ""), target_strike, "P", "SMART")
            b_opt = Option(ticker, back_exp.replace("-", ""), target_strike, "P", "SMART")
            
            # Resolve contracts
            f_con, b_con = await asyncio.gather(ib.qualifyContractsAsync(f_opt), ib.qualifyContractsAsync(b_opt))
            if not f_con or not b_con:
                log.warning(f"  {ticker}: Failed to qualify put contracts.")
                continue
            
            # Execute as a BAG (Market order as requested earlier)
            bag = Bag(symbol=ticker, exchange="SMART", currency="USD", comboLegs=[
                ComboLeg(conId=f_con[0].conId, ratio=1, action="SELL"),
                ComboLeg(conId=b_con[0].conId, ratio=1, action="BUY")
            ])
            
            log.info(f"  Executing {contracts}x Put Calendar for {ticker} at MARKET...")
            trade = ib.placeOrder(bag, MarketOrder("BUY", contracts))
            
            # Wait for fill
            while not trade.isDone():
                await asyncio.sleep(1)
            
            if trade.status == "Filled":
                fill_price = trade.avgFillPrice()
                log.info(f"  FILLED @ ${fill_price:.2f}")
                
                # Update portfolio.json
                pos["spread_type"] = "double"
                pos["put_strike"] = target_strike
                pos["put_cost"] = fill_price
                pos["n_legs"] = 4
                pos["cost_per_share"] = round(pos["cost_per_share"] + fill_price, 2)
                pos["total_deployed"] = round(pos["cost_per_share"] * contracts * CONTRACT_MULT, 2)
                
                save_portfolio(portfolio_data)
                log.info(f"  {ticker} successfully upgraded to Double Calendar.")
            else:
                log.warning(f"  {ticker}: Trade failed or cancelled.")

        except Exception as e:
            log.error(f"  Error repairing {ticker}: {e}")

    ib.disconnect()

if __name__ == "__main__":
    asyncio.run(repair_portfolio())
