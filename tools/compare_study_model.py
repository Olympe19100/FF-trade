
import numpy as np
from pathlib import Path
import logging

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("StudyComparator")

CACHE = Path(r"C:\Users\ANTEC MSI\Desktop\pro\Option trading\cache")

# Study Coefficients (from README.txt)
# Note: we need to match the order and scaling
# Signal 1: sig_impl_vs_last_impl (ratio) -> -0.9596
# Signal 2: sig_impl_minus_last_real (diff in %) -> -0.1880
# Signal 3: sig_impl_vs_avg_impl (ratio) -> -1.1505
# Signal 4: sig_impl_minus_avg_real (diff in %) -> -0.6233
# Const: 3.3773 (prediction in %)

STUDY_COEFS = {
    "sig_impl_vs_last_impl": -0.9596,
    "sig_impl_minus_last_real": -0.1880,
    "sig_impl_vs_avg_impl": -1.1505,
    "sig_impl_minus_avg_real": -0.6233,
    "intercept": 3.3773
}

def apply_study_model(df):
    """Apply study coefficients to the signal dataframe."""
    # Scale differences to %
    s1 = df["sig_impl_vs_last_impl"]
    s2 = df["sig_impl_minus_last_real"] * 100
    s3 = df["sig_impl_vs_avg_impl"]
    s4 = df["sig_impl_minus_avg_real"] * 100
    
    pred = (
        STUDY_COEFS["intercept"] +
        STUDY_COEFS["sig_impl_vs_last_impl"] * s1 +
        STUDY_COEFS["sig_impl_minus_last_real"] * s2 +
        STUDY_COEFS["sig_impl_vs_avg_impl"] * s3 +
        STUDY_COEFS["sig_impl_minus_avg_real"] * s4
    )
    return pred / 100.0 # Return to decimal

def compare():
    from core.straddle import compute_signals, build_earnings_straddle_history
    
    # 1. Load history
    history = build_earnings_straddle_history()
    if history.empty:
        log.error("No history")
        return
        
    # 2. Compute signals
    signals = compute_signals(history)
    
    # 3. Apply Study Model
    signals["study_pred"] = apply_study_model(signals)
    
    # 4. Filter for Study Trades (pred > 0)
    study_trades = signals[signals["study_pred"] > 0].copy()
    
    # 5. Evaluate Performance
    # Note: we use gross_return for model evaluation
    y_true = signals["gross_return"]
    y_study = signals["study_pred"]
    
    mask = y_true.notna() & y_study.notna()
    corr = np.corrcoef(y_study[mask], y_true[mask])[0, 1]
    
    log.info("=" * 60)
    log.info("STUDY MODEL EVALUATION ON LOCAL DATA")
    log.info("=" * 60)
    log.info(f"OOS Correlation: {corr:.4f}")
    log.info(f"Tradeable events: {len(study_trades)} / {len(signals)}")
    log.info(f"Mean return (study picks): {study_trades['gross_return'].mean()*100:.2f}%")
    log.info(f"Median return (study picks): {study_trades['gross_return'].median()*100:.2f}%")
    log.info(f"Win Rate (study picks): {(study_trades['gross_return'] > 0).mean()*100:.1f}%")

if __name__ == "__main__":
    compare()
