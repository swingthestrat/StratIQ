import yfinance as yf
import pandas as pd
from database import Session, Alert, OHLCV
from engine import get_strat_candle, calculate_ftfc, calculate_tto
from datetime import datetime

def refresh_ticker(ticker):
    session = Session()
    print(f"Refreshing {ticker}...")
    
    # 1. Fetch Data
    # Fetch enough history for all timeframes
    # We need to re-calculate everything for this ticker
    
    # For simplicity, let's just update the 1M timeframe which is the issue
    # But ideally we update all.
    
    # Let's use the existing logic from populate_alerts.py but simplified
    # We need to fetch daily data and resample
    
    print("Fetching data...")
    data = yf.download(ticker, period="2y", interval="1d", progress=False)
    
    if data.empty:
        print("No data found.")
        return

    # Flatten columns if multi-index
    if isinstance(data.columns, pd.MultiIndex):
        data.columns = data.columns.get_level_values(0)
    
    # Rename columns to lowercase
    data.columns = [c.lower() for c in data.columns]
    data.index.name = 'date'
    
    # Resample to 1M
    # Logic from engine.py/populate_alerts.py
    # We need to handle the resampling carefully
    
    # Actually, let's just use yfinance to fetch 1M directly for accuracy check
    df_1m = yf.download(ticker, period="2y", interval="1mo", progress=False)
    if isinstance(df_1m.columns, pd.MultiIndex):
        df_1m.columns = df_1m.columns.get_level_values(0)
    df_1m.columns = [c.lower() for c in df_1m.columns]
    
    # Calculate Strat Candle for last few months
    # We need at least 3 rows
    if len(df_1m) < 3:
        print("Not enough 1M data")
        return
        
    # Current (Nov), Prev1 (Oct), Prev2 (Sep)
    # yfinance returns current incomplete candle too.
    # Let's assume the last row is current.
    
    curr = df_1m.iloc[-1]
    prev1 = df_1m.iloc[-2]
    prev2 = df_1m.iloc[-3]
    prev3 = df_1m.iloc[-4]
    
    print(f"\nData Analysis for {ticker} (1M):")
    print(f"Sep High: {prev2['high']:.2f}, Low: {prev2['low']:.2f}")
    print(f"Aug High: {prev3['high']:.2f}, Low: {prev3['low']:.2f}")
    
    # Recalculate conditions
    cond_sep = get_strat_candle(prev2, prev3) # Should be '1'
    cond_oct = get_strat_candle(prev1, prev2)
    cond_nov = get_strat_candle(curr, prev1)
    
    print(f"Sep Condition (Prev2): {cond_sep}")
    print(f"Oct Condition (Prev1): {cond_oct}")
    print(f"Nov Condition (Curr): {cond_nov}")
    
    # Update Database
    # Find the alert
    today = datetime.now().date()
    alert = session.query(Alert).filter_by(ticker=ticker, timeframe='1M', date=today).first()
    
    if alert:
        print(f"\nUpdating DB Record (ID: {alert.id})...")
        print(f"Old: {alert.prev_cond_2} - {alert.prev_cond_1} - {alert.curr_cond}")
        
        alert.prev_cond_2 = cond_sep
        alert.prev_cond_1 = cond_oct
        alert.curr_cond = cond_nov
        
        session.commit()
        print(f"New: {alert.prev_cond_2} - {alert.prev_cond_1} - {alert.curr_cond}")
        print("Done.")
    else:
        print("Alert not found in DB.")

if __name__ == "__main__":
    refresh_ticker("CI")
