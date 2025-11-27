import pandas as pd
import numpy as np
from database import Session, OHLCV

def get_strat_candle(curr, prev):
    h, l = curr['high'], curr['low']
    ph, pl = prev['high'], prev['low']
    open_, close = curr['open'], curr['close']
    is_green = close >= open_ # Treat doji as green or handle separately? Assuming Green for >=
    
    inside = h <= ph and l >= pl
    outside = h > ph and l < pl
    up = h > ph and l >= pl
    down = l < pl and h <= ph
    
    if inside: return '1'
    
    if outside:
        return '3u' if is_green else '3d'
        
    if up:
        return '2u' if is_green else '2uR'
        
    if down:
        return '2dG' if is_green else '2d'
        
    return '?'

def is_green(row):
    return row['close'] > row['open']

def is_hammer(row):
    # Pinescript-like definition:
    # Small body (e.g., < 33% of range)
    # Long lower wick (e.g., > 2 * body)
    # Close in upper third
    range_len = row['high'] - row['low']
    if range_len == 0: return False
    
    body = abs(row['close'] - row['open'])
    lower_wick = min(row['close'], row['open']) - row['low']
    upper_wick = row['high'] - max(row['close'], row['open'])
    
    # Logic: Body is small, Lower wick is long, Upper wick is small
    # Using standard Strat definition often cited:
    # Hammer: Body in upper 1/3 (or 1/4), Lower wick >= 2 * Body
    return (lower_wick >= 2 * body) and (upper_wick <= body)

def is_shooter(row):
    # Inverse of Hammer
    range_len = row['high'] - row['low']
    if range_len == 0: return False
    
    body = abs(row['close'] - row['open'])
    lower_wick = min(row['close'], row['open']) - row['low']
    upper_wick = row['high'] - max(row['close'], row['open'])
    
    # Logic: Body in lower 1/3, Upper wick >= 2 * Body
    return (upper_wick >= 2 * body) and (lower_wick <= body)

def calculate_ftfc(df_all):
    # Full Timeframe Continuity
    # Check direction of M, W, D
    # Simple logic: Green = Bullish, Red = Bearish
    
    tfs = ['1M', '1W', '1D']
    directions = []
    
    for tf in tfs:
        df_tf = df_all[df_all['timeframe'] == tf]
        if not df_tf.empty:
            curr = df_tf.iloc[-1]
            directions.append('Bull' if is_green(curr) else 'Bear')
        else:
            directions.append('?')
            
    if all(d == 'Bull' for d in directions): return "Bullish"
    if all(d == 'Bear' for d in directions): return "Bearish"
    return "Mixed"

def calculate_tto(df_all):
    """
    Triangle Target Output (TTO) Logic:
    Spectrum: 1D, 2D, 3D, 5D, 1W, 2W, 3W, 1M, 1Q, 1Y
    Condition: In any 4 continuous timeframe blocks:
    1. 3 out of 4 are same color.
    2. First and Last of the block are same color.
    """
    tfs_order = ['1D', '2D', '3D', '5D', '1W', '2W', '3W', '1M', '1Q', '1Y']
    
    # Get latest color for each TF
    # 1 = Green, -1 = Red, 0 = Unknown/Doji
    colors = {}
    for tf in tfs_order:
        df_tf = df_all[df_all['timeframe'] == tf]
        if not df_tf.empty:
            curr = df_tf.iloc[-1]
            if curr['close'] > curr['open']:
                colors[tf] = 1 # Green
            elif curr['close'] < curr['open']:
                colors[tf] = -1 # Red
            else:
                colors[tf] = 0 # Doji
        else:
            colors[tf] = 0 # Missing data
            
    # Check blocks of 4
    for i in range(len(tfs_order) - 3):
        block_tfs = tfs_order[i:i+4]
        block_vals = [colors[tf] for tf in block_tfs]
        
        # Skip if any missing data (0) in block? Or treat as neutral?
        # User said "color", implying Green/Red. Let's ignore 0s for "same color" count.
        if 0 in block_vals: continue
        
        first = block_vals[0]
        last = block_vals[-1]
        
        # Condition 2: First and Last same color
        if first != last: continue
        
        # Condition 1: 3 of same color
        # Count occurrences of the 'first' color (since first==last, this covers the majority color)
        count_same = block_vals.count(first)
        
        if count_same >= 3:
            return 1 # TTO Met
            
    return 0

def run_scan(ticker, spy_data=None):
    session = Session()
    alerts = []
    
    try:
        query = session.query(OHLCV).filter_by(symbol=ticker).order_by(OHLCV.date)
        df_all = pd.read_sql(query.statement, session.bind)
        
        if df_all.empty: return []

        ftfc = calculate_ftfc(df_all)
        tto = calculate_tto(df_all)
        
        # Calculate ADR (14-Day) - ALWAYS based on Daily Data
        adr = 0
        df_d = df_all[df_all['timeframe'] == '1D']
        if len(df_d) >= 14:
            daily_ranges = df_d['high'] - df_d['low']
            # SMA of ranges over 14 days
            adr_val = daily_ranges.rolling(14).mean().iloc[-1]
            # ADR% = (ADR / Current Daily Close) * 100
            curr_d_close = df_d.iloc[-1]['close']
            if curr_d_close > 0:
                adr = (adr_val / curr_d_close) * 100
        
        # Helper to get latest change % for a specific TF (Ticker)
        def get_perf(target_tf, df_source):
            rows = df_source[df_source['timeframe'] == target_tf]
            if not rows.empty:
                latest = rows.iloc[-1]
                return ((latest['close'] - latest['open']) / latest['open']) * 100
            return 0

        # Helper to get latest change % for SPY
        def get_spy_perf(target_tf):
            if not spy_data or target_tf not in spy_data: return 0
            df_spy = spy_data[target_tf]
            if not df_spy.empty:
                latest = df_spy.iloc[-1]
                return ((latest['close'] - latest['open']) / latest['open']) * 100
            return 0

        # Process per timeframe
        for tf in df_all['timeframe'].unique():
            df_tf = df_all[df_all['timeframe'] == tf].copy()
            # Ensure enough data for patterns (need 3 candles)
            # For Yearly, we might only have 1 or 2 if history is short.
            # If < 3, we can still check single candle patterns (Hammer/Shooter/Inside) 
            # but combo patterns need history.
            if len(df_tf) < 1: continue
            
            curr = df_tf.iloc[-1]
            prev = df_tf.iloc[-2] if len(df_tf) >= 2 else None
            prev2 = df_tf.iloc[-3] if len(df_tf) >= 3 else None
            
            strat = get_strat_candle(curr, prev) if prev is not None else '?'
            strat_prev = get_strat_candle(prev, prev2) if prev is not None and prev2 is not None else '?'
            strat_prev2 = get_strat_candle(prev2, df_tf.iloc[-4]) if prev2 is not None and len(df_tf) >= 4 else '?'
            
            # Common Data
            change_pct = ((curr['close'] - curr['open']) / curr['open']) * 100
            vol = curr['volume']
            
            gap = 0
            if prev is not None:
                gap = ((curr['open'] - prev['close']) / prev['close']) * 100
                
            change_from_open = ((curr['close'] - curr['open']) / curr['open']) * 100
            
            # Performance Metrics (WTD, MTD, QTD, YTD)
            wtd = 0
            mtd = 0
            qtd = 0
            ytd = 0
            perf_3m = 0
            
            # RS Metrics
            rs_1d = 0
            rs_1w = 0
            rs_1m = 0
            rs_3m = 0
            
            try:
                wtd = get_perf('1W', df_all)
                mtd = get_perf('1M', df_all)
                qtd = get_perf('1Q', df_all)
                ytd = get_perf('1Y', df_all)
                perf_3m = get_perf('3M', df_all)
                
                # Calculate RS (Excess Return vs SPY)
                # RS = Ticker_Perf - SPY_Perf
                # 1D
                perf_1d = get_perf('1D', df_all)
                spy_1d = get_spy_perf('1D')
                rs_1d = perf_1d - spy_1d
                
                # 1W
                spy_1w = get_spy_perf('1W')
                rs_1w = wtd - spy_1w
                
                # 1M
                spy_1m = get_spy_perf('1M')
                rs_1m = mtd - spy_1m
                
                # 3M
                spy_3m = get_spy_perf('3M')
                rs_3m = perf_3m - spy_3m
                
            except Exception:
                pass 
            
            # Calculate Avg Dollar Volume (20D)
            avg_dollar_volume = 0
            if tf == '1D':
                # Use the daily dataframe subset we already have? 
                # We need to access the main df_d (Daily Data)
                # Let's just use the current df_tf if it is 1D, or fallback to df_d
                target_df = df_tf if tf == '1D' else df_d
                
                if len(target_df) >= 20:
                    last_20 = target_df.iloc[-20:]
                    dollar_vols = last_20['close'] * last_20['volume']
                    avg_dollar_volume = dollar_vols.mean()
            else:
                # For non-daily TFs, we still want the daily ADVol
                if len(df_d) >= 20:
                    last_20 = df_d.iloc[-20:]
                    dollar_vols = last_20['close'] * last_20['volume']
                    avg_dollar_volume = dollar_vols.mean() 
            
            # Detailed Strat History
            curr_cond = strat
            prev_cond_1 = strat_prev
            prev_cond_2 = strat_prev2
            
            # Combo Order: Prev[2] -> Prev[1] -> Current
            pattern_str = f"{strat_prev}-{strat}"
            full_pattern = f"{strat_prev2}-{strat_prev}-{strat}"
            
            # --- STATUS: IN FORCE (Signal) ---
            status = "In Force"
            
            # Helper to check structural type (ignoring color)
            def is_2u(s): return s.startswith('2u')
            def is_2d(s): return s.startswith('2d')
            def is_3(s): return s.startswith('3')
            def is_1(s): return s == '1'
            
            # 1. 2d Green (Generic)
            if strat == '2dG':
                 alerts.append(create_alert(ticker, f"2d Green {tf}", tf, curr, pattern_str, status, ftfc, strat, tto,
                                   adr, gap, change_from_open, wtd, mtd, qtd, ytd, perf_3m, avg_dollar_volume, 
                                   rs_1d, rs_1w, rs_1m, rs_3m,
                                   prev_cond_1, prev_cond_2, curr_cond))

            # 2-2 Reversals (In Force)
            if is_2u(strat) and is_2d(strat_prev):
                alerts.append(create_alert(ticker, "Rev Strat (2d-2u)", tf, curr, pattern_str, status, ftfc, strat, tto,
                                           adr, gap, change_from_open, wtd, mtd, qtd, ytd, perf_3m, avg_dollar_volume, 
                                           rs_1d, rs_1w, rs_1m, rs_3m,
                                           prev_cond_1, prev_cond_2, curr_cond))
            if is_2d(strat) and is_2u(strat_prev):
                alerts.append(create_alert(ticker, "Rev Strat (2u-2d)", tf, curr, pattern_str, status, ftfc, strat, tto,
                                           adr, gap, change_from_open, wtd, mtd, qtd, ytd, perf_3m, avg_dollar_volume, 
                                           rs_1d, rs_1w, rs_1m, rs_3m,
                                           prev_cond_1, prev_cond_2, curr_cond))
                
            # 2-1-2 Reversals (In Force)
            if is_2u(strat) and is_1(strat_prev) and is_2d(strat_prev2):
                alerts.append(create_alert(ticker, "2-1-2 Bullish", tf, curr, full_pattern, status, ftfc, strat, tto,
                                           adr, gap, change_from_open, wtd, mtd, qtd, ytd, perf_3m, avg_dollar_volume, 
                                           rs_1d, rs_1w, rs_1m, rs_3m,
                                           prev_cond_1, prev_cond_2, curr_cond))
            if is_2d(strat) and is_1(strat_prev) and is_2u(strat_prev2):
                alerts.append(create_alert(ticker, "2-1-2 Bearish", tf, curr, full_pattern, status, ftfc, strat, tto,
                                           adr, gap, change_from_open, wtd, mtd, qtd, ytd, perf_3m, avg_dollar_volume, 
                                           rs_1d, rs_1w, rs_1m, rs_3m,
                                           prev_cond_1, prev_cond_2, curr_cond))
                
            # 3-1-2 Reversals (In Force)
            if is_2u(strat) and is_1(strat_prev) and is_3(strat_prev2):
                alerts.append(create_alert(ticker, "3-1-2 Bullish", tf, curr, full_pattern, status, ftfc, strat, tto,
                                           adr, gap, change_from_open, wtd, mtd, qtd, ytd, perf_3m, avg_dollar_volume, 
                                           rs_1d, rs_1w, rs_1m, rs_3m,
                                           prev_cond_1, prev_cond_2, curr_cond))
            if is_2d(strat) and is_1(strat_prev) and is_3(strat_prev2):
                alerts.append(create_alert(ticker, "3-1-2 Bearish", tf, curr, full_pattern, status, ftfc, strat, tto,
                                           adr, gap, change_from_open, wtd, mtd, qtd, ytd, perf_3m, avg_dollar_volume, 
                                           rs_1d, rs_1w, rs_1m, rs_3m,
                                           prev_cond_1, prev_cond_2, curr_cond))

            # --- STATUS: SETUP (Actionable Next) ---
            status = "Setup"
            
            # Inside Bar (1)
            if is_1(strat):
                alerts.append(create_alert(ticker, "Inside Bar", tf, curr, strat, status, ftfc, strat, tto,
                                           adr, gap, change_from_open, wtd, mtd, qtd, ytd, perf_3m, avg_dollar_volume, 
                                           rs_1d, rs_1w, rs_1m, rs_3m,
                                           prev_cond_1, prev_cond_2, curr_cond))
                
            # Hammer / Shooter (Shape)
            if is_hammer(curr):
                alerts.append(create_alert(ticker, "Hammer", tf, curr, "Hammer", status, ftfc, strat, tto,
                                           adr, gap, change_from_open, wtd, mtd, qtd, ytd, perf_3m, avg_dollar_volume, 
                                           rs_1d, rs_1w, rs_1m, rs_3m,
                                           prev_cond_1, prev_cond_2, curr_cond))
            if is_shooter(curr):
                alerts.append(create_alert(ticker, "Shooter", tf, curr, "Shooter", status, ftfc, strat, tto,
                                           adr, gap, change_from_open, wtd, mtd, qtd, ytd, perf_3m, avg_dollar_volume, 
                                           rs_1d, rs_1w, rs_1m, rs_3m,
                                           prev_cond_1, prev_cond_2, curr_cond))

    except Exception as e:
        print(f"Error scanning {ticker}: {e}")
    finally:
        session.close()
        
    return alerts

def create_alert(ticker, type_, tf, row, pattern, status, ftfc, candle_state, tto=0,
                 adr=0, gap=0, change_from_open=0, wtd=0, mtd=0, qtd=0, ytd=0, perf_3m=0, avg_dollar_volume=0,
                 rs_1d=0, rs_1w=0, rs_1m=0, rs_3m=0,
                 prev_cond_1="", prev_cond_2="", curr_cond=""):
    return {
        "ticker": ticker,
        "type": type_,
        "timeframe": tf,
        "price": row['close'],
        "desc": f"{type_} ({status})",
        "pattern": pattern,
        "change_pct": ((row['close'] - row['open']) / row['open']) * 100,
        "volume": row['volume'],
        "status": status,
        "ftfc": ftfc,
        "tto": tto,
        "candle_state": candle_state,
        "industry": "Tech", # Mock for now, would need sector data
        "adr": adr,
        "gap": gap,
        "change_from_open": change_from_open,
        "wtd": wtd,
        "mtd": mtd,
        "qtd": qtd,
        "ytd": ytd,
        "perf_3m": perf_3m,
        "avg_dollar_volume": avg_dollar_volume,
        "rs_1d": rs_1d,
        "rs_1w": rs_1w,
        "rs_1m": rs_1m,
        "rs_3m": rs_3m,
        "prev_cond_1": prev_cond_1,
        "prev_cond_2": prev_cond_2,
        "curr_cond": curr_cond
    }
