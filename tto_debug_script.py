from database import Session, OHLCV
import pandas as pd

def check_tto(ticker):
    session = Session()
    try:
        # Fetch all latest candles for the ticker
        # We need the latest date for each timeframe
        # Actually, let's just fetch all rows for the ticker and group by timeframe to find the latest date
        
        # Efficient way:
        # We need to know what "latest" means. Usually the max date for that timeframe.
        
        rows = session.query(OHLCV).filter_by(symbol=ticker).all()
        if not rows:
            print(f"No data for {ticker}")
            return

        df = pd.DataFrame([r.__dict__ for r in rows])
        
        # Filter for latest date per timeframe
        # For each timeframe, find max date
        latest_candles = []
        for tf in df['timeframe'].unique():
            df_tf = df[df['timeframe'] == tf]
            latest_date = df_tf['date'].max()
            latest_row = df_tf[df_tf['date'] == latest_date].iloc[0]
            latest_candles.append(latest_row)
            
        df_latest = pd.DataFrame(latest_candles)
        
        tfs_order = ['1D', '2D', '3D', '5D', '1W', '2W', '3W', '1M', '1Q', '1Y']
        
        colors = {}
        print(f"\nAnalysis for {ticker}:")
        print(f"{'Timeframe':<10} | {'Date':<12} | {'Open':<10} | {'Close':<10} | {'Color'}")
        print("-" * 60)
        
        for tf in tfs_order:
            row = df_latest[df_latest['timeframe'] == tf]
            if not row.empty:
                r = row.iloc[0]
                color = "Green" if r['close'] > r['open'] else "Red" if r['close'] < r['open'] else "Doji"
                val = 1 if color == "Green" else -1 if color == "Red" else 0
                colors[tf] = val
                print(f"{tf:<10} | {str(r['date']):<12} | {r['open']:<10.2f} | {r['close']:<10.2f} | {color}")
            else:
                colors[tf] = 0
                print(f"{tf:<10} | {'MISSING':<12} | {'-':<10} | {'-':<10} | -")

        # Check TTO Logic
        print("\nChecking Blocks:")
        tto_found = False
        for i in range(len(tfs_order) - 3):
            block_tfs = tfs_order[i:i+4]
            block_vals = [colors[tf] for tf in block_tfs]
            
            block_str = ", ".join([f"{tf}({colors[tf]})" for tf in block_tfs])
            
            if 0 in block_vals:
                print(f"Block {block_tfs}: SKIPPED (Missing Data)")
                continue
                
            first = block_vals[0]
            last = block_vals[-1]
            
            if first != last:
                print(f"Block {block_tfs}: FAIL (First != Last)")
                continue
                
            count_same = block_vals.count(first)
            if count_same >= 3:
                print(f"Block {block_tfs}: PASS (Count {count_same}/4, First==Last)")
                tto_found = True
            else:
                print(f"Block {block_tfs}: FAIL (Count {count_same}/4)")
                
        print(f"\nFinal TTO Result: {tto_found}")

    finally:
        session.close()

if __name__ == "__main__":
    check_tto("INFY")
    check_tto("ALL")
