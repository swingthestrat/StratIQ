#!/usr/bin/env python3
"""
Update RS (Relative Strength vs SPY) values for existing alerts
"""
from database import Session, Alert, OHLCV
from datetime import datetime, timedelta
import pandas as pd

def calculate_rs_metrics(ticker_data, spy_data):
    """Calculate relative strength metrics vs SPY as RAW VALUES for percentile ranking"""
    rs_metrics = {}
    
    # Merge on date
    merged = ticker_data.merge(spy_data, on=['date', 'timeframe'], suffixes=('_ticker', '_spy'))
    
    if merged.empty:
        return {'rs_1d': 0, 'rs_1w': 0, 'rs_1m': 0, 'rs_3m': 0}
   
    # Calculate RS ratio
    merged['rs_ratio'] = merged['close_ticker'] / merged['close_spy']
    merged = merged.sort_values('date')
    
    # Calculate RAW RATIO CHANGES (not percentages) for percentile ranking later
    if len(merged) >= 2:
        rs_1d = (merged.iloc[-1]['rs_ratio'] / merged.iloc[-2]['rs_ratio']) - 1
        rs_metrics['rs_1d'] = rs_1d
    
    if len(merged) >= 5:
        rs_1w = (merged.iloc[-1]['rs_ratio'] / merged.iloc[-min(5, len(merged))]['rs_ratio']) - 1
        rs_metrics['rs_1w'] = rs_1w
    
    if len(merged) >= 21:
        rs_1m = (merged.iloc[-1]['rs_ratio'] / merged.iloc[-min(21, len(merged))]['rs_ratio']) - 1
        rs_metrics['rs_1m'] = rs_1m
    
    if len(merged) >= 63:
        rs_3m = (merged.iloc[-1]['rs_ratio'] / merged.iloc[-min(63, len(merged))]['rs_ratio']) - 1
        rs_metrics['rs_3m'] = rs_3m
    
    return rs_metrics

def main():
    session = Session()
    today = datetime.now().date()
    
    try:
        # Get SPY data
        print("Fetching SPY data...")
        spy_query = session.query(OHLCV).filter_by(symbol='SPY').order_by(OHLCV.date)
        df_spy_all = pd.read_sql(spy_query.statement, session.bind)
        
        if df_spy_all.empty:
            print("ERROR: No SPY data found!")
            return
        
        print(f"Found {len(df_spy_all)} SPY records")
        
        # Get all alerts for today
        alerts = session.query(Alert).filter_by(date=today).all()
        print(f"Found {len(alerts)} alerts to update")
        
        updated_count = 0
        
        for alert in alerts:
            # Get ticker data
            ticker_query = session.query(OHLCV).filter_by(
                symbol=alert.ticker,
                timeframe=alert.timeframe
            ).order_by(OHLCV.date)
            
            df_ticker = pd.read_sql(ticker_query.statement, session.bind)
            
            if df_ticker.empty:
                continue
            
            # Filter SPY  data for same timeframe
            df_spy = df_spy_all[df_spy_all['timeframe'] == alert.timeframe].copy()
            
            if df_spy.empty:
                continue
            
            # Calculate RS metrics
            rs_metrics = calculate_rs_metrics(df_ticker, df_spy)
            
            # Update alert with RAW RS values (will convert to percentiles later)
            alert.rs_1d = rs_metrics.get('rs_1d', 0)
            alert.rs_1w = rs_metrics.get('rs_1w', 0)
            alert.rs_1m = rs_metrics.get('rs_1m', 0)
            alert.rs_3m = rs_metrics.get('rs_3m', 0)
            
            # Also calculate perf_3m and avg_dollar_volume
            if len(df_ticker) >= 63:
                perf_3m = ((df_ticker.iloc[-1]['close'] / df_ticker.iloc[-63]['close']) - 1) * 100
                alert.perf_3m = round(perf_3m, 2)
            
            if len(df_ticker) >= 20:
                recent_volume = df_ticker.tail(20).copy()
                recent_volume['dollar_vol'] = recent_volume['close'] * recent_volume['volume']
                alert.avg_dollar_volume = recent_volume['dollar_vol'].mean()
            
            updated_count += 1
            
            if updated_count % 100 == 0:
                print(f"Updated {updated_count}/{len(alerts)} alerts...")
                session.commit()
        
        # Commit after first pass
        session.commit()
        print(f"\n✓ First pass complete: {updated_count} alerts with raw RS values")
        
        # SECOND PASS: Convert raw RS values to percentiles
        print("\nCalculating percentile rankings...")
        for timeframe in ['1D', '1W', '2W', '3W', '1M', '1Q', '1Y']:
            # Get all alerts for this timeframe
            tf_alerts = session.query(Alert).filter_by(date=today, timeframe=timeframe).all()
            
            if not tf_alerts:
                continue
            
            # Extract RS values into lists (filter out None/0)
            rs_1d_vals = [a.rs_1d for a in tf_alerts if a.rs_1d is not None and a.rs_1d != 0]
            rs_1w_vals = [a.rs_1w for a in tf_alerts if a.rs_1w is not None and a.rs_1w != 0]
            rs_1m_vals = [a.rs_1m for a in tf_alerts if a.rs_1m is not None and a.rs_1m != 0]
            rs_3m_vals = [a.rs_3m for a in tf_alerts if a.rs_3m is not None and a.rs_3m != 0]
            
            # Sort to calculate percentiles
            rs_1d_sorted = sorted(rs_1d_vals)
            rs_1w_sorted = sorted(rs_1w_vals)
            rs_1m_sorted = sorted(rs_1m_vals)
            rs_3m_sorted = sorted(rs_3m_vals)
            
            # Convert each alert's RS to percentile
            for alert in tf_alerts:
                if alert.rs_1d and alert.rs_1d != 0 and len(rs_1d_sorted) > 0:
                    rank = sum(1 for v in rs_1d_sorted if v < alert.rs_1d)
                    alert.rs_1d = round((rank / len(rs_1d_sorted)) * 100, 1)
                else:
                    alert.rs_1d = None
                    
                if alert.rs_1w and alert.rs_1w != 0 and len(rs_1w_sorted) > 0:
                    rank = sum(1 for v in rs_1w_sorted if v < alert.rs_1w)
                    alert.rs_1w = round((rank / len(rs_1w_sorted)) * 100, 1)
                else:
                    alert.rs_1w = None
                    
                if alert.rs_1m and alert.rs_1m != 0 and len(rs_1m_sorted) > 0:
                    rank = sum(1 for v in rs_1m_sorted if v < alert.rs_1m)
                    alert.rs_1m = round((rank / len(rs_1m_sorted)) * 100, 1)
                else:
                    alert.rs_1m = None
                    
                if alert.rs_3m and alert.rs_3m != 0 and len(rs_3m_sorted) > 0:
                    rank = sum(1 for v in rs_3m_sorted if v < alert.rs_3m)
                    alert.rs_3m = round((rank / len(rs_3m_sorted)) * 100, 1)
                else:
                    alert.rs_3m = None
            
            print(f"  {timeframe}: Converted {len(tf_alerts)} alerts to percentiles")
            session.commit()
        
        print(f"\n✓ Successfully updated {updated_count} alerts with RS percentiles!")
        
    except Exception as e:
        print(f"ERROR: {e}")
        session.rollback()
    finally:
        session.close()

if __name__ == '__main__':
    main()
