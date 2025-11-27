from database import Session, ThemeTicker, Alert, init_db
from engine import run_scan
from datetime import datetime
import sys

def save_alerts(alerts):
    session = Session()
    today = datetime.now().date()
    try:
        count = 0
        for a in alerts:
            # Overwrite or add
            exists = session.query(Alert).filter_by(
                date=today, ticker=a['ticker'], type=a['type'], timeframe=a['timeframe']
            ).first()
            if not exists:
                alert = Alert(
                    date=today, ticker=a['ticker'], type=a['type'], timeframe=a['timeframe'],
                    price=a['price'], desc=a['desc'], color=0, is_theme=0,
                    pattern=a['pattern'], change_pct=a['change_pct'], volume=a['volume'],
                    status=a['status'], candle_state=a['candle_state'], ftfc=a['ftfc'], tto=a.get('tto', 0),
                    industry=a.get('industry', ''), adr=a.get('adr', 0), gap=a.get('gap', 0),
                    change_from_open=a.get('change_from_open', 0),
                    wtd=a.get('wtd', 0), mtd=a.get('mtd', 0), qtd=a.get('qtd', 0), ytd=a.get('ytd', 0),
                    perf_3m=a.get('perf_3m', 0), avg_dollar_volume=a.get('avg_dollar_volume', 0),
                    rs_1d=a.get('rs_1d', 0), rs_1w=a.get('rs_1w', 0), rs_1m=a.get('rs_1m', 0), rs_3m=a.get('rs_3m', 0),
                    prev_cond_1=a.get('prev_cond_1', ''), prev_cond_2=a.get('prev_cond_2', ''), curr_cond=a.get('curr_cond', '')
                )
                session.add(alert)
                count += 1
        session.commit()
        return count
    except Exception as e:
        print(f"Error saving alerts: {e}")
        return 0
    finally:
        session.close()

def get_spy_data():
    session = Session()
    try:
        from database import OHLCV
        import pandas as pd
        
        print("Fetching SPY data for RS calculation...")
        query = session.query(OHLCV).filter_by(symbol='SPY').order_by(OHLCV.date)
        df_spy_all = pd.read_sql(query.statement, session.bind)
        
        if df_spy_all.empty:
            print("WARNING: No SPY data found! RS metrics will be 0.")
            return {}
            
        spy_data = {}
        for tf in df_spy_all['timeframe'].unique():
            spy_data[tf] = df_spy_all[df_spy_all['timeframe'] == tf].copy()
            
        return spy_data
    finally:
        session.close()

def main():
    print("Initializing DB...")
    init_db()
    
    # Fetch SPY Data once
    spy_data = get_spy_data()
    
    session = Session()
    tickers = [r.ticker for r in session.query(ThemeTicker).distinct(ThemeTicker.ticker).all()]
    session.close()
    
    print(f"Found {len(tickers)} tickers to scan.")
    
    total_alerts = 0
    for i, ticker in enumerate(tickers):
        try:
            alerts = run_scan(ticker, spy_data)
            saved = save_alerts(alerts)
            total_alerts += saved
            if (i+1) % 10 == 0:
                print(f"[{i+1}/{len(tickers)}] Scanned {ticker}. Saved {saved} alerts. Total: {total_alerts}")
        except Exception as e:
            print(f"Error scanning {ticker}: {e}")
            
    print(f"Scan complete. Total alerts saved: {total_alerts}")

if __name__ == "__main__":
    main()
