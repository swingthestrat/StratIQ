import yfinance as yf
import pandas as pd
import csv
from datetime import datetime
from database import Session, OHLCV, Theme, ThemeTicker, init_db
from sqlalchemy.dialects.sqlite import insert

UNIVERSE_FILE = '/Users/nigeljohnson/AntiGravity/StratIQ/Themes - Sheet1.csv'

def get_universe():
    tickers = set()
    themes = {} # theme_name -> [tickers]
    
    try:
        with open(UNIVERSE_FILE, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                ticker = row.get('Ticker', '').strip()
                theme = row.get('Theme/Category', '').strip()
                if ticker:
                    tickers.add(ticker)
                    if theme:
                        if theme not in themes: themes[theme] = []
                        themes[theme].append(ticker)
    except Exception as e:
        print(f"Error reading universe: {e}")
    return list(tickers), themes

def sync_themes(themes_dict):
    session = Session()
    try:
        for theme_name, tickers in themes_dict.items():
            # Create/Get Theme
            theme = session.query(Theme).filter_by(name=theme_name).first()
            if not theme:
                theme = Theme(name=theme_name, description="Imported from CSV")
                session.add(theme)
                session.flush() # Get ID
            
            # Sync Tickers
            for ticker in tickers:
                exists = session.query(ThemeTicker).filter_by(theme_id=theme.id, ticker=ticker).first()
                if not exists:
                    session.add(ThemeTicker(theme_id=theme.id, ticker=ticker))
        session.commit()
    except Exception as e:
        print(f"Error syncing themes: {e}")
        session.rollback()
    finally:
        session.close()

def aggregate_trading_days(df, days):
    """
    Aggregate trading days into N-day candles.
    Anchored to the first trading day of each year to match TradingView.
    """
    # Ensure sorted by date
    df = df.sort_index()
    
    # Reset index to work with DataFrame
    df_reset = df.reset_index()
    df_reset['year'] = df_reset['Date'].dt.year
    
    # Group by year and calculate offset from first trading day of that year
    def assign_groups(year_df):
        year_df = year_df.sort_values('Date').copy()
        year_df = year_df.reset_index(drop=True)
        # Index within the year (0, 1, 2, ...)
        # Group every 'days' rows
        year_df['group_id'] = year_df.index // days
        return year_df
    
    df_grouped = df_reset.groupby('year', group_keys=False).apply(assign_groups)
    
    # Create unique group identifier across years
    df_grouped['unique_group'] = df_grouped['year'].astype(str) + '_' + df_grouped['group_id'].astype(str)
    
    # Aggregation rules
    agg_dict = {
        'Date': 'last',  # Use the last date in the group
        'Open': 'first',
        'High': 'max',
        'Low': 'min',
        'Close': 'last',
        'Volume': 'sum'
    }
    
    # Group and aggregate
    df_agg = df_grouped.groupby('unique_group').agg(agg_dict)
    
    # Set index and return
    df_agg.set_index('Date', inplace=True)
    
    return df_agg

def aggregate_data(df_daily):
    aggs = {}
    
    # Resampling rules for standard timeframes
    rules = {'Open': 'first', 'High': 'max', 'Low': 'min', 'Close': 'last', 'Volume': 'sum'}
    
    # 1D (Base)
    aggs['1D'] = df_daily
    
    # 2D - Use Trading Days
    aggs['2D'] = aggregate_trading_days(df_daily, 2)
    
    # 3D - Use Trading Days
    aggs['3D'] = aggregate_trading_days(df_daily, 3)
    
    # 5D - Use Trading Days (User request)
    aggs['5D'] = aggregate_trading_days(df_daily, 5)
    
    # Weekly (End on Friday)
    aggs['1W'] = df_daily.resample('W-FRI').agg(rules).dropna()
    
    # 2 Weeks
    aggs['2W'] = df_daily.resample('2W-FRI').agg(rules).dropna()
    
    # 3 Weeks
    aggs['3W'] = df_daily.resample('3W-FRI').agg(rules).dropna()
    
    # Monthly
    aggs['1M'] = df_daily.resample('ME').agg(rules).dropna()
    
    # Quarterly
    aggs['1Q'] = df_daily.resample('QE').agg(rules).dropna()
    
    # Yearly
    aggs['1Y'] = df_daily.resample('YE').agg(rules).dropna()
    
    return aggs

def save_ohlcv(symbol, aggs):
    session = Session()
    try:
        for tf, df in aggs.items():
            for date, row in df.iterrows():
                stmt = insert(OHLCV).values(
                    symbol=symbol,
                    date=date.date(),
                    open=row['Open'],
                    high=row['High'],
                    low=row['Low'],
                    close=row['Close'],
                    volume=row['Volume'],
                    timeframe=tf
                )
                # Upsert
                stmt = stmt.on_conflict_do_update(
                    index_elements=['symbol', 'date', 'timeframe'],
                    set_=dict(
                        open=stmt.excluded.open,
                        high=stmt.excluded.high,
                        low=stmt.excluded.low,
                        close=stmt.excluded.close,
                        volume=stmt.excluded.volume
                    )
                )
                session.execute(stmt)
        session.commit()
    except Exception as e:
        print(f"Error saving {symbol}: {e}")
        session.rollback()
    finally:
        session.close()

from universe import update_universe

def run_ingestion():
    init_db()
    
    # 1. Update Universe from ETFs
    print("Updating Universe from ETFs...")
    update_universe()
    
    # 2. Fetch Data
    session = Session()
    tickers = [r.ticker for r in session.query(ThemeTicker).distinct(ThemeTicker.ticker).all()]
    session.close()
    
    print(f"Fetching data for {len(tickers)} tickers...")
    
    from sqlalchemy import func
    
    for i, ticker in enumerate(tickers):
        try:
            # Check for existing data to do incremental update
            session = Session()
            last_date = session.query(func.max(OHLCV.date)).filter_by(symbol=ticker).scalar()
            session.close()
            
            t = yf.Ticker(ticker)
            
            if last_date:
                # Fetch from last date (inclusive, upsert handles duplicates)
                # yfinance expects string or datetime
                print(f"[{i+1}/{len(tickers)}] Updating {ticker} from {last_date}...")
                df = t.history(start=str(last_date), interval="1d")
            else:
                # New ticker, fetch full history
                print(f"[{i+1}/{len(tickers)}] Initial fetch for {ticker} (5y)...")
                df = t.history(period="5y", interval="1d")
            
            if len(df) > 0:
                aggs = aggregate_data(df)
                save_ohlcv(ticker, aggs)
            else:
                print(f"[{i+1}/{len(tickers)}] No data for {ticker}")
                
        except Exception as e:
            print(f"Error processing {ticker}: {e}")

if __name__ == "__main__":
    run_ingestion()
