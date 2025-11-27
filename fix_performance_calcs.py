#!/usr/bin/env python3
"""
Fix WTD/MTD/QTD/YTD calculations to use calendar-based logic
"""
from database import Session, Alert, OHLCV
from datetime import datetime, timedelta
import pandas as pd

def get_calendar_based_performance(df_daily, current_date):
    """
    Calculate calendar-based performance metrics:
    - WTD: From this week's Monday open to current close
    - MTD: From this month's 1st trading day open to current close
    - QTD: From this quarter's 1st trading day open to current close
    - YTD: From this year's 1st trading day open to current close
    """
    if df_daily.empty:
        return {'wtd': 0, 'mtd': 0, 'qtd': 0, 'ytd': 0}
    
    # Ensure sorted by date
    df_daily = df_daily.sort_values('date')
    
    # Get current row
    current_row = df_daily[df_daily['date'] == current_date]
    if current_row.empty:
        current_row = df_daily.iloc[-1]
    else:
        current_row = current_row.iloc[0]
    
    current_close = current_row['close']
    current_dt = pd.to_datetime(current_row['date'])
    
    perf = {}
    
    # WTD: Find Monday of current week
    days_since_monday = current_dt.weekday()  # 0=Monday, 6=Sunday
    monday_date = current_dt - timedelta(days=days_since_monday)
    
    # Find first trading day >= Monday
    wtd_rows = df_daily[pd.to_datetime(df_daily['date']) >= monday_date]
    if not wtd_rows.empty:
        week_open = wtd_rows.iloc[0]['open']
        perf['wtd'] = ((current_close - week_open) / week_open) * 100
    else:
        perf['wtd'] = 0
    
    # MTD: Find 1st day of current month
    month_start = current_dt.replace(day=1)
    mtd_rows = df_daily[pd.to_datetime(df_daily['date']) >= month_start]
    if not mtd_rows.empty:
        month_open = mtd_rows.iloc[0]['open']
        perf['mtd'] = ((current_close - month_open) / month_open) * 100
    else:
        perf['mtd'] = 0
    
    # QTD: Find 1st day of current quarter
    quarter_month = ((current_dt.month - 1) // 3) * 3 + 1  # 1, 4, 7, 10
    quarter_start = current_dt.replace(month=quarter_month, day=1)
    qtd_rows = df_daily[pd.to_datetime(df_daily['date']) >= quarter_start]
    if not qtd_rows.empty:
        quarter_open = qtd_rows.iloc[0]['open']
        perf['qtd'] = ((current_close - quarter_open) / quarter_open) * 100
    else:
        perf['qtd'] = 0
    
    # YTD: Find 1st day of current year
    year_start = current_dt.replace(month=1, day=1)
    ytd_rows = df_daily[pd.to_datetime(df_daily['date']) >= year_start]
    if not ytd_rows.empty:
        year_open = ytd_rows.iloc[0]['open']
        perf['ytd'] = ((current_close - year_open) / year_open) * 100
    else:
        perf['ytd'] = 0
    
    return perf

def main():
    session = Session()
    today = datetime.now().date()
    
    try:
        # Get all alerts for today
        alerts = session.query(Alert).filter_by(date=today).all()
        print(f"Updating {len(alerts)} alerts with calendar-based performance...")
        
        updated_count = 0
        
        for alert in alerts:
            # Get daily data for this ticker
            query = session.query(OHLCV).filter_by(
                symbol=alert.ticker,
                timeframe='1D'
            ).order_by(OHLCV.date)
            
            df_daily = pd.read_sql(query.statement, session.bind)
            
            if df_daily.empty:
                continue
            
            # Calculate calendar-based performance
            perf = get_calendar_based_performance(df_daily, today)
            
            # Update alert
            alert.wtd = round(perf['wtd'], 2)
            alert.mtd = round(perf['mtd'], 2)
            alert.qtd = round(perf['qtd'], 2)
            alert.ytd = round(perf['ytd'], 2)
            
            # Also calculate change_from_open (today's daily candle)
            today_row = df_daily[df_daily['date'] == str(today)]
            if not today_row.empty:
                today_row = today_row.iloc[0]
                change_from_open = ((today_row['close'] - today_row['open']) / today_row['open']) * 100
                alert.change_from_open = round(change_from_open, 2)
            
            updated_count += 1
            
            if updated_count % 100 == 0:
                print(f"Updated {updated_count}/{len(alerts)} alerts...")
                session.commit()
        
        session.commit()
        print(f"\n✓ Successfully updated {updated_count} alerts with calendar-based performance!")
        
    except Exception as e:
        print(f"ERROR: {e}")
        import traceback
        traceback.print_exc()
        session.rollback()
    finally:
        session.close()

if __name__ == '__main__':
    main()
