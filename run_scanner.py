import csv
import requests
import yfinance as yf
import time
import pandas as pd
import numpy as np
from ta.volatility import BollingerBands, KeltnerChannel
from ta.trend import MACD, EMAIndicator
from ta.momentum import RSIIndicator

# Configuration
UNIVERSE_FILE = '/Users/nigeljohnson/AntiGravity/StratIQ/Themes - Sheet1.csv'
WEBHOOK_FILE = '/Users/nigeljohnson/AntiGravity/StratIQ/Discord/webhook'

def get_webhook_url():
    try:
        with open(WEBHOOK_FILE, 'r') as f:
            return f.read().strip()
    except Exception as e:
        print(f"Error reading webhook file: {e}")
        return None

def get_universe():
    tickers = []
    try:
        with open(UNIVERSE_FILE, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                if 'Ticker' in row and row['Ticker']:
                    tickers.append(row['Ticker'].strip())
    except Exception as e:
        print(f"Error reading universe file: {e}")
    return tickers

def get_strat_candle(curr, prev):
    """
    Determines the Strat candle type: 1 (Inside), 2u (Up), 2d (Down), 3 (Outside).
    Also returns '2uR' (Red 2u) and '2dG' (Green 2d).
    """
    h, l, o, c = curr['High'], curr['Low'], curr['Open'], curr['Close']
    ph, pl = prev['High'], prev['Low']
    
    inside = h <= ph and l >= pl
    outside = h > ph and l < pl
    up = h > ph and l >= pl
    down = l < pl and h <= ph
    
    if inside: return '1'
    if outside: return '3' # Simplified 3 for now, logic can be expanded
    if up: return '2u'
    if down: return '2d'
    return '?'

def is_green(row):
    return row['Close'] > row['Open']

def calculate_alma(series, window=9, sigma=6, offset=0.85):
    """
    Calculates Arnaud Legoux Moving Average (ALMA).
    """
    m = offset * (window - 1)
    s = window / sigma
    w = np.exp(-((np.arange(window) - m) ** 2) / (2 * s * s))
    w = w / w.sum()
    return series.rolling(window).apply(lambda x: (x * w).sum(), raw=True)

def check_alerts(ticker):
    try:
        # Fetch data for multiple timeframes
        # We need Daily, Weekly, Monthly
        # Fetching 1 year of daily data to calculate indicators
        t = yf.Ticker(ticker)
        df_d = t.history(period="1y", interval="1d")
        
        if len(df_d) < 50: return [] # Not enough data

        # Resample for Weekly and Monthly
        df_w = df_d.resample('W').agg({'Open': 'first', 'High': 'max', 'Low': 'min', 'Close': 'last'})
        df_m = df_d.resample('ME').agg({'Open': 'first', 'High': 'max', 'Low': 'min', 'Close': 'last'})

        # Ensure we have enough data
        if len(df_w) < 2 or len(df_m) < 2: return []

        # --- Indicators (Daily) ---
        # ALMA
        df_d['ALMA'] = calculate_alma(df_d['Close'], window=50)
        
        # Squeeze (BB vs KC)
        bb = BollingerBands(close=df_d['Close'], window=20, window_dev=2.0)
        kc = KeltnerChannel(high=df_d['High'], low=df_d['Low'], close=df_d['Close'], window=20, window_atr=10)
        
        df_d['BB_Upper'] = bb.bollinger_hband()
        df_d['BB_Lower'] = bb.bollinger_lband()
        df_d['KC_Upper'] = kc.keltner_channel_hband()
        df_d['KC_Lower'] = kc.keltner_channel_lband()
        
        # IML Indicators
        # EMA 6 (Trigger), EMA 21 (Trend)
        df_d['EMA6'] = EMAIndicator(close=df_d['Close'], window=6).ema_indicator()
        df_d['EMA21'] = EMAIndicator(close=df_d['Close'], window=21).ema_indicator()
        
        # MACD (6, 20, 9)
        macd = MACD(close=df_d['Close'], window_slow=20, window_fast=6, window_sign=9)
        df_d['MACD_Hist'] = macd.macd_diff()

        # --- Current State ---
        curr_d = df_d.iloc[-1]
        prev_d = df_d.iloc[-2]
        
        curr_w = df_w.iloc[-1]
        prev_w = df_w.iloc[-2]
        
        curr_m = df_m.iloc[-1]
        prev_m = df_m.iloc[-2]

        # Strat Candles
        strat_d = get_strat_candle(curr_d, prev_d)
        strat_w = get_strat_candle(curr_w, prev_w)
        strat_m = get_strat_candle(curr_m, prev_m)
        
        strat_m_prev = get_strat_candle(prev_m, df_m.iloc[-3]) if len(df_m) > 2 else '?'

        # --- Alert Logic ---
        alerts = []
        price = curr_d['Close']
        
        # 1. 2d Green Month (2dgM)
        # Monthly is 2d AND Green
        is_2d_m = strat_m == '2d'
        is_green_m = is_green(curr_m)
        # Daily confirmation: 2u or 2dG or 3u (Bullish)
        is_bull_d = strat_d in ['2u', '3'] or (strat_d == '2d' and is_green(curr_d))
        
        if is_2d_m and is_green_m and is_bull_d:
            alerts.append({
                "title": "2d Green Month (2dgM)",
                "color": 65280, # Green
                "desc": f"**Monthly:** 2d Green\\n**Daily:** {strat_d}\\n**Price:** ${price:.2f}"
            })

        # 2. Inside Month Break (IMBO)
        # Prev Month was 1, Current is 2u or 3
        is_inside_prev = strat_m_prev == '1'
        is_break_curr = strat_m in ['2u', '3']
        
        if is_inside_prev and is_break_curr and is_bull_d:
            alerts.append({
                "title": "Inside Month Break (IMBO)",
                "color": 16705372, # Yellow/Orange
                "desc": f"**Monthly:** Inside Break ({strat_m})\\n**Daily:** {strat_d}\\n**Price:** ${price:.2f}"
            })

        # 3. StratIQ Squeeze
        # BB inside KC (Tight) -> Fired (BB expands outside KC)
        # Simplified: Just check if BB was inside KC recently and now expanding?
        # Or just check "Fired" state: BB Upper > KC Upper OR BB Lower < KC Lower (Expansion)
        # AND check if it was tight before.
        # For now, let's stick to a basic Squeeze Fired check:
        # BB Width expanding after low volatility?
        # Let's use the logic: BB inside KC is Squeeze. Fired is when it breaks out.
        
        # Check previous bar for squeeze (BB inside KC)
        prev_squeeze = (prev_d['BB_Upper'] <= prev_d['KC_Upper']) and (prev_d['BB_Lower'] >= prev_d['KC_Lower'])
        # Check current bar for fire (BB outside KC)
        curr_fire = (curr_d['BB_Upper'] > curr_d['KC_Upper']) or (curr_d['BB_Lower'] < curr_d['KC_Lower'])
        
        if prev_squeeze and curr_fire:
             alerts.append({
                "title": "StratIQ Squeeze Fired",
                "color": 16711935, # Fuchsia
                "desc": f"**Volatility Expansion**\\n**Price:** ${price:.2f}"
            })

        # 4. Inmerelo (IML) Reclaim
        # Close > EMA6
        # Prior closes (e.g. 2) below EMA6
        # MACD Hist > 0
        
        reclaim = curr_d['Close'] > curr_d['EMA6']
        priors_below = (prev_d['Close'] < prev_d['EMA6']) # Simplified to 1 prior for now
        macd_bull = curr_d['MACD_Hist'] > 0
        
        if reclaim and priors_below and macd_bull:
             alerts.append({
                "title": "Inmerelo (IML) Reclaim",
                "color": 3066993, # Teal
                "desc": f"**Reclaim of EMA 6**\\n**MACD Bullish**\\n**Price:** ${price:.2f}"
            })

        return alerts

    except Exception as e:
        print(f"Error checking {ticker}: {e}")
        return []

def send_discord_alert(webhook_url, ticker, alert_data):
    data = {
        "embeds": [{
            "title": f"{ticker} - {alert_data['title']}",
            "description": alert_data['desc'],
            "color": alert_data['color']
        }]
    }
    try:
        requests.post(webhook_url, json=data)
        print(f"Sent {alert_data['title']} for {ticker}")
    except Exception as e:
        print(f"Error sending alert: {e}")

def main():
    print("Starting StratIQ Scanner...")
    webhook_url = get_webhook_url()
    if not webhook_url: return

    tickers = get_universe()
    print(f"Scanning {len(tickers)} tickers...")

    for ticker in tickers:
        alerts = check_alerts(ticker)
        for alert in alerts:
            print(f"MATCH: {ticker} - {alert['title']}")
            send_discord_alert(webhook_url, ticker, alert)
        time.sleep(0.1)

    print("Scan complete.")

if __name__ == "__main__":
    main()
