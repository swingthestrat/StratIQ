from database import Session, Theme, ThemeTicker

def get_theme_alerts(all_alerts):
    """
    Aggregates individual ticker alerts into theme-level alerts.
    all_alerts: list of dicts {ticker, type, timeframe, price, desc}
    """
    session = Session()
    theme_alerts = []
    
    try:
        # Map ticker -> [themes]
        ticker_themes = {}
        themes = session.query(Theme).all()
        
        for theme in themes:
            tickers = session.query(ThemeTicker).filter_by(theme_id=theme.id).all()
            for t in tickers:
                if t.ticker not in ticker_themes: ticker_themes[t.ticker] = []
                ticker_themes[t.ticker].append(theme.name)
        
        # Group alerts by Theme + Alert Type
        # Key: (Theme Name, Alert Type) -> [Tickers]
        grouped = {}
        
        for alert in all_alerts:
            ticker = alert['ticker']
            alert_type = alert['type']
            
            if ticker in ticker_themes:
                for theme_name in ticker_themes[ticker]:
                    key = (theme_name, alert_type)
                    if key not in grouped: grouped[key] = []
                    grouped[key].append(ticker)
        
        # Generate Alerts
        for (theme_name, alert_type), tickers in grouped.items():
            # Threshold: Alert if at least 1 ticker (or configurable % later)
            # For now, just report if > 0
            
            count = len(tickers)
            # Get total tickers in theme for context
            theme_obj = session.query(Theme).filter_by(name=theme_name).first()
            total = session.query(ThemeTicker).filter_by(theme_id=theme_obj.id).count()
            
            theme_alerts.append({
                "title": f"{theme_name} Theme Alert",
                "desc": f"**Setup:** {alert_type}\\n**Tickers:** {', '.join(tickers)} ({count}/{total} in theme)",
                "color": 16776960 # Yellow
            })
            
    except Exception as e:
        print(f"Error aggregating themes: {e}")
    finally:
        session.close()
        
    return theme_alerts
