import pandas as pd
from database import Session, Theme, ThemeTicker, init_db
import re

# Hardcoded Top Holdings (Fallback)
ETF_HOLDINGS = {
    "SPY": ["AAPL", "MSFT", "NVDA", "AMZN", "GOOGL", "META", "TSLA", "BRK-B", "LLY", "AVGO", "JPM", "V", "UNH", "XOM", "MA", "HD", "PG", "COST", "JNJ", "ABBV"],
    "QQQ": ["AAPL", "MSFT", "NVDA", "AMZN", "GOOGL", "META", "TSLA", "AVGO", "COST", "PEP", "NFLX", "AMD", "ADBE", "CSCO", "TMUS", "INTC", "CMCSA", "INTU", "AMGN", "TXN"],
    "DIA": ["UNH", "GS", "MSFT", "HD", "CAT", "CRM", "V", "MCD", "AMGN", "TRV", "BA", "HON", "AXP", "CVX", "JPM", "IBM", "AAPL", "PG", "WMT", "JNJ"],
    "XLK": ["MSFT", "AAPL", "NVDA", "AVGO", "ADBE", "CRM", "AMD", "ACN", "CSCO", "ORCL", "INTU", "INTC", "QCOM", "TXN", "IBM", "AMAT", "NOW", "LRCX", "ADI", "MU"],
    "XLV": ["LLY", "UNH", "JNJ", "ABBV", "MRK", "TMO", "AMGN", "ABT", "DHR", "PFE", "ISRG", "VRTX", "ELV", "SYK", "REGN", "BSX", "GILD", "BMY", "ZTS", "CI"],
    "XLI": ["GE", "CAT", "HON", "UNP", "UBER", "ETN", "RTX", "DE", "ADP", "LMT", "WM", "UPS", "BA", "GD", "ITW", "PH", "TDG", "EMR", "CSX", "NSC"],
    "XLP": ["PG", "COST", "PEP", "WMT", "KO", "PM", "MO", "MDLZ", "CL", "TGT", "KMB", "GIS", "EL", "SYY", "STZ", "KR", "HSY", "K", "MKC", "CLX"],
    "XLF": ["BRK-B", "JPM", "V", "MA", "BAC", "WFC", "SPGI", "GS", "AXP", "MS", "BLK", "C", "CB", "MMC", "PGR", "SCHW", "ICE", "CME", "AON", "USB"],
    "ARKK": ["TSLA", "COIN", "ROKU", "UIPath", "CRSP", "SQ", "RBLX", "ZM", "DKNG", "PATH", "U", "PLTR", "HOOD", "TWLO", "NTLA", "BEAM", "EXAS", "PACB", "TXG", "DNA"],
    "ARKG": ["CRSP", "NTLA", "PACB", "TWST", "EXAS", "TXG", "DNA", "BEAM", "PATH", "SDGR", "RXRX", "IONS", "VRTX", "REGN", "ILMN", "NVTA", "ADPT", "FATE", "EDIT", "CLLS"],
    "HACK": ["CRWD", "PANW", "FTNT", "CSCO", "NET", "OKTA", "ZS", "CYBR", "TENB", "QLYS", "SENT", "RPD", "VRNS", "GEN", "CHKP", "AKAM", "FEYE", "MIME", "PFPT", "SAIL"],
    "SMH": ["NVDA", "TSM", "AVGO", "AMD", "INTC", "TXN", "QCOM", "AMAT", "LRCX", "MU", "ADI", "KLAC", "MRVL", "STM", "NXPI", "MCHP", "ON", "MPWR", "ENTG", "TER"],
    "IWM": ["MSTR", "CVNA", "FTNT", "VST", "ELF", "DKNG", "HOOD", "RIVN", "AFRM", "SOFI", "MARA", "CLSK", "RIOT", "HUT", "BITF", "GME", "AMC", "CHPT", "LCID", "PLUG"]
}

def parse_txt_file(file_path):
    """Parse a text file (Universe.txt or ThematicETFs.txt) and extract tickers organized by theme"""
    themes = {}
    
    try:
        with open(file_path, 'r') as f:
            content = f.read()
            
        # Split by ### to get sections
        sections = content.split('###')
        
        for section in sections:
            if not section.strip():
                continue
                
            lines = section.strip().split(',', 1)
            if len(lines) < 2:
                continue
                
            theme_name = lines[0].strip()
            tickers_str = lines[1]
            
            # Extract tickers (remove exchange prefixes like NASDAQ:, NYSE:)
            ticker_list = []
            for item in tickers_str.split(','):
                item = item.strip()
                # Remove exchange prefix
                if ':' in item:
                    ticker = item.split(':')[1].strip()
                else:
                    ticker = item.strip()
                
                # Skip empty or theme headers
                if ticker and not ticker.startswith('###'):
                    ticker_list.append(ticker.upper())
            
            if ticker_list:
                themes[theme_name] = ticker_list
                
        print(f"Parsed {len(themes)} themes from {file_path}")
        return themes
        
    except Exception as e:
        print(f"Error parsing {file_path}: {e}")
        return {}

def parse_constituent_file(file_path):
    """Parses a constituent file (space or comma separated)."""
    try:
        with open(file_path, 'r') as f:
            content = f.read()
        # Split by comma or whitespace
        tickers = [t.strip() for t in re.split(r'[,\s]+', content) if t.strip()]
        # Remove trailing dots if any (user input had 'DOW .')
        tickers = [t.rstrip('.') for t in tickers]
        return tickers
    except FileNotFoundError:
        print(f"File not found: {file_path}")
        return []

def update_universe():
    init_db()
    session = Session()
    
    try:
        total_added = 0
        
        # 1. Add ETF Holdings
        for etf_symbol, tickers in ETF_HOLDINGS.items():
            print(f"Processing {etf_symbol}...")
            
            # Create/Get Theme (ETF Name)
            theme = session.query(Theme).filter_by(name=etf_symbol).first()
            if not theme:
                theme = Theme(name=etf_symbol, description=f"Constituents of {etf_symbol}")
                session.add(theme)
                session.flush()
            
            # Add Tickers
            count = 0
            for ticker in tickers:
                # Basic cleanup
                ticker = ticker.strip().upper()
                exists = session.query(ThemeTicker).filter_by(theme_id=theme.id, ticker=ticker).first()
                if not exists:
                    session.add(ThemeTicker(theme_id=theme.id, ticker=ticker))
                    count += 1
        # 1. Parse ThematicETFs.txt (Existing logic)
        thematic_etfs = parse_txt_file("Universe/ThematicETFs.txt")
        
        # 2. Parse Constituent Files (New logic)
        # Map Theme Name -> File Path
        constituent_files = {
            "SPY": "Universe/SPYConstituents",
            "QQQ": "Universe/QQQConstituents",
            "DIA": "Universe/DIAconstituends ", # Note trailing space
            "IWM": "Universe/IWMConstituents",
            "SECTORS": "Universe/Sectors",
            "IPO": "Universe/IPO"
        }
        
        # Combine all data
        # Start with thematic ETFs
        all_themes = thematic_etfs.copy()
        
        # Add/Override with constituent files
        for theme_name, file_path in constituent_files.items():
            tickers = parse_constituent_file(file_path)
            if tickers:
                all_themes[theme_name] = tickers
                print(f"Loaded {len(tickers)} tickers for {theme_name} from {file_path}")
            else:
                # Fallback to hardcoded if file empty/missing (only for those in ETF_HOLDINGS)
                if theme_name in ETF_HOLDINGS and theme_name not in all_themes:
                     all_themes[theme_name] = ETF_HOLDINGS[theme_name]
                     print(f"Using fallback holdings for {theme_name}")

        # 3. Update Database
        for theme_name, tickers in all_themes.items():
            # Create/Get Theme
            theme = session.query(Theme).filter_by(name=theme_name).first()
            if not theme:
                theme = Theme(name=theme_name)
                session.add(theme)
                session.flush() # Use flush to get theme.id before commit
            else:
                # Clear existing tickers for this theme to ensure it matches the file exactly
                # This fixes the issue where old tickers (like KSS in SPY) remain.
                session.query(ThemeTicker).filter_by(theme_id=theme.id).delete()
                session.flush()
                
            # Add new tickers
            new_count = 0
            for ticker in tickers:
                tt = ThemeTicker(theme_id=theme.id, ticker=ticker)
                session.add(tt)
                new_count += 1
            
            if new_count > 0:
                print(f"  Updated {theme_name} with {new_count} tickers")
            
            if new_count > 0:
                print(f"  Added {new_count} new tickers to {theme_name}")
                
        session.commit()
        print(f"Universe update complete.")
        
    except Exception as e:
        print(f"Error updating universe: {e}")
        session.rollback()
    finally:
        session.close()

if __name__ == "__main__":
    init_db()
    update_universe()
