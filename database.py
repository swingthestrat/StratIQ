from sqlalchemy import create_engine, Column, Integer, String, Float, Date, UniqueConstraint
from sqlalchemy.orm import declarative_base, sessionmaker
import os

Base = declarative_base()

class OHLCV(Base):
    __tablename__ = 'ohlcv'
    id = Column(Integer, primary_key=True)
    symbol = Column(String, index=True)
    date = Column(Date, index=True)
    open = Column(Float)
    high = Column(Float)
    low = Column(Float)
    close = Column(Float)
    volume = Column(Float)
    timeframe = Column(String, index=True) # 1D, 2D, 1W, 1M, etc.

    __table_args__ = (UniqueConstraint('symbol', 'date', 'timeframe', name='uix_symbol_date_tf'),)

class Theme(Base):
    __tablename__ = 'themes'
    id = Column(Integer, primary_key=True)
    name = Column(String, unique=True)
    description = Column(String)

class ThemeTicker(Base):
    __tablename__ = 'theme_tickers'
    id = Column(Integer, primary_key=True)
    theme_id = Column(Integer, index=True)
    ticker = Column(String, index=True)
    
    __table_args__ = (UniqueConstraint('theme_id', 'ticker', name='uix_theme_ticker'),)

class Alert(Base):
    __tablename__ = 'alerts'
    id = Column(Integer, primary_key=True)
    date = Column(Date, index=True)
    ticker = Column(String, index=True)
    type = Column(String) # 2dgM, IMBO, etc.
    timeframe = Column(String)
    price = Column(Float)
    desc = Column(String)
    color = Column(Integer)
    is_theme = Column(Integer, default=0)
    
    # New Fields for 'Pure Price Action' UI
    pattern = Column(String) # e.g. "2d-2u"
    change_pct = Column(Float)
    volume = Column(Float)
    sector = Column(String)
    
    # New Fields for Redesign
    status = Column(String) # "In Force" or "Setup"
    candle_state = Column(String) # "1", "2u", "2d", "3"
    ftfc = Column(String) # "Bullish", "Bearish", "Mixed"
    tto = Column(Integer, default=0) # 1 if TTO condition met, else 0
    
    # New Fields for 'Swing The Strat' UI
    industry = Column(String)
    adr = Column(Float) # Average Daily Range %
    gap = Column(Float) # Gap %
    change_from_open = Column(Float) # % Change from Open
    
    # Performance
    wtd = Column(Float)
    mtd = Column(Float)
    qtd = Column(Float)
    ytd = Column(Float)
    perf_3m = Column(Float) # 3-Month Rolling Return
    avg_dollar_volume = Column(Float) # 20-Day Average Dollar Volume
    
    # Relative Strength (vs SPY)
    rs_1d = Column(Float)
    rs_1w = Column(Float)
    rs_1m = Column(Float)
    rs_3m = Column(Float)
    
    # Detailed Strat History
    prev_cond_1 = Column(String) # Previous Candle
    prev_cond_2 = Column(String) # 2 Candles Ago
    curr_cond = Column(String)   # Current Candle

# Create DB - Support both local SQLite and Turso
# Database Connection Logic
DATABASE_URL = os.getenv("DATABASE_URL")

if DATABASE_URL:
    # Production: Use PostgreSQL (Neon/Supabase/etc)
    # Fix for SQLAlchemy < 1.4 handling of postgres:// vs postgresql://
    if DATABASE_URL.startswith("postgres://"):
        DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)
        
    engine = create_engine(DATABASE_URL, echo=False)
    print("Using Production Database")
else:
    # Local development: Use SQLite
    engine = create_engine(
        'sqlite:///stratiq.db',
        connect_args={"check_same_thread": False},
        echo=True
    )
    print("Using local SQLite database")

Session = sessionmaker(bind=engine)

def init_db():
    Base.metadata.create_all(engine)

if __name__ == "__main__":
    init_db()
    print("Database initialized.")
