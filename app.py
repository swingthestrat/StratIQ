import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from datetime import datetime
from database import Session, Theme, ThemeTicker, Alert, init_db, OHLCV
from ingest import run_ingestion
from engine import run_scan
from universe import update_universe

# Must be first
st.set_page_config(page_title="Swing The Strat", layout="wide", initial_sidebar_state="collapsed")

# Initialize DB
init_db()

# --- CSS Styling ---
st.markdown("""
<style>
    .stApp { background-color: #F8FAFC; color: #1E293B; } /* Light Theme Background */
    
    /* Header */
    .header-title { font-size: 1.5rem; font-weight: bold; color: #0F172A; display: flex; align-items: center; gap: 10px; }
    .header-icon { background-color: #2563EB; color: white; padding: 5px 10px; border-radius: 8px; }
    .market-status { font-size: 0.9rem; color: #64748B; }
    .status-open { color: #16A34A; font-weight: bold; }
    
    /* Filter Section */
    .filter-group-title { font-size: 0.75rem; font-weight: bold; color: #94A3B8; text-transform: uppercase; margin-bottom: 5px; }
    
    /* Table Styling */
    [data-testid="stDataFrame"] { font-family: 'Inter', sans-serif; }
    
    /* Badges */
    .badge-2u { background-color: #DCFCE7; color: #15803D; padding: 2px 6px; border-radius: 4px; font-weight: bold; }
    .badge-2uR { background-color: #FEE2E2; color: #B91C1C; padding: 2px 6px; border-radius: 4px; font-weight: bold; border: 1px solid #15803D; } /* Red body, Up structure */
    .badge-2d { background-color: #FEE2E2; color: #B91C1C; padding: 2px 6px; border-radius: 4px; font-weight: bold; }
    .badge-2dG { background-color: #DCFCE7; color: #15803D; padding: 2px 6px; border-radius: 4px; font-weight: bold; border: 1px solid #B91C1C; } /* Green body, Down structure */
    .badge-1 { background-color: #DBEAFE; color: #1E40AF; padding: 2px 6px; border-radius: 4px; font-weight: bold; }
    .badge-3 { background-color: #FEF9C3; color: #854D0E; padding: 2px 6px; border-radius: 4px; font-weight: bold; }
    .badge-3u { background-color: #FEF9C3; color: #15803D; padding: 2px 6px; border-radius: 4px; font-weight: bold; }
    .badge-3d { background-color: #FEF9C3; color: #B91C1C; padding: 2px 6px; border-radius: 4px; font-weight: bold; }
</style>
""", unsafe_allow_html=True)

# --- Helper Functions ---
def save_alerts(alerts):
    session = Session()
    today = datetime.now().date()
    try:
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
                    status=a['status'], candle_state=a['candle_state'], ftfc=a['ftfc'],
                    industry=a.get('industry', ''), adr=a.get('adr', 0), gap=a.get('gap', 0),
                    change_from_open=a.get('change_from_open', 0),
                    wtd=a.get('wtd', 0), mtd=a.get('mtd', 0), qtd=a.get('qtd', 0), ytd=a.get('ytd', 0),
                    prev_cond_1=a.get('prev_cond_1', ''), prev_cond_2=a.get('prev_cond_2', ''), curr_cond=a.get('curr_cond', '')
                )
                session.add(alert)
        session.commit()
    except Exception as e:
        print(f"Error saving alerts: {e}")
    finally:
        session.close()

def get_alerts(filters):
    session = Session()
    query = session.query(Alert).filter(Alert.is_theme == 0)
    
    # Apply Filters
    # 1. Universe (Theme) - Not fully linked yet, assumes 'All' for now or filters by Ticker list if implemented
    # 2. Actionable Setups (Type/Pattern)
    if filters['setups'] and 'ALL' not in filters['setups']:
        # Map UI options to DB values
        conditions = []
        if '2d Green' in filters['setups']: conditions.append(Alert.type.contains("2d Green"))
        if '2u Red' in filters['setups']: conditions.append(Alert.type.contains("2u Red")) 
        if 'HAMMER' in filters['setups']: conditions.append(Alert.type.contains("Hammer"))
        if 'SHOOTER' in filters['setups']: conditions.append(Alert.type.contains("Shooter"))
        if 'INSIDE' in filters['setups']: conditions.append(Alert.type.contains("Inside"))
        
        # New Reversal Patterns
        if 'Rev Strat Bull' in filters['setups']: conditions.append(Alert.type.contains("Rev Strat (2d-2u)"))
        if 'Rev Strat Bear' in filters['setups']: conditions.append(Alert.type.contains("Rev Strat (2u-2d)"))
        if '2-1-2 Bull' in filters['setups']: conditions.append(Alert.type.contains("2-1-2 Bullish"))
        if '2-1-2 Bear' in filters['setups']: conditions.append(Alert.type.contains("2-1-2 Bearish"))
        if '3-1-2 Bull' in filters['setups']: conditions.append(Alert.type.contains("3-1-2 Bullish"))
        if '3-1-2 Bear' in filters['setups']: conditions.append(Alert.type.contains("3-1-2 Bearish"))
        
        if conditions:
            from sqlalchemy import or_
            query = query.filter(or_(*conditions))

    # 3. In Force (Status/Candle)
    if filters['in_force'] and 'NONE' not in filters['in_force']:
        # Map UI options
        conditions = []
        if 'HTF In-Force' in filters['in_force']: conditions.append(Alert.status == "In Force")
        # Add specific candle logic if needed (e.g. 2U-2U)
        
        if conditions:
            from sqlalchemy import or_
            query = query.filter(or_(*conditions))

    # 4. FTFC
    if filters['ftfc'] and 'NO FTFC' not in filters['ftfc']:
         query = query.filter(Alert.ftfc.in_(filters['ftfc']))

    # 5. Timeframe
    if filters['timeframe']:
        query = query.filter(Alert.timeframe.in_(filters['timeframe']))

    alerts = query.order_by(Alert.date.desc(), Alert.id.desc()).all()
    session.close()
    return alerts

# --- Main Layout ---

# 1. Header
col_h1, col_h2 = st.columns([3, 1])
with col_h1:
    st.markdown("""
    <div class="header-title">
        <span class="header-icon">S</span> Swing The Strat
    </div>
    """, unsafe_allow_html=True)
with col_h2:
    st.markdown("""
    <div style="text-align: right;" class="market-status">
        Market Status: <span class="status-open">OPEN</span>
    </div>
    """, unsafe_allow_html=True)

st.markdown("---")

# 2. Filter Section (Grouped)
with st.container():
    c1, c2, c3, c4, c5, c6 = st.columns(6)
    
    with c1:
        st.markdown('<div class="filter-group-title">UNIVERSE</div>', unsafe_allow_html=True)
        f_universe = st.multiselect("Universe", ['SPY', 'QQQ', 'DIA', 'IWM', 'SECTORS', 'ALL'], default=['ALL'], label_visibility="collapsed")
        
    with c2:
        st.markdown('<div class="filter-group-title">FILTERS</div>', unsafe_allow_html=True)
        f_filters = st.selectbox("Filters", ['LIQUID LEADERS', 'STRONG RS', 'WEAK RS', 'NONE'], index=3, label_visibility="collapsed")
        
    with c3:
        st.markdown('<div class="filter-group-title">ACTIONABLE SETUPS</div>', unsafe_allow_html=True)
        # Updated with more options
        setup_options = [
            '2d Green', '2u Red', 'HAMMER', 'SHOOTER', 'INSIDE', 
            'Rev Strat Bull', 'Rev Strat Bear', 
            '2-1-2 Bull', '2-1-2 Bear', 
            '3-1-2 Bull', '3-1-2 Bear',
            'ALL'
        ]
        f_setups = st.multiselect("Setups", setup_options, default=['ALL'], label_visibility="collapsed")
        
    with c4:
        st.markdown('<div class="filter-group-title">IN FORCE</div>', unsafe_allow_html=True)
        f_in_force = st.multiselect("In Force", ['1-2U', '1-2D', 'HTF In-Force', 'NONE'], default=['HTF In-Force'], label_visibility="collapsed")
        
    with c5:
        st.markdown('<div class="filter-group-title">FTFC</div>', unsafe_allow_html=True)
        f_ftfc = st.multiselect("FTFC", ['Bullish', 'Bearish', 'Mixed', 'NO FTFC'], default=['Bullish'], label_visibility="collapsed")
        
    with c6:
        st.markdown('<div class="filter-group-title">TIMEFRAME</div>', unsafe_allow_html=True)
        # Mandatory Timeframe: Default to 1D, 1W, 1M. User can change but it shouldn't be empty ideally.
        f_tf = st.multiselect("Timeframe", ['1D', '2D', '3D', '1W', '1M', '1Q', '1Y'], default=['1D', '1W', '1M'], label_visibility="collapsed")
        if not f_tf:
            st.warning("Please select at least one timeframe.")

    # Run Scan Button (to refresh/process)
    if st.button("RUN SCAN", type="primary", use_container_width=True):
        with st.spinner("Scanning..."):
            session = Session()
            tickers = [r.ticker for r in session.query(ThemeTicker).distinct(ThemeTicker.ticker).all()]
            session.close()
            
            all_alerts = []
            progress_bar = st.progress(0)
            for i, ticker in enumerate(tickers):
                alerts = run_scan(ticker)
                all_alerts.extend(alerts)
                progress_bar.progress((i + 1) / len(tickers))
            
            save_alerts(all_alerts)
            st.rerun()

# 3. Data Table
filters = {
    'universe': f_universe,
    'filters': f_filters,
    'setups': f_setups,
    'in_force': f_in_force,
    'ftfc': f_ftfc,
    'timeframe': f_tf
}

alerts = get_alerts(filters)

if alerts:
    # Convert list of Alert objects to a DataFrame for easier processing
    # Extract relevant attributes into a list of dictionaries
    alerts_data = []
    for a in alerts:
        alerts_data.append({
            "ticker": a.ticker,
            "type": a.type, # Keep raw type for 'Setup' column
            "adr": a.adr,
            "price": a.price,
            "industry": a.industry,
            "prev_cond_2": a.prev_cond_2,
            "prev_cond_1": a.prev_cond_1,
            "curr_cond": a.curr_cond,
            "gap": a.gap,
            "change_from_open": a.change_from_open,
            "wtd": a.wtd,
            "mtd": a.mtd,
            "qtd": a.qtd,
            "ytd": a.ytd,
            "timeframe": a.timeframe
        })
    df_display = pd.DataFrame(alerts_data)

    # Display Table
    if not df_display.empty:
        # Reorder columns: Prev(2) -> Prev(1) -> Curr
        cols = ['Ticker', 'Setup', 'ADR%', 'Price', 'Industry', 'Prev (2)', 'Prev (1)', 'Curr', 'Gap%', 'Chg Open', 'WTD', 'MTD', 'QTD', 'YTD', 'TF']
        
        # Map DB fields to Display columns
        df_show = pd.DataFrame()
        df_show['Ticker'] = df_display['ticker']
        df_show['Setup'] = df_display['type']
        df_show['ADR%'] = df_display['adr'].apply(lambda x: f"{x:.2f}%")
        df_show['Price'] = df_display['price'].apply(lambda x: f"${x:.2f}")
        df_show['Industry'] = df_display['industry']
        df_show['Prev (2)'] = df_display['prev_cond_2']
        df_show['Prev (1)'] = df_display['prev_cond_1']
        df_show['Curr'] = df_display['curr_cond']
        df_show['Gap%'] = df_display['gap'].apply(lambda x: f"{x:.2f}%")
        df_show['Chg Open'] = df_display['change_from_open'].apply(lambda x: f"{x:.2f}%")
        df_show['WTD'] = df_display['wtd'].apply(lambda x: f"{x:.2f}%")
        df_show['MTD'] = df_display['mtd'].apply(lambda x: f"{x:.2f}%")
        df_show['QTD'] = df_display['qtd'].apply(lambda x: f"{x:.2f}%")
        df_show['YTD'] = df_display['ytd'].apply(lambda x: f"{x:.2f}%")
        df_show['TF'] = df_display['timeframe']
        
        # Apply Badge Styling (using Pandas Styler or just raw HTML if Streamlit supports it better)
        # Streamlit dataframe supports simple styling. For badges, we might need column configuration.
        
        st.dataframe(
            df_show[cols],
            use_container_width=True,
            hide_index=True,
            column_config={
                "Setup": st.column_config.TextColumn("Setup", width="medium"),
                "Price": st.column_config.NumberColumn(format="$%.2f"),
                "Industry": st.column_config.TextColumn(width="medium"),
                "Prev (2)": st.column_config.TextColumn("Prev (2)"),
                "Prev (1)": st.column_config.TextColumn("Prev (1)"),
                "Curr": st.column_config.TextColumn("Curr"),
            },
            height=600
        )
    
    st.caption(f"Showing {len(df_show)} entries")

else:
    st.info("No alerts found matching the criteria. Click RUN SCAN.")
