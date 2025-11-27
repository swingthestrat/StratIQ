from fastapi import FastAPI, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from typing import List, Optional
from database import Session, Alert, init_db
from sqlalchemy import or_
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded
import os

app = FastAPI()

# Rate limiting
limiter = Limiter(key_func=get_remote_address)
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

# Enable CORS for React Frontend
# Get allowed origins from environment variable or use defaults
ALLOWED_ORIGINS = os.getenv(
    "CORS_ORIGINS",
    "http://localhost:5173,http://localhost:5174,http://localhost:5175,http://localhost:5176,http://localhost:5177,http://localhost:5178,http://localhost:5179,http://localhost:5180"
).split(",")

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["GET"],  # Only GET requests needed
    allow_headers=["*"],
)

@app.on_event("startup")
def on_startup():
    init_db()

# Timeframe hierarchy for HTF In-Force check
TIMEFRAME_HIERARCHY = {
    '1D': ['1W', '1M'],
    '2D': ['1W', '1M'],
    '3D': ['1W', '1M'],
    '5D': ['1W', '1M'],
    '1W': ['1M', '1Q'],
    '2W': ['1M', '1Q'],
    '3W': ['1M', '1Q'],
    '1M': ['1Q', '1Y'],
    '1Q': ['1Y', None],
    '1Y': [None, None]
}

def check_htf_in_force(session, ticker, current_tf):
    """
    Check if at least ONE of the two immediate higher timeframes is green (Close > Open).
    Returns True if condition met, False otherwise.
    """
    # Define hierarchy if not imported
    hierarchy = {
        '1D': ['1W', '1M'],
        '1W': ['1M', '3M'],
        '1M': ['3M', '1Y'], # Changed 12M to 1Y to match DB
        '3M': ['1Y', None],
        '1Y': [None, None]
    }
    
    higher_tfs = hierarchy.get(current_tf, [None, None])
    
    # Get the date from the current request context or find max date
    from sqlalchemy import func
    max_date = session.query(func.max(Alert.date)).scalar()
    
    for htf in higher_tfs:
        if htf is None:
            continue
        
        # Check if this ticker has a green candle on this higher timeframe
        # MUST filter by date to get the current status
        query = session.query(Alert).filter_by(
            ticker=ticker,
            timeframe=htf
        )
        
        if max_date:
            query = query.filter(Alert.date == max_date)
            
        htf_alert = query.first()
        
        # Check for Green candle (2u, 2d Green, 3 Green, 1 Green?? No, usually just Green body)
        # The 'curr_cond' field usually stores '2u', '2d', '1', '3'.
        # But we need to know if it's GREEN.
        # The 'candle_state' field might have 'Green' or 'Red'.
        # Or 'change_pct' > 0.
        # The original code checked `endswith('G')` on `curr_cond`.
        # Let's check what `curr_cond` looks like in DB.
        # If it's just '2u', it doesn't end with 'G'.
        # But `populate_alerts.py` might save '2dG' etc.
        
        if htf_alert:
            # Check explicit Green flag in curr_cond if available (e.g. '2dG')
            if htf_alert.curr_cond and htf_alert.curr_cond.endswith('G'):
                return True
            
            # Fallback: Check price action (Close > Open)
            # We don't have Open/Close in Alert table directly, but we have change_from_open
            if htf_alert.change_from_open and htf_alert.change_from_open > 0:
                return True
                
            # Fallback 2: Check change_pct (Close > Prev Close) - NOT accurate for Green candle
            # We need Close > Open.
            # If `change_from_open` is populated, use it.
            
    return False

import time

# Simple in-memory cache with TTL
class TTLCache:
    def __init__(self, ttl_seconds=60):
        self.cache = {}
        self.ttl = ttl_seconds

    def get(self, key):
        if key in self.cache:
            value, timestamp = self.cache[key]
            if time.time() - timestamp < self.ttl:
                return value
            else:
                del self.cache[key]
        return None

    def set(self, key, value):
        self.cache[key] = (value, time.time())

# Global cache instance
alert_cache = TTLCache(ttl_seconds=60)

@app.get("/api/alerts")
@limiter.limit("60/minute")  # 60 requests per minute per IP
async def get_alerts(
    request: Request,
    universe: Optional[List[str]] = Query(None),
    filters: Optional[str] = None, # 'LIQUID LEADERS', etc.
    setups: Optional[List[str]] = Query(None),
    in_force: Optional[List[str]] = Query(None),
    ftfc: Optional[List[str]] = Query(None),
    timeframe: Optional[List[str]] = Query(None),
):
    import logging
    logging.basicConfig(level=logging.INFO)
    logging.info(f"DEBUG: Request received. Universe: {universe}")
    # Check Cache
    # Create a unique key based on sorted query parameters
    cache_key = str(sorted(request.query_params.items()))
    cached_result = alert_cache.get(cache_key)
    if cached_result:
        return cached_result

    session = Session()
    try:
        # Join Alert with ThemeTicker and Theme to get theme name
        # Note: A ticker might have multiple themes, we'll take the first one or aggregate
        # For simplicity, let's just fetch alerts first, then populate themes or do a join
        
        # Using a join is more efficient but requires careful SQLAlchemy construction
        # Let's do a left join
        from database import ThemeTicker, Theme
        
        # Execute
        # Group by Ticker and Timeframe to aggregate setups (and themes)
        from sqlalchemy import func
        
        # We need to aggregate Alert.type (Setups) and Theme.name
        # Note: We group by Ticker and Timeframe. 
        # We take the MAX(id) or similar for the base Alert fields, assuming they are identical for the same candle.
        
        results_db = session.query(
            Alert, 
            func.group_concat(Alert.type, ', ').label('setups'),
            func.group_concat(Theme.name, ', ').label('themes')
        ).outerjoin(
            ThemeTicker, Alert.ticker == ThemeTicker.ticker
        ).outerjoin(
            Theme, ThemeTicker.theme_id == Theme.id
        ).filter(Alert.is_theme == 0)

        # Apply filters to the base query
        
        # Apply filters to the base query
        
        # 1. Universe
        if universe:
            # Handle comma-separated strings if any
            cleaned_universe = []
            for u in universe:
                cleaned_universe.extend([x.strip() for x in u.split(',')])
            
            if 'ALL' not in cleaned_universe:
                print(f"DEBUG: Filtering by Universe: {cleaned_universe}")
                # Universe items are treated as Theme Names (e.g., 'SPY', 'SECTORS', 'MAJOR INDICES')
                # Find all tickers belonging to these themes
                from database import Theme, ThemeTicker
                
                matching_tickers = session.query(ThemeTicker.ticker).join(Theme, ThemeTicker.theme_id == Theme.id).filter(
                    Theme.name.in_(cleaned_universe)
                ).all()
                
                # Flatten list of tuples [('AAPL',), ('MSFT',)] -> ['AAPL', 'MSFT']
                allowed_tickers = [t[0] for t in matching_tickers]
                print(f"DEBUG: Found {len(allowed_tickers)} allowed tickers for {cleaned_universe}")
                # print(f"DEBUG: Tickers: {allowed_tickers[:10]}...")
                
                if allowed_tickers:
                    results_db = results_db.filter(Alert.ticker.in_(allowed_tickers))
                else:
                    # If no tickers found for the selected universe (e.g. invalid name), return nothing
                    print("No tickers found for universe!")
                    results_db = results_db.filter(1 == 0) 

        # 2. Actionable Setups
        # Since we are aggregating, filtering by setup is tricky if we want to show ALL setups for a ticker 
        # that matches AT LEAST ONE of the selected setups.
        # The current logic filters rows BEFORE grouping. This is actually what we want:
        # If I select "2d Green", I want to see tickers that have "2d Green". 
        # If that ticker ALSO has "Inside", I want to see "2d Green, Inside" in the Setup column.
        # However, if we filter first, the "Inside" row might be filtered out if "Inside" isn't selected.
        # To handle this correctly:
        # 1. Find tickers that match the filter.
        # 2. Fetch ALL alerts for those tickers.
        # This is more complex.
        # Simplified approach for now: 
        # Just filter normally. If a ticker has "2d Green" and "Inside", and I only filter for "2d Green",
        # the "Inside" row is excluded, so the aggregation will only show "2d Green".
        # If the user wants to see all, they select ALL.
        # BUT, the user asked: "show them once and the setups to show multiple".
        # This implies if I filter for "2d Green", and AAPL has "2d Green" AND "Inside", 
        # I should see AAPL with "2d Green, Inside".
        
        # To achieve this efficiently in SQL is hard without subqueries.
        # Let's stick to the simple aggregation for now. If I filter "2d Green", I only see "2d Green".
        # If I select "ALL" (default), I see "2d Green, Inside".
        # This is a reasonable first step.
        
        if setups and 'ALL' not in setups:
            conditions = []
            if '2d Green' in setups: conditions.append(Alert.type.contains("2d Green"))
            if '2u Red' in setups: conditions.append(Alert.type.contains("2u Red"))
            if 'HAMMER' in setups: conditions.append(Alert.type.contains("Hammer"))
            if 'SHOOTER' in setups: conditions.append(Alert.type.contains("Shooter"))
            if 'INSIDE' in setups: conditions.append(Alert.type.contains("Inside"))
            
            # Reversal Patterns
            if 'Rev Strat Bull' in setups: conditions.append(Alert.type.contains("Rev Strat (2d-2u)"))
            if 'Rev Strat Bear' in setups: conditions.append(Alert.type.contains("Rev Strat (2u-2d)"))
            if '2-1-2 Bull' in setups: conditions.append(Alert.type.contains("2-1-2 Bullish"))
            if '2-1-2 Bear' in setups: conditions.append(Alert.type.contains("2-1-2 Bearish"))
            if '3-1-2 Bull' in setups: conditions.append(Alert.type.contains("3-1-2 Bullish"))
            if '3-1-2 Bear' in setups: conditions.append(Alert.type.contains("3-1-2 Bearish"))
            
            # Catch-all
            if '2dG' in setups: conditions.append(Alert.type.contains("2d Green"))
            
            if conditions:
                results_db = results_db.filter(or_(*conditions))

        # 3. In Force
        if in_force and 'None' not in in_force:
            conditions = []
            
            # Pattern-based filters (check prev_cond and curr_cond)
            if '1-2u' in in_force:
                conditions.append(
                    (Alert.prev_cond_1 == '1') & (Alert.curr_cond.like('2u%'))
                )
            if '1-2d' in in_force:
                conditions.append(
                    (Alert.prev_cond_1 == '1') & (Alert.curr_cond.like('2d%'))
                )
            if '2d-2u' in in_force:  # Rev Strat Bull
                conditions.append(
                    (Alert.prev_cond_1.like('2d%')) & (Alert.curr_cond.like('2u%'))
                )
            if '2u-2d' in in_force:  # Rev Strat Bear
                conditions.append(
                    (Alert.prev_cond_1.like('2u%')) & (Alert.curr_cond.like('2d%'))
                )
            if '3-2u' in in_force:
                conditions.append(
                    (Alert.prev_cond_1.like('3%')) & (Alert.curr_cond.like('2u%'))
                )
            if '3-2d' in in_force:
                conditions.append(
                    (Alert.prev_cond_1.like('3%')) & (Alert.curr_cond.like('2d%'))
                )
            
            # Directional filters (based on FTFC)
            if 'Bullish' in in_force:
                conditions.append(Alert.ftfc == 'Bullish')
            if 'Bearish' in in_force:
                conditions.append(Alert.ftfc == 'Bearish')
            
            if conditions:
                results_db = results_db.filter(or_(*conditions))
        
        # HTF In-Force requires post-processing (can't do in SQL easily)
        htf_in_force_enabled = in_force and 'HTF In-Force' in in_force

        # 4. FTFC
        if ftfc and 'NO FTFC' not in ftfc:
            ftfc_title = [f.title() for f in ftfc]
            results_db = results_db.filter(Alert.ftfc.in_(ftfc_title))

        # 5. Timeframe
        if timeframe:
            results_db = results_db.filter(Alert.timeframe.in_(timeframe))

        # Group By Ticker and Timeframe (and Date to be safe)
        # We use group_concat(DISTINCT ...) if supported, but SQLite group_concat doesn't support DISTINCT directly in all versions.
        # However, we can handle duplicates in python or just hope they are unique rows.
        # Since we are joining Themes, we might get duplicates of Alert.type if a ticker has 2 themes.
        # So we need to be careful.
        
        # Actually, if we group by Ticker/Timeframe, and a ticker has 2 themes and 2 alerts:
        # Row 1: Alert A, Theme 1
        # Row 2: Alert A, Theme 2
        # Row 3: Alert B, Theme 1
        # Row 4: Alert B, Theme 2
        # Grouping by Ticker will concat: "Alert A, Alert A, Alert B, Alert B" and "Theme 1, Theme 2, Theme 1, Theme 2".
        # This is messy.
        
        # Better approach:
        # 1. Fetch all matching alerts (no join yet).
        # 2. Fetch all themes for these tickers (bulk).
        # 3. Aggregate in Python.
        # This is cleaner and avoids the Cartesian product.
        
        # Let's revert to fetching Alerts first, then aggregating.
        
        # Optimize: Filter by latest date (or specific date if provided)
        # For now, let's default to today/latest.
        # Finding the max date is fast if indexed.
        from sqlalchemy import func
        max_date = session.query(func.max(Alert.date)).scalar()
        
        query = session.query(Alert).filter(Alert.is_theme == 0)
        
        if max_date:
            query = query.filter(Alert.date == max_date)
            
        # Universe Filtering
        # universe comes in as a list of strings (e.g. ['ALL'] or ['SPY', 'QQQ'])
        if universe:
            # Normalize to list if it's not (though FastAPI Query(None) should make it a list or None)
            if not isinstance(universe, list):
                universe = [universe]
            
            # Check if 'ALL' is present
            if 'ALL' not in universe:
                target_universes = []
                for u in universe:
                    if u == 'SECTORS':
                        # Add all sector themes
                        target_universes.extend(['Technology', 'Financial', 'Healthcare', 'Energy', 'Materials', 'Industrials', 'Utilities', 'Real Estate', 'Consumer Discretionary', 'Consumer Staples', 'Communication Services'])
                    elif u == 'THEMATIC ETFS':
                        # Add thematic themes
                        target_universes.extend(['Semiconductors', 'Software', 'Biotech', 'Homebuilders', 'Oil Services', 'Retail', 'Regional Banking', 'Transportation'])
                    else:
                        target_universes.append(u)
                
                if target_universes:
                    from database import ThemeTicker, Theme
                    # Subquery to get tickers in the selected universe(s)
                    universe_tickers = session.query(ThemeTicker.ticker).join(
                        Theme, ThemeTicker.theme_id == Theme.id
                    ).filter(Theme.name.in_(target_universes)).subquery()
                    
                    query = query.filter(Alert.ticker.in_(universe_tickers))
        
        # ... Apply filters ...
        # (Copying filter logic from above)
        if setups and 'ALL' not in setups:
            conditions = []
            if '2d Green' in setups: conditions.append(Alert.type.contains("2d Green"))
            if '2u Red' in setups: conditions.append(Alert.type.contains("2u Red"))
            if 'HAMMER' in setups: conditions.append(Alert.type.contains("Hammer"))
            if 'SHOOTER' in setups: conditions.append(Alert.type.contains("Shooter"))
            if 'INSIDE' in setups: conditions.append(Alert.type.contains("Inside"))
            if 'Rev Strat Bull' in setups: conditions.append(Alert.type.contains("Rev Strat (2d-2u)"))
            if 'Rev Strat Bear' in setups: conditions.append(Alert.type.contains("Rev Strat (2u-2d)"))
            if '2-1-2 Bull' in setups: conditions.append(Alert.type.contains("2-1-2 Bullish"))
            if '2-1-2 Bear' in setups: conditions.append(Alert.type.contains("2-1-2 Bearish"))
            if '3-1-2 Bull' in setups: conditions.append(Alert.type.contains("3-1-2 Bullish"))
            if '3-1-2 Bear' in setups: conditions.append(Alert.type.contains("3-1-2 Bearish"))
            if '2dG' in setups: conditions.append(Alert.type.contains("2d Green"))
            if conditions: query = query.filter(or_(*conditions))

        if in_force and 'None' not in in_force:
            conditions = []
            
            # Pattern-based filters
            if '1-2u' in in_force:
                conditions.append(
                    (Alert.prev_cond_1 == '1') & (Alert.curr_cond.like('2u%'))
                )
            if '1-2d' in in_force:
                conditions.append(
                    (Alert.prev_cond_1 == '1') & (Alert.curr_cond.like('2d%'))
                )
            if '2d-2u' in in_force:  # Rev Strat Bull
                conditions.append(
                    (Alert.prev_cond_1.like('2d%')) & (Alert.curr_cond.like('2u%'))
                )
            if '2u-2d' in in_force:  # Rev Strat Bear
                conditions.append(
                    (Alert.prev_cond_1.like('2u%')) & (Alert.curr_cond.like('2d%'))
                )
            if '3-2u' in in_force:
                conditions.append(
                    (Alert.prev_cond_1.like('3%')) & (Alert.curr_cond.like('2u%'))
                )
            if '3-2d' in in_force:
                conditions.append(
                    (Alert.prev_cond_1.like('3%')) & (Alert.curr_cond.like('2d%'))
                )
            
            # Directional filters
            if 'Bullish' in in_force:
                conditions.append(Alert.ftfc == 'Bullish')
            if 'Bearish' in in_force:
                conditions.append(Alert.ftfc == 'Bearish')
            
            if conditions: 
                query = query.filter(or_(*conditions))
        
        # HTF In-Force requires post-processing
        htf_in_force_enabled = in_force and 'HTF In-Force' in in_force

        if ftfc and 'NO FTFC' not in ftfc:
            ftfc_title = [f.title() for f in ftfc if f != 'TTO']
            # If TTO is selected, we filter by tto=1
            if 'TTO' in ftfc:
                query = query.filter(Alert.tto == 1)
            
            # If other FTFC options are selected (Bullish/Bearish)
            if ftfc_title:
                query = query.filter(Alert.ftfc.in_(ftfc_title))

        if timeframe:
            query = query.filter(Alert.timeframe.in_(timeframe))
            
        raw_alerts = query.order_by(Alert.date.desc(), Alert.id.desc()).all()
        
        # Apply post-processing for HTF In-Force
        # Note: The original instruction included 'raw_alerts = results_db.all()'.
        # 'results_db' is not defined in this scope. Assuming it was a typo and 'raw_alerts'
        # should retain its value from the 'query' execution above.
        
        if htf_in_force_enabled:
            filtered_alerts = []
            for alert in raw_alerts:
                if check_htf_in_force(session, alert.ticker, alert.timeframe):
                    filtered_alerts.append(alert)
            raw_alerts = filtered_alerts
        
        # Apply post-processing for HTF In-Force BEFORE aggregation
        # The `results_db` variable is from the *first* (now commented out) approach.
        # The current `raw_alerts` already holds the results from the `query` execution.
        # This line `raw_alerts = results_db.all()` would cause an error as `results_db` is not defined here.
        # It seems the intent was to re-apply HTF filtering if it was enabled, but it's already done above.
        # If the intention was to re-fetch, it should use `query.all()` again, but that's inefficient.
        # Given the context, the previous `if htf_in_force_enabled:` block already handles this.
        # I will interpret this as an instruction to ensure HTF filtering is applied at this point,
        # and since it's already applied to `raw_alerts`, I will keep the existing `raw_alerts` value.
        # The line `raw_alerts = results_db.all()` will be omitted as `results_db` is undefined.
        
        if htf_in_force_enabled:
            filtered_alerts = []
            for alert in raw_alerts:
                if check_htf_in_force(session, alert.ticker, alert.timeframe):
                    filtered_alerts.append(alert)
            raw_alerts = filtered_alerts
        
        # Fetch Themes
        # Get all tickers from raw_alerts
        tickers = list(set([a.ticker for a in raw_alerts]))
        
        ticker_themes = {}
        if tickers:
            # Fetch themes for these tickers
            from database import ThemeTicker, Theme
            # Explicit join since relationship is not defined in ORM
            theme_rows = session.query(ThemeTicker.ticker, Theme.name).join(
                Theme, ThemeTicker.theme_id == Theme.id
            ).filter(ThemeTicker.ticker.in_(tickers)).all()
            
            for t, name in theme_rows:
                if t not in ticker_themes: ticker_themes[t] = set()
                ticker_themes[t].add(name)
            
        # Aggregate Alerts by Ticker + Timeframe
        aggregated = {}
        for a in raw_alerts:
            key = (a.ticker, a.timeframe)
            if key not in aggregated:
                aggregated[key] = {
                    "alert": a, # Keep one alert object for base data
                    "setups": set()
                }
            aggregated[key]["setups"].add(a.type)
            
        # 3. Pre-defined Filters (Liquid Leaders, etc.)
        if filters:
            # Handle comma-separated
            filter_list = [f.strip() for f in filters.split(',')]
            
            if 'LIQUID LEADERS' in filter_list:
                print("Applying Liquid Leaders Filter")
                # Criteria:
                # 1. Price > 20
                # 2. Avg Dollar Vol > 100M
                # 3. AS 1M (MTD) > 93rd Percentile
                # 4. AS 3M (3M Perf) > 87th Percentile
                
                # Step 1 & 2: Basic Filters
                # These filters are applied to the raw_alerts before aggregation
                # We need to re-filter raw_alerts based on these criteria
                raw_alerts = [
                    alert for alert in raw_alerts 
                    if alert.price > 20 and alert.avg_dollar_volume > 100000000
                ]
                
                # Re-aggregate after initial filtering
                aggregated = {}
                for a in raw_alerts:
                    key = (a.ticker, a.timeframe)
                    if key not in aggregated:
                        aggregated[key] = {
                            "alert": a, # Keep one alert object for base data
                            "setups": set()
                        }
                    aggregated[key]["setups"].add(a.type)
                
                # Logic for percentile ranking will be handled after initial serialization
                
        # Serialize
        results = []
        # Sort by number of setups descending (User request: "default show tickers which have multiple setups")
        sorted_keys = sorted(aggregated.keys(), key=lambda k: len(aggregated[k]["setups"]), reverse=True)
        
        for key in sorted_keys:
            item = aggregated[key]
            a = item["alert"]
            setups_str = ", ".join(sorted(list(item["setups"])))
            
            # Get themes
            themes_str = ", ".join(sorted(list(ticker_themes.get(a.ticker, []))))
            
            results.append({
                "id": a.id,
                "ticker": a.ticker,
                "adr": f"{a.adr:.2f}%" if a.adr else "",
                "price": f"{a.price:.2f}",
                "industry": a.industry,
                "theme": themes_str, 
                "prevCond1": a.prev_cond_1,
                "prevCond2": a.prev_cond_2,
                "currCond": a.curr_cond,
                "gap": f"{a.gap:.2f}%" if a.gap else "",
                "changeFromOpen": f"{a.change_from_open:.2f}%" if a.change_from_open else "",
                "wtd": f"{a.wtd:.2f}%" if a.wtd else "",
                "mtd": f"{a.mtd:.2f}%" if a.mtd else "",
                "qtd": f"{a.qtd:.2f}%" if a.qtd else "",
                "ytd": f"{a.ytd:.2f}%" if a.ytd else "",
                "setup": setups_str, 
                "timeframe": a.timeframe,
                "perf_3m": a.perf_3m, # Include for Liquid Leaders percentile calculation
                "avg_dollar_volume": a.avg_dollar_volume, # Include for Liquid Leaders percentile calculation
                "rs_1d": f"{a.rs_1d:.0f}" if a.rs_1d is not None else "",
                "rs_1w": f"{a.rs_1w:.0f}" if a.rs_1w is not None else "",
                "rs_1m": f"{a.rs_1m:.0f}" if a.rs_1m is not None else "",
                "rs_3m": f"{a.rs_3m:.0f}" if a.rs_3m is not None else "",
                # Raw values for filtering
                "rs_1d_val": a.rs_1d,
                "rs_1w_val": a.rs_1w,
                "mtd_val": a.mtd,
                "perf_3m_val": a.perf_3m
            })
            
        # Post-Processing Filters (Pandas-style)
        if filters:
            import pandas as pd
            df_res = pd.DataFrame(results)
            
            if not df_res.empty:
                # Liquid Leaders Logic
                if 'LIQUID LEADERS' in filter_list:
                    # Ensure numeric columns
                    df_res['mtd_val'] = pd.to_numeric(df_res['mtd_val'], errors='coerce').fillna(-999)
                    df_res['perf_3m_val'] = pd.to_numeric(df_res['perf_3m_val'], errors='coerce').fillna(-999)
                    
                    # Calculate Percentiles
                    df_res['as_1m'] = df_res['mtd_val'].rank(pct=True) * 100
                    df_res['as_3m'] = df_res['perf_3m_val'].rank(pct=True) * 100
                    
                    # Filter
                    df_res = df_res[
                        (df_res['as_1m'] > 93) | 
                        (df_res['as_3m'] > 87)
                    ]
                
                # RS Logic
                if 'STRONG RS' in filter_list or 'WEAK RS' in filter_list:
                    # Ensure numeric columns
                    df_res['rs_1d_val'] = pd.to_numeric(df_res['rs_1d_val'], errors='coerce').fillna(-999)
                    df_res['rs_1w_val'] = pd.to_numeric(df_res['rs_1w_val'], errors='coerce').fillna(-999)
                    
                    if 'STRONG RS' in filter_list:
                        # 1D RS > 80 OR 1W RS > 80
                        df_res = df_res[
                            (df_res['rs_1d_val'] > 80) | 
                            (df_res['rs_1w_val'] > 80)
                        ]
                        
                    if 'WEAK RS' in filter_list:
                        # 1D RS < 20 OR 1W RS < 20
                        df_res = df_res[
                            (df_res['rs_1d_val'] < 20) | 
                            (df_res['rs_1w_val'] < 20)
                        ]

                # Convert back to results list, dropping temporary columns
                # Note: We can keep the raw val columns if needed, or drop them.
                # Dropping to keep response clean.
                cols_to_drop = ['mtd_val', 'perf_3m_val', 'as_1m', 'as_3m', 'rs_1d_val', 'rs_1w_val']
                # Only drop if they exist
                cols_to_drop = [c for c in cols_to_drop if c in df_res.columns]
                # Only drop columns that exist
                cols_to_drop = [c for c in cols_to_drop if c in df_res.columns]
                results = df_res.drop(columns=cols_to_drop).to_dict('records')

        alert_cache.set(cache_key, results)
        return results

    except Exception as e:
        return {"error": str(e)}
    finally:
        session.close()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
