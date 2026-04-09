#!/usr/bin/env python3
"""
Market Data Enrichment — Price Store & Signal Entry Prices
Fetches historical prices via yfinance for signal tickers,
stores in daily_prices table, and computes signal entry prices.

Professional quant pattern: store prices as source of truth,
compute forward returns on demand from daily_prices.

Usage:
  python3 scripts/enrich-prices.py [max_tickers] [start_year]
  python3 scripts/enrich-prices.py 100 2024    # Small test
  python3 scripts/enrich-prices.py 5000 2020   # Full enrichment
"""

import psycopg2
import psycopg2.extras
import sys
import time
import os
from datetime import datetime, timedelta
import yfinance as yf
import pandas as pd
import numpy as np
import json

DB_URL = os.environ.get("DATABASE_URL", "postgresql://postgres@localhost:5432/insider_signal")

def get_db():
    conn = psycopg2.connect(DB_URL)
    conn.autocommit = False
    # Create helper tables if not exists
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS enrichment_failed_tickers (
                ticker TEXT PRIMARY KEY,
                fail_count INTEGER DEFAULT 1,
                last_failed_at TEXT
            )
        """)
        # Ensure daily_prices unique constraint
        cur.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS idx_daily_prices_ticker_date_uniq 
            ON daily_prices(ticker, date)
        """)
    conn.commit()
    return conn

def get_signals_to_enrich(conn, start_year=2020, max_signals=5000):
    """Get signals that need enrichment, prioritized by score and recency.
    Uses LEFT JOIN (not NOT IN) for performance — critical with 200K+ entry prices.
    Excludes tickers that have permanently failed (no data on Yahoo Finance)."""
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("""
            SELECT ps.id, ps.issuer_ticker, ps.signal_date, ps.signal_score, ps.avg_purchase_price
            FROM purchase_signals ps
            LEFT JOIN signal_entry_prices sep ON sep.signal_id = ps.id
            LEFT JOIN enrichment_failed_tickers eft ON eft.ticker = ps.issuer_ticker
            WHERE ps.issuer_ticker IS NOT NULL
              AND ps.issuer_ticker != ''
              AND ps.issuer_ticker != 'NONE'
              AND ps.issuer_ticker != 'N/A'
              AND ps.signal_date >= %s
              AND sep.signal_id IS NULL
              AND eft.ticker IS NULL
            ORDER BY ps.signal_score DESC, ps.signal_date DESC
            LIMIT %s
        """, (f"{start_year}-01-01", max_signals))
        return cur.fetchall()

def mark_ticker_failed(conn, ticker):
    """Mark a ticker as failed so it's skipped in future batches."""
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO enrichment_failed_tickers (ticker, fail_count, last_failed_at)
            VALUES (%s, 1, %s)
            ON CONFLICT(ticker) DO UPDATE SET
              fail_count = enrichment_failed_tickers.fail_count + 1,
              last_failed_at = %s
        """, (ticker, datetime.now().isoformat(), datetime.now().isoformat()))
    conn.commit()

def store_daily_prices(conn, ticker, prices):
    """Store daily OHLCV prices into daily_prices table (idempotent via ON CONFLICT)."""
    if not prices:
        return 0
    
    rows = [(ticker, p["date"], p.get("open"), p.get("high"), p.get("low"), 
             p["close"], p.get("volume")) for p in prices]
    
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, """
            INSERT INTO daily_prices (ticker, date, open, high, low, close, volume)
            VALUES %s
            ON CONFLICT (ticker, date) DO UPDATE SET
              open = COALESCE(EXCLUDED.open, daily_prices.open),
              high = COALESCE(EXCLUDED.high, daily_prices.high),
              low = COALESCE(EXCLUDED.low, daily_prices.low),
              close = EXCLUDED.close,
              volume = COALESCE(EXCLUDED.volume, daily_prices.volume)
        """, rows, page_size=500)
    return len(rows)

def store_spy_prices(conn, spy_data):
    """Store SPY benchmark prices into daily_prices table."""
    rows = [("SPY", date, d.get("open"), d.get("high"), d.get("low"), 
             d["close"], d.get("volume")) 
            for date, d in spy_data.items() if d.get("close", 0) > 0]
    
    if not rows:
        return 0
    
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, """
            INSERT INTO daily_prices (ticker, date, open, high, low, close, volume)
            VALUES %s
            ON CONFLICT (ticker, date) DO UPDATE SET
              open = COALESCE(EXCLUDED.open, daily_prices.open),
              high = COALESCE(EXCLUDED.high, daily_prices.high),
              low = COALESCE(EXCLUDED.low, daily_prices.low),
              close = EXCLUDED.close,
              volume = COALESCE(EXCLUDED.volume, daily_prices.volume)
        """, rows, page_size=500)
    conn.commit()
    return len(rows)

def fetch_spy_data():
    """Fetch SPY benchmark data for the full period."""
    print("[SPY] Fetching benchmark data (2015-2026)...")
    spy = yf.download("SPY", start="2015-01-01", end="2026-12-31", progress=False)
    if spy.empty:
        print("[SPY] WARNING: No SPY data returned!")
        return {}
    
    # Flatten MultiIndex if present
    if isinstance(spy.columns, pd.MultiIndex):
        spy.columns = spy.columns.get_level_values(0)
    
    result = {}
    for idx, row in spy.iterrows():
        date_str = idx.strftime("%Y-%m-%d")
        result[date_str] = {
            "open": float(row.get("Open", 0)),
            "high": float(row.get("High", 0)),
            "low": float(row.get("Low", 0)),
            "close": float(row.get("Close", 0)),
            "volume": float(row.get("Volume", 0)),
        }
    print(f"[SPY] Loaded {len(result)} trading days")
    return result

def fetch_ticker_prices(ticker, start_date, end_date, max_retries=2):
    """Fetch daily OHLCV for a single ticker."""
    for attempt in range(max_retries):
        try:
            data = yf.download(
                ticker, start=start_date, end=end_date, 
                progress=False, timeout=10
            )
            if data.empty:
                return []
            
            # Flatten MultiIndex if present
            if isinstance(data.columns, pd.MultiIndex):
                data.columns = data.columns.get_level_values(0)
            
            prices = []
            for idx, row in data.iterrows():
                close = float(row.get("Close", 0))
                if close <= 0 or np.isnan(close):
                    continue
                prices.append({
                    "date": idx.strftime("%Y-%m-%d"),
                    "open": float(row.get("Open", 0)),
                    "high": float(row.get("High", 0)),
                    "low": float(row.get("Low", 0)),
                    "close": close,
                    "volume": float(row.get("Volume", 0)),
                })
            return prices
        except Exception as e:
            if attempt < max_retries - 1:
                time.sleep(1)
            continue
    return []

def compute_entry_prices(conn, signal, prices, spy_data):
    """Compute and store entry prices for a signal.
    Prices are already stored in daily_prices — we just compute entry reference points."""
    signal_id = signal["id"]
    filing_date = signal["signal_date"]
    insider_tx_price = signal["avg_purchase_price"] or 0
    
    if not prices:
        return False
    
    price_map = {p["date"]: p for p in prices}
    
    # Find entry prices
    fd = datetime.strptime(filing_date, "%Y-%m-%d")
    
    prior_close = None
    next_open_day = None
    
    # Prior close: last trading day on or before filing
    for offset in range(0, 10):
        d = (fd - timedelta(days=offset)).strftime("%Y-%m-%d")
        if d in price_map:
            prior_close = price_map[d]
            break
    
    # Next open: first trading day after filing
    for offset in range(1, 7):
        d = (fd + timedelta(days=offset)).strftime("%Y-%m-%d")
        if d in price_map:
            next_open_day = price_map[d]
            break
    
    # Fallback: filing day itself
    if not next_open_day and filing_date in price_map:
        next_open_day = price_map[filing_date]
    
    if not next_open_day:
        return False
    
    entry_open = next_open_day["open"] if next_open_day["open"] > 0 else next_open_day["close"]
    
    # Overnight gap
    overnight_gap = None
    if prior_close and prior_close["close"] > 0:
        overnight_gap = (entry_open / prior_close["close"]) - 1
    
    # Price drift from insider's transaction price
    price_drift = None
    if insider_tx_price > 0 and prior_close and prior_close["close"] > 0:
        price_drift = (prior_close["close"] / insider_tx_price) - 1
    
    # Save entry prices (skip duplicates gracefully)
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO signal_entry_prices 
            (signal_id, filing_timestamp, prior_close, ah_price, ah_spread_pct,
             next_open, next_vwap, overnight_gap, ah_net_premium, insider_tx_price, created_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT DO NOTHING
        """, (
            signal_id, None,
            prior_close["close"] if prior_close else None,
            None, None,  # AH data not available from daily
            entry_open,
            next_open_day["close"],  # Approximate VWAP with close
            overnight_gap, None,
            insider_tx_price,
            datetime.now().isoformat()
        ))
    
    # Enrich insider_transactions with market context data
    updates = {}
    
    if price_drift is not None:
        updates["price_drift_from_tx"] = price_drift
    
    # Prior 30d return
    fd30 = (fd - timedelta(days=45)).strftime("%Y-%m-%d")
    prices_before = [p for p in prices if fd30 <= p["date"] <= filing_date]
    if len(prices_before) >= 2 and prices_before[-1]["close"] > 0 and prices_before[0]["close"] > 0:
        updates["prior_return_30d"] = (prices_before[-1]["close"] / prices_before[0]["close"]) - 1
    
    # Prior 90d return
    fd90 = (fd - timedelta(days=120)).strftime("%Y-%m-%d")
    prices_before_90 = [p for p in prices if fd90 <= p["date"] <= filing_date]
    if len(prices_before_90) >= 2 and prices_before_90[-1]["close"] > 0 and prices_before_90[0]["close"] > 0:
        updates["prior_return_90d"] = (prices_before_90[-1]["close"] / prices_before_90[0]["close"]) - 1
    
    # 52-week high distance
    fd_365 = (fd - timedelta(days=365)).strftime("%Y-%m-%d")
    year_prices = [p for p in prices if fd_365 <= p["date"] <= filing_date]
    if year_prices and prior_close:
        high_52w = max(p["high"] for p in year_prices)
        if high_52w > 0:
            updates["distance_from_52w_high"] = prior_close["close"] / high_52w
    
    # Average daily volume (30-day)
    recent = [p for p in prices_before if p["volume"] > 0][-22:]
    if recent:
        updates["avg_daily_volume"] = sum(p["volume"] for p in recent) / len(recent)
        # Volume spike
        last5 = recent[-5:]
        if last5 and updates["avg_daily_volume"] > 0:
            avg5 = sum(p["volume"] for p in last5) / len(last5)
            updates["recent_volume_spike"] = avg5 / updates["avg_daily_volume"]
    
    if updates:
        set_clause = ", ".join(f"{k} = %s" for k in updates.keys())
        with conn.cursor() as cur:
            cur.execute(f"""
                UPDATE insider_transactions SET {set_clause}
                WHERE issuer_ticker = %s AND filing_date = %s
            """, list(updates.values()) + [signal["issuer_ticker"], filing_date])
    
    return True

def main():
    max_tickers = int(sys.argv[1]) if len(sys.argv) > 1 else 500
    start_year = int(sys.argv[2]) if len(sys.argv) > 2 else 2020
    
    print(f"=== Market Data Enrichment (Python/yfinance) ===")
    print(f"Max tickers: {max_tickers}, Start year: {start_year}\n")
    
    conn = get_db()
    
    # Fetch SPY benchmark
    spy_data = fetch_spy_data()
    if not spy_data:
        print("ERROR: Could not fetch SPY data. Aborting.")
        return
    
    # Store SPY prices in daily_prices
    spy_stored = store_spy_prices(conn, spy_data)
    print(f"[SPY] Stored {spy_stored} days in daily_prices\n")
    
    # Get signals to enrich
    signals = get_signals_to_enrich(conn, start_year, max_tickers * 10)
    print(f"[signals] {len(signals)} signals to enrich")
    
    if len(signals) == 0:
        print("0 signals to enrich — all done!")
        return
    
    # Group by ticker
    by_ticker = {}
    for sig in signals:
        t = sig["issuer_ticker"]
        if t not in by_ticker:
            by_ticker[t] = []
        by_ticker[t].append(sig)
    
    tickers = list(by_ticker.keys())[:max_tickers]
    print(f"[tickers] {len(tickers)} unique tickers to process\n")
    
    success_count = 0
    fail_count = 0
    prices_stored = 0
    
    for i, ticker in enumerate(tickers):
        ticker_signals = by_ticker[ticker]
        
        # Date range for this ticker
        dates = sorted([s["signal_date"] for s in ticker_signals])
        start_d = (datetime.strptime(dates[0], "%Y-%m-%d") - timedelta(days=400)).strftime("%Y-%m-%d")
        end_d = (datetime.strptime(dates[-1], "%Y-%m-%d") + timedelta(days=400)).strftime("%Y-%m-%d")
        
        # Fetch prices
        prices = fetch_ticker_prices(ticker, start_d, end_d)
        
        if not prices:
            fail_count += 1
            mark_ticker_failed(conn, ticker)
            if (i + 1) % 50 == 0:
                print(f"  [{i+1}/{len(tickers)}] {ticker}: no data | Total: {success_count} enriched, {fail_count} failed")
            continue
        
        # Store prices in daily_prices table (idempotent)
        stored = store_daily_prices(conn, ticker, prices)
        prices_stored += stored
        
        # Rate limit
        time.sleep(0.15)
        
        # Compute entry prices for each signal
        ticker_success = 0
        for sig in ticker_signals:
            try:
                ok = compute_entry_prices(conn, sig, prices, spy_data)
                if ok:
                    success_count += 1
                    ticker_success += 1
            except Exception as e:
                pass  # Likely duplicate — already enriched
        
        # If no signals for this ticker were enriched, mark it as failed
        if ticker_success == 0:
            fail_count += 1
            mark_ticker_failed(conn, ticker)
        
        # Commit every 10 tickers
        if (i + 1) % 10 == 0:
            conn.commit()
            pct = ((i + 1) / len(tickers)) * 100
            print(f"  [{i+1}/{len(tickers)}] ({pct:.1f}%) | {success_count} enriched, {fail_count} failed, {prices_stored} price rows stored")
    
    conn.commit()
    
    # Final stats
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM signal_entry_prices")
        entry_count = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM daily_prices")
        price_count = cur.fetchone()[0]
        cur.execute("SELECT COUNT(DISTINCT ticker) FROM daily_prices")
        ticker_count = cur.fetchone()[0]
    
    print(f"\n=== Enrichment Complete ===")
    print(f"Tickers processed: {len(tickers)}")
    print(f"Signals enriched: {success_count}")
    print(f"Failed tickers: {fail_count}")
    print(f"Entry price records: {entry_count}")
    print(f"Daily price rows: {price_count} ({ticker_count} tickers)")
    
    conn.close()

if __name__ == "__main__":
    main()
