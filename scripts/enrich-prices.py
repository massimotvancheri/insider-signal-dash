#!/usr/bin/env python3
"""
Market Data Enrichment — Price & Forward Returns
Fetches historical prices via yfinance for all signal tickers,
computes forward returns, and stores in SQLite.

Usage:
  python3 scripts/enrich-prices.py [max_tickers] [start_year]
  python3 scripts/enrich-prices.py 100 2024    # Small test
  python3 scripts/enrich-prices.py 5000 2020   # Full enrichment
"""

import sqlite3
import sys
import time
from datetime import datetime, timedelta
import yfinance as yf
import pandas as pd
import numpy as np
import json

DB_PATH = "data.db"

def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=5000")
    # Create failed tickers table if not exists
    conn.execute("""
        CREATE TABLE IF NOT EXISTS enrichment_failed_tickers (
            ticker TEXT PRIMARY KEY,
            fail_count INTEGER DEFAULT 1,
            last_failed_at TEXT
        )
    """)
    conn.commit()
    return conn

def get_signals_to_enrich(conn, start_year=2020, max_signals=5000):
    """Get signals that need enrichment, prioritized by score and recency.
    Excludes tickers that have permanently failed (no data on Yahoo Finance)."""
    cursor = conn.execute("""
        SELECT ps.id, ps.issuer_ticker, ps.signal_date, ps.signal_score, ps.avg_purchase_price
        FROM purchase_signals ps
        WHERE ps.issuer_ticker IS NOT NULL
          AND ps.issuer_ticker != ''
          AND ps.issuer_ticker != 'NONE'
          AND ps.issuer_ticker != 'N/A'
          AND ps.signal_date >= ?
          AND ps.id NOT IN (SELECT signal_id FROM signal_entry_prices)
          AND ps.issuer_ticker NOT IN (SELECT ticker FROM enrichment_failed_tickers WHERE fail_count >= 2)
        ORDER BY ps.signal_score DESC, ps.signal_date DESC
        LIMIT ?
    """, (f"{start_year}-01-01", max_signals))
    return cursor.fetchall()

def mark_ticker_failed(conn, ticker):
    """Mark a ticker as failed so it's skipped in future batches."""
    conn.execute("""
        INSERT INTO enrichment_failed_tickers (ticker, fail_count, last_failed_at)
        VALUES (?, 1, ?)
        ON CONFLICT(ticker) DO UPDATE SET
          fail_count = fail_count + 1,
          last_failed_at = ?
    """, (ticker, datetime.now().isoformat(), datetime.now().isoformat()))
    conn.commit()

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

def compute_forward_returns(conn, signal, prices, spy_data):
    """Compute and store entry prices + daily forward returns for a signal."""
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
    
    # Save entry prices
    conn.execute("""
        INSERT INTO signal_entry_prices 
        (signal_id, filing_timestamp, prior_close, ah_price, ah_spread_pct,
         next_open, next_vwap, overnight_gap, ah_net_premium, insider_tx_price, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
    
    # Compute daily forward returns (day 0 through 252)
    forward_prices = [p for p in prices if p["date"] >= filing_date]
    forward_prices.sort(key=lambda p: p["date"])
    
    entry_date = next_open_day["date"]
    spy_entry = spy_data.get(entry_date, {})
    spy_entry_close = spy_entry.get("close", 0)
    
    rows_to_insert = []
    for d_idx, day_price in enumerate(forward_prices[:253]):
        if entry_open <= 0:
            continue
        
        ret_from_open = (day_price["close"] / entry_open) - 1
        
        spy_day = spy_data.get(day_price["date"], {})
        spy_close = spy_day.get("close", 0)
        bench_ret = (spy_close / spy_entry_close - 1) if spy_entry_close > 0 and spy_close > 0 else None
        
        excess = (ret_from_open - bench_ret) if bench_ret is not None else None
        
        rows_to_insert.append((
            signal_id, d_idx, day_price["date"],
            day_price["close"],
            spy_close if spy_close > 0 else None,
            ret_from_open, None,  # AH entry return
            excess, None,  # AH excess
        ))
    
    if rows_to_insert:
        conn.executemany("""
            INSERT INTO daily_forward_returns
            (signal_id, trading_day, calendar_date, close_price, benchmark_close,
             return_from_next_open, return_from_ah_entry, excess_from_next_open, excess_from_ah_entry)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, rows_to_insert)
    
    # Update transaction records with market context
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
        set_clause = ", ".join(f"{k} = ?" for k in updates.keys())
        conn.execute(f"""
            UPDATE insider_transactions SET {set_clause}
            WHERE issuer_ticker = ? AND filing_date = ?
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
    
    # Get signals to enrich
    signals = get_signals_to_enrich(conn, start_year, max_tickers * 10)
    print(f"[signals] {len(signals)} signals to enrich")
    
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
    total_fwd_days = 0
    
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
        
        # Rate limit
        time.sleep(0.15)
        
        # Enrich each signal
        for sig in ticker_signals:
            try:
                ok = compute_forward_returns(conn, sig, prices, spy_data)
                if ok:
                    success_count += 1
            except Exception as e:
                pass
        
        # Commit every 10 tickers
        if (i + 1) % 10 == 0:
            conn.commit()
            pct = ((i + 1) / len(tickers)) * 100
            print(f"  [{i+1}/{len(tickers)}] ({pct:.1f}%) | {success_count} enriched, {fail_count} failed")
    
    conn.commit()
    
    # Final stats
    entry_count = conn.execute("SELECT COUNT(*) FROM signal_entry_prices").fetchone()[0]
    fwd_count = conn.execute("SELECT COUNT(*) FROM daily_forward_returns").fetchone()[0]
    
    print(f"\n=== Enrichment Complete ===")
    print(f"Tickers processed: {len(tickers)}")
    print(f"Signals enriched: {success_count}")
    print(f"Failed tickers: {fail_count}")
    print(f"Entry price records: {entry_count}")
    print(f"Forward return data points: {fwd_count}")
    
    conn.close()

if __name__ == "__main__":
    main()
