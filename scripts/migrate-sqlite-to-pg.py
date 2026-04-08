#!/usr/bin/env python3
"""
SQLite → PostgreSQL Data Migration

Migrates all data from data.db (SQLite) to insider_signal (PostgreSQL).

KEY CHANGE: Instead of migrating the 57.5M-row daily_forward_returns table,
we extract unique (ticker, date, close) tuples into the new daily_prices table.
This is the professional quant pattern — store prices as source of truth,
compute forward returns on demand via SQL LATERAL joins.

Expected data reduction: ~57.5M rows → ~5M rows (~90% smaller)

Usage:
  python3 scripts/migrate-sqlite-to-pg.py
"""

import sqlite3
import psycopg2
import psycopg2.extras
import sys
import time
import os
from io import StringIO

SQLITE_PATH = os.environ.get("SQLITE_PATH", "data.db")
PG_URL = os.environ.get("DATABASE_URL", "postgresql://postgres@localhost:5432/insider_signal")

# Tables to migrate, in dependency order
# NOTE: daily_forward_returns is NOT migrated — replaced by daily_prices extraction
TABLES = [
    {
        "name": "insider_transactions",
        "sqlite_query": "SELECT * FROM insider_transactions",
        "pg_columns": [
            "id", "accession_number", "filing_date", "filing_timestamp", "filing_market_state",
            "issuer_cik", "issuer_name", "issuer_ticker", "reporting_person_name",
            "reporting_person_cik", "reporting_person_title", "is_director", "is_officer",
            "is_ten_percent_owner", "transaction_type", "transaction_date", "transaction_code",
            "shares_traded", "price_per_share", "total_value", "shares_owned_after",
            "ownership_type", "ownership_nature", "security_title", "filing_lag_days",
            "ownership_change_pct", "indirect_account_type", "is_cluster_buy",
            "is_opportunistic", "price_drift_from_tx", "prior_return_30d", "prior_return_90d",
            "distance_from_52w_high", "avg_daily_volume", "recent_volume_spike",
            "market_cap_at_filing", "sector", "industry", "created_at"
        ],
    },
    {
        "name": "purchase_signals",
        "sqlite_query": "SELECT * FROM purchase_signals",
        "pg_columns": [
            "id", "issuer_ticker", "issuer_name", "signal_date", "cluster_size",
            "total_purchase_value", "avg_purchase_price", "insider_names", "insider_titles",
            "has_ceo", "has_cfo", "has_director", "max_ownership_change_pct",
            "min_filing_lag", "has_opportunistic", "sector", "industry",
            "signal_score", "score_tier", "score_components", "created_at"
        ],
    },
    {
        "name": "insider_history",
        "sqlite_query": "SELECT * FROM insider_history",
        "pg_columns": [
            "id", "reporting_person_cik", "reporting_person_name", "total_purchases",
            "total_sales", "total_purchase_value", "total_sale_value", "avg_purchase_return_1y",
            "win_rate_1y", "companies_bought", "first_filing_date", "last_filing_date",
            "is_serial_buyer", "is_cluster_participant", "reputation_score", "updated_at"
        ],
    },
    {
        "name": "signal_entry_prices",
        "sqlite_query": "SELECT * FROM signal_entry_prices",
        "pg_columns": [
            "id", "signal_id", "filing_timestamp", "prior_close", "ah_price",
            "ah_spread_pct", "next_open", "next_vwap", "overnight_gap",
            "ah_net_premium", "insider_tx_price", "created_at"
        ],
    },
    # daily_forward_returns is NOT migrated — replaced by daily_prices extraction below
    {
        "name": "factor_analysis",
        "sqlite_query": "SELECT * FROM factor_analysis",
        "pg_columns": [
            "id", "factor_name", "slice_name", "horizon", "sample_size",
            "mean_excess_return", "median_excess_return", "std_dev", "t_stat",
            "win_rate", "information_ratio", "window_start", "window_end", "computed_at"
        ],
    },
    {
        "name": "model_weights",
        "sqlite_query": "SELECT * FROM model_weights",
        "pg_columns": [
            "id", "factor_name", "data_weight", "prior_weight", "effective_weight",
            "sample_size", "information_ratio", "optimal_horizon", "confidence_level",
            "last_updated"
        ],
    },
    {
        "name": "trade_executions",
        "sqlite_query": "SELECT * FROM trade_executions",
        "pg_columns": [
            "id", "source", "external_id", "ticker", "company_name", "side",
            "quantity", "avg_price", "total_cost", "execution_date", "execution_time",
            "account_id", "signal_id", "signal_score", "created_at"
        ],
    },
    {
        "name": "closed_trades",
        "sqlite_query": "SELECT * FROM closed_trades",
        "pg_columns": [
            "id", "ticker", "company_name", "entry_date", "exit_date",
            "entry_price", "exit_price", "quantity", "realized_pnl", "realized_pnl_pct",
            "holding_days", "signal_classification", "signal_id", "signal_score_at_entry",
            "exit_type", "created_at"
        ],
    },
    {
        "name": "execution_deviations",
        "sqlite_query": "SELECT * FROM execution_deviations",
        "pg_columns": [
            "id", "user_trade_id", "signal_id", "classification", "entry_delay_days",
            "entry_price_gap_pct", "exit_type", "pnl_difference", "alpha_cost", "created_at"
        ],
    },
    {
        "name": "portfolio_positions",
        "sqlite_query": "SELECT * FROM portfolio_positions",
        "pg_columns": [
            "id", "ticker", "company_name", "quantity", "avg_cost_basis",
            "current_price", "market_value", "unrealized_pnl", "unrealized_pnl_pct",
            "day_change", "day_change_pct", "signal_id", "signal_score_at_entry",
            "recommended_hold_days", "holding_days", "last_updated"
        ],
    },
    {
        "name": "strategy_snapshots",
        "sqlite_query": "SELECT * FROM strategy_snapshots",
        "pg_columns": [
            "id", "date", "daily_return", "cumulative_return", "sharpe_ratio",
            "sortino_ratio", "max_drawdown", "current_drawdown", "strategy_daily_return",
            "strategy_cumulative_return", "strategy_sharpe", "strategy_sortino",
            "benchmark_daily", "benchmark_cumulative", "alpha_vs_benchmark",
            "alpha_vs_strategy", "deviation_cost", "created_at"
        ],
    },
    {
        "name": "strategy_recommendations",
        "sqlite_query": "SELECT * FROM strategy_recommendations",
        "pg_columns": [
            "id", "signal_id", "recommended_action", "composite_score",
            "factor_alignment_score", "entry_timing_score", "exit_timing_score",
            "risk_score", "recommended_entry_price", "recommended_stop_loss",
            "recommended_take_profit", "recommended_hold_days", "current_status",
            "created_at", "updated_at"
        ],
    },
    {
        "name": "schwab_tokens",
        "sqlite_query": "SELECT * FROM schwab_tokens",
        "pg_columns": [
            "id", "access_token", "refresh_token", "token_type", "expires_at",
            "scope", "created_at", "updated_at"
        ],
    },
]

# Also migrate enrichment_failed_tickers (not a Drizzle table but used by Python scripts)
EXTRA_TABLES = [
    {
        "name": "enrichment_failed_tickers",
        "sqlite_query": "SELECT * FROM enrichment_failed_tickers",
        "create_sql": """
            CREATE TABLE IF NOT EXISTS enrichment_failed_tickers (
                ticker TEXT PRIMARY KEY,
                fail_count INTEGER DEFAULT 1,
                last_failed_at TEXT
            )
        """,
        "pg_columns": ["ticker", "fail_count", "last_failed_at"],
    },
]


def migrate_table(sqlite_conn, pg_conn, table_config, batch_size=10000):
    """Migrate a single table from SQLite to PostgreSQL."""
    name = table_config["name"]
    bs = table_config.get("batch_size", batch_size)
    
    print(f"\n  [{name}] Starting migration...")
    
    # Count rows in SQLite
    try:
        count = sqlite_conn.execute(f"SELECT COUNT(*) FROM {name}").fetchone()[0]
    except Exception:
        print(f"  [{name}] Table not found in SQLite, skipping")
        return 0
    
    if count == 0:
        print(f"  [{name}] Empty, skipping")
        return 0
    
    print(f"  [{name}] {count:,} rows to migrate")
    
    # Clear target table
    with pg_conn.cursor() as cur:
        cur.execute(f"TRUNCATE {name} CASCADE")
    pg_conn.commit()
    
    # Get column names from SQLite
    sqlite_cursor = sqlite_conn.execute(f"SELECT * FROM {name} LIMIT 1")
    sqlite_columns = [desc[0] for desc in sqlite_cursor.description]
    
    # Map SQLite column names to PostgreSQL column names
    pg_columns = table_config.get("pg_columns")
    if not pg_columns:
        pg_columns = sqlite_columns
    
    # Build column mapping — match by position
    col_count = min(len(sqlite_columns), len(pg_columns))
    
    # Migrate in batches using execute_batch for speed
    migrated = 0
    offset = 0
    start_time = time.time()
    
    while offset < count:
        # Fetch batch from SQLite
        rows = sqlite_conn.execute(
            f"SELECT * FROM {name} LIMIT {bs} OFFSET {offset}"
        ).fetchall()
        
        if not rows:
            break
        
        # Use execute_values for bulk insert (faster than individual inserts)
        placeholders = ",".join(["%s"] * col_count)
        insert_sql = f"INSERT INTO {name} ({','.join(pg_columns[:col_count])}) VALUES ({placeholders})"
        
        # Convert rows, handling boolean conversion (SQLite uses 0/1)
        converted_rows = []
        for row in rows:
            converted = list(row[:col_count])
            converted_rows.append(converted)
        
        with pg_conn.cursor() as cur:
            psycopg2.extras.execute_batch(cur, insert_sql, converted_rows, page_size=1000)
        pg_conn.commit()
        
        migrated += len(rows)
        offset += bs
        
        elapsed = time.time() - start_time
        rate = migrated / elapsed if elapsed > 0 else 0
        pct = (migrated / count) * 100
        print(f"  [{name}] {migrated:,}/{count:,} ({pct:.1f}%) — {rate:.0f} rows/sec")
    
    # Reset sequence to max ID
    with pg_conn.cursor() as cur:
        try:
            cur.execute(f"SELECT MAX(id) FROM {name}")
            max_id = cur.fetchone()[0]
            if max_id:
                cur.execute(f"SELECT setval('{name}_id_seq', {max_id})")
        except Exception:
            pass  # Table might not have an id sequence
    pg_conn.commit()
    
    elapsed = time.time() - start_time
    print(f"  [{name}] Done: {migrated:,} rows in {elapsed:.1f}s")
    return migrated


def extract_daily_prices(sqlite_conn, pg_conn):
    """
    Extract unique (ticker, date, close) from SQLite daily_forward_returns
    into the new daily_prices table.
    
    The old table has ~57.5M rows with many duplicates per (ticker, date).
    We extract unique price points — expected ~5M rows.
    
    Also extracts SPY benchmark prices from the benchmark_close column.
    """
    print("\n" + "=" * 60)
    print("  Extracting daily_prices from daily_forward_returns")
    print("=" * 60)
    
    # First, ensure daily_prices table exists
    with pg_conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS daily_prices (
                id SERIAL PRIMARY KEY,
                ticker TEXT NOT NULL,
                date TEXT NOT NULL,
                open DOUBLE PRECISION,
                high DOUBLE PRECISION,
                low DOUBLE PRECISION,
                close DOUBLE PRECISION NOT NULL,
                volume DOUBLE PRECISION
            )
        """)
        cur.execute("TRUNCATE daily_prices")
        # Create unique constraint for upserts
        cur.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS idx_daily_prices_ticker_date 
            ON daily_prices(ticker, date)
        """)
    pg_conn.commit()
    
    # Check if daily_forward_returns exists in SQLite
    try:
        total = sqlite_conn.execute("SELECT COUNT(*) FROM daily_forward_returns").fetchone()[0]
    except Exception:
        print("  daily_forward_returns not found in SQLite, skipping")
        return 0
    
    print(f"  Source: {total:,} rows in daily_forward_returns")
    
    # Step 1: Extract unique stock prices
    # Group by signal → get ticker from purchase_signals, then deduplicate
    print("\n  [Step 1] Extracting unique stock prices...")
    
    # We need to join with purchase_signals to get the ticker for each signal
    # Then extract unique (ticker, date, close) combinations
    start_time = time.time()
    
    # Get distinct signal_ids and their tickers
    ticker_map = {}
    rows = sqlite_conn.execute("""
        SELECT DISTINCT dfr.signal_id, ps.issuer_ticker
        FROM daily_forward_returns dfr
        JOIN purchase_signals ps ON ps.id = dfr.signal_id
        WHERE ps.issuer_ticker IS NOT NULL
    """).fetchall()
    
    for row in rows:
        ticker_map[row[0]] = row[1]
    
    print(f"  Found {len(ticker_map):,} signals across unique tickers")
    
    # Extract unique (ticker, date, close) in batches
    # Process by signal to build the deduplicated price dataset
    batch_size = 100000
    offset = 0
    price_buffer = {}  # (ticker, date) -> close
    spy_buffer = {}    # date -> close (from benchmark_close)
    
    while offset < total:
        rows = sqlite_conn.execute(f"""
            SELECT signal_id, calendar_date, close_price, benchmark_close
            FROM daily_forward_returns
            WHERE close_price IS NOT NULL
            LIMIT {batch_size} OFFSET {offset}
        """).fetchall()
        
        if not rows:
            break
        
        for row in rows:
            signal_id, cal_date, close_price, benchmark_close = row
            
            if signal_id in ticker_map and cal_date:
                ticker = ticker_map[signal_id]
                key = (ticker, cal_date)
                if key not in price_buffer:
                    price_buffer[key] = close_price
                
                # Also capture SPY benchmark prices
                if benchmark_close and cal_date not in spy_buffer:
                    spy_buffer[cal_date] = benchmark_close
        
        offset += batch_size
        elapsed = time.time() - start_time
        pct = min(100, (offset / total) * 100)
        print(f"  Scanned {min(offset, total):,}/{total:,} ({pct:.1f}%) — "
              f"{len(price_buffer):,} stock prices, {len(spy_buffer):,} SPY prices")
    
    # Add SPY prices to buffer
    for date, close in spy_buffer.items():
        price_buffer[("SPY", date)] = close
    
    total_prices = len(price_buffer)
    print(f"\n  Total unique prices to insert: {total_prices:,}")
    print(f"  Compression ratio: {total:,} → {total_prices:,} ({total_prices/total*100:.1f}%)")
    
    # Step 2: Bulk insert into daily_prices
    print("\n  [Step 2] Inserting into daily_prices...")
    
    insert_sql = """
        INSERT INTO daily_prices (ticker, date, close) 
        VALUES (%s, %s, %s)
        ON CONFLICT (ticker, date) DO NOTHING
    """
    
    items = list(price_buffer.items())
    inserted = 0
    insert_batch = 5000
    start_time = time.time()
    
    for i in range(0, len(items), insert_batch):
        batch = items[i:i+insert_batch]
        rows = [(ticker, date, close) for (ticker, date), close in batch]
        
        with pg_conn.cursor() as cur:
            psycopg2.extras.execute_batch(cur, insert_sql, rows, page_size=1000)
        pg_conn.commit()
        
        inserted += len(batch)
        elapsed = time.time() - start_time
        rate = inserted / elapsed if elapsed > 0 else 0
        pct = (inserted / total_prices) * 100
        print(f"  Inserted {inserted:,}/{total_prices:,} ({pct:.1f}%) — {rate:.0f} rows/sec")
    
    # Create additional indexes
    print("\n  [Step 3] Creating indexes...")
    with pg_conn.cursor() as cur:
        cur.execute("CREATE INDEX IF NOT EXISTS idx_daily_prices_ticker ON daily_prices(ticker)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_daily_prices_date ON daily_prices(date)")
    pg_conn.commit()
    
    # Reset sequence
    with pg_conn.cursor() as cur:
        cur.execute("SELECT MAX(id) FROM daily_prices")
        max_id = cur.fetchone()[0]
        if max_id:
            cur.execute(f"SELECT setval('daily_prices_id_seq', {max_id})")
    pg_conn.commit()
    
    elapsed = time.time() - start_time
    print(f"\n  daily_prices extraction complete: {inserted:,} rows in {elapsed:.1f}s")
    
    return inserted


def create_pg_schema(pg_conn):
    """Create PostgreSQL tables using the schema from create-pg-schema.sql if it exists."""
    schema_path = os.path.join(os.path.dirname(__file__), "..", "create-pg-schema.sql")
    if os.path.exists(schema_path):
        print("[SCHEMA] Applying create-pg-schema.sql...")
        with open(schema_path) as f:
            sql = f.read()
        with pg_conn.cursor() as cur:
            cur.execute(sql)
        pg_conn.commit()
        print("[SCHEMA] Done")
    else:
        print("[SCHEMA] No create-pg-schema.sql found, assuming tables already exist")


def create_indexes(pg_conn):
    """Create performance indexes after data migration."""
    print("\n[INDEXES] Creating performance indexes...")
    indexes = [
        # daily_prices indexes (replacing daily_forward_returns indexes)
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_daily_prices_ticker_date ON daily_prices(ticker, date)",
        "CREATE INDEX IF NOT EXISTS idx_daily_prices_ticker ON daily_prices(ticker)",
        "CREATE INDEX IF NOT EXISTS idx_daily_prices_date ON daily_prices(date)",
        # Standard indexes
        "CREATE INDEX IF NOT EXISTS idx_it_ticker_date ON insider_transactions(issuer_ticker, filing_date)",
        "CREATE INDEX IF NOT EXISTS idx_it_tx_type ON insider_transactions(transaction_type)",
        "CREATE INDEX IF NOT EXISTS idx_it_accession ON insider_transactions(accession_number)",
        "CREATE INDEX IF NOT EXISTS idx_ps_ticker ON purchase_signals(issuer_ticker)",
        "CREATE INDEX IF NOT EXISTS idx_ps_date ON purchase_signals(signal_date)",
        "CREATE INDEX IF NOT EXISTS idx_ps_score ON purchase_signals(signal_score DESC)",
        "CREATE INDEX IF NOT EXISTS idx_sep_signal ON signal_entry_prices(signal_id)",
        "CREATE INDEX IF NOT EXISTS idx_fa_factor_horizon ON factor_analysis(factor_name, horizon)",
        "CREATE INDEX IF NOT EXISTS idx_te_ticker ON trade_executions(ticker)",
        "CREATE INDEX IF NOT EXISTS idx_ed_trade ON execution_deviations(user_trade_id)",
        "CREATE INDEX IF NOT EXISTS idx_ed_signal ON execution_deviations(signal_id)",
        "CREATE INDEX IF NOT EXISTS idx_ct_ticker ON closed_trades(ticker)",
        "CREATE INDEX IF NOT EXISTS idx_ct_exit_date ON closed_trades(exit_date)",
        "CREATE INDEX IF NOT EXISTS idx_ss_date ON strategy_snapshots(date)",
    ]
    
    with pg_conn.cursor() as cur:
        for idx_sql in indexes:
            try:
                cur.execute(idx_sql)
                idx_name = idx_sql.split("EXISTS ")[1].split(" ON")[0]
                print(f"  Created: {idx_name}")
            except Exception as e:
                print(f"  Warning: {e}")
    pg_conn.commit()
    print("[INDEXES] Done")


def main():
    print("=" * 60)
    print("  SQLite → PostgreSQL Data Migration")
    print("  (daily_prices extraction mode)")
    print("=" * 60)
    print(f"\n  SQLite: {SQLITE_PATH}")
    print(f"  PostgreSQL: {PG_URL}\n")
    
    # Connect to both databases
    sqlite_conn = sqlite3.connect(SQLITE_PATH)
    sqlite_conn.row_factory = sqlite3.Row
    
    pg_conn = psycopg2.connect(PG_URL)
    
    # Create schema if needed
    create_pg_schema(pg_conn)
    
    # Create extra tables
    for extra in EXTRA_TABLES:
        with pg_conn.cursor() as cur:
            cur.execute(extra["create_sql"])
        pg_conn.commit()
    
    # Migrate each standard table (NOT daily_forward_returns)
    total_rows = 0
    start_time = time.time()
    
    for table_config in TABLES:
        try:
            rows = migrate_table(sqlite_conn, pg_conn, table_config)
            total_rows += rows
        except Exception as e:
            print(f"  [{table_config['name']}] ERROR: {e}")
            pg_conn.rollback()
    
    # Migrate extra tables
    for extra in EXTRA_TABLES:
        try:
            rows = migrate_table(sqlite_conn, pg_conn, extra)
            total_rows += rows
        except Exception as e:
            print(f"  [{extra['name']}] ERROR: {e}")
            pg_conn.rollback()
    
    # Extract daily_prices from daily_forward_returns
    price_rows = extract_daily_prices(sqlite_conn, pg_conn)
    total_rows += price_rows
    
    # Create indexes
    create_indexes(pg_conn)
    
    # Final summary
    elapsed = time.time() - start_time
    print(f"\n{'=' * 60}")
    print(f"  Migration Complete")
    print(f"  Total rows: {total_rows:,}")
    print(f"  Total time: {elapsed:.1f}s ({elapsed/60:.1f} min)")
    print(f"{'=' * 60}")
    
    # Verify counts
    print("\n  Verification:")
    for table_config in TABLES:
        name = table_config["name"]
        try:
            sqlite_count = sqlite_conn.execute(f"SELECT COUNT(*) FROM {name}").fetchone()[0]
        except:
            sqlite_count = 0
        with pg_conn.cursor() as cur:
            try:
                cur.execute(f"SELECT COUNT(*) FROM {name}")
                pg_count = cur.fetchone()[0]
            except:
                pg_count = 0
        status = "✓" if sqlite_count == pg_count else "✗ MISMATCH"
        if sqlite_count > 0 or pg_count > 0:
            print(f"    {name:35s} SQLite: {sqlite_count:>12,} | PG: {pg_count:>12,} {status}")
    
    # Show daily_prices extraction result
    with pg_conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM daily_prices")
        dp_count = cur.fetchone()[0]
    try:
        dfr_count = sqlite_conn.execute("SELECT COUNT(*) FROM daily_forward_returns").fetchone()[0]
    except:
        dfr_count = 0
    print(f"    {'daily_prices (from forward_returns)':35s} SQLite: {dfr_count:>12,} | PG: {dp_count:>12,} (extracted)")
    
    sqlite_conn.close()
    pg_conn.close()


if __name__ == "__main__":
    main()
