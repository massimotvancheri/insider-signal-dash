#!/usr/bin/env python3
"""
Factor Research Engine

For every measurable factor dimension, slices forward returns and computes:
- Mean excess return
- Median excess return
- Standard deviation
- t-statistic
- Win rate
- Information ratio (mean/std)

Stores results in factor_analysis table. The scoring model derives
its weights from these information ratios.

Usage:
  python3 scripts/factor-research.py
"""

import psycopg2
import psycopg2.extras
import math
import json
import os
from datetime import datetime

DB_URL = os.environ.get("DATABASE_URL", "postgresql://postgres@localhost:5432/insider_signal")

# Horizons to analyze (trading days from filing)
HORIZONS = [1, 2, 3, 5, 10, 21, 42, 63, 126, 252]

# Use signal_entry_prices instead of scanning 47M+ daily_forward_returns
# for the enriched signal set. Much faster.
ENRICHED_SIGNAL_FILTER = "ps.id IN (SELECT signal_id FROM signal_entry_prices)"

def get_db():
    conn = psycopg2.connect(DB_URL)
    conn.autocommit = False
    return conn

def compute_stats(values):
    """Compute statistical metrics for a list of return values."""
    values = [v for v in values if v is not None and not math.isnan(v)]
    n = len(values)
    if n < 5:
        return None
    
    mean = sum(values) / n
    sorted_vals = sorted(values)
    median = sorted_vals[n // 2]
    
    variance = sum((v - mean) ** 2 for v in values) / (n - 1)
    std = math.sqrt(variance) if variance > 0 else 0.0001
    
    t_stat = (mean / std) * math.sqrt(n)
    win_rate = sum(1 for v in values if v > 0) / n
    ir = mean / std if std > 0 else 0
    
    return {
        "n": n,
        "mean": mean,
        "median": median,
        "std": std,
        "t_stat": t_stat,
        "win_rate": win_rate,
        "ir": ir,
    }

def analyze_factor(conn, factor_name, sql_query, slice_labels=None):
    """
    Run a factor analysis query. The query should return rows with:
    - slice_name: the factor slice label
    - signal_id: the signal ID
    
    We join with daily_forward_returns to get excess returns at each horizon.
    """
    print(f"\n  [{factor_name}] Analyzing...")
    
    # Get signal IDs and their slice assignments
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(sql_query)
        rows = cur.fetchall()
    
    signal_slices = {}
    for row in rows:
        sid = row["signal_id"]
        slice_name = str(row["slice_name"])
        signal_slices[sid] = slice_name
    
    if not signal_slices:
        print(f"  [{factor_name}] No data")
        return 0
    
    # Get unique slices
    slices = set(signal_slices.values())
    print(f"  [{factor_name}] {len(signal_slices)} signals across {len(slices)} slices: {sorted(slices)}")
    
    # For each horizon, get excess returns grouped by slice
    results_count = 0
    for horizon in HORIZONS:
        # Fetch excess returns for this horizon
        signal_ids = list(signal_slices.keys())
        
        # Build query in batches to avoid memory issues
        returns_by_slice = {s: [] for s in slices}
        
        batch_size = 500
        for i in range(0, len(signal_ids), batch_size):
            batch = signal_ids[i:i+batch_size]
            placeholders = ",".join(["%s"] * len(batch))
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(f"""
                    SELECT signal_id, excess_from_next_open
                    FROM daily_forward_returns
                    WHERE signal_id IN ({placeholders}) AND trading_day = %s
                    AND excess_from_next_open IS NOT NULL
                """, batch + [horizon])
                
                for row in cur:
                    sid = row["signal_id"]
                    if sid in signal_slices:
                        slice_name = signal_slices[sid]
                        returns_by_slice[slice_name].append(row["excess_from_next_open"])
        
        # Compute stats for each slice
        for slice_name, returns in returns_by_slice.items():
            stats = compute_stats(returns)
            if stats is None:
                continue
            
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO factor_analysis
                    (factor_name, slice_name, horizon, sample_size, mean_excess_return,
                     median_excess_return, std_dev, t_stat, win_rate, information_ratio,
                     window_start, window_end, computed_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (factor_name, slice_name, horizon) DO UPDATE SET
                      sample_size = EXCLUDED.sample_size,
                      mean_excess_return = EXCLUDED.mean_excess_return,
                      median_excess_return = EXCLUDED.median_excess_return,
                      std_dev = EXCLUDED.std_dev,
                      t_stat = EXCLUDED.t_stat,
                      win_rate = EXCLUDED.win_rate,
                      information_ratio = EXCLUDED.information_ratio,
                      window_start = EXCLUDED.window_start,
                      window_end = EXCLUDED.window_end,
                      computed_at = EXCLUDED.computed_at
                """, (
                    factor_name, slice_name, horizon,
                    stats["n"], stats["mean"], stats["median"],
                    stats["std"], stats["t_stat"], stats["win_rate"], stats["ir"],
                    "2020-01-01", datetime.now().strftime("%Y-%m-%d"),
                    datetime.now().isoformat(),
                ))
            results_count += 1
    
    conn.commit()
    return results_count

def run_all_analyses(conn):
    """Run factor analysis for every measurable dimension."""
    
    total_results = 0
    
    # === Factor 1: Filing Lag ===
    total_results += analyze_factor(conn, "filing_lag", f"""
        SELECT ps.id as signal_id,
            CASE 
                WHEN it.filing_lag_days <= 1 THEN '0-1 days'
                WHEN it.filing_lag_days = 2 THEN '2 days (on-time)'
                WHEN it.filing_lag_days BETWEEN 3 AND 5 THEN '3-5 days (late)'
                WHEN it.filing_lag_days > 5 THEN '6+ days (very late)'
                ELSE 'unknown'
            END as slice_name
        FROM purchase_signals ps
        JOIN insider_transactions it ON it.issuer_ticker = ps.issuer_ticker 
            AND it.filing_date = ps.signal_date AND it.transaction_type = 'P'
        WHERE {ENRICHED_SIGNAL_FILTER}
            AND it.filing_lag_days IS NOT NULL AND it.filing_lag_days >= 0
        GROUP BY ps.id, it.filing_lag_days
    """)
    
    # === Factor 2: Direct vs Indirect ===
    total_results += analyze_factor(conn, "ownership_type", f"""
        SELECT ps.id as signal_id,
            CASE 
                WHEN it.indirect_account_type = 'direct' THEN 'Direct'
                WHEN it.indirect_account_type IN ('family_spouse', 'family_child', 'family_other') THEN 'Indirect-Family'
                WHEN it.indirect_account_type = 'trust' THEN 'Indirect-Trust'
                WHEN it.indirect_account_type IN ('retirement') THEN 'Indirect-Retirement'
                WHEN it.indirect_account_type = 'foundation' THEN 'Indirect-Foundation'
                ELSE 'Indirect-Other'
            END as slice_name
        FROM purchase_signals ps
        JOIN insider_transactions it ON it.issuer_ticker = ps.issuer_ticker 
            AND it.filing_date = ps.signal_date AND it.transaction_type = 'P'
        WHERE {ENRICHED_SIGNAL_FILTER}
            AND it.indirect_account_type IS NOT NULL
        GROUP BY ps.id, it.indirect_account_type
    """)
    
    # === Factor 3: Routine vs Opportunistic ===
    total_results += analyze_factor(conn, "opportunistic", f"""
        SELECT ps.id as signal_id,
            CASE 
                WHEN it.is_opportunistic = true THEN 'Opportunistic'
                WHEN it.is_opportunistic = false THEN 'Routine'
                ELSE 'Unknown'
            END as slice_name
        FROM purchase_signals ps
        JOIN insider_transactions it ON it.issuer_ticker = ps.issuer_ticker 
            AND it.filing_date = ps.signal_date AND it.transaction_type = 'P'
        WHERE {ENRICHED_SIGNAL_FILTER}
            AND it.is_opportunistic IS NOT NULL
        GROUP BY ps.id, it.is_opportunistic
    """)
    
    # === Factor 4: Cluster Size ===
    total_results += analyze_factor(conn, "cluster_size", f"""
        SELECT id as signal_id,
            CASE 
                WHEN cluster_size = 1 THEN '1 (isolated)'
                WHEN cluster_size = 2 THEN '2 insiders'
                WHEN cluster_size >= 3 THEN '3+ insiders'
            END as slice_name
        FROM purchase_signals ps
        WHERE {ENRICHED_SIGNAL_FILTER}
    """)
    
    # === Factor 5: Insider Role ===
    total_results += analyze_factor(conn, "insider_role", f"""
        SELECT ps.id as signal_id,
            CASE 
                WHEN it.reporting_person_title LIKE '%%CEO%%' OR it.reporting_person_title LIKE '%%Chief Executive%%' THEN 'CEO'
                WHEN it.reporting_person_title LIKE '%%CFO%%' OR it.reporting_person_title LIKE '%%Chief Financial%%' THEN 'CFO'
                WHEN it.reporting_person_title LIKE '%%COO%%' OR it.reporting_person_title LIKE '%%Chief Operating%%' THEN 'COO'
                WHEN it.reporting_person_title LIKE '%%Chief%%' OR it.reporting_person_title LIKE '%%President%%' THEN 'Other C-Suite'
                WHEN it.is_director = true AND it.is_officer = false THEN 'Director'
                WHEN it.is_ten_percent_owner = true THEN '10%% Owner'
                ELSE 'Other Officer'
            END as slice_name
        FROM purchase_signals ps
        JOIN insider_transactions it ON it.issuer_ticker = ps.issuer_ticker 
            AND it.filing_date = ps.signal_date AND it.transaction_type = 'P'
        WHERE {ENRICHED_SIGNAL_FILTER}
        GROUP BY ps.id, it.reporting_person_title, it.is_director, it.is_officer, it.is_ten_percent_owner
    """)
    
    # === Factor 6: Ownership Change % ===
    total_results += analyze_factor(conn, "ownership_change_pct", f"""
        SELECT ps.id as signal_id,
            CASE 
                WHEN AVG(it.ownership_change_pct) > 50 THEN '>50%% increase'
                WHEN AVG(it.ownership_change_pct) > 20 THEN '20-50%% increase'
                WHEN AVG(it.ownership_change_pct) > 5 THEN '5-20%% increase'
                WHEN AVG(it.ownership_change_pct) > 1 THEN '1-5%% increase'
                ELSE '<1%% increase'
            END as slice_name
        FROM purchase_signals ps
        JOIN insider_transactions it ON it.issuer_ticker = ps.issuer_ticker 
            AND it.filing_date = ps.signal_date AND it.transaction_type = 'P'
        WHERE {ENRICHED_SIGNAL_FILTER}
            AND it.ownership_change_pct IS NOT NULL AND it.ownership_change_pct >= 0
            AND it.ownership_change_pct < 10000
        GROUP BY ps.id
    """)
    
    # === Factor 7: Transaction Value ===
    total_results += analyze_factor(conn, "transaction_value", f"""
        SELECT id as signal_id,
            CASE 
                WHEN total_purchase_value >= 1000000 THEN '$1M+'
                WHEN total_purchase_value >= 250000 THEN '$250K-$1M'
                WHEN total_purchase_value >= 50000 THEN '$50K-$250K'
                ELSE '<$50K'
            END as slice_name
        FROM purchase_signals ps
        WHERE {ENRICHED_SIGNAL_FILTER}
            AND total_purchase_value > 0
    """)
    
    # === Factor 8: Prior 30d Momentum ===
    total_results += analyze_factor(conn, "prior_momentum_30d", f"""
        SELECT ps.id as signal_id,
            CASE 
                WHEN it.prior_return_30d < -0.15 THEN 'Down >15%%'
                WHEN it.prior_return_30d < -0.05 THEN 'Down 5-15%%'
                WHEN it.prior_return_30d < 0.05 THEN 'Flat (-5%% to +5%%)'
                WHEN it.prior_return_30d < 0.15 THEN 'Up 5-15%%'
                ELSE 'Up >15%%'
            END as slice_name
        FROM purchase_signals ps
        JOIN insider_transactions it ON it.issuer_ticker = ps.issuer_ticker 
            AND it.filing_date = ps.signal_date AND it.transaction_type = 'P'
        WHERE {ENRICHED_SIGNAL_FILTER}
            AND it.prior_return_30d IS NOT NULL
        GROUP BY ps.id, it.prior_return_30d
    """)
    
    # === Factor 9: Distance from 52-Week High ===
    total_results += analyze_factor(conn, "distance_52w_high", f"""
        SELECT ps.id as signal_id,
            CASE 
                WHEN it.distance_from_52w_high < 0.5 THEN '<50%% of high'
                WHEN it.distance_from_52w_high < 0.7 THEN '50-70%% of high'
                WHEN it.distance_from_52w_high < 0.85 THEN '70-85%% of high'
                WHEN it.distance_from_52w_high < 0.95 THEN '85-95%% of high'
                ELSE 'Near high (>95%%)'
            END as slice_name
        FROM purchase_signals ps
        JOIN insider_transactions it ON it.issuer_ticker = ps.issuer_ticker 
            AND it.filing_date = ps.signal_date AND it.transaction_type = 'P'
        WHERE {ENRICHED_SIGNAL_FILTER}
            AND it.distance_from_52w_high IS NOT NULL
            AND it.distance_from_52w_high > 0 AND it.distance_from_52w_high <= 1.1
        GROUP BY ps.id, it.distance_from_52w_high
    """)
    
    # === Factor 10: Price Drift from Insider Transaction ===
    total_results += analyze_factor(conn, "price_drift_from_tx", f"""
        SELECT ps.id as signal_id,
            CASE 
                WHEN it.price_drift_from_tx < -0.05 THEN 'Stock dropped >5%% since insider bought'
                WHEN it.price_drift_from_tx < 0.02 THEN 'Minimal drift (±2%%)'
                WHEN it.price_drift_from_tx < 0.05 THEN 'Up 2-5%% since insider bought'
                WHEN it.price_drift_from_tx < 0.10 THEN 'Up 5-10%% since insider bought'
                ELSE 'Up >10%% since insider bought'
            END as slice_name
        FROM purchase_signals ps
        JOIN insider_transactions it ON it.issuer_ticker = ps.issuer_ticker 
            AND it.filing_date = ps.signal_date AND it.transaction_type = 'P'
        WHERE {ENRICHED_SIGNAL_FILTER}
            AND it.price_drift_from_tx IS NOT NULL
            AND ABS(it.price_drift_from_tx) < 5
        GROUP BY ps.id, it.price_drift_from_tx
    """)
    
    # === Factor 11: Volume Spike ===
    total_results += analyze_factor(conn, "volume_spike", f"""
        SELECT ps.id as signal_id,
            CASE 
                WHEN it.recent_volume_spike > 3.0 THEN 'High spike (>3x)'
                WHEN it.recent_volume_spike > 1.5 THEN 'Moderate spike (1.5-3x)'
                WHEN it.recent_volume_spike > 0.7 THEN 'Normal (0.7-1.5x)'
                ELSE 'Low volume (<0.7x)'
            END as slice_name
        FROM purchase_signals ps
        JOIN insider_transactions it ON it.issuer_ticker = ps.issuer_ticker 
            AND it.filing_date = ps.signal_date AND it.transaction_type = 'P'
        WHERE {ENRICHED_SIGNAL_FILTER}
            AND it.recent_volume_spike IS NOT NULL AND it.recent_volume_spike > 0
        GROUP BY ps.id, it.recent_volume_spike
    """)
    
    return total_results

def derive_model_weights(conn):
    """Derive scoring model weights from factor information ratios."""
    print("\n=== Deriving Model Weights ===")
    
    # For each factor, find the horizon with the best information ratio
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("""
            SELECT factor_name, 
                MAX(ABS(information_ratio)) as best_ir,
                horizon as best_horizon,
                sample_size
            FROM factor_analysis
            WHERE sample_size >= 20 AND ABS(t_stat) >= 1.5
            GROUP BY factor_name, horizon, sample_size
            ORDER BY best_ir DESC
        """)
        factors = cur.fetchall()
    
    if not factors:
        print("  No significant factors found!")
        return
    
    # Total IR for normalization
    total_ir = sum(abs(f["best_ir"]) for f in factors)
    
    # Academic prior weights (from our research)
    priors = {
        "opportunistic": 0.25, "cluster_size": 0.12, "ownership_type": 0.12,
        "filing_lag": 0.10, "ownership_change_pct": 0.08, "insider_role": 0.06,
        "transaction_value": 0.06, "prior_momentum_30d": 0.05, "distance_52w_high": 0.05,
        "price_drift_from_tx": 0.04, "volume_spike": 0.03,
    }
    
    prior_strength = 500  # Equivalent to ~2 years of signals
    
    for f in factors:
        factor_name = f["factor_name"]
        data_weight = abs(f["best_ir"]) / total_ir if total_ir > 0 else 0
        prior_weight = priors.get(factor_name, 0.03)
        n = f["sample_size"]
        
        # Bayesian blend
        effective = (n / (n + prior_strength)) * data_weight + (prior_strength / (n + prior_strength)) * prior_weight
        confidence = min(n / 500, 1.0)
        
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO model_weights
                (factor_name, data_weight, prior_weight, effective_weight,
                 sample_size, information_ratio, optimal_horizon,
                 confidence_level, last_updated)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (factor_name) DO UPDATE SET
                  data_weight = EXCLUDED.data_weight,
                  prior_weight = EXCLUDED.prior_weight,
                  effective_weight = EXCLUDED.effective_weight,
                  sample_size = EXCLUDED.sample_size,
                  information_ratio = EXCLUDED.information_ratio,
                  optimal_horizon = EXCLUDED.optimal_horizon,
                  confidence_level = EXCLUDED.confidence_level,
                  last_updated = EXCLUDED.last_updated
            """, (
                factor_name, data_weight, prior_weight, effective,
                n, f["best_ir"], f["best_horizon"],
                confidence, datetime.now().isoformat(),
            ))
        
        print(f"  {factor_name:30s} | IR: {f['best_ir']:7.4f} | Data: {data_weight:.3f} | Prior: {prior_weight:.3f} | Effective: {effective:.3f} | N: {n}")
    
    conn.commit()

def main():
    print("=== Factor Research Engine ===\n")
    
    conn = get_db()
    
    # Clear previous results
    with conn.cursor() as cur:
        cur.execute("DELETE FROM factor_analysis")
        cur.execute("DELETE FROM model_weights")
    conn.commit()
    
    # Run all factor analyses
    total = run_all_analyses(conn)
    print(f"\n=== Factor Analysis Complete: {total} result rows ===")
    
    # Show key findings
    print("\n=== Key Findings (63-day horizon) ===")
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("""
            SELECT factor_name, slice_name, sample_size,
                ROUND(mean_excess_return * 100, 2) as mean_pct,
                ROUND(t_stat::numeric, 2) as t_stat,
                ROUND(win_rate * 100, 1) as win_rate_pct,
                ROUND(information_ratio::numeric, 4) as ir
            FROM factor_analysis
            WHERE horizon = 63 AND sample_size >= 10
            ORDER BY factor_name, mean_excess_return DESC
        """)
        results = cur.fetchall()
    
    current_factor = ""
    for r in results:
        if r["factor_name"] != current_factor:
            current_factor = r["factor_name"]
            print(f"\n  {current_factor}:")
        sig = "***" if abs(float(r["t_stat"])) >= 2.0 else "**" if abs(float(r["t_stat"])) >= 1.5 else "*" if abs(float(r["t_stat"])) >= 1.0 else ""
        print(f"    {r['slice_name']:35s} N={r['sample_size']:4d} | Excess: {float(r['mean_pct']):+7.2f}% | t={float(r['t_stat']):+6.2f}{sig} | Win: {float(r['win_rate_pct']):.1f}% | IR: {float(r['ir']):.4f}")
    
    # Derive model weights
    derive_model_weights(conn)
    
    conn.close()

if __name__ == "__main__":
    main()
