/**
 * V3 Strategy Engine
 * 
 * Provides API-ready functions for:
 * - Factor research results
 * - Data-derived signal scoring
 * - Strategy recommendations
 * - Execution deviation analysis
 * - Alpha decay curves
 */

import { db, pool } from "./db";
import { 
  factorAnalysis, modelWeights, purchaseSignals, signalEntryPrices,
  dailyPrices, insiderTransactions, insiderHistory,
  tradeExecutions, executionDeviations, closedTrades,
  portfolioPositions, strategySnapshots, strategyRecommendations
} from "@shared/schema";
import { eq, desc, sql, and, gte, lte, isNull, asc } from "drizzle-orm";

// ============================================================
// QUERY CACHE — prevent expensive re-computation on every page load
// ============================================================
const queryCache = new Map<string, { data: any; timestamp: number }>();
const CACHE_TTL = 30 * 60 * 1000; // 30 minutes

async function cachedQuery<T>(key: string, fn: () => Promise<T>): Promise<T> {
  const cached = queryCache.get(key);
  if (cached && (Date.now() - cached.timestamp) < CACHE_TTL) {
    return cached.data as T;
  }
  const result = await fn();
  queryCache.set(key, { data: result, timestamp: Date.now() });
  return result;
}

// ============================================================
// FACTOR RESEARCH API
// ============================================================

/** Get all factor analysis results, optionally filtered by horizon */
export async function getFactorResults(horizon?: number) {
  let query = db.select().from(factorAnalysis);
  if (horizon) {
    return await query.where(eq(factorAnalysis.horizon, horizon))
      .orderBy(factorAnalysis.factorName, desc(factorAnalysis.meanExcessReturn));
  }
  return await query.orderBy(factorAnalysis.factorName, factorAnalysis.horizon);
}

/** Get factor effectiveness summary — best IR per factor */
export async function getFactorEffectiveness() {
  const result = await pool.query(`
    SELECT factor_name,
      MAX(ABS(information_ratio)) as best_ir,
      (SELECT horizon FROM factor_analysis fa2 
       WHERE fa2.factor_name = fa.factor_name 
       ORDER BY ABS(information_ratio) DESC LIMIT 1) as best_horizon,
      (SELECT slice_name FROM factor_analysis fa3
       WHERE fa3.factor_name = fa.factor_name AND fa3.horizon = 63
       ORDER BY mean_excess_return DESC LIMIT 1) as best_slice_63d,
      (SELECT ROUND(mean_excess_return * 100, 2) FROM factor_analysis fa4
       WHERE fa4.factor_name = fa.factor_name AND fa4.horizon = 63
       ORDER BY mean_excess_return DESC LIMIT 1) as best_return_63d_pct,
      (SELECT t_stat FROM factor_analysis fa5
       WHERE fa5.factor_name = fa.factor_name AND fa5.horizon = 63
       ORDER BY mean_excess_return DESC LIMIT 1) as best_t_stat_63d,
      SUM(sample_size) / COUNT(DISTINCT horizon) as avg_sample_size
    FROM factor_analysis fa
    WHERE sample_size >= 10
    GROUP BY factor_name
    ORDER BY best_ir DESC
  `);
  return result.rows;
}

/** Get heatmap data: factor × horizon matrix */
export async function getFactorHeatmap(factorName: string) {
  return await db.select().from(factorAnalysis)
    .where(eq(factorAnalysis.factorName, factorName))
    .orderBy(factorAnalysis.sliceName, factorAnalysis.horizon);
}

export const ALPHA_DECAY_CACHE_PATH = "/opt/insider-signal-dash/alpha-decay-cache.json";
const fs = require("fs");

/** Get alpha decay curve — reads from pre-computed JSON file.
 *  The actual aggregation (23M+ rows) is done via admin endpoint, not on demand. */
export function getAlphaDecayCurve(options: {
  factorName?: string;
  sliceName?: string;
  scoreTier?: number;
} = {}) {
  try {
    if (fs.existsSync(ALPHA_DECAY_CACHE_PATH)) {
      return JSON.parse(fs.readFileSync(ALPHA_DECAY_CACHE_PATH, "utf-8"));
    }
  } catch (e) {
    console.error("[ALPHA DECAY] Failed to read cache file:", (e as any).message);
  }
  return [];
}

/** Pre-compute alpha decay data from daily_prices table.
 *  Computes forward returns on-demand by joining signal entries to daily prices.
 *  Results cached to JSON file — run via POST /api/admin/precompute-alpha-decay. */
export async function precomputeAlphaDecay(): Promise<void> {
  console.log("[ALPHA DECAY] Pre-computing alpha decay curve from daily_prices...");
  try {
    // Step 1: Get all enriched signals with entry prices and tickers
    const signals = await pool.query(`
      SELECT ps.id as signal_id, ps.issuer_ticker as ticker, 
             sep.next_open as entry_price, ps.signal_date
      FROM purchase_signals ps
      JOIN signal_entry_prices sep ON sep.signal_id = ps.id
      WHERE sep.next_open > 0 AND ps.issuer_ticker IS NOT NULL
    `);
    console.log(`[ALPHA DECAY] Processing ${signals.rows.length} signals...`);
    if (signals.rows.length === 0) {
      fs.writeFileSync(ALPHA_DECAY_CACHE_PATH, JSON.stringify([]));
      return;
    }

    // Step 2: For each trading_day horizon, compute avg return and excess return
    // Use a single efficient query that computes returns for key horizons
    const horizons = Array.from({ length: 253 }, (_, i) => i); // 0-252
    const decayData: any[] = [];

    // Batch process: for each horizon, compute returns across all signals
    for (const horizon of horizons) {
      const result = await pool.query(`
        WITH signal_entries AS (
          SELECT ps.id as signal_id, ps.issuer_ticker as ticker,
                 sep.next_open as entry_price, ps.signal_date
          FROM purchase_signals ps
          JOIN signal_entry_prices sep ON sep.signal_id = ps.id
          WHERE sep.next_open > 0 AND ps.issuer_ticker IS NOT NULL
        ),
        with_entry_date AS (
          SELECT se.*, dp.date as entry_date, dp.close as entry_close
          FROM signal_entries se
          JOIN LATERAL (
            SELECT date, close FROM daily_prices 
            WHERE ticker = se.ticker AND date > se.signal_date 
            ORDER BY date LIMIT 1
          ) dp ON true
        ),
        with_horizon AS (
          SELECT wed.signal_id, wed.entry_price, wed.entry_date,
                 dp_h.close as horizon_close, dp_h.date as horizon_date
          FROM with_entry_date wed
          JOIN LATERAL (
            SELECT close, date FROM daily_prices 
            WHERE ticker = wed.ticker AND date >= wed.entry_date 
            ORDER BY date OFFSET $1 LIMIT 1
          ) dp_h ON true
        ),
        with_spy AS (
          SELECT wh.signal_id, wh.entry_price, wh.horizon_close,
                 spy_0.close as spy_entry, spy_h.close as spy_horizon
          FROM with_horizon wh
          JOIN daily_prices spy_0 ON spy_0.ticker = 'SPY' AND spy_0.date = wh.entry_date
          JOIN daily_prices spy_h ON spy_h.ticker = 'SPY' AND spy_h.date = wh.horizon_date
          WHERE spy_0.close > 0
        )
        SELECT 
          COUNT(*) as sample_size,
          ROUND(AVG(((horizon_close / entry_price) - 1) - ((spy_horizon / spy_entry) - 1)) * 100, 3) as avg_excess_pct,
          ROUND(AVG((horizon_close / entry_price) - 1) * 100, 3) as avg_return_pct
        FROM with_spy
        WHERE horizon_close IS NOT NULL AND entry_price > 0
      `, [horizon]);

      const row = result.rows[0];
      if (row && Number(row.sample_size) > 0) {
        decayData.push({
          trading_day: horizon,
          sample_size: Number(row.sample_size),
          avg_excess_pct: Number(row.avg_excess_pct),
          avg_return_pct: Number(row.avg_return_pct),
        });
      }

      // Log progress every 50 horizons
      if (horizon > 0 && horizon % 50 === 0) {
        console.log(`[ALPHA DECAY] Processed horizon ${horizon}/252...`);
      }
    }

    fs.writeFileSync(ALPHA_DECAY_CACHE_PATH, JSON.stringify(decayData));
    console.log(`[ALPHA DECAY] Done: ${decayData.length} data points cached`);
  } catch (err: any) {
    console.error("[ALPHA DECAY] Failed:", err.message);
    throw err;
  }
}

/** Get model weights */
export async function getModelWeights() {
  return await db.select().from(modelWeights).orderBy(desc(modelWeights.effectiveWeight));
}

// ============================================================
// SIGNAL SCORING & RECOMMENDATIONS
// ============================================================

/** Get scored signals with factor breakdowns — deduplicated by (ticker, date), keeping highest ID */
export async function getScoredSignals(limit = 50, minScore?: number) {
  // Fetch signals with entry prices using Drizzle ORM
  const results = await db.select({
    signal: purchaseSignals,
    entryPrice: signalEntryPrices,
  })
    .from(purchaseSignals)
    .leftJoin(signalEntryPrices, eq(purchaseSignals.id, signalEntryPrices.signalId))
    .orderBy(desc(purchaseSignals.signalScore), desc(purchaseSignals.signalDate))
    .limit(limit * 3); // fetch extra to account for dedup

  // Deduplicate in JS: keep only the highest ID per (ticker, date)
  const seen = new Map<string, number>();
  const deduped: typeof results = [];

  for (const r of results) {
    const key = `${r.signal.issuerTicker}|${r.signal.signalDate}`;
    const existingId = seen.get(key);
    if (existingId === undefined || r.signal.id > existingId) {
      // Remove previous entry for this key if exists
      if (existingId !== undefined) {
        const idx = deduped.findIndex(d => d.signal.id === existingId);
        if (idx !== -1) deduped.splice(idx, 1);
      }
      seen.set(key, r.signal.id);
      deduped.push(r);
    }
  }

  return deduped.slice(0, limit).map(r => ({
    ...r.signal,
    entryPrices: r.entryPrice,
  }));
}

/** Get strategy recommendations */
export async function getStrategyRecommendations(status = "active") {
  return await db.select({
    rec: strategyRecommendations,
    signal: purchaseSignals,
  })
    .from(strategyRecommendations)
    .leftJoin(purchaseSignals, eq(strategyRecommendations.signalId, purchaseSignals.id))
    .where(eq(strategyRecommendations.currentStatus, status))
    .orderBy(desc(strategyRecommendations.compositeScore));
}

// ============================================================
// PERFORMANCE — THREE-WAY COMPARISON
// ============================================================

/** Get performance snapshots for strategy vs user vs benchmark */
export async function getPerformanceSnapshots(days = 90) {
  const rows = await db.select().from(strategySnapshots)
    .orderBy(desc(strategySnapshots.date))
    .limit(days);
  return rows.reverse(); // Oldest first for charts
}

/** Get performance summary KPIs */
export async function getPerformanceSummary() {
  const snapshots = await db.select().from(strategySnapshots)
    .orderBy(desc(strategySnapshots.date))
    .limit(252); // 1 year
  
  if (snapshots.length === 0) return null;
  
  const latest = snapshots[0];
  
  return {
    // Your performance
    yourReturn: latest.cumulativeReturn,
    yourSharpe: latest.sharpeRatio,
    yourSortino: latest.sortinoRatio,
    yourMaxDrawdown: latest.maxDrawdown,
    yourCurrentDrawdown: latest.currentDrawdown,
    
    // Strategy performance
    strategyReturn: latest.strategyCumulativeReturn,
    strategySharpe: latest.strategySharpe,
    strategySortino: latest.strategySortino,
    
    // Benchmark
    benchmarkReturn: latest.benchmarkCumulative,
    
    // Deviations
    alphaVsBenchmark: latest.alphaVsBenchmark,
    alphaVsStrategy: latest.alphaVsStrategy,
    deviationCost: latest.deviationCost,
  };
}

// ============================================================
// EXECUTION ANALYSIS
// ============================================================

/** Get execution deviation summary KPIs */
export async function getExecutionSummary() {
  const deviations = await db.select().from(executionDeviations);
  
  const signalAligned = deviations.filter(d => d.classification === "signal_aligned");
  const independent = deviations.filter(d => d.classification === "independent");
  
  // Signal coverage: how many of user's trades were signal-aligned?
  const totalTradeCount = deviations.length;
  const tradedSignals = signalAligned.length;

  return {
    signalCoverage: totalTradeCount > 0 ? tradedSignals / totalTradeCount : 0,
    signalCoverageCount: `${tradedSignals}/${totalTradeCount}`,
    avgEntryDelay: signalAligned.length > 0 
      ? signalAligned.reduce((s, d) => s + (d.entryDelayDays || 0), 0) / signalAligned.length 
      : 0,
    exitDiscipline: signalAligned.length > 0
      ? signalAligned.filter(d => d.exitType === "time" || d.exitType === "stop").length / signalAligned.length
      : 0,
    totalDeviationCost: deviations.reduce((s, d) => s + (d.alphaCost || 0), 0),
    independentAlpha: independent.reduce((s, d) => s + (d.pnlDifference || 0), 0),
    totalTrades: deviations.length,
    signalAlignedCount: signalAligned.length,
    independentCount: independent.length,
  };
}

/** Get trade-level deviation details */
export async function getTradeDeviations(limit = 100) {
  return await db.select({
    deviation: executionDeviations,
    trade: tradeExecutions,
    signal: purchaseSignals,
  })
    .from(executionDeviations)
    .leftJoin(tradeExecutions, eq(executionDeviations.userTradeId, tradeExecutions.id))
    .leftJoin(purchaseSignals, eq(executionDeviations.signalId, purchaseSignals.id))
    .orderBy(desc(executionDeviations.createdAt))
    .limit(limit);
}

/** Get missed signals (tier 1-2 signals the user didn't trade) */
export async function getMissedSignals(days = 90) {
  const result = await pool.query(`
    SELECT ps.*, sep.next_open as entry_price,
      -- 63-day excess return computed from daily_prices
      (SELECT ROUND(((dp_h.close / sep2.next_open) - 1 - ((spy_h.close / spy_0.close) - 1)) * 100, 2)
       FROM signal_entry_prices sep2
       JOIN LATERAL (SELECT date, close FROM daily_prices WHERE ticker = ps.issuer_ticker AND date > ps.signal_date ORDER BY date LIMIT 1) dp_0 ON true
       JOIN LATERAL (SELECT date, close FROM daily_prices WHERE ticker = ps.issuer_ticker AND date >= dp_0.date ORDER BY date OFFSET 63 LIMIT 1) dp_h ON true
       JOIN daily_prices spy_0 ON spy_0.ticker = 'SPY' AND spy_0.date = dp_0.date
       JOIN daily_prices spy_h ON spy_h.ticker = 'SPY' AND spy_h.date = dp_h.date
       WHERE sep2.signal_id = ps.id AND sep2.next_open > 0
       LIMIT 1
      ) as excess_63d_pct,
      -- 63-day raw return computed from daily_prices
      (SELECT ROUND(((dp_h.close / sep2.next_open) - 1) * 100, 2)
       FROM signal_entry_prices sep2
       JOIN LATERAL (SELECT date, close FROM daily_prices WHERE ticker = ps.issuer_ticker AND date > ps.signal_date ORDER BY date LIMIT 1) dp_0 ON true
       JOIN LATERAL (SELECT close FROM daily_prices WHERE ticker = ps.issuer_ticker AND date >= dp_0.date ORDER BY date OFFSET 63 LIMIT 1) dp_h ON true
       WHERE sep2.signal_id = ps.id AND sep2.next_open > 0
       LIMIT 1
      ) as return_63d_pct
    FROM purchase_signals ps
    LEFT JOIN signal_entry_prices sep ON sep.signal_id = ps.id
    WHERE ps.score_tier <= 2
      AND ps.signal_date >= CURRENT_DATE - INTERVAL '1 day' * $1
      AND ps.id NOT IN (
        SELECT DISTINCT signal_id FROM execution_deviations WHERE signal_id IS NOT NULL
      )
    ORDER BY ps.signal_score DESC
    LIMIT 50
  `, [days]);
  return result.rows;
}

// ============================================================
// PORTFOLIO — ENHANCED WITH SIGNAL TAGS
// ============================================================

/** Get portfolio positions with signal health indicators — fetches live Schwab data */
export async function getPortfolioWithSignalHealth(getSchwabAccessToken?: () => Promise<string | null>) {
  let positions: any[] = [];

  // Try to fetch live positions from Schwab API
  if (getSchwabAccessToken) {
    try {
      const token = await getSchwabAccessToken();
      if (token) {
        const resp = await fetch("https://api.schwabapi.com/trader/v1/accounts?fields=positions", {
          headers: { "Authorization": `Bearer ${token}` },
        });
        if (resp.ok) {
          const accounts = await resp.json() as any[];
          for (const acct of accounts || []) {
            const acctPositions = acct.securitiesAccount?.positions || [];
            for (const pos of acctPositions) {
              const ticker = pos.instrument?.symbol;
              if (!ticker || pos.instrument?.assetType !== "EQUITY") continue;
              const qty = pos.longQuantity - (pos.shortQuantity || 0);
              const currentPrice = qty > 0 ? pos.marketValue / qty : 0;
              positions.push({
                ticker,
                companyName: pos.instrument?.description || ticker,
                quantity: qty,
                avgCostBasis: pos.averagePrice || 0,
                currentPrice,
                marketValue: pos.marketValue || 0,
                unrealizedPnl: qty * (currentPrice - (pos.averagePrice || 0)),
                unrealizedPnlPct: pos.averagePrice ? ((currentPrice - pos.averagePrice) / pos.averagePrice * 100) : 0,
                dayChange: pos.currentDayProfitLoss || 0,
                dayChangePct: pos.currentDayProfitLossPercentage || 0,
                source: "schwab",
              });
            }
          }
        }
      }
    } catch (err: any) {
      console.error("[PORTFOLIO] Schwab API error, falling back to DB:", err.message);
    }
  }

  // Fall back to DB positions if Schwab fetch returned nothing
  if (positions.length === 0) {
    positions = await db.select().from(portfolioPositions);
  }

  // For each position, check if ticker matches a recent purchase signal (last 90 days)
  const recentSignalsResult = await pool.query(`
    SELECT id, issuer_ticker, signal_date, signal_score, score_tier
    FROM purchase_signals
    WHERE signal_date >= CURRENT_DATE - INTERVAL '90 days'
    ORDER BY signal_date DESC
  `);
  const recentSignals = recentSignalsResult.rows;

  const signalByTicker = new Map<string, any>();
  for (const sig of recentSignals) {
    const ticker = sig.issuer_ticker;
    if (ticker && !signalByTicker.has(ticker)) {
      signalByTicker.set(ticker, sig);
    }
  }

  return positions.map(pos => {
    const matchingSignal = signalByTicker.get(pos.ticker);
    let signalClassification = "independent";
    let signalHealth = "independent";
    let daysRemaining = null;
    let shouldExit = false;
    let signalId = pos.signalId || null;
    let signalScoreAtEntry = pos.signalScoreAtEntry || null;

    if (matchingSignal) {
      signalClassification = "signal_aligned";
      signalId = matchingSignal.id;
      signalScoreAtEntry = matchingSignal.signal_score;
      // Calculate days since signal
      const signalDate = new Date(matchingSignal.signal_date);
      const now = new Date();
      const daysSinceSignal = Math.floor((now.getTime() - signalDate.getTime()) / (1000 * 60 * 60 * 24));
      const optimalHold = 63; // ~3 months based on typical alpha decay
      daysRemaining = optimalHold - daysSinceSignal;
      if (daysRemaining <= 0) {
        signalHealth = "past_optimal_hold";
        shouldExit = true;
      } else if (daysRemaining <= optimalHold * 0.25) {
        signalHealth = "approaching_exit";
      } else {
        signalHealth = "on_track";
      }
    } else if (pos.signalId && pos.recommendedHoldDays && pos.holdingDays) {
      // Fallback to DB-stored signal info
      signalClassification = "signal_aligned";
      daysRemaining = pos.recommendedHoldDays - pos.holdingDays;
      if (daysRemaining <= 0) {
        signalHealth = "past_optimal_hold";
        shouldExit = true;
      } else if (daysRemaining <= pos.recommendedHoldDays * 0.25) {
        signalHealth = "approaching_exit";
      } else {
        signalHealth = "on_track";
      }
    }

    // P&L-based health status (overrides signal-based for underperformers)
    const pnlPct = pos.unrealizedPnlPct || 0;
    let healthStatus = signalHealth;
    if (pnlPct <= -30) {
      healthStatus = "at_risk";
    } else if (pnlPct <= -15) {
      healthStatus = "underperforming";
    } else if (pnlPct < 0) {
      healthStatus = "monitor";
    } else if (signalHealth === "independent" || signalHealth === "on_track") {
      healthStatus = "on_track";
    }

    return {
      ...pos,
      signalClassification,
      signalId,
      signalScoreAtEntry,
      signalHealth: healthStatus,
      daysRemaining,
      shouldExit,
    };
  });
}

// ============================================================
// DATA PIPELINE STATUS
// ============================================================

// Cache pipeline status for 60s — avoids blocking the event loop with heavy COUNT(*) queries
let _pipelineStatusCache: any = null;
let _pipelineStatusCacheTime = 0;
const PIPELINE_STATUS_CACHE_TTL = 60_000; // 60 seconds

export function invalidatePipelineStatusCache() {
  _pipelineStatusCache = null;
  _pipelineStatusCacheTime = 0;
}

export async function getDataPipelineStatus() {
  const now = Date.now();
  if (_pipelineStatusCache && (now - _pipelineStatusCacheTime) < PIPELINE_STATUS_CACHE_TTL) {
    return _pipelineStatusCache;
  }

  // Run all count queries in parallel for speed
  const [txCount, signalCount, enrichedCount, fwdReturnApprox, factorCount, weightCount, insiderCount, failedTickers] = await Promise.all([
    pool.query(`SELECT count(*) as count FROM insider_transactions WHERE transaction_type = 'P'`),
    pool.query(`SELECT count(*) as count FROM purchase_signals`),
    pool.query(`SELECT count(DISTINCT signal_id) as count FROM signal_entry_prices`),
    // Daily prices count (source of truth for forward return computation)
    pool.query(`SELECT GREATEST(reltuples::bigint, 0) as count FROM pg_class WHERE relname = 'daily_prices'`).catch(() => ({ rows: [{ count: 0 }] })),
    pool.query(`SELECT count(*) as count FROM factor_analysis`),
    pool.query(`SELECT count(*) as count FROM model_weights`),
    pool.query(`SELECT count(*) as count FROM insider_history`),
    pool.query(`SELECT COUNT(*) as count FROM enrichment_failed_tickers`).catch(() => ({ rows: [{ count: 0 }] })),
  ]);
  
  const totalAll = Number(signalCount.rows[0]?.count || 0);
  const enriched = Number(enrichedCount.rows[0]?.count || 0);
  
  _pipelineStatusCache = {
    totalPurchases: Number(txCount.rows[0]?.count || 0),
    totalSignals: totalAll,
    enrichedSignals: enriched,
    failedTickers: Number(failedTickers.rows[0]?.count || 0),
    dailyPriceDataPoints: Number(fwdReturnApprox.rows[0]?.count || 0),
    factorAnalysisResults: Number(factorCount.rows[0]?.count || 0),
    modelFactors: Number(weightCount.rows[0]?.count || 0),
    insiderProfiles: Number(insiderCount.rows[0]?.count || 0),
    enrichmentProgress: totalAll > 0 ? Math.round((enriched / totalAll) * 100) : 0,
  };
  _pipelineStatusCacheTime = now;
  return _pipelineStatusCache;
}
