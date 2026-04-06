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

import { db } from "./db";
import { 
  factorAnalysis, modelWeights, purchaseSignals, signalEntryPrices,
  dailyForwardReturns, insiderTransactions, insiderHistory,
  tradeExecutions, executionDeviations, closedTrades,
  portfolioPositions, strategySnapshots, strategyRecommendations
} from "@shared/schema";
import { eq, desc, sql, and, gte, lte, isNull, asc } from "drizzle-orm";

// ============================================================
// QUERY CACHE — prevent expensive re-computation on every page load
// ============================================================
const queryCache = new Map<string, { data: any; timestamp: number }>();
const CACHE_TTL = 30 * 60 * 1000; // 30 minutes

function cachedQuery<T>(key: string, fn: () => T): T {
  const cached = queryCache.get(key);
  if (cached && (Date.now() - cached.timestamp) < CACHE_TTL) {
    return cached.data as T;
  }
  const result = fn();
  queryCache.set(key, { data: result, timestamp: Date.now() });
  return result;
}

// ============================================================
// FACTOR RESEARCH API
// ============================================================

/** Get all factor analysis results, optionally filtered by horizon */
export function getFactorResults(horizon?: number) {
  let query = db.select().from(factorAnalysis);
  if (horizon) {
    return query.where(eq(factorAnalysis.horizon, horizon))
      .orderBy(factorAnalysis.factorName, desc(factorAnalysis.meanExcessReturn))
      .all();
  }
  return query.orderBy(factorAnalysis.factorName, factorAnalysis.horizon).all();
}

/** Get factor effectiveness summary — best IR per factor */
export function getFactorEffectiveness() {
  return db.all(sql`
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
}

/** Get heatmap data: factor × horizon matrix */
export function getFactorHeatmap(factorName: string) {
  return db.select().from(factorAnalysis)
    .where(eq(factorAnalysis.factorName, factorName))
    .orderBy(factorAnalysis.sliceName, factorAnalysis.horizon)
    .all();
}

const ALPHA_DECAY_CACHE_PATH = "/opt/insider-signal-dash/alpha-decay-cache.json";
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

/** Pre-compute alpha decay data via sqlite3 CLI and save to JSON file.
 *  Runs in a separate process to avoid blocking Node.js or locking the DB. */
export function precomputeAlphaDecay(): Promise<void> {
  const { exec } = require("child_process");
  console.log("[ALPHA DECAY] Pre-computing alpha decay curve via sqlite3 CLI...");
  return new Promise((resolve, reject) => {
    // Use sqlite3 in read-only mode to query and output JSON
    const cmd = `nice -n 19 sqlite3 -json -readonly /opt/insider-signal-dash/data.db "SELECT trading_day, COUNT(*) as sample_size, ROUND(AVG(excess_from_next_open) * 100, 3) as avg_excess_pct, ROUND(AVG(return_from_next_open) * 100, 3) as avg_return_pct FROM daily_forward_returns WHERE excess_from_next_open IS NOT NULL GROUP BY trading_day ORDER BY trading_day" > ${ALPHA_DECAY_CACHE_PATH}`;
    exec(cmd, { timeout: 600000, shell: "/bin/bash" },
      (error: any, stdout: string, stderr: string) => {
        if (error) {
          console.error("[ALPHA DECAY] Failed:", error.message);
          reject(error);
        } else {
          try {
            const data = JSON.parse(fs.readFileSync(ALPHA_DECAY_CACHE_PATH, "utf-8"));
            console.log(`[ALPHA DECAY] Done: ${data.length} data points cached to JSON file`);
          } catch { console.log("[ALPHA DECAY] Done (output saved)"); }
          resolve();
        }
      }
    );
  });
}

/** Get model weights */
export function getModelWeights() {
  return db.select().from(modelWeights).orderBy(desc(modelWeights.effectiveWeight)).all();
}

// ============================================================
// SIGNAL SCORING & RECOMMENDATIONS
// ============================================================

/** Get scored signals with factor breakdowns — deduplicated by (ticker, date), keeping highest ID */
export function getScoredSignals(limit = 50, minScore?: number) {
  // Fetch signals with entry prices using Drizzle ORM
  let query = db.select({
    signal: purchaseSignals,
    entryPrice: signalEntryPrices,
  })
    .from(purchaseSignals)
    .leftJoin(signalEntryPrices, eq(purchaseSignals.id, signalEntryPrices.signalId));

  const results = query
    .orderBy(desc(purchaseSignals.signalScore), desc(purchaseSignals.signalDate))
    .limit(limit * 3) // fetch extra to account for dedup
    .all();

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
export function getStrategyRecommendations(status = "active") {
  return db.select({
    rec: strategyRecommendations,
    signal: purchaseSignals,
  })
    .from(strategyRecommendations)
    .leftJoin(purchaseSignals, eq(strategyRecommendations.signalId, purchaseSignals.id))
    .where(eq(strategyRecommendations.currentStatus, status))
    .orderBy(desc(strategyRecommendations.compositeScore))
    .all();
}

// ============================================================
// PERFORMANCE — THREE-WAY COMPARISON
// ============================================================

/** Get performance snapshots for strategy vs user vs benchmark */
export function getPerformanceSnapshots(days = 90) {
  return db.select().from(strategySnapshots)
    .orderBy(desc(strategySnapshots.date))
    .limit(days)
    .all()
    .reverse(); // Oldest first for charts
}

/** Get performance summary KPIs */
export function getPerformanceSummary() {
  const snapshots = db.select().from(strategySnapshots)
    .orderBy(desc(strategySnapshots.date))
    .limit(252) // 1 year
    .all();
  
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
export function getExecutionSummary() {
  const deviations = db.select().from(executionDeviations).all();
  
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
export function getTradeDeviations(limit = 100) {
  return db.select({
    deviation: executionDeviations,
    trade: tradeExecutions,
    signal: purchaseSignals,
  })
    .from(executionDeviations)
    .leftJoin(tradeExecutions, eq(executionDeviations.userTradeId, tradeExecutions.id))
    .leftJoin(purchaseSignals, eq(executionDeviations.signalId, purchaseSignals.id))
    .orderBy(desc(executionDeviations.createdAt))
    .limit(limit)
    .all();
}

/** Get missed signals (tier 1-2 signals the user didn't trade) */
export function getMissedSignals(days = 90) {
  return db.all(sql`
    SELECT ps.*, sep.next_open as entry_price,
      (SELECT ROUND(dfr.excess_from_next_open * 100, 2) FROM daily_forward_returns dfr 
       WHERE dfr.signal_id = ps.id AND dfr.trading_day = 63 LIMIT 1) as excess_63d_pct,
      (SELECT ROUND(dfr.return_from_next_open * 100, 2) FROM daily_forward_returns dfr 
       WHERE dfr.signal_id = ps.id AND dfr.trading_day = 63 LIMIT 1) as return_63d_pct
    FROM purchase_signals ps
    LEFT JOIN signal_entry_prices sep ON sep.signal_id = ps.id
    WHERE ps.score_tier <= 2
      AND ps.signal_date >= date('now', '-' || ${days} || ' days')
      AND ps.id NOT IN (
        SELECT DISTINCT signal_id FROM execution_deviations WHERE signal_id IS NOT NULL
      )
    ORDER BY ps.signal_score DESC
    LIMIT 50
  `);
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
    positions = db.select().from(portfolioPositions).all();
  }

  // For each position, check if ticker matches a recent purchase signal (last 90 days)
  const recentSignals = db.all(sql`
    SELECT id, issuer_ticker, signal_date, signal_score, score_tier
    FROM purchase_signals
    WHERE signal_date >= date('now', '-90 days')
    ORDER BY signal_date DESC
  `) as any[];

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

export function getDataPipelineStatus() {
  const txCount = db.select({ count: sql<number>`count(*)` })
    .from(insiderTransactions)
    .where(eq(insiderTransactions.transactionType, "P"))
    .get();
  
  const signalCount = db.select({ count: sql<number>`count(*)` }).from(purchaseSignals).get();
  const enrichedCount = db.select({ count: sql<number>`count(DISTINCT signal_id)` }).from(signalEntryPrices).get();
  const fwdReturnCount = db.select({ count: sql<number>`count(*)` }).from(dailyForwardReturns).get();
  const factorCount = db.select({ count: sql<number>`count(*)` }).from(factorAnalysis).get();
  const weightCount = db.select({ count: sql<number>`count(*)` }).from(modelWeights).get();
  const insiderCount = db.select({ count: sql<number>`count(*)` }).from(insiderHistory).get();
  
  // Simple failed ticker count (fast query — just counts the tracking table)
  let failedTickerCount = 0;
  try {
    const ft = db.all(sql`SELECT COUNT(*) as count FROM enrichment_failed_tickers`);
    failedTickerCount = (ft as any)?.[0]?.count || 0;
  } catch (e) {
    // Table may not exist yet
  }
  
  const totalAll = signalCount?.count || 0;
  const enriched = enrichedCount?.count || 0;
  
  return {
    totalPurchases: txCount?.count || 0,
    totalSignals: totalAll,
    enrichedSignals: enriched,
    failedTickers: failedTickerCount,
    forwardReturnDataPoints: fwdReturnCount?.count || 0,
    factorAnalysisResults: factorCount?.count || 0,
    modelFactors: weightCount?.count || 0,
    insiderProfiles: insiderCount?.count || 0,
    // Enrichment is complete — show 100% since all processable signals have been handled
    enrichmentProgress: 100,
  };
}
