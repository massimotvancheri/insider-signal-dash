/**
 * Trade Engine — FIFO matching, performance analytics, signal-trade matching
 *
 * Provides:
 * - FIFO closed-trade P&L computation from trade_executions
 * - Performance summary (realized + unrealized P&L, win rate, profit factor, etc.)
 * - Equity curve (cumulative P&L series)
 * - Signal-trade matching (update execution_deviations)
 * - Missed signals (high-score signals user didn't trade)
 */

import { db, pool } from "./db";
import {
  tradeExecutions, closedTrades, executionDeviations,
  portfolioPositions, purchaseSignals, signalEntryPrices
} from "@shared/schema";
import { eq, desc, sql, and, asc } from "drizzle-orm";

// ============================================================
// FIFO CLOSED TRADE MATCHING
// ============================================================

interface TradeRow {
  id: number;
  ticker: string;
  companyName: string | null;
  side: string;
  quantity: number;
  avgPrice: number;
  totalCost: number;
  executionDate: string;
  signalId: number | null;
  signalScore: number | null;
}

interface FifoLot {
  tradeId: number;
  ticker: string;
  companyName: string | null;
  entryDate: string;
  entryPrice: number;
  remainingQty: number;
  signalId: number | null;
  signalScore: number | null;
}

export async function runFifoMatching(): Promise<{ matched: number; errors: string[] }> {
  const errors: string[] = [];
  let matched = 0;

  // Clear existing closed trades so we can recompute cleanly
  await pool.query("DELETE FROM closed_trades");

  // Get all executions ordered by date ASC for FIFO
  const allTrades = await db.select().from(tradeExecutions)
    .orderBy(asc(tradeExecutions.executionDate), asc(tradeExecutions.id)) as TradeRow[];

  // Group by ticker
  const byTicker = new Map<string, TradeRow[]>();
  for (const t of allTrades) {
    const key = t.ticker.toUpperCase();
    if (!byTicker.has(key)) byTicker.set(key, []);
    byTicker.get(key)!.push(t);
  }

  for (const [ticker, trades] of byTicker) {
    // FIFO queue of open buy lots
    const openLots: FifoLot[] = [];

    for (const trade of trades) {
      if (trade.side === "BUY") {
        openLots.push({
          tradeId: trade.id,
          ticker: trade.ticker,
          companyName: trade.companyName,
          entryDate: trade.executionDate,
          entryPrice: trade.avgPrice,
          remainingQty: trade.quantity,
          signalId: trade.signalId,
          signalScore: trade.signalScore,
        });
      } else if (trade.side === "SELL") {
        let sellQtyRemaining = trade.quantity;
        const exitDate = trade.executionDate;
        const exitPrice = trade.avgPrice;

        while (sellQtyRemaining > 0 && openLots.length > 0) {
          const lot = openLots[0];
          const matchQty = Math.min(sellQtyRemaining, lot.remainingQty);

          if (matchQty <= 0) break;

          const entryPrice = lot.entryPrice;
          const realizedPnl = (exitPrice - entryPrice) * matchQty;
          const realizedPnlPct = entryPrice > 0 ? ((exitPrice - entryPrice) / entryPrice) * 100 : 0;
          const holdingDays = Math.max(0, Math.floor(
            (new Date(exitDate).getTime() - new Date(lot.entryDate).getTime()) / (1000 * 60 * 60 * 24)
          ));

          // Look up signal classification from execution_deviations for this buy
          const devResult = await pool.query(`
            SELECT classification, signal_id FROM execution_deviations
            WHERE user_trade_id = $1 LIMIT 1
          `, [lot.tradeId]);
          const devRow = devResult.rows;

          try {
            await db.insert(closedTrades).values({
              ticker: lot.ticker,
              companyName: lot.companyName,
              entryDate: lot.entryDate,
              exitDate,
              entryPrice,
              exitPrice,
              quantity: matchQty,
              realizedPnl,
              realizedPnlPct,
              holdingDays,
              signalClassification: devRow[0]?.classification || "independent",
              signalId: devRow[0]?.signal_id || lot.signalId || null,
              signalScoreAtEntry: lot.signalScore || null,
              exitType: "discretionary",
              createdAt: new Date().toISOString(),
            });
            matched++;
          } catch (e: any) {
            errors.push(`${ticker}: ${e.message}`);
          }

          lot.remainingQty -= matchQty;
          sellQtyRemaining -= matchQty;

          if (lot.remainingQty <= 0.001) {
            openLots.shift(); // Remove fully consumed lot
          }
        }

        if (sellQtyRemaining > 0.001) {
          errors.push(`${ticker}: ${sellQtyRemaining.toFixed(2)} shares sold with no matching buy lot`);
        }
      }
    }
  }

  return { matched, errors };
}

// ============================================================
// PERFORMANCE ANALYTICS
// ============================================================

export async function getPerformanceAnalytics() {
  const closed = await db.select().from(closedTrades)
    .orderBy(asc(closedTrades.exitDate));

  if (closed.length === 0) {
    return null;
  }

  // Get unrealized P&L from portfolio positions
  const positions = await db.select().from(portfolioPositions);
  const totalUnrealizedPnl = positions.reduce((s, p) => s + (p.unrealizedPnl || 0), 0);
  const totalMarketValue = positions.reduce((s, p) => s + (p.marketValue || 0), 0);

  // Realized P&L
  const totalRealizedPnl = closed.reduce((s, t) => s + t.realizedPnl, 0);
  const combinedPnl = totalRealizedPnl + totalUnrealizedPnl;

  // Win/Loss analysis
  const winners = closed.filter(t => t.realizedPnl > 0);
  const losers = closed.filter(t => t.realizedPnl < 0);
  const winRate = closed.length > 0 ? (winners.length / closed.length) * 100 : 0;

  const grossProfits = winners.reduce((s, t) => s + t.realizedPnl, 0);
  const grossLosses = Math.abs(losers.reduce((s, t) => s + t.realizedPnl, 0));
  const profitFactor = grossLosses > 0 ? grossProfits / grossLosses : (grossProfits > 0 ? Infinity : 0);

  const avgWinDollar = winners.length > 0 ? grossProfits / winners.length : 0;
  const avgLossDollar = losers.length > 0 ? grossLosses / losers.length : 0;
  const avgWinPct = winners.length > 0 ? winners.reduce((s, t) => s + t.realizedPnlPct, 0) / winners.length : 0;
  const avgLossPct = losers.length > 0 ? losers.reduce((s, t) => s + t.realizedPnlPct, 0) / losers.length : 0;

  // Best/worst trade
  const bestTrade = closed.reduce((best, t) => t.realizedPnl > best.realizedPnl ? t : best, closed[0]);
  const worstTrade = closed.reduce((worst, t) => t.realizedPnl < worst.realizedPnl ? t : worst, closed[0]);

  // Average holding period
  const avgHoldingPeriod = closed.reduce((s, t) => s + (t.holdingDays || 0), 0) / closed.length;

  // Expectancy: (winRate * avgWin) - (lossRate * avgLoss)
  const expectancy = (winRate / 100 * avgWinDollar) - ((1 - winRate / 100) * avgLossDollar);

  return {
    totalRealizedPnl,
    totalUnrealizedPnl,
    combinedPnl,
    totalMarketValue,
    closedTradeCount: closed.length,
    openPositionCount: positions.length,
    winRate,
    winCount: winners.length,
    lossCount: losers.length,
    grossProfits,
    grossLosses,
    profitFactor,
    avgWinDollar,
    avgLossDollar,
    avgWinPct,
    avgLossPct,
    bestTrade: {
      ticker: bestTrade.ticker,
      pnl: bestTrade.realizedPnl,
      pnlPct: bestTrade.realizedPnlPct,
      date: bestTrade.exitDate,
    },
    worstTrade: {
      ticker: worstTrade.ticker,
      pnl: worstTrade.realizedPnl,
      pnlPct: worstTrade.realizedPnlPct,
      date: worstTrade.exitDate,
    },
    avgHoldingPeriod,
    expectancy,
  };
}

// ============================================================
// EQUITY CURVE (Cumulative P&L over time)
// ============================================================

export async function getEquityCurve() {
  const closed = await db.select().from(closedTrades)
    .orderBy(asc(closedTrades.exitDate));

  if (closed.length === 0) return [];

  let cumulativePnl = 0;
  let cumulativeInvested = 0;
  const curve: { date: string; cumulativePnl: number; cumulativePnlPct: number; tradeCount: number }[] = [];

  // Group by exit date for daily aggregation
  const byDate = new Map<string, typeof closed>();
  for (const t of closed) {
    const d = t.exitDate;
    if (!byDate.has(d)) byDate.set(d, []);
    byDate.get(d)!.push(t);
  }

  let tradeCount = 0;
  for (const [date, trades] of [...byDate.entries()].sort()) {
    for (const t of trades) {
      cumulativePnl += t.realizedPnl;
      cumulativeInvested += t.entryPrice * t.quantity;
      tradeCount++;
    }
    curve.push({
      date,
      cumulativePnl,
      cumulativePnlPct: cumulativeInvested > 0 ? (cumulativePnl / cumulativeInvested) * 100 : 0,
      tradeCount,
    });
  }

  return curve;
}

// ============================================================
// SIGNAL-TRADE MATCHING
// ============================================================

export async function runSignalTradeMatching(): Promise<{ matched: number; unmatched: number; errors: string[] }> {
  const errors: string[] = [];
  let matched = 0;
  let unmatched = 0;

  // Get all execution deviations with their trade info
  const deviationsResult = await pool.query(`
    SELECT ed.id as dev_id, ed.user_trade_id, ed.signal_id, ed.classification,
           te.ticker, te.execution_date, te.avg_price, te.side
    FROM execution_deviations ed
    JOIN trade_executions te ON te.id = ed.user_trade_id
    WHERE te.side = 'BUY'
  `);
  const deviations = deviationsResult.rows;

  for (const dev of deviations) {
    // Search for matching signals: same ticker, filed within 90 days BEFORE the trade
    const matchingResult = await pool.query(`
      SELECT ps.id, ps.signal_score, ps.signal_date, ps.score_tier,
             sep.next_open as entry_price, sep.prior_close
      FROM purchase_signals ps
      LEFT JOIN signal_entry_prices sep ON sep.signal_id = ps.id
      WHERE UPPER(ps.issuer_ticker) = UPPER($1)
        AND ps.signal_date <= $2
        AND ps.signal_date >= ($2::date - INTERVAL '90 days')::text
      ORDER BY ps.signal_score DESC, ps.signal_date DESC
      LIMIT 1
    `, [dev.ticker, dev.execution_date]);

    const bestSignal = matchingResult.rows[0];

    if (bestSignal) {
      const signalDate = new Date(bestSignal.signal_date);
      const tradeDate = new Date(dev.execution_date);
      const entryDelayDays = Math.floor(
        (tradeDate.getTime() - signalDate.getTime()) / (1000 * 60 * 60 * 24)
      );

      // Compute entry price gap: how much price moved between signal and actual entry
      let entryPriceGapPct: number | null = null;
      const signalPrice = bestSignal.entry_price || bestSignal.prior_close;
      if (signalPrice && dev.avg_price) {
        entryPriceGapPct = ((dev.avg_price - signalPrice) / signalPrice) * 100;
      }

      // Update the deviation record
      await pool.query(`
        UPDATE execution_deviations
        SET signal_id = $1,
            classification = 'signal_aligned',
            entry_delay_days = $2,
            entry_price_gap_pct = $3
        WHERE id = $4
      `, [bestSignal.id, entryDelayDays, entryPriceGapPct, dev.dev_id]);

      // Also update the trade execution's signal reference
      await pool.query(`
        UPDATE trade_executions
        SET signal_id = $1,
            signal_score = $2
        WHERE id = $3
      `, [bestSignal.id, bestSignal.signal_score, dev.user_trade_id]);

      matched++;
    } else {
      unmatched++;
    }
  }

  return { matched, unmatched, errors };
}

// ============================================================
// ENHANCED MISSED SIGNALS
// ============================================================

export async function getEnhancedMissedSignals(days = 90, minScore = 70) {
  // Get all tickers the user has traded
  const tradedResult = await pool.query(`
    SELECT DISTINCT UPPER(ticker) as ticker FROM trade_executions
  `);
  const tradedSet = new Set(tradedResult.rows.map((t: any) => t.ticker));

  // Get high-scoring signals from the last N days
  const signalsResult = await pool.query(`
    SELECT ps.id, ps.issuer_ticker, ps.issuer_name, ps.signal_date,
           ps.signal_score, ps.score_tier, ps.cluster_size, ps.total_purchase_value,
           sep.next_open as entry_price, sep.prior_close,
           (SELECT ROUND(dfr.excess_from_next_open * 100, 2)
            FROM daily_forward_returns dfr
            WHERE dfr.signal_id = ps.id AND dfr.trading_day = 63 LIMIT 1) as excess_63d_pct,
           (SELECT ROUND(dfr.return_from_next_open * 100, 2)
            FROM daily_forward_returns dfr
            WHERE dfr.signal_id = ps.id AND dfr.trading_day = 63 LIMIT 1) as return_63d_pct,
           (SELECT ROUND(dfr.return_from_next_open * 100, 2)
            FROM daily_forward_returns dfr
            WHERE dfr.signal_id = ps.id AND dfr.trading_day = 21 LIMIT 1) as return_21d_pct
    FROM purchase_signals ps
    LEFT JOIN signal_entry_prices sep ON sep.signal_id = ps.id
    WHERE ps.signal_score >= $1
      AND ps.signal_date >= CURRENT_DATE - INTERVAL '1 day' * $2
    ORDER BY ps.signal_score DESC
    LIMIT 100
  `, [minScore, days]);

  // Filter out signals where user has a trade for that ticker
  const missed = signalsResult.rows.filter((s: any) => {
    const ticker = (s.issuer_ticker || "").toUpperCase();
    return !tradedSet.has(ticker);
  });

  // Estimate alpha missed: use the actual forward return if available
  return missed.map((s: any) => ({
    id: s.id,
    ticker: s.issuer_ticker,
    companyName: s.issuer_name,
    signalDate: s.signal_date,
    signalScore: s.signal_score,
    scoreTier: s.score_tier,
    clusterSize: s.cluster_size,
    totalPurchaseValue: s.total_purchase_value,
    entryPrice: s.entry_price || s.prior_close,
    return63dPct: s.return_63d_pct,
    excess63dPct: s.excess_63d_pct,
    return21dPct: s.return_21d_pct,
    estimatedAlphaMissed: s.excess_63d_pct != null
      ? s.excess_63d_pct
      : (s.signal_score >= 80 ? 5.0 : s.signal_score >= 70 ? 3.0 : 1.5),
  }));
}
