/**
 * V2 Demo Data Seeder
 * 
 * Seeds realistic portfolio positions, closed trades, strategy snapshots,
 * and trade executions to demonstrate the full quant dashboard.
 */

import { storage } from "./storage";
import type { InsertPosition, InsertClosedTrade, InsertSnapshot, InsertTradeExecution } from "@shared/schema";

export function seedV2DemoData() {
  // Only seed if we haven't already
  const existingPositions = storage.getPositions();
  if (existingPositions.length > 0) return;

  const now = new Date();
  
  // === 1. Portfolio Positions (current holdings based on insider signals) ===
  const positions: InsertPosition[] = [
    {
      ticker: "NVDA", companyName: "NVIDIA Corp", quantity: 150, avgCostBasis: 824.50,
      currentPrice: 892.30, marketValue: 133845, unrealizedPnl: 10170, unrealizedPnlPct: 8.22,
      dayChange: 1285.50, dayChangePct: 0.97, signalScoreAtEntry: 87,
      entryDate: "2026-02-18", holdingDays: 40, lastSyncedAt: now.toISOString(),
      source: "manual", createdAt: now.toISOString(),
    },
    {
      ticker: "GS", companyName: "Goldman Sachs Group Inc", quantity: 200, avgCostBasis: 518.20,
      currentPrice: 548.90, marketValue: 109780, unrealizedPnl: 6140, unrealizedPnlPct: 5.93,
      dayChange: -438, dayChangePct: -0.40, signalScoreAtEntry: 72,
      entryDate: "2026-03-01", holdingDays: 29, lastSyncedAt: now.toISOString(),
      source: "manual", createdAt: now.toISOString(),
    },
    {
      ticker: "AAPL", companyName: "Apple Inc", quantity: 400, avgCostBasis: 192.80,
      currentPrice: 198.45, marketValue: 79380, unrealizedPnl: 2260, unrealizedPnlPct: 2.93,
      dayChange: 396, dayChangePct: 0.50, signalScoreAtEntry: 65,
      entryDate: "2026-03-10", holdingDays: 20, lastSyncedAt: now.toISOString(),
      source: "manual", createdAt: now.toISOString(),
    },
    {
      ticker: "JPM", companyName: "JPMorgan Chase & Co", quantity: 300, avgCostBasis: 207.50,
      currentPrice: 218.30, marketValue: 65490, unrealizedPnl: 3240, unrealizedPnlPct: 5.20,
      dayChange: 285, dayChangePct: 0.44, signalScoreAtEntry: 78,
      entryDate: "2026-02-24", holdingDays: 34, lastSyncedAt: now.toISOString(),
      source: "manual", createdAt: now.toISOString(),
    },
    {
      ticker: "META", companyName: "Meta Platforms Inc", quantity: 100, avgCostBasis: 498.70,
      currentPrice: 532.15, marketValue: 53215, unrealizedPnl: 3345, unrealizedPnlPct: 6.71,
      dayChange: 532.15, dayChangePct: 1.01, signalScoreAtEntry: 82,
      entryDate: "2026-03-05", holdingDays: 25, lastSyncedAt: now.toISOString(),
      source: "manual", createdAt: now.toISOString(),
    },
    {
      ticker: "BAC", companyName: "Bank of America Corp", quantity: 1500, avgCostBasis: 38.90,
      currentPrice: 41.25, marketValue: 61875, unrealizedPnl: 3525, unrealizedPnlPct: 6.04,
      dayChange: -247.50, dayChangePct: -0.40, signalScoreAtEntry: 58,
      entryDate: "2026-02-12", holdingDays: 46, lastSyncedAt: now.toISOString(),
      source: "manual", createdAt: now.toISOString(),
    },
    {
      ticker: "MSFT", companyName: "Microsoft Corp", quantity: 120, avgCostBasis: 420.80,
      currentPrice: 438.60, marketValue: 52632, unrealizedPnl: 2136, unrealizedPnlPct: 4.23,
      dayChange: 263.16, dayChangePct: 0.50, signalScoreAtEntry: 74,
      entryDate: "2026-03-15", holdingDays: 15, lastSyncedAt: now.toISOString(),
      source: "manual", createdAt: now.toISOString(),
    },
    {
      ticker: "PFE", companyName: "Pfizer Inc", quantity: 2000, avgCostBasis: 27.90,
      currentPrice: 26.45, marketValue: 52900, unrealizedPnl: -2900, unrealizedPnlPct: -5.20,
      dayChange: -398, dayChangePct: -0.75, signalScoreAtEntry: 44,
      entryDate: "2026-02-20", holdingDays: 38, lastSyncedAt: now.toISOString(),
      source: "manual", createdAt: now.toISOString(),
    },
  ];

  for (const pos of positions) {
    try { storage.upsertPosition(pos); } catch (e) { /* skip */ }
  }

  // === 2. Closed Trades (historical P&L) ===
  const closedTradesData: InsertClosedTrade[] = [
    { ticker: "TSLA", companyName: "Tesla Inc", entryDate: "2025-12-15", exitDate: "2026-01-22", entryPrice: 165.40, exitPrice: 189.80, quantity: 300, realizedPnl: 7320, realizedPnlPct: 14.75, holdingDays: 38, signalScoreAtEntry: 75, signalId: 1, createdAt: now.toISOString() },
    { ticker: "AMZN", companyName: "Amazon.com Inc", entryDate: "2026-01-05", exitDate: "2026-02-10", entryPrice: 178.20, exitPrice: 195.50, quantity: 250, realizedPnl: 4325, realizedPnlPct: 9.71, holdingDays: 36, signalScoreAtEntry: 82, signalId: 2, createdAt: now.toISOString() },
    { ticker: "GOOGL", companyName: "Alphabet Inc", entryDate: "2026-01-12", exitDate: "2026-02-20", entryPrice: 152.30, exitPrice: 168.90, quantity: 400, realizedPnl: 6640, realizedPnlPct: 10.90, holdingDays: 39, signalScoreAtEntry: 70, signalId: 3, createdAt: now.toISOString() },
    { ticker: "C", companyName: "Citigroup Inc", entryDate: "2026-01-20", exitDate: "2026-02-28", entryPrice: 62.50, exitPrice: 68.20, quantity: 800, realizedPnl: 4560, realizedPnlPct: 9.12, holdingDays: 39, signalScoreAtEntry: 63, signalId: 4, createdAt: now.toISOString() },
    { ticker: "IBM", companyName: "Intl Business Machines Corp", entryDate: "2026-01-15", exitDate: "2026-02-05", entryPrice: 198.40, exitPrice: 188.10, quantity: 200, realizedPnl: -2060, realizedPnlPct: -5.19, holdingDays: 21, signalScoreAtEntry: 48, signalId: 5, createdAt: now.toISOString() },
    { ticker: "T", companyName: "AT&T Inc", entryDate: "2026-01-08", exitDate: "2026-01-30", entryPrice: 21.80, exitPrice: 20.50, quantity: 2000, realizedPnl: -2600, realizedPnlPct: -5.96, holdingDays: 22, signalScoreAtEntry: 38, signalId: 6, createdAt: now.toISOString() },
    { ticker: "SBUX", companyName: "Starbucks Corp", entryDate: "2026-02-01", exitDate: "2026-03-05", entryPrice: 95.20, exitPrice: 102.80, quantity: 500, realizedPnl: 3800, realizedPnlPct: 7.98, holdingDays: 32, signalScoreAtEntry: 69, signalId: 7, createdAt: now.toISOString() },
    { ticker: "KO", companyName: "Coca-Cola Co", entryDate: "2026-02-05", exitDate: "2026-03-10", entryPrice: 61.80, exitPrice: 64.50, quantity: 800, realizedPnl: 2160, realizedPnlPct: 4.37, holdingDays: 33, signalScoreAtEntry: 55, signalId: 8, createdAt: now.toISOString() },
    { ticker: "PG", companyName: "Procter & Gamble Co", entryDate: "2026-02-10", exitDate: "2026-03-15", entryPrice: 165.90, exitPrice: 172.40, quantity: 300, realizedPnl: 1950, realizedPnlPct: 3.92, holdingDays: 33, signalScoreAtEntry: 61, signalId: 9, createdAt: now.toISOString() },
    { ticker: "BRK.B", companyName: "Berkshire Hathaway Inc", entryDate: "2025-12-20", exitDate: "2026-02-15", entryPrice: 412.30, exitPrice: 435.70, quantity: 100, realizedPnl: 2340, realizedPnlPct: 5.68, holdingDays: 57, signalScoreAtEntry: 85, signalId: 10, createdAt: now.toISOString() },
    { ticker: "PEP", companyName: "PepsiCo Inc", entryDate: "2026-02-15", exitDate: "2026-03-20", entryPrice: 170.30, exitPrice: 163.80, quantity: 200, realizedPnl: -1300, realizedPnlPct: -3.82, holdingDays: 33, signalScoreAtEntry: 42, signalId: 11, createdAt: now.toISOString() },
    { ticker: "JNJ", companyName: "Johnson & Johnson", entryDate: "2026-01-25", exitDate: "2026-03-01", entryPrice: 153.40, exitPrice: 161.20, quantity: 350, realizedPnl: 2730, realizedPnlPct: 5.08, holdingDays: 35, signalScoreAtEntry: 67, signalId: 12, createdAt: now.toISOString() },
  ];

  for (const trade of closedTradesData) {
    try { storage.insertClosedTrade(trade); } catch (e) { /* skip */ }
  }

  // === 3. Trade Executions (buy/sell log) ===
  const executions: InsertTradeExecution[] = [
    // Buys for current positions
    { ticker: "NVDA", companyName: "NVIDIA Corp", side: "BUY", quantity: 150, avgPrice: 824.50, totalCost: 123675, executionDate: "2026-02-18", signalId: 1, signalScore: 87, source: "manual", status: "filled", createdAt: now.toISOString() },
    { ticker: "GS", companyName: "Goldman Sachs Group Inc", side: "BUY", quantity: 200, avgPrice: 518.20, totalCost: 103640, executionDate: "2026-03-01", signalId: 2, signalScore: 72, source: "manual", status: "filled", createdAt: now.toISOString() },
    { ticker: "AAPL", companyName: "Apple Inc", side: "BUY", quantity: 400, avgPrice: 192.80, totalCost: 77120, executionDate: "2026-03-10", signalId: 3, signalScore: 65, source: "manual", status: "filled", createdAt: now.toISOString() },
    { ticker: "JPM", companyName: "JPMorgan Chase & Co", side: "BUY", quantity: 300, avgPrice: 207.50, totalCost: 62250, executionDate: "2026-02-24", signalId: 4, signalScore: 78, source: "manual", status: "filled", createdAt: now.toISOString() },
    { ticker: "META", companyName: "Meta Platforms Inc", side: "BUY", quantity: 100, avgPrice: 498.70, totalCost: 49870, executionDate: "2026-03-05", signalId: 5, signalScore: 82, source: "manual", status: "filled", createdAt: now.toISOString() },
    { ticker: "BAC", companyName: "Bank of America Corp", side: "BUY", quantity: 1500, avgPrice: 38.90, totalCost: 58350, executionDate: "2026-02-12", signalId: 6, signalScore: 58, source: "manual", status: "filled", createdAt: now.toISOString() },
    { ticker: "MSFT", companyName: "Microsoft Corp", side: "BUY", quantity: 120, avgPrice: 420.80, totalCost: 50496, executionDate: "2026-03-15", signalId: 7, signalScore: 74, source: "manual", status: "filled", createdAt: now.toISOString() },
    { ticker: "PFE", companyName: "Pfizer Inc", side: "BUY", quantity: 2000, avgPrice: 27.90, totalCost: 55800, executionDate: "2026-02-20", signalId: 8, signalScore: 44, source: "manual", status: "filled", createdAt: now.toISOString() },
    // Sells for closed trades
    { ticker: "TSLA", companyName: "Tesla Inc", side: "SELL", quantity: 300, avgPrice: 189.80, totalCost: 56940, executionDate: "2026-01-22", signalId: null, signalScore: null, source: "manual", status: "filled", createdAt: now.toISOString() },
    { ticker: "AMZN", companyName: "Amazon.com Inc", side: "SELL", quantity: 250, avgPrice: 195.50, totalCost: 48875, executionDate: "2026-02-10", signalId: null, signalScore: null, source: "manual", status: "filled", createdAt: now.toISOString() },
  ];

  for (const exec of executions) {
    try { storage.insertTradeExecution(exec); } catch (e) { /* skip */ }
  }

  // === 4. Strategy Snapshots (daily performance history — 90 days) ===
  const startNav = 1_000_000; // $1M starting capital
  let currentNav = startNav;
  let benchmarkNav = startNav;
  let peak = startNav;
  let cumulativeReturn = 0;
  let benchmarkCumReturn = 0;
  const allDailyReturns: number[] = [];
  const allBenchmarkReturns: number[] = [];

  for (let i = 90; i >= 0; i--) {
    const d = new Date(now);
    d.setDate(d.getDate() - i);
    // Skip weekends
    if (d.getDay() === 0 || d.getDay() === 6) continue;

    const dateStr = d.toISOString().split("T")[0];
    
    // Simulate daily returns — insider strategy slightly outperforms
    // Strategy has higher vol but higher mean (alpha from signals)
    const strategyDailyReturn = (Math.random() - 0.46) * 0.018; // slight positive bias
    const benchmarkDailyReturn = (Math.random() - 0.48) * 0.012;
    
    currentNav *= (1 + strategyDailyReturn);
    benchmarkNav *= (1 + benchmarkDailyReturn);
    cumulativeReturn = (currentNav - startNav) / startNav;
    benchmarkCumReturn = (benchmarkNav - startNav) / startNav;
    
    if (currentNav > peak) peak = currentNav;
    const currentDD = (peak - currentNav) / peak;
    
    allDailyReturns.push(strategyDailyReturn);
    allBenchmarkReturns.push(benchmarkDailyReturn);
    
    // Rolling Sharpe (trailing 20 days)
    const recentReturns = allDailyReturns.slice(-20);
    const dailyRf = 0.053 / 252;
    const excessRet = recentReturns.map(r => r - dailyRf);
    const meanExcess = excessRet.reduce((a, b) => a + b, 0) / excessRet.length;
    const varExcess = excessRet.reduce((s, r) => s + Math.pow(r - meanExcess, 2), 0) / Math.max(excessRet.length - 1, 1);
    const sharpe = varExcess > 0 ? (meanExcess / Math.sqrt(varExcess)) * Math.sqrt(252) : 0;
    
    // Rolling max drawdown
    const recentNavs = [];
    let tempNav = startNav;
    for (const r of allDailyReturns) {
      tempNav *= (1 + r);
      recentNavs.push(tempNav);
    }
    let maxDD = 0;
    let ddPeak = recentNavs[0] || startNav;
    for (const n of recentNavs) {
      if (n > ddPeak) ddPeak = n;
      const dd = (ddPeak - n) / ddPeak;
      if (dd > maxDD) maxDD = dd;
    }

    const closedCount = closedTradesData.filter(t => t.exitDate <= dateStr).length;
    const winCount = closedTradesData.filter(t => t.exitDate <= dateStr && t.realizedPnl > 0).length;
    const winRate = closedCount > 0 ? winCount / closedCount : 0;

    const snap: InsertSnapshot = {
      date: dateStr,
      portfolioValue: Math.round(currentNav * 0.85),
      cashBalance: Math.round(currentNav * 0.15),
      totalNav: Math.round(currentNav),
      dailyReturn: Math.round(strategyDailyReturn * 10000) / 10000,
      cumulativeReturn: Math.round(cumulativeReturn * 10000) / 10000,
      benchmarkReturn: Math.round(benchmarkDailyReturn * 10000) / 10000,
      benchmarkCumulative: Math.round(benchmarkCumReturn * 10000) / 10000,
      alpha: Math.round((cumulativeReturn - benchmarkCumReturn) * 10000) / 10000,
      openPositions: positions.length,
      tradesExecuted: closedCount,
      winRate: Math.round(winRate * 100) / 100,
      avgWinPct: 7.72,
      avgLossPct: -4.99,
      sharpeRatio: Math.round(sharpe * 100) / 100,
      sortinoRatio: Math.round(sharpe * 1.35 * 100) / 100,
      maxDrawdown: Math.round(maxDD * 10000) / 10000,
      currentDrawdown: Math.round(currentDD * 10000) / 10000,
      createdAt: now.toISOString(),
    };

    try { storage.insertSnapshot(snap); } catch (e) { /* skip */ }
  }
}
