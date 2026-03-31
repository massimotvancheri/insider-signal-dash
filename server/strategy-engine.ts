/**
 * Strategy Performance Engine
 * 
 * Computes institutional-quality performance metrics for the insider signal strategy:
 * - Sharpe Ratio (risk-adjusted return)
 * - Sortino Ratio (downside-risk adjusted)
 * - Maximum Drawdown
 * - Win Rate & Expectancy
 * - Signal Attribution (which signal scores generate best returns)
 * - Benchmark Comparison (vs SPY)
 */

import { storage } from "./storage";
import type { ClosedTrade, StrategySnapshot, PortfolioPosition } from "@shared/schema";

// Risk-free rate (annualized, ~5.3% for US T-Bills 2024-2026)
const RISK_FREE_RATE = 0.053;
const TRADING_DAYS_PER_YEAR = 252;

export interface StrategyMetrics {
  // Returns
  totalReturn: number;
  annualizedReturn: number;
  benchmarkReturn: number;
  alpha: number;
  
  // Risk
  sharpeRatio: number;
  sortinoRatio: number;
  maxDrawdown: number;
  currentDrawdown: number;
  volatility: number;
  downstdDev: number;
  
  // Trade Stats
  totalTrades: number;
  winningTrades: number;
  losingTrades: number;
  winRate: number;
  avgWin: number;
  avgLoss: number;
  profitFactor: number;
  expectancy: number;
  avgHoldingDays: number;
  
  // Portfolio
  totalNav: number;
  cashBalance: number;
  openPositions: number;
  portfolioValue: number;

  // Signal Attribution
  signalAttribution: SignalAttribution[];
}

export interface SignalAttribution {
  scoreRange: string;
  trades: number;
  avgReturn: number;
  winRate: number;
  avgHoldingDays: number;
}

/**
 * Calculate Sharpe Ratio from daily returns
 * Sharpe = (E[R - Rf]) / σ(R - Rf) × √252
 */
export function calculateSharpe(dailyReturns: number[]): number {
  if (dailyReturns.length < 2) return 0;
  
  const dailyRf = RISK_FREE_RATE / TRADING_DAYS_PER_YEAR;
  const excessReturns = dailyReturns.map(r => r - dailyRf);
  const mean = excessReturns.reduce((a, b) => a + b, 0) / excessReturns.length;
  const variance = excessReturns.reduce((s, r) => s + Math.pow(r - mean, 2), 0) / (excessReturns.length - 1);
  const stdDev = Math.sqrt(variance);
  
  if (stdDev === 0) return 0;
  return (mean / stdDev) * Math.sqrt(TRADING_DAYS_PER_YEAR);
}

/**
 * Calculate Sortino Ratio — only penalizes downside volatility
 * Sortino = (E[R - Rf]) / σ_down(R) × √252
 */
export function calculateSortino(dailyReturns: number[]): number {
  if (dailyReturns.length < 2) return 0;
  
  const dailyRf = RISK_FREE_RATE / TRADING_DAYS_PER_YEAR;
  const excessReturns = dailyReturns.map(r => r - dailyRf);
  const mean = excessReturns.reduce((a, b) => a + b, 0) / excessReturns.length;
  
  // Downside deviation: only negative excess returns
  const downsideReturns = excessReturns.filter(r => r < 0);
  if (downsideReturns.length === 0) return mean > 0 ? 999 : 0;
  
  const downsideVariance = downsideReturns.reduce((s, r) => s + r * r, 0) / downsideReturns.length;
  const downsideStdDev = Math.sqrt(downsideVariance);
  
  if (downsideStdDev === 0) return 0;
  return (mean / downsideStdDev) * Math.sqrt(TRADING_DAYS_PER_YEAR);
}

/**
 * Calculate Maximum Drawdown from equity curve
 */
export function calculateMaxDrawdown(navSeries: number[]): { maxDrawdown: number; currentDrawdown: number } {
  if (navSeries.length < 2) return { maxDrawdown: 0, currentDrawdown: 0 };
  
  let peak = navSeries[0];
  let maxDD = 0;
  
  for (const nav of navSeries) {
    if (nav > peak) peak = nav;
    const dd = (peak - nav) / peak;
    if (dd > maxDD) maxDD = dd;
  }
  
  const lastNav = navSeries[navSeries.length - 1];
  const lastPeak = Math.max(...navSeries);
  const currentDD = (lastPeak - lastNav) / lastPeak;
  
  return { maxDrawdown: maxDD, currentDrawdown: currentDD };
}

/**
 * Calculate volatility (annualized standard deviation of returns)
 */
export function calculateVolatility(dailyReturns: number[]): number {
  if (dailyReturns.length < 2) return 0;
  const mean = dailyReturns.reduce((a, b) => a + b, 0) / dailyReturns.length;
  const variance = dailyReturns.reduce((s, r) => s + Math.pow(r - mean, 2), 0) / (dailyReturns.length - 1);
  return Math.sqrt(variance) * Math.sqrt(TRADING_DAYS_PER_YEAR);
}

/**
 * Calculate trade-level statistics
 */
function calculateTradeStats(trades: ClosedTrade[]) {
  if (trades.length === 0) {
    return { totalTrades: 0, winningTrades: 0, losingTrades: 0, winRate: 0, avgWin: 0, avgLoss: 0, profitFactor: 0, expectancy: 0, avgHoldingDays: 0 };
  }
  
  const winners = trades.filter(t => t.realizedPnl > 0);
  const losers = trades.filter(t => t.realizedPnl <= 0);
  
  const totalWin = winners.reduce((s, t) => s + t.realizedPnl, 0);
  const totalLoss = Math.abs(losers.reduce((s, t) => s + t.realizedPnl, 0));
  
  const winRate = winners.length / trades.length;
  const avgWin = winners.length > 0 ? winners.reduce((s, t) => s + t.realizedPnlPct, 0) / winners.length : 0;
  const avgLoss = losers.length > 0 ? losers.reduce((s, t) => s + t.realizedPnlPct, 0) / losers.length : 0;
  const profitFactor = totalLoss > 0 ? totalWin / totalLoss : totalWin > 0 ? 999 : 0;
  const expectancy = winRate * avgWin + (1 - winRate) * avgLoss;
  const avgHoldingDays = trades.reduce((s, t) => s + (t.holdingDays || 0), 0) / trades.length;
  
  return {
    totalTrades: trades.length,
    winningTrades: winners.length,
    losingTrades: losers.length,
    winRate,
    avgWin,
    avgLoss,
    profitFactor,
    expectancy,
    avgHoldingDays,
  };
}

/**
 * Signal attribution — which signal score ranges produce best results
 */
function calculateSignalAttribution(trades: ClosedTrade[]): SignalAttribution[] {
  const ranges = [
    { label: "80-100 (Strong)", min: 80, max: 100 },
    { label: "60-79 (Good)", min: 60, max: 79 },
    { label: "40-59 (Moderate)", min: 40, max: 59 },
    { label: "0-39 (Weak)", min: 0, max: 39 },
  ];
  
  return ranges.map(range => {
    const inRange = trades.filter(t => {
      const score = t.signalScoreAtEntry || 0;
      return score >= range.min && score <= range.max;
    });
    
    const winners = inRange.filter(t => t.realizedPnl > 0);
    
    return {
      scoreRange: range.label,
      trades: inRange.length,
      avgReturn: inRange.length > 0 ? inRange.reduce((s, t) => s + t.realizedPnlPct, 0) / inRange.length : 0,
      winRate: inRange.length > 0 ? winners.length / inRange.length : 0,
      avgHoldingDays: inRange.length > 0 ? inRange.reduce((s, t) => s + (t.holdingDays || 0), 0) / inRange.length : 0,
    };
  });
}

/**
 * Get full strategy metrics from stored data
 */
export function getStrategyMetrics(): StrategyMetrics {
  const snapshots = storage.getSnapshots(365);
  const closedTradesData = storage.getClosedTrades(1000);
  const positions = storage.getPositions();
  const latestSnap = storage.getLatestSnapshot();
  
  // Daily returns from snapshots
  const dailyReturns = snapshots.filter(s => s.dailyReturn != null).map(s => s.dailyReturn!);
  const navSeries = snapshots.map(s => s.totalNav);
  const benchmarkReturns = snapshots.filter(s => s.benchmarkReturn != null).map(s => s.benchmarkReturn!);
  
  // Calculate risk metrics
  const sharpe = calculateSharpe(dailyReturns);
  const sortino = calculateSortino(dailyReturns);
  const { maxDrawdown, currentDrawdown } = calculateMaxDrawdown(navSeries);
  const vol = calculateVolatility(dailyReturns);
  const downReturns = dailyReturns.filter(r => r < 0);
  const downstdDev = downReturns.length > 0 
    ? Math.sqrt(downReturns.reduce((s, r) => s + r * r, 0) / downReturns.length) * Math.sqrt(TRADING_DAYS_PER_YEAR)
    : 0;
  
  // Total returns
  const totalReturn = latestSnap?.cumulativeReturn || 0;
  const daysActive = snapshots.length;
  const annualizedReturn = daysActive > 0 
    ? Math.pow(1 + totalReturn, TRADING_DAYS_PER_YEAR / daysActive) - 1 
    : 0;
  const benchmarkCum = latestSnap?.benchmarkCumulative || 0;
  
  // Trade stats
  const tradeStats = calculateTradeStats(closedTradesData);
  
  // Signal attribution
  const signalAttribution = calculateSignalAttribution(closedTradesData);
  
  // Portfolio summary
  const portfolioValue = positions.reduce((s, p) => s + (p.marketValue || 0), 0);
  const cashBalance = latestSnap?.cashBalance || 0;
  
  return {
    totalReturn,
    annualizedReturn,
    benchmarkReturn: benchmarkCum,
    alpha: totalReturn - benchmarkCum,
    sharpeRatio: sharpe,
    sortinoRatio: sortino,
    maxDrawdown,
    currentDrawdown,
    volatility: vol,
    downstdDev,
    ...tradeStats,
    totalNav: portfolioValue + cashBalance,
    cashBalance,
    openPositions: positions.length,
    portfolioValue,
    signalAttribution,
  };
}

/**
 * Get performance chart data — equity curve with benchmark overlay
 */
export function getPerformanceChartData(days = 90) {
  return storage.getSnapshots(days).map(s => ({
    date: s.date,
    strategy: s.cumulativeReturn ? Math.round(s.cumulativeReturn * 10000) / 100 : 0,
    benchmark: s.benchmarkCumulative ? Math.round(s.benchmarkCumulative * 10000) / 100 : 0,
    alpha: s.alpha ? Math.round(s.alpha * 10000) / 100 : 0,
    nav: s.totalNav,
    drawdown: s.currentDrawdown ? Math.round(s.currentDrawdown * 10000) / 100 : 0,
  }));
}
