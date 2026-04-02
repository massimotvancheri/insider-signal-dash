import {
  insiderTransactions,
  purchaseSignals,
  pollingState,
  tradeExecutions,
  executionDeviations,
  portfolioPositions,
  strategySnapshots,
  schwabConfig,
  closedTrades,
  type InsertTransaction,
  type InsiderTransaction,
  type InsertSignal,
  type PurchaseSignal,
  type PollingState,
  type InsertTradeExecution,
  type TradeExecution,
  type InsertPosition,
  type PortfolioPosition,
  type InsertSnapshot,
  type StrategySnapshot,
  type InsertClosedTrade,
  type ClosedTrade,
  type SchwabConfig,
  type InsertDeviation,
  type ExecutionDeviation,
} from "@shared/schema";
import { eq, desc, sql, and, gte, lte, count, asc } from "drizzle-orm";
import { db, sqlite } from "./db";

export interface IStorage {
  // Transactions
  insertTransaction(tx: InsertTransaction): InsiderTransaction;
  getTransactions(limit?: number, offset?: number): InsiderTransaction[];
  getPurchaseTransactions(limit?: number, days?: number): InsiderTransaction[];
  getTransactionByAccession(accessionNumber: string): InsiderTransaction | undefined;
  getTransactionCount(): number;
  getPurchaseCount(days?: number): number;
  getRecentPurchaseVolume(days: number): number;
  getTopBuyers(days?: number, limit?: number): { name: string; title: string; ticker: string; totalValue: number; count: number }[];
  getDailyPurchaseVolume(days: number): { date: string; volume: number; count: number }[];
  
  // Signals
  insertSignal(signal: InsertSignal): PurchaseSignal;
  getSignals(limit?: number): PurchaseSignal[];
  getTopSignals(limit?: number): PurchaseSignal[];
  
  // Polling state
  getPollingState(): PollingState | undefined;
  updatePollingState(lastPolledAt: string, lastAccession?: string, totalProcessed?: number): void;
  
  // Analytics
  getSectorBreakdown(days?: number): { ticker: string; name: string; totalValue: number; count: number }[];
  getInsiderTypeBreakdown(days?: number): { type: string; count: number; totalValue: number }[];
  getClusterBuys(days?: number): { ticker: string; name: string; insiderCount: number; totalValue: number; avgPrice: number; dates: string }[];

  // V2: Trade Executions
  insertTradeExecution(trade: InsertTradeExecution): TradeExecution;
  getTradeExecutions(limit?: number): TradeExecution[];
  getTradeExecutionsByTicker(ticker: string): TradeExecution[];

  // V2: Portfolio Positions
  upsertPosition(pos: InsertPosition): PortfolioPosition;
  getPositions(): PortfolioPosition[];
  getPositionByTicker(ticker: string): PortfolioPosition | undefined;
  deletePosition(ticker: string): void;

  // V2: Strategy Snapshots
  insertSnapshot(snap: InsertSnapshot): StrategySnapshot;
  getSnapshots(days?: number): StrategySnapshot[];
  getLatestSnapshot(): StrategySnapshot | undefined;

  // V2: Closed Trades
  insertClosedTrade(trade: InsertClosedTrade): ClosedTrade;
  getClosedTrades(limit?: number): ClosedTrade[];

  // V2: Execution Deviations
  insertExecutionDeviation(dev: InsertDeviation): ExecutionDeviation;
  getTradeExecutionByOrderId(orderId: string): TradeExecution | undefined;

  // V2: Schwab Config
  getSchwabConfig(): SchwabConfig | undefined;
  upsertSchwabConfig(config: Partial<SchwabConfig>): void;
}

export class DatabaseStorage implements IStorage {
  insertTransaction(tx: InsertTransaction): InsiderTransaction {
    return db.insert(insiderTransactions).values(tx).returning().get();
  }

  getTransactions(limit = 100, offset = 0): InsiderTransaction[] {
    return db.select().from(insiderTransactions)
      .orderBy(desc(insiderTransactions.filingDate))
      .limit(limit)
      .offset(offset)
      .all();
  }

  getPurchaseTransactions(limit = 100, days = 30): InsiderTransaction[] {
    const cutoff = new Date();
    cutoff.setDate(cutoff.getDate() - days);
    const cutoffStr = cutoff.toISOString().split("T")[0];
    return db.select().from(insiderTransactions)
      .where(and(
        eq(insiderTransactions.transactionType, "P"),
        gte(insiderTransactions.filingDate, cutoffStr)
      ))
      .orderBy(desc(insiderTransactions.filingDate))
      .limit(limit)
      .all();
  }

  getTransactionByAccession(accessionNumber: string): InsiderTransaction | undefined {
    return db.select().from(insiderTransactions)
      .where(eq(insiderTransactions.accessionNumber, accessionNumber))
      .get();
  }

  getTransactionCount(): number {
    const result = db.select({ count: count() }).from(insiderTransactions).get();
    return result?.count ?? 0;
  }

  getPurchaseCount(days = 30): number {
    const cutoff = new Date();
    cutoff.setDate(cutoff.getDate() - days);
    const cutoffStr = cutoff.toISOString().split("T")[0];
    const result = db.select({ count: count() }).from(insiderTransactions)
      .where(and(
        eq(insiderTransactions.transactionType, "P"),
        gte(insiderTransactions.filingDate, cutoffStr)
      ))
      .get();
    return result?.count ?? 0;
  }

  getRecentPurchaseVolume(days: number): number {
    const cutoff = new Date();
    cutoff.setDate(cutoff.getDate() - days);
    const cutoffStr = cutoff.toISOString().split("T")[0];
    // Cap individual transaction values at $500M to filter SEC data outliers
    const result = db.select({
      total: sql<number>`COALESCE(SUM(CASE WHEN ${insiderTransactions.totalValue} <= 500000000 THEN ${insiderTransactions.totalValue} ELSE 0 END), 0)`
    }).from(insiderTransactions)
      .where(and(
        eq(insiderTransactions.transactionType, "P"),
        gte(insiderTransactions.filingDate, cutoffStr)
      ))
      .get();
    return result?.total ?? 0;
  }

  getTopBuyers(days = 30, limit = 10) {
    const cutoff = new Date();
    cutoff.setDate(cutoff.getDate() - days);
    const cutoffStr = cutoff.toISOString().split("T")[0];
    return db.select({
      name: insiderTransactions.reportingPersonName,
      title: sql<string>`MAX(${insiderTransactions.reportingPersonTitle})`,
      ticker: sql<string>`MAX(${insiderTransactions.issuerTicker})`,
      totalValue: sql<number>`COALESCE(SUM(${insiderTransactions.totalValue}), 0)`,
      count: count(),
    }).from(insiderTransactions)
      .where(and(
        eq(insiderTransactions.transactionType, "P"),
        gte(insiderTransactions.filingDate, cutoffStr)
      ))
      .groupBy(insiderTransactions.reportingPersonName)
      .orderBy(sql`SUM(${insiderTransactions.totalValue}) DESC`)
      .limit(limit)
      .all();
  }

  getDailyPurchaseVolume(days: number) {
    const cutoff = new Date();
    cutoff.setDate(cutoff.getDate() - days);
    const cutoffStr = cutoff.toISOString().split("T")[0];
    return db.select({
      date: insiderTransactions.filingDate,
      volume: sql<number>`COALESCE(SUM(${insiderTransactions.totalValue}), 0)`,
      count: count(),
    }).from(insiderTransactions)
      .where(and(
        eq(insiderTransactions.transactionType, "P"),
        gte(insiderTransactions.filingDate, cutoffStr)
      ))
      .groupBy(insiderTransactions.filingDate)
      .orderBy(insiderTransactions.filingDate)
      .all();
  }

  insertSignal(signal: InsertSignal): PurchaseSignal {
    return db.insert(purchaseSignals).values(signal).returning().get();
  }

  getSignals(limit = 50): PurchaseSignal[] {
    return db.select().from(purchaseSignals)
      .orderBy(desc(purchaseSignals.signalDate))
      .limit(limit)
      .all();
  }

  getTopSignals(limit = 20): PurchaseSignal[] {
    return db.select().from(purchaseSignals)
      .orderBy(desc(purchaseSignals.signalScore))
      .limit(limit)
      .all();
  }

  getPollingState(): PollingState | undefined {
    return db.select().from(pollingState).get();
  }

  updatePollingState(lastPolledAt: string, lastAccession?: string, totalProcessed?: number): void {
    const existing = this.getPollingState();
    if (existing) {
      db.update(pollingState)
        .set({
          lastPolledAt,
          ...(lastAccession ? { lastAccessionNumber: lastAccession } : {}),
          ...(totalProcessed !== undefined ? { totalFilingsProcessed: totalProcessed } : {}),
        })
        .where(eq(pollingState.id, existing.id))
        .run();
    } else {
      db.insert(pollingState).values({
        lastPolledAt,
        lastAccessionNumber: lastAccession ?? null,
        totalFilingsProcessed: totalProcessed ?? 0,
        status: "active",
      }).run();
    }
  }

  getSectorBreakdown(days = 30) {
    const cutoff = new Date();
    cutoff.setDate(cutoff.getDate() - days);
    const cutoffStr = cutoff.toISOString().split("T")[0];
    return db.select({
      ticker: sql<string>`COALESCE(${insiderTransactions.issuerTicker}, 'N/A')`,
      name: sql<string>`MAX(${insiderTransactions.issuerName})`,
      totalValue: sql<number>`COALESCE(SUM(${insiderTransactions.totalValue}), 0)`,
      count: count(),
    }).from(insiderTransactions)
      .where(and(
        eq(insiderTransactions.transactionType, "P"),
        gte(insiderTransactions.filingDate, cutoffStr)
      ))
      .groupBy(insiderTransactions.issuerTicker)
      .orderBy(sql`SUM(${insiderTransactions.totalValue}) DESC`)
      .limit(20)
      .all();
  }

  getInsiderTypeBreakdown(days = 30) {
    const cutoff = new Date();
    cutoff.setDate(cutoff.getDate() - days);
    const cutoffStr = cutoff.toISOString().split("T")[0];
    return db.select({
      type: sql<string>`
        CASE 
          WHEN ${insiderTransactions.isOfficer} = 1 THEN 'Officer'
          WHEN ${insiderTransactions.isDirector} = 1 THEN 'Director'
          WHEN ${insiderTransactions.isTenPercentOwner} = 1 THEN '10%+ Owner'
          ELSE 'Other'
        END`,
      count: count(),
      totalValue: sql<number>`COALESCE(SUM(${insiderTransactions.totalValue}), 0)`,
    }).from(insiderTransactions)
      .where(and(
        eq(insiderTransactions.transactionType, "P"),
        gte(insiderTransactions.filingDate, cutoffStr)
      ))
      .groupBy(sql`CASE 
        WHEN ${insiderTransactions.isOfficer} = 1 THEN 'Officer'
        WHEN ${insiderTransactions.isDirector} = 1 THEN 'Director'
        WHEN ${insiderTransactions.isTenPercentOwner} = 1 THEN '10%+ Owner'
        ELSE 'Other'
      END`)
      .all();
  }

  getClusterBuys(days = 30) {
    const cutoff = new Date();
    cutoff.setDate(cutoff.getDate() - days);
    const cutoffStr = cutoff.toISOString().split("T")[0];
    return db.select({
      ticker: sql<string>`COALESCE(${insiderTransactions.issuerTicker}, 'N/A')`,
      name: sql<string>`MAX(${insiderTransactions.issuerName})`,
      insiderCount: sql<number>`COUNT(DISTINCT ${insiderTransactions.reportingPersonName})`,
      totalValue: sql<number>`COALESCE(SUM(${insiderTransactions.totalValue}), 0)`,
      avgPrice: sql<number>`COALESCE(AVG(${insiderTransactions.pricePerShare}), 0)`,
      dates: sql<string>`MIN(${insiderTransactions.filingDate}) || ' to ' || MAX(${insiderTransactions.filingDate})`,
    }).from(insiderTransactions)
      .where(and(
        eq(insiderTransactions.transactionType, "P"),
        gte(insiderTransactions.filingDate, cutoffStr)
      ))
      .groupBy(insiderTransactions.issuerTicker)
      .having(sql`COUNT(DISTINCT ${insiderTransactions.reportingPersonName}) >= 2`)
      .orderBy(sql`COUNT(DISTINCT ${insiderTransactions.reportingPersonName}) DESC`)
      .limit(20)
      .all();
  }

  // ========== V2: Trade Executions ==========

  insertTradeExecution(trade: InsertTradeExecution): TradeExecution {
    return db.insert(tradeExecutions).values(trade).returning().get();
  }

  getTradeExecutions(limit = 100): TradeExecution[] {
    return db.select().from(tradeExecutions)
      .orderBy(desc(tradeExecutions.executionDate))
      .limit(limit)
      .all();
  }

  getTradeExecutionsByTicker(ticker: string): TradeExecution[] {
    return db.select().from(tradeExecutions)
      .where(eq(tradeExecutions.ticker, ticker))
      .orderBy(desc(tradeExecutions.executionDate))
      .all();
  }

  // ========== V2: Portfolio Positions ==========

  upsertPosition(pos: InsertPosition): PortfolioPosition {
    const existing = this.getPositionByTicker(pos.ticker);
    if (existing) {
      db.update(portfolioPositions)
        .set({
          quantity: pos.quantity,
          avgCostBasis: pos.avgCostBasis,
          currentPrice: pos.currentPrice ?? existing.currentPrice,
          marketValue: pos.marketValue ?? existing.marketValue,
          unrealizedPnl: pos.unrealizedPnl ?? existing.unrealizedPnl,
          unrealizedPnlPct: pos.unrealizedPnlPct ?? existing.unrealizedPnlPct,
          dayChange: pos.dayChange ?? existing.dayChange,
          dayChangePct: pos.dayChangePct ?? existing.dayChangePct,
          signalScoreAtEntry: pos.signalScoreAtEntry ?? existing.signalScoreAtEntry,
          lastSyncedAt: pos.lastSyncedAt ?? existing.lastSyncedAt,
          source: pos.source ?? existing.source,
        })
        .where(eq(portfolioPositions.ticker, pos.ticker))
        .run();
      return this.getPositionByTicker(pos.ticker)!;
    }
    return db.insert(portfolioPositions).values(pos).returning().get();
  }

  getPositions(): PortfolioPosition[] {
    return db.select().from(portfolioPositions)
      .orderBy(desc(portfolioPositions.marketValue))
      .all();
  }

  getPositionByTicker(ticker: string): PortfolioPosition | undefined {
    return db.select().from(portfolioPositions)
      .where(eq(portfolioPositions.ticker, ticker))
      .get();
  }

  deletePosition(ticker: string): void {
    db.delete(portfolioPositions)
      .where(eq(portfolioPositions.ticker, ticker))
      .run();
  }

  // ========== V2: Strategy Snapshots ==========

  insertSnapshot(snap: InsertSnapshot): StrategySnapshot {
    return db.insert(strategySnapshots).values(snap).returning().get();
  }

  getSnapshots(days = 90): StrategySnapshot[] {
    const cutoff = new Date();
    cutoff.setDate(cutoff.getDate() - days);
    const cutoffStr = cutoff.toISOString().split("T")[0];
    return db.select().from(strategySnapshots)
      .where(gte(strategySnapshots.date, cutoffStr))
      .orderBy(asc(strategySnapshots.date))
      .all();
  }

  getLatestSnapshot(): StrategySnapshot | undefined {
    return db.select().from(strategySnapshots)
      .orderBy(desc(strategySnapshots.date))
      .limit(1)
      .get();
  }

  // ========== V2: Closed Trades ==========

  insertClosedTrade(trade: InsertClosedTrade): ClosedTrade {
    return db.insert(closedTrades).values(trade).returning().get();
  }

  getClosedTrades(limit = 100): ClosedTrade[] {
    return db.select().from(closedTrades)
      .orderBy(desc(closedTrades.exitDate))
      .limit(limit)
      .all();
  }

  // ========== V2: Execution Deviations ==========

  insertExecutionDeviation(dev: InsertDeviation): ExecutionDeviation {
    return db.insert(executionDeviations).values(dev).returning().get();
  }

  getTradeExecutionByOrderId(orderId: string): TradeExecution | undefined {
    return db.select().from(tradeExecutions)
      .where(eq(tradeExecutions.orderId, orderId))
      .get();
  }

  // ========== V2: Schwab Config ==========

  getSchwabConfig(): SchwabConfig | undefined {
    return db.select().from(schwabConfig).get();
  }

  upsertSchwabConfig(config: Partial<SchwabConfig>): void {
    const existing = this.getSchwabConfig();
    if (existing) {
      db.update(schwabConfig)
        .set(config as any)
        .where(eq(schwabConfig.id, existing.id))
        .run();
    } else {
      db.insert(schwabConfig).values({
        ...config,
        createdAt: new Date().toISOString(),
      } as any).run();
    }
  }
}

export const storage = new DatabaseStorage();
