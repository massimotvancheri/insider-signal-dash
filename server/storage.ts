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
import { db, pool } from "./db";

export interface IStorage {
  // Transactions
  insertTransaction(tx: InsertTransaction): Promise<InsiderTransaction>;
  getTransactions(limit?: number, offset?: number): Promise<InsiderTransaction[]>;
  getPurchaseTransactions(limit?: number, days?: number): Promise<InsiderTransaction[]>;
  getTransactionByAccession(accessionNumber: string): Promise<InsiderTransaction | undefined>;
  getTransactionCount(): Promise<number>;
  getPurchaseCount(days?: number): Promise<number>;
  getRecentPurchaseVolume(days: number): Promise<number>;
  getTopBuyers(days?: number, limit?: number): Promise<{ name: string; title: string; ticker: string; totalValue: number; count: number }[]>;
  getDailyPurchaseVolume(days: number): Promise<{ date: string; volume: number; count: number }[]>;
  
  // Signals
  insertSignal(signal: InsertSignal): Promise<PurchaseSignal>;
  getSignals(limit?: number): Promise<PurchaseSignal[]>;
  getTopSignals(limit?: number): Promise<PurchaseSignal[]>;
  
  // Polling state
  getPollingState(): Promise<PollingState | undefined>;
  updatePollingState(lastPolledAt: string, lastAccession?: string, totalProcessed?: number): Promise<void>;
  
  // Analytics
  getSectorBreakdown(days?: number): Promise<{ ticker: string; name: string; totalValue: number; count: number }[]>;
  getInsiderTypeBreakdown(days?: number): Promise<{ type: string; count: number; totalValue: number }[]>;
  getClusterBuys(days?: number): Promise<{ ticker: string; name: string; insiderCount: number; totalValue: number; avgPrice: number; dates: string }[]>;

  // V2: Trade Executions
  insertTradeExecution(trade: InsertTradeExecution): Promise<TradeExecution>;
  getTradeExecutions(limit?: number): Promise<TradeExecution[]>;
  getTradeExecutionsByTicker(ticker: string): Promise<TradeExecution[]>;

  // V2: Portfolio Positions
  upsertPosition(pos: InsertPosition): Promise<PortfolioPosition>;
  getPositions(): Promise<PortfolioPosition[]>;
  getPositionByTicker(ticker: string): Promise<PortfolioPosition | undefined>;
  deletePosition(ticker: string): Promise<void>;

  // V2: Strategy Snapshots
  insertSnapshot(snap: InsertSnapshot): Promise<StrategySnapshot>;
  getSnapshots(days?: number): Promise<StrategySnapshot[]>;
  getLatestSnapshot(): Promise<StrategySnapshot | undefined>;

  // V2: Closed Trades
  insertClosedTrade(trade: InsertClosedTrade): Promise<ClosedTrade>;
  getClosedTrades(limit?: number): Promise<ClosedTrade[]>;

  // V2: Execution Deviations
  insertExecutionDeviation(dev: InsertDeviation): Promise<ExecutionDeviation>;
  getTradeExecutionByOrderId(orderId: string): Promise<TradeExecution | undefined>;

  // V2: Schwab Config
  getSchwabConfig(): Promise<SchwabConfig | undefined>;
  upsertSchwabConfig(config: Partial<SchwabConfig>): Promise<void>;
}

export class DatabaseStorage implements IStorage {
  async insertTransaction(tx: InsertTransaction): Promise<InsiderTransaction> {
    const rows = await db.insert(insiderTransactions).values(tx).returning();
    return rows[0];
  }

  async getTransactions(limit = 100, offset = 0): Promise<InsiderTransaction[]> {
    return db.select().from(insiderTransactions)
      .orderBy(desc(insiderTransactions.filingDate))
      .limit(limit)
      .offset(offset);
  }

  async getPurchaseTransactions(limit = 100, days = 30): Promise<InsiderTransaction[]> {
    const cutoff = new Date();
    cutoff.setDate(cutoff.getDate() - days);
    const cutoffStr = cutoff.toISOString().split("T")[0];
    return db.select().from(insiderTransactions)
      .where(and(
        eq(insiderTransactions.transactionType, "P"),
        gte(insiderTransactions.filingDate, cutoffStr)
      ))
      .orderBy(desc(insiderTransactions.filingDate))
      .limit(limit);
  }

  async getTransactionByAccession(accessionNumber: string): Promise<InsiderTransaction | undefined> {
    const rows = await db.select().from(insiderTransactions)
      .where(eq(insiderTransactions.accessionNumber, accessionNumber));
    return rows[0];
  }

  async getTransactionCount(): Promise<number> {
    const result = await db.select({ count: count() }).from(insiderTransactions);
    return result[0]?.count ?? 0;
  }

  async getPurchaseCount(days = 30): Promise<number> {
    const cutoff = new Date();
    cutoff.setDate(cutoff.getDate() - days);
    const cutoffStr = cutoff.toISOString().split("T")[0];
    const result = await db.select({ count: count() }).from(insiderTransactions)
      .where(and(
        eq(insiderTransactions.transactionType, "P"),
        gte(insiderTransactions.filingDate, cutoffStr)
      ));
    return result[0]?.count ?? 0;
  }

  async getRecentPurchaseVolume(days: number): Promise<number> {
    const cutoff = new Date();
    cutoff.setDate(cutoff.getDate() - days);
    const cutoffStr = cutoff.toISOString().split("T")[0];
    // Cap individual transaction values at $500M to filter SEC data outliers
    const result = await db.select({
      total: sql<number>`COALESCE(SUM(CASE WHEN ${insiderTransactions.totalValue} <= 500000000 THEN ${insiderTransactions.totalValue} ELSE 0 END), 0)`
    }).from(insiderTransactions)
      .where(and(
        eq(insiderTransactions.transactionType, "P"),
        gte(insiderTransactions.filingDate, cutoffStr)
      ));
    return result[0]?.total ?? 0;
  }

  async getTopBuyers(days = 30, limit = 10) {
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
      .limit(limit);
  }

  async getDailyPurchaseVolume(days: number) {
    const cutoff = new Date();
    cutoff.setDate(cutoff.getDate() - days);
    const cutoffStr = cutoff.toISOString().split("T")[0];
    return db.select({
      date: insiderTransactions.filingDate,
      volume: sql<number>`COALESCE(SUM(CASE WHEN ${insiderTransactions.totalValue} <= 500000000 THEN ${insiderTransactions.totalValue} ELSE 0 END), 0)`,
      count: count(),
    }).from(insiderTransactions)
      .where(and(
        eq(insiderTransactions.transactionType, "P"),
        gte(insiderTransactions.filingDate, cutoffStr)
      ))
      .groupBy(insiderTransactions.filingDate)
      .orderBy(insiderTransactions.filingDate);
  }

  async insertSignal(signal: InsertSignal): Promise<PurchaseSignal> {
    const rows = await db.insert(purchaseSignals).values(signal).returning();
    return rows[0];
  }

  async getSignals(limit = 50): Promise<PurchaseSignal[]> {
    return db.select().from(purchaseSignals)
      .orderBy(desc(purchaseSignals.signalDate))
      .limit(limit);
  }

  async getTopSignals(limit = 20): Promise<PurchaseSignal[]> {
    return db.select().from(purchaseSignals)
      .orderBy(desc(purchaseSignals.signalScore))
      .limit(limit);
  }

  async getPollingState(): Promise<PollingState | undefined> {
    const rows = await db.select().from(pollingState);
    return rows[0];
  }

  async updatePollingState(lastPolledAt: string, lastAccession?: string, totalProcessed?: number): Promise<void> {
    const existing = await this.getPollingState();
    if (existing) {
      await db.update(pollingState)
        .set({
          lastPolledAt,
          ...(lastAccession ? { lastAccessionNumber: lastAccession } : {}),
          ...(totalProcessed !== undefined ? { totalFilingsProcessed: totalProcessed } : {}),
        })
        .where(eq(pollingState.id, existing.id));
    } else {
      await db.insert(pollingState).values({
        lastPolledAt,
        lastAccessionNumber: lastAccession ?? null,
        totalFilingsProcessed: totalProcessed ?? 0,
        status: "active",
      });
    }
  }

  async getSectorBreakdown(days = 30) {
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
      .limit(20);
  }

  async getInsiderTypeBreakdown(days = 30) {
    const cutoff = new Date();
    cutoff.setDate(cutoff.getDate() - days);
    const cutoffStr = cutoff.toISOString().split("T")[0];
    return db.select({
      type: sql<string>`
        CASE 
          WHEN ${insiderTransactions.isOfficer} = true THEN 'Officer'
          WHEN ${insiderTransactions.isDirector} = true THEN 'Director'
          WHEN ${insiderTransactions.isTenPercentOwner} = true THEN '10%+ Owner'
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
        WHEN ${insiderTransactions.isOfficer} = true THEN 'Officer'
        WHEN ${insiderTransactions.isDirector} = true THEN 'Director'
        WHEN ${insiderTransactions.isTenPercentOwner} = true THEN '10%+ Owner'
        ELSE 'Other'
      END`);
  }

  async getClusterBuys(days = 30) {
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
      .limit(20);
  }

  // ========== V2: Trade Executions ==========

  async insertTradeExecution(trade: InsertTradeExecution): Promise<TradeExecution> {
    const rows = await db.insert(tradeExecutions).values(trade).returning();
    return rows[0];
  }

  async getTradeExecutions(limit = 100): Promise<TradeExecution[]> {
    return db.select().from(tradeExecutions)
      .orderBy(desc(tradeExecutions.executionDate))
      .limit(limit);
  }

  async getTradeExecutionsByTicker(ticker: string): Promise<TradeExecution[]> {
    return db.select().from(tradeExecutions)
      .where(eq(tradeExecutions.ticker, ticker))
      .orderBy(desc(tradeExecutions.executionDate));
  }

  // ========== V2: Portfolio Positions ==========

  async upsertPosition(pos: InsertPosition): Promise<PortfolioPosition> {
    const existing = await this.getPositionByTicker(pos.ticker);
    if (existing) {
      await db.update(portfolioPositions)
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
        .where(eq(portfolioPositions.ticker, pos.ticker));
      return (await this.getPositionByTicker(pos.ticker))!;
    }
    const rows = await db.insert(portfolioPositions).values(pos).returning();
    return rows[0];
  }

  async getPositions(): Promise<PortfolioPosition[]> {
    return db.select().from(portfolioPositions)
      .orderBy(desc(portfolioPositions.marketValue));
  }

  async getPositionByTicker(ticker: string): Promise<PortfolioPosition | undefined> {
    const rows = await db.select().from(portfolioPositions)
      .where(eq(portfolioPositions.ticker, ticker));
    return rows[0];
  }

  async deletePosition(ticker: string): Promise<void> {
    await db.delete(portfolioPositions)
      .where(eq(portfolioPositions.ticker, ticker));
  }

  // ========== V2: Strategy Snapshots ==========

  async insertSnapshot(snap: InsertSnapshot): Promise<StrategySnapshot> {
    const rows = await db.insert(strategySnapshots).values(snap).returning();
    return rows[0];
  }

  async getSnapshots(days = 90): Promise<StrategySnapshot[]> {
    const cutoff = new Date();
    cutoff.setDate(cutoff.getDate() - days);
    const cutoffStr = cutoff.toISOString().split("T")[0];
    return db.select().from(strategySnapshots)
      .where(gte(strategySnapshots.date, cutoffStr))
      .orderBy(asc(strategySnapshots.date));
  }

  async getLatestSnapshot(): Promise<StrategySnapshot | undefined> {
    const rows = await db.select().from(strategySnapshots)
      .orderBy(desc(strategySnapshots.date))
      .limit(1);
    return rows[0];
  }

  // ========== V2: Closed Trades ==========

  async insertClosedTrade(trade: InsertClosedTrade): Promise<ClosedTrade> {
    const rows = await db.insert(closedTrades).values(trade).returning();
    return rows[0];
  }

  async getClosedTrades(limit = 100): Promise<ClosedTrade[]> {
    return db.select().from(closedTrades)
      .orderBy(desc(closedTrades.exitDate))
      .limit(limit);
  }

  // ========== V2: Execution Deviations ==========

  async insertExecutionDeviation(dev: InsertDeviation): Promise<ExecutionDeviation> {
    const rows = await db.insert(executionDeviations).values(dev).returning();
    return rows[0];
  }

  async getTradeExecutionByOrderId(orderId: string): Promise<TradeExecution | undefined> {
    const rows = await db.select().from(tradeExecutions)
      .where(eq(tradeExecutions.orderId, orderId));
    return rows[0];
  }

  // ========== V2: Schwab Config ==========

  async getSchwabConfig(): Promise<SchwabConfig | undefined> {
    const rows = await db.select().from(schwabConfig);
    return rows[0];
  }

  async upsertSchwabConfig(config: Partial<SchwabConfig>): Promise<void> {
    const existing = await this.getSchwabConfig();
    if (existing) {
      await db.update(schwabConfig)
        .set(config as any)
        .where(eq(schwabConfig.id, existing.id));
    } else {
      await db.insert(schwabConfig).values({
        ...config,
        createdAt: new Date().toISOString(),
      } as any);
    }
  }
}

export const storage = new DatabaseStorage();
