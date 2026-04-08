import { pgTable, serial, text, integer, doublePrecision, boolean } from "drizzle-orm/pg-core";
import { createInsertSchema } from "drizzle-zod";
import { z } from "zod";

// ============================================================
// V3 SCHEMA — Quantitative Research & Execution Platform
// PostgreSQL Edition
// ============================================================

// ==================== CORE DATA LAYER =======================

// Core insider transaction — one row per Form 4 non-derivative transaction line
export const insiderTransactions = pgTable("insider_transactions", {
  id: serial("id").primaryKey(),
  accessionNumber: text("accession_number").notNull(),

  // Filing metadata
  filingDate: text("filing_date").notNull(),           // YYYY-MM-DD
  filingTimestamp: text("filing_timestamp"),            // YYYY-MM-DD HH:mm:ss (EDGAR acceptance time)
  filingMarketState: text("filing_market_state"),       // pre_market, regular, after_hours, weekend

  // Issuer
  issuerCik: text("issuer_cik").notNull(),
  issuerName: text("issuer_name").notNull(),
  issuerTicker: text("issuer_ticker"),

  // Reporting person
  reportingPersonName: text("reporting_person_name").notNull(),
  reportingPersonCik: text("reporting_person_cik"),
  reportingPersonTitle: text("reporting_person_title"),
  isDirector: boolean("is_director").default(false),
  isOfficer: boolean("is_officer").default(false),
  isTenPercentOwner: boolean("is_ten_percent_owner").default(false),

  // Transaction core
  transactionType: text("transaction_type").notNull(),  // P, S, A, M, G, etc.
  transactionDate: text("transaction_date"),            // YYYY-MM-DD
  transactionCode: text("transaction_code"),
  sharesTraded: doublePrecision("shares_traded"),
  pricePerShare: doublePrecision("price_per_share"),
  totalValue: doublePrecision("total_value"),
  sharesOwnedAfter: doublePrecision("shares_owned_after"),
  ownershipType: text("ownership_type"),                // D=Direct, I=Indirect
  ownershipNature: text("ownership_nature"),            // Nature text from Form 4 (By Spouse, By Trust, etc.)
  securityTitle: text("security_title"),

  // V3: Computed filing attributes
  filingLagDays: integer("filing_lag_days"),            // Business days: filingDate - transactionDate
  ownershipChangePct: doublePrecision("ownership_change_pct"),     // sharesTraded / (sharesOwnedAfter - sharesTraded) * 100
  indirectAccountType: text("indirect_account_type"),   // direct, family_spouse, family_child, family_other, trust, retirement, foundation

  // V3: Insider profile at signal time
  isOpportunistic: integer("is_opportunistic"),         // 1=opportunistic, 0=routine, null=insufficient data
  isFirstPurchase: integer("is_first_purchase"),        // 1 if first open-market purchase of this stock by this insider
  insiderHistoricalAlpha30d: doublePrecision("insider_historical_alpha_30d"),
  insiderHistoricalAlpha63d: doublePrecision("insider_historical_alpha_63d"),
  insiderPastTradeCount: integer("insider_past_trade_count"),

  // V3: Company attributes at signal time
  marketCapAtFiling: doublePrecision("market_cap_at_filing"),
  sectorCode: text("sector_code"),                     // GICS sector
  industryName: text("industry_name"),
  securityType: text("security_type"),                 // operating_company, reit, closed_end_fund, spac, bdc, other
  exchangeListing: text("exchange_listing"),            // NYSE, NASDAQ, OTC, etc.
  bookToMarket: doublePrecision("book_to_market"),
  hasActiveBuyback: integer("has_active_buyback"),      // 1 if company has active repurchase program

  // V3: Market context at signal time
  avgDailyVolume: doublePrecision("avg_daily_volume"),             // 30-day ADV
  typicalSpreadPct: doublePrecision("typical_spread_pct"),         // Typical bid-ask spread as % of price
  priceDriftFromTx: doublePrecision("price_drift_from_tx"),       // (filing price - tx price) / tx price
  distanceFrom52wHigh: doublePrecision("distance_from_52w_high"), // current price / 52-week high
  priorReturn30d: doublePrecision("prior_return_30d"),
  priorReturn90d: doublePrecision("prior_return_90d"),
  analystConsensus: text("analyst_consensus"),          // buy, hold, sell, null
  recentDowngrade: integer("recent_downgrade"),         // 1 if downgrade within 30 days
  analystCount: integer("analyst_count"),               // Number of analysts covering
  recentVolumeSpike: doublePrecision("recent_volume_spike"),       // 5d avg volume / 30d avg volume

  createdAt: text("created_at").notNull(),
});

// Purchase signals — clusters of insider buying at the same company
export const purchaseSignals = pgTable("purchase_signals", {
  id: serial("id").primaryKey(),
  issuerCik: text("issuer_cik").notNull(),
  issuerName: text("issuer_name").notNull(),
  issuerTicker: text("issuer_ticker"),
  signalDate: text("signal_date").notNull(),
  
  // V3: Multi-factor composite score (data-derived weights)
  signalScore: doublePrecision("signal_score").notNull(),          // 0-100 composite
  scoreTier: integer("score_tier"),                     // 1-4, based on score distribution percentiles
  factorBreakdown: text("factor_breakdown"),            // JSON: { factorName: { score, rawValue, weight }, ... }
  
  // Cluster attributes
  clusterSize: integer("cluster_size").notNull(),
  totalPurchaseValue: doublePrecision("total_purchase_value").notNull(),
  avgPurchasePrice: doublePrecision("avg_purchase_price"),
  insiderNames: text("insider_names").notNull(),        // JSON array
  insiderTitles: text("insider_titles").notNull(),      // JSON array
  cSuiteCount: integer("c_suite_count").default(0),
  directorCount: integer("director_count").default(0),
  daysSpan: integer("days_span"),
  
  // V3: Comparable signals
  comparableCount: integer("comparable_count"),          // How many similar historical signals exist
  comparableAvgReturn63d: doublePrecision("comparable_avg_return_63d"),
  comparableWinRate: doublePrecision("comparable_win_rate"),

  createdAt: text("created_at").notNull(),
});

// ==================== RESEARCH ENGINE =======================

// Entry prices — multiple reference prices per signal for execution analysis
export const signalEntryPrices = pgTable("signal_entry_prices", {
  id: serial("id").primaryKey(),
  signalId: integer("signal_id").notNull(),             // FK to purchaseSignals
  filingTimestamp: text("filing_timestamp"),
  priorClose: doublePrecision("prior_close"),                     // Last regular-session close before filing
  ahPrice: doublePrecision("ah_price"),                           // After-hours price at filing time
  ahSpreadPct: doublePrecision("ah_spread_pct"),                  // Estimated AH bid-ask spread %
  nextOpen: doublePrecision("next_open"),                         // Next regular-session open
  nextVwap: doublePrecision("next_vwap"),                         // Next regular-session VWAP
  overnightGap: doublePrecision("overnight_gap"),                 // (nextOpen / priorClose) - 1
  ahNetPremium: doublePrecision("ah_net_premium"),                // AH entry advantage net of spread
  insiderTxPrice: doublePrecision("insider_tx_price"),             // The insider's actual transaction price
  createdAt: text("created_at").notNull(),
});

// Daily prices — OHLCV source of truth for all return computations
// Forward returns are computed on demand from this table (professional quant pattern)
export const dailyPrices = pgTable("daily_prices", {
  id: serial("id").primaryKey(),
  ticker: text("ticker").notNull(),
  date: text("date").notNull(),                         // YYYY-MM-DD
  open: doublePrecision("open"),
  high: doublePrecision("high"),
  low: doublePrecision("low"),
  close: doublePrecision("close").notNull(),
  volume: doublePrecision("volume"),
});

// Factor analysis results — computed statistics for each factor slice
export const factorAnalysis = pgTable("factor_analysis", {
  id: serial("id").primaryKey(),
  factorName: text("factor_name").notNull(),            // marketCap, sector, role, etc.
  sliceName: text("slice_name").notNull(),              // micro, small, mid, large (or sector name, etc.)
  horizon: integer("horizon").notNull(),                // Trading days: 1, 2, 3, 5, 10, 21, 63, 126, 252
  sampleSize: integer("sample_size").notNull(),
  meanExcessReturn: doublePrecision("mean_excess_return"),
  medianExcessReturn: doublePrecision("median_excess_return"),
  stdDev: doublePrecision("std_dev"),
  tStat: doublePrecision("t_stat"),
  winRate: doublePrecision("win_rate"),                            // % of signals with positive excess return
  informationRatio: doublePrecision("information_ratio"),          // mean / std
  windowStart: text("window_start"),                   // Analysis window start (YYYY-MM-DD)
  windowEnd: text("window_end"),
  computedAt: text("computed_at").notNull(),
});

// Model weights — data-derived scoring model weights
export const modelWeights = pgTable("model_weights", {
  id: serial("id").primaryKey(),
  factorName: text("factor_name").notNull().unique(),
  dataWeight: doublePrecision("data_weight"),                     // Weight derived from information ratios
  priorWeight: doublePrecision("prior_weight"),                   // Academic prior weight
  effectiveWeight: doublePrecision("effective_weight"),            // Bayesian blend
  sampleSize: integer("sample_size"),                  // N used to derive weight
  informationRatio: doublePrecision("information_ratio"),          // Factor's IR at optimal horizon
  optimalHorizon: integer("optimal_horizon"),           // Trading days where this factor's IR peaks
  confidenceLevel: doublePrecision("confidence_level"),            // 0-1
  lastUpdated: text("last_updated").notNull(),
});

// Insider history — for routine/opportunistic classification & track record
export const insiderHistory = pgTable("insider_history", {
  id: serial("id").primaryKey(),
  reportingPersonCik: text("reporting_person_cik").notNull().unique(),
  reportingPersonName: text("reporting_person_name"),
  tradingMonths: text("trading_months"),                // JSON array of YYYY-MM months with purchases
  totalPurchaseCount: integer("total_purchase_count").default(0),
  isRoutine: integer("is_routine"),                    // 1 = same-month pattern 3+ years, 0 = no
  routineConfidence: doublePrecision("routine_confidence"),        // 0-1
  avgAlpha30d: doublePrecision("avg_alpha_30d"),                  // Average 30d excess return of past purchases
  avgAlpha63d: doublePrecision("avg_alpha_63d"),
  winRate30d: doublePrecision("win_rate_30d"),
  lastUpdated: text("last_updated").notNull(),
});

// ==================== STRATEGY LAYER ========================

// Strategy recommendations — what the data-derived model says to do for each signal
export const strategyRecommendations = pgTable("strategy_recommendations", {
  id: serial("id").primaryKey(),
  signalId: integer("signal_id").notNull(),
  compositeScore: doublePrecision("composite_score"),
  scoreTier: integer("score_tier"),                    // 1-4
  recommendedAction: text("recommended_action"),       // BUY, WATCHLIST, SKIP
  recommendedAllocationPct: doublePrecision("recommended_allocation_pct"),
  idealEntryDate: text("ideal_entry_date"),
  idealEntryPrice: doublePrecision("ideal_entry_price"),
  recommendedHoldDays: integer("recommended_hold_days"),
  trailingStopPct: doublePrecision("trailing_stop_pct"),
  currentStatus: text("current_status").default("active"), // active, exited_time, exited_stop, expired
  theoreticalEntryPrice: doublePrecision("theoretical_entry_price"),
  theoreticalExitPrice: doublePrecision("theoretical_exit_price"),
  theoreticalExitDate: text("theoretical_exit_date"),
  theoreticalPnl: doublePrecision("theoretical_pnl"),
  theoreticalPnlPct: doublePrecision("theoretical_pnl_pct"),
  factorBreakdown: text("factor_breakdown"),            // JSON
  createdAt: text("created_at").notNull(),
});

// ==================== EXECUTION LAYER =======================

// Trade executions — your actual trades (manual or Schwab sync)
export const tradeExecutions = pgTable("trade_executions", {
  id: serial("id").primaryKey(),
  ticker: text("ticker").notNull(),
  companyName: text("company_name"),
  side: text("side").notNull(),                        // BUY, SELL
  quantity: doublePrecision("quantity").notNull(),
  avgPrice: doublePrecision("avg_price").notNull(),
  totalCost: doublePrecision("total_cost").notNull(),
  executionDate: text("execution_date").notNull(),
  executionTime: text("execution_time"),
  isAfterHours: integer("is_after_hours"),             // V3: was this an AH execution?
  signalId: integer("signal_id"),
  signalScore: doublePrecision("signal_score"),
  source: text("source").default("manual"),            // manual, schwab_sync, api
  orderId: text("order_id"),
  status: text("status").default("filled"),
  notes: text("notes"),
  createdAt: text("created_at").notNull(),
});

// Execution deviations — gap between your trades and the ideal strategy
export const executionDeviations = pgTable("execution_deviations", {
  id: serial("id").primaryKey(),
  userTradeId: integer("user_trade_id").notNull(),     // FK to tradeExecutions
  signalId: integer("signal_id"),                      // FK to purchaseSignals (null for independent)
  classification: text("classification").notNull(),    // signal_aligned, signal_adjacent, independent, contra_signal
  entryDelayDays: integer("entry_delay_days"),
  entryPriceGapPct: doublePrecision("entry_price_gap_pct"),
  sizingDeviationPct: doublePrecision("sizing_deviation_pct"),    // (actual - recommended) / recommended
  holdDeviationDays: integer("hold_deviation_days"),   // actual - recommended (negative = early exit)
  exitType: text("exit_type"),                         // time, stop, discretionary, refresh
  pnlDifference: doublePrecision("pnl_difference"),               // your P&L minus strategy P&L
  alphaCost: doublePrecision("alpha_cost"),                       // alpha left on table (or added by discretion)
  createdAt: text("created_at").notNull(),
});

// Portfolio positions — current holdings
export const portfolioPositions = pgTable("portfolio_positions", {
  id: serial("id").primaryKey(),
  ticker: text("ticker").notNull().unique(),
  companyName: text("company_name"),
  quantity: doublePrecision("quantity").notNull(),
  avgCostBasis: doublePrecision("avg_cost_basis").notNull(),
  currentPrice: doublePrecision("current_price"),
  marketValue: doublePrecision("market_value"),
  unrealizedPnl: doublePrecision("unrealized_pnl"),
  unrealizedPnlPct: doublePrecision("unrealized_pnl_pct"),
  dayChange: doublePrecision("day_change"),
  dayChangePct: doublePrecision("day_change_pct"),
  // V3: Signal relationship
  signalClassification: text("signal_classification"), // signal_aligned, signal_adjacent, independent
  signalId: integer("signal_id"),
  signalScoreAtEntry: doublePrecision("signal_score_at_entry"),
  recommendedHoldDays: integer("recommended_hold_days"),
  recommendedExitDate: text("recommended_exit_date"),
  entryDate: text("entry_date"),
  holdingDays: integer("holding_days"),
  lastSyncedAt: text("last_synced_at"),
  source: text("source").default("manual"),
  createdAt: text("created_at").notNull(),
});

// Closed trades — for P&L tracking with signal attribution
export const closedTrades = pgTable("closed_trades", {
  id: serial("id").primaryKey(),
  ticker: text("ticker").notNull(),
  companyName: text("company_name"),
  entryDate: text("entry_date").notNull(),
  exitDate: text("exit_date").notNull(),
  entryPrice: doublePrecision("entry_price").notNull(),
  exitPrice: doublePrecision("exit_price").notNull(),
  quantity: doublePrecision("quantity").notNull(),
  realizedPnl: doublePrecision("realized_pnl").notNull(),
  realizedPnlPct: doublePrecision("realized_pnl_pct").notNull(),
  holdingDays: integer("holding_days"),
  // V3: Signal attribution
  signalClassification: text("signal_classification"),
  signalId: integer("signal_id"),
  signalScoreAtEntry: doublePrecision("signal_score_at_entry"),
  exitType: text("exit_type"),                         // time, stop, discretionary, refresh
  theoreticalPnlPct: doublePrecision("theoretical_pnl_pct"),      // What strategy would have made
  pnlDeviation: doublePrecision("pnl_deviation"),                 // Your P&L - strategy P&L
  createdAt: text("created_at").notNull(),
});

// Strategy snapshots — daily NAV & metrics for three tracks
export const strategySnapshots = pgTable("strategy_snapshots", {
  id: serial("id").primaryKey(),
  date: text("date").notNull(),

  // Model portfolio (ideal strategy)
  strategyNav: doublePrecision("strategy_nav"),
  strategyDailyReturn: doublePrecision("strategy_daily_return"),
  strategyCumulativeReturn: doublePrecision("strategy_cumulative_return"),

  // Your actual portfolio
  portfolioValue: doublePrecision("portfolio_value"),
  cashBalance: doublePrecision("cash_balance").default(0),
  totalNav: doublePrecision("total_nav"),
  dailyReturn: doublePrecision("daily_return"),
  cumulativeReturn: doublePrecision("cumulative_return"),

  // Benchmark (SPY)
  benchmarkReturn: doublePrecision("benchmark_return"),
  benchmarkCumulative: doublePrecision("benchmark_cumulative"),

  // Derived
  alphaVsBenchmark: doublePrecision("alpha_vs_benchmark"),         // Your return - SPY
  alphaVsStrategy: doublePrecision("alpha_vs_strategy"),           // Your return - strategy
  deviationCost: doublePrecision("deviation_cost"),                // Strategy return - your return

  // Risk metrics (rolling)
  openPositions: integer("open_positions").default(0),
  sharpeRatio: doublePrecision("sharpe_ratio"),
  sortinoRatio: doublePrecision("sortino_ratio"),
  maxDrawdown: doublePrecision("max_drawdown"),
  currentDrawdown: doublePrecision("current_drawdown"),
  strategySharpe: doublePrecision("strategy_sharpe"),
  strategySortino: doublePrecision("strategy_sortino"),

  createdAt: text("created_at").notNull(),
});

// ==================== INFRASTRUCTURE ========================

// Polling state
export const pollingState = pgTable("polling_state", {
  id: serial("id").primaryKey(),
  lastPolledAt: text("last_polled_at").notNull(),
  lastAccessionNumber: text("last_accession_number"),
  totalFilingsProcessed: integer("total_filings_processed").default(0),
  status: text("status").default("active"),
});

// Schwab API configuration
export const schwabConfig = pgTable("schwab_config", {
  id: serial("id").primaryKey(),
  appKey: text("app_key"),
  appSecret: text("app_secret"),
  accessToken: text("access_token"),
  refreshToken: text("refresh_token"),
  tokenExpiresAt: text("token_expires_at"),
  accountHash: text("account_hash"),
  accountNumber: text("account_number"),
  isConnected: boolean("is_connected").default(false),
  lastSyncAt: text("last_sync_at"),
  status: text("status").default("disconnected"),
  createdAt: text("created_at").notNull(),
});

// Data pipeline status — track backfill and enrichment progress
export const pipelineStatus = pgTable("pipeline_status", {
  id: serial("id").primaryKey(),
  taskName: text("task_name").notNull(),                // sec_backfill, price_enrichment, factor_analysis, etc.
  status: text("status").default("pending"),            // pending, running, completed, error
  lastRunAt: text("last_run_at"),
  progress: doublePrecision("progress"),                           // 0-100%
  totalItems: integer("total_items"),
  processedItems: integer("processed_items"),
  errorMessage: text("error_message"),
  metadata: text("metadata"),                           // JSON for task-specific data
});

// ==================== INSERT SCHEMAS ========================

export const insertTransactionSchema = createInsertSchema(insiderTransactions).omit({ id: true });
export const insertSignalSchema = createInsertSchema(purchaseSignals).omit({ id: true });
export const insertPollingStateSchema = createInsertSchema(pollingState).omit({ id: true });
export const insertTradeExecutionSchema = createInsertSchema(tradeExecutions).omit({ id: true });
export const insertPositionSchema = createInsertSchema(portfolioPositions).omit({ id: true });
export const insertSnapshotSchema = createInsertSchema(strategySnapshots).omit({ id: true });
export const insertClosedTradeSchema = createInsertSchema(closedTrades).omit({ id: true });
export const insertSchwabConfigSchema = createInsertSchema(schwabConfig).omit({ id: true });
export const insertEntryPriceSchema = createInsertSchema(signalEntryPrices).omit({ id: true });
export const insertDailyPriceSchema = createInsertSchema(dailyPrices).omit({ id: true });
export const insertFactorAnalysisSchema = createInsertSchema(factorAnalysis).omit({ id: true });
export const insertModelWeightSchema = createInsertSchema(modelWeights).omit({ id: true });
export const insertInsiderHistorySchema = createInsertSchema(insiderHistory).omit({ id: true });
export const insertRecommendationSchema = createInsertSchema(strategyRecommendations).omit({ id: true });
export const insertDeviationSchema = createInsertSchema(executionDeviations).omit({ id: true });
export const insertPipelineStatusSchema = createInsertSchema(pipelineStatus).omit({ id: true });

// ==================== TYPES =================================

export type InsertTransaction = z.infer<typeof insertTransactionSchema>;
export type InsiderTransaction = typeof insiderTransactions.$inferSelect;
export type InsertSignal = z.infer<typeof insertSignalSchema>;
export type PurchaseSignal = typeof purchaseSignals.$inferSelect;
export type PollingState = typeof pollingState.$inferSelect;
export type InsertTradeExecution = z.infer<typeof insertTradeExecutionSchema>;
export type TradeExecution = typeof tradeExecutions.$inferSelect;
export type InsertPosition = z.infer<typeof insertPositionSchema>;
export type PortfolioPosition = typeof portfolioPositions.$inferSelect;
export type InsertSnapshot = z.infer<typeof insertSnapshotSchema>;
export type StrategySnapshot = typeof strategySnapshots.$inferSelect;
export type InsertClosedTrade = z.infer<typeof insertClosedTradeSchema>;
export type ClosedTrade = typeof closedTrades.$inferSelect;
export type SchwabConfig = typeof schwabConfig.$inferSelect;
export type SignalEntryPrice = typeof signalEntryPrices.$inferSelect;
export type InsertEntryPrice = z.infer<typeof insertEntryPriceSchema>;
export type DailyPrice = typeof dailyPrices.$inferSelect;
export type InsertDailyPrice = z.infer<typeof insertDailyPriceSchema>;
export type FactorAnalysisRow = typeof factorAnalysis.$inferSelect;
export type InsertFactorAnalysis = z.infer<typeof insertFactorAnalysisSchema>;
export type ModelWeight = typeof modelWeights.$inferSelect;
export type InsertModelWeight = z.infer<typeof insertModelWeightSchema>;
export type InsiderHistoryRecord = typeof insiderHistory.$inferSelect;
export type InsertInsiderHistory = z.infer<typeof insertInsiderHistorySchema>;
export type StrategyRecommendation = typeof strategyRecommendations.$inferSelect;
export type InsertRecommendation = z.infer<typeof insertRecommendationSchema>;
export type ExecutionDeviation = typeof executionDeviations.$inferSelect;
export type InsertDeviation = z.infer<typeof insertDeviationSchema>;
export type PipelineStatusRecord = typeof pipelineStatus.$inferSelect;
