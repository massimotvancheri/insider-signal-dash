import { sqliteTable, text, integer, real } from "drizzle-orm/sqlite-core";
import { createInsertSchema } from "drizzle-zod";
import { z } from "zod";

// ============================================================
// V3 SCHEMA — Quantitative Research & Execution Platform
// ============================================================

// ==================== CORE DATA LAYER =======================

// Core insider transaction — one row per Form 4 non-derivative transaction line
export const insiderTransactions = sqliteTable("insider_transactions", {
  id: integer("id").primaryKey({ autoIncrement: true }),
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
  isDirector: integer("is_director", { mode: "boolean" }).default(false),
  isOfficer: integer("is_officer", { mode: "boolean" }).default(false),
  isTenPercentOwner: integer("is_ten_percent_owner", { mode: "boolean" }).default(false),

  // Transaction core
  transactionType: text("transaction_type").notNull(),  // P, S, A, M, G, etc.
  transactionDate: text("transaction_date"),            // YYYY-MM-DD
  transactionCode: text("transaction_code"),
  sharesTraded: real("shares_traded"),
  pricePerShare: real("price_per_share"),
  totalValue: real("total_value"),
  sharesOwnedAfter: real("shares_owned_after"),
  ownershipType: text("ownership_type"),                // D=Direct, I=Indirect
  ownershipNature: text("ownership_nature"),            // Nature text from Form 4 (By Spouse, By Trust, etc.)
  securityTitle: text("security_title"),

  // V3: Computed filing attributes
  filingLagDays: integer("filing_lag_days"),            // Business days: filingDate - transactionDate
  ownershipChangePct: real("ownership_change_pct"),     // sharesTraded / (sharesOwnedAfter - sharesTraded) * 100
  indirectAccountType: text("indirect_account_type"),   // direct, family_spouse, family_child, family_other, trust, retirement, foundation

  // V3: Insider profile at signal time
  isOpportunistic: integer("is_opportunistic"),         // 1=opportunistic, 0=routine, null=insufficient data
  isFirstPurchase: integer("is_first_purchase"),        // 1 if first open-market purchase of this stock by this insider
  insiderHistoricalAlpha30d: real("insider_historical_alpha_30d"),
  insiderHistoricalAlpha63d: real("insider_historical_alpha_63d"),
  insiderPastTradeCount: integer("insider_past_trade_count"),

  // V3: Company attributes at signal time
  marketCapAtFiling: real("market_cap_at_filing"),
  sectorCode: text("sector_code"),                     // GICS sector
  industryName: text("industry_name"),
  securityType: text("security_type"),                 // operating_company, reit, closed_end_fund, spac, bdc, other
  exchangeListing: text("exchange_listing"),            // NYSE, NASDAQ, OTC, etc.
  bookToMarket: real("book_to_market"),
  hasActiveBuyback: integer("has_active_buyback"),      // 1 if company has active repurchase program

  // V3: Market context at signal time
  avgDailyVolume: real("avg_daily_volume"),             // 30-day ADV
  typicalSpreadPct: real("typical_spread_pct"),         // Typical bid-ask spread as % of price
  priceDriftFromTx: real("price_drift_from_tx"),       // (filing price - tx price) / tx price
  distanceFrom52wHigh: real("distance_from_52w_high"), // current price / 52-week high
  priorReturn30d: real("prior_return_30d"),
  priorReturn90d: real("prior_return_90d"),
  analystConsensus: text("analyst_consensus"),          // buy, hold, sell, null
  recentDowngrade: integer("recent_downgrade"),         // 1 if downgrade within 30 days
  analystCount: integer("analyst_count"),               // Number of analysts covering
  recentVolumeSpike: real("recent_volume_spike"),       // 5d avg volume / 30d avg volume

  createdAt: text("created_at").notNull(),
});

// Purchase signals — clusters of insider buying at the same company
export const purchaseSignals = sqliteTable("purchase_signals", {
  id: integer("id").primaryKey({ autoIncrement: true }),
  issuerCik: text("issuer_cik").notNull(),
  issuerName: text("issuer_name").notNull(),
  issuerTicker: text("issuer_ticker"),
  signalDate: text("signal_date").notNull(),
  
  // V3: Multi-factor composite score (data-derived weights)
  signalScore: real("signal_score").notNull(),          // 0-100 composite
  scoreTier: integer("score_tier"),                     // 1-4, based on score distribution percentiles
  factorBreakdown: text("factor_breakdown"),            // JSON: { factorName: { score, rawValue, weight }, ... }
  
  // Cluster attributes
  clusterSize: integer("cluster_size").notNull(),
  totalPurchaseValue: real("total_purchase_value").notNull(),
  avgPurchasePrice: real("avg_purchase_price"),
  insiderNames: text("insider_names").notNull(),        // JSON array
  insiderTitles: text("insider_titles").notNull(),      // JSON array
  cSuiteCount: integer("c_suite_count").default(0),
  directorCount: integer("director_count").default(0),
  daysSpan: integer("days_span"),
  
  // V3: Comparable signals
  comparableCount: integer("comparable_count"),          // How many similar historical signals exist
  comparableAvgReturn63d: real("comparable_avg_return_63d"),
  comparableWinRate: real("comparable_win_rate"),

  createdAt: text("created_at").notNull(),
});

// ==================== RESEARCH ENGINE =======================

// Entry prices — multiple reference prices per signal for execution analysis
export const signalEntryPrices = sqliteTable("signal_entry_prices", {
  id: integer("id").primaryKey({ autoIncrement: true }),
  signalId: integer("signal_id").notNull(),             // FK to purchaseSignals
  filingTimestamp: text("filing_timestamp"),
  priorClose: real("prior_close"),                     // Last regular-session close before filing
  ahPrice: real("ah_price"),                           // After-hours price at filing time
  ahSpreadPct: real("ah_spread_pct"),                  // Estimated AH bid-ask spread %
  nextOpen: real("next_open"),                         // Next regular-session open
  nextVwap: real("next_vwap"),                         // Next regular-session VWAP
  overnightGap: real("overnight_gap"),                 // (nextOpen / priorClose) - 1
  ahNetPremium: real("ah_net_premium"),                // AH entry advantage net of spread
  insiderTxPrice: real("insider_tx_price"),             // The insider's actual transaction price
  createdAt: text("created_at").notNull(),
});

// Daily forward returns — full daily series for every signal (day 0-252)
export const dailyForwardReturns = sqliteTable("daily_forward_returns", {
  id: integer("id").primaryKey({ autoIncrement: true }),
  signalId: integer("signal_id").notNull(),
  tradingDay: integer("trading_day").notNull(),         // 0, 1, 2, ... 252
  calendarDate: text("calendar_date").notNull(),
  closePrice: real("close_price"),
  benchmarkClose: real("benchmark_close"),              // SPY close
  returnFromNextOpen: real("return_from_next_open"),    // Cumulative return from next-open entry
  returnFromAhEntry: real("return_from_ah_entry"),     // Cumulative return from AH entry
  excessFromNextOpen: real("excess_from_next_open"),   // Return minus benchmark
  excessFromAhEntry: real("excess_from_ah_entry"),
});

// Factor analysis results — computed statistics for each factor slice
export const factorAnalysis = sqliteTable("factor_analysis", {
  id: integer("id").primaryKey({ autoIncrement: true }),
  factorName: text("factor_name").notNull(),            // marketCap, sector, role, etc.
  sliceName: text("slice_name").notNull(),              // micro, small, mid, large (or sector name, etc.)
  horizon: integer("horizon").notNull(),                // Trading days: 1, 2, 3, 5, 10, 21, 63, 126, 252
  sampleSize: integer("sample_size").notNull(),
  meanExcessReturn: real("mean_excess_return"),
  medianExcessReturn: real("median_excess_return"),
  stdDev: real("std_dev"),
  tStat: real("t_stat"),
  winRate: real("win_rate"),                            // % of signals with positive excess return
  informationRatio: real("information_ratio"),          // mean / std
  windowStart: text("window_start"),                   // Analysis window start (YYYY-MM-DD)
  windowEnd: text("window_end"),
  computedAt: text("computed_at").notNull(),
});

// Model weights — data-derived scoring model weights
export const modelWeights = sqliteTable("model_weights", {
  id: integer("id").primaryKey({ autoIncrement: true }),
  factorName: text("factor_name").notNull().unique(),
  dataWeight: real("data_weight"),                     // Weight derived from information ratios
  priorWeight: real("prior_weight"),                   // Academic prior weight
  effectiveWeight: real("effective_weight"),            // Bayesian blend
  sampleSize: integer("sample_size"),                  // N used to derive weight
  informationRatio: real("information_ratio"),          // Factor's IR at optimal horizon
  optimalHorizon: integer("optimal_horizon"),           // Trading days where this factor's IR peaks
  confidenceLevel: real("confidence_level"),            // 0-1
  lastUpdated: text("last_updated").notNull(),
});

// Insider history — for routine/opportunistic classification & track record
export const insiderHistory = sqliteTable("insider_history", {
  id: integer("id").primaryKey({ autoIncrement: true }),
  reportingPersonCik: text("reporting_person_cik").notNull().unique(),
  reportingPersonName: text("reporting_person_name"),
  tradingMonths: text("trading_months"),                // JSON array of YYYY-MM months with purchases
  totalPurchaseCount: integer("total_purchase_count").default(0),
  isRoutine: integer("is_routine"),                    // 1 = same-month pattern 3+ years, 0 = no
  routineConfidence: real("routine_confidence"),        // 0-1
  avgAlpha30d: real("avg_alpha_30d"),                  // Average 30d excess return of past purchases
  avgAlpha63d: real("avg_alpha_63d"),
  winRate30d: real("win_rate_30d"),
  lastUpdated: text("last_updated").notNull(),
});

// ==================== STRATEGY LAYER ========================

// Strategy recommendations — what the data-derived model says to do for each signal
export const strategyRecommendations = sqliteTable("strategy_recommendations", {
  id: integer("id").primaryKey({ autoIncrement: true }),
  signalId: integer("signal_id").notNull(),
  compositeScore: real("composite_score"),
  scoreTier: integer("score_tier"),                    // 1-4
  recommendedAction: text("recommended_action"),       // BUY, WATCHLIST, SKIP
  recommendedAllocationPct: real("recommended_allocation_pct"),
  idealEntryDate: text("ideal_entry_date"),
  idealEntryPrice: real("ideal_entry_price"),
  recommendedHoldDays: integer("recommended_hold_days"),
  trailingStopPct: real("trailing_stop_pct"),
  currentStatus: text("current_status").default("active"), // active, exited_time, exited_stop, expired
  theoreticalEntryPrice: real("theoretical_entry_price"),
  theoreticalExitPrice: real("theoretical_exit_price"),
  theoreticalExitDate: text("theoretical_exit_date"),
  theoreticalPnl: real("theoretical_pnl"),
  theoreticalPnlPct: real("theoretical_pnl_pct"),
  factorBreakdown: text("factor_breakdown"),            // JSON
  createdAt: text("created_at").notNull(),
});

// ==================== EXECUTION LAYER =======================

// Trade executions — your actual trades (manual or Schwab sync)
export const tradeExecutions = sqliteTable("trade_executions", {
  id: integer("id").primaryKey({ autoIncrement: true }),
  ticker: text("ticker").notNull(),
  companyName: text("company_name"),
  side: text("side").notNull(),                        // BUY, SELL
  quantity: real("quantity").notNull(),
  avgPrice: real("avg_price").notNull(),
  totalCost: real("total_cost").notNull(),
  executionDate: text("execution_date").notNull(),
  executionTime: text("execution_time"),
  isAfterHours: integer("is_after_hours"),             // V3: was this an AH execution?
  signalId: integer("signal_id"),
  signalScore: real("signal_score"),
  source: text("source").default("manual"),            // manual, schwab_sync, api
  orderId: text("order_id"),
  status: text("status").default("filled"),
  notes: text("notes"),
  createdAt: text("created_at").notNull(),
});

// Execution deviations — gap between your trades and the ideal strategy
export const executionDeviations = sqliteTable("execution_deviations", {
  id: integer("id").primaryKey({ autoIncrement: true }),
  userTradeId: integer("user_trade_id").notNull(),     // FK to tradeExecutions
  signalId: integer("signal_id"),                      // FK to purchaseSignals (null for independent)
  classification: text("classification").notNull(),    // signal_aligned, signal_adjacent, independent, contra_signal
  entryDelayDays: integer("entry_delay_days"),
  entryPriceGapPct: real("entry_price_gap_pct"),
  sizingDeviationPct: real("sizing_deviation_pct"),    // (actual - recommended) / recommended
  holdDeviationDays: integer("hold_deviation_days"),   // actual - recommended (negative = early exit)
  exitType: text("exit_type"),                         // time, stop, discretionary, refresh
  pnlDifference: real("pnl_difference"),               // your P&L minus strategy P&L
  alphaCost: real("alpha_cost"),                       // alpha left on table (or added by discretion)
  createdAt: text("created_at").notNull(),
});

// Portfolio positions — current holdings
export const portfolioPositions = sqliteTable("portfolio_positions", {
  id: integer("id").primaryKey({ autoIncrement: true }),
  ticker: text("ticker").notNull().unique(),
  companyName: text("company_name"),
  quantity: real("quantity").notNull(),
  avgCostBasis: real("avg_cost_basis").notNull(),
  currentPrice: real("current_price"),
  marketValue: real("market_value"),
  unrealizedPnl: real("unrealized_pnl"),
  unrealizedPnlPct: real("unrealized_pnl_pct"),
  dayChange: real("day_change"),
  dayChangePct: real("day_change_pct"),
  // V3: Signal relationship
  signalClassification: text("signal_classification"), // signal_aligned, signal_adjacent, independent
  signalId: integer("signal_id"),
  signalScoreAtEntry: real("signal_score_at_entry"),
  recommendedHoldDays: integer("recommended_hold_days"),
  recommendedExitDate: text("recommended_exit_date"),
  entryDate: text("entry_date"),
  holdingDays: integer("holding_days"),
  lastSyncedAt: text("last_synced_at"),
  source: text("source").default("manual"),
  createdAt: text("created_at").notNull(),
});

// Closed trades — for P&L tracking with signal attribution
export const closedTrades = sqliteTable("closed_trades", {
  id: integer("id").primaryKey({ autoIncrement: true }),
  ticker: text("ticker").notNull(),
  companyName: text("company_name"),
  entryDate: text("entry_date").notNull(),
  exitDate: text("exit_date").notNull(),
  entryPrice: real("entry_price").notNull(),
  exitPrice: real("exit_price").notNull(),
  quantity: real("quantity").notNull(),
  realizedPnl: real("realized_pnl").notNull(),
  realizedPnlPct: real("realized_pnl_pct").notNull(),
  holdingDays: integer("holding_days"),
  // V3: Signal attribution
  signalClassification: text("signal_classification"),
  signalId: integer("signal_id"),
  signalScoreAtEntry: real("signal_score_at_entry"),
  exitType: text("exit_type"),                         // time, stop, discretionary, refresh
  theoreticalPnlPct: real("theoretical_pnl_pct"),      // What strategy would have made
  pnlDeviation: real("pnl_deviation"),                 // Your P&L - strategy P&L
  createdAt: text("created_at").notNull(),
});

// Strategy snapshots — daily NAV & metrics for three tracks
export const strategySnapshots = sqliteTable("strategy_snapshots", {
  id: integer("id").primaryKey({ autoIncrement: true }),
  date: text("date").notNull(),

  // Model portfolio (ideal strategy)
  strategyNav: real("strategy_nav"),
  strategyDailyReturn: real("strategy_daily_return"),
  strategyCumulativeReturn: real("strategy_cumulative_return"),

  // Your actual portfolio
  portfolioValue: real("portfolio_value"),
  cashBalance: real("cash_balance").default(0),
  totalNav: real("total_nav"),
  dailyReturn: real("daily_return"),
  cumulativeReturn: real("cumulative_return"),

  // Benchmark (SPY)
  benchmarkReturn: real("benchmark_return"),
  benchmarkCumulative: real("benchmark_cumulative"),

  // Derived
  alphaVsBenchmark: real("alpha_vs_benchmark"),         // Your return - SPY
  alphaVsStrategy: real("alpha_vs_strategy"),           // Your return - strategy
  deviationCost: real("deviation_cost"),                // Strategy return - your return

  // Risk metrics (rolling)
  openPositions: integer("open_positions").default(0),
  sharpeRatio: real("sharpe_ratio"),
  sortinoRatio: real("sortino_ratio"),
  maxDrawdown: real("max_drawdown"),
  currentDrawdown: real("current_drawdown"),
  strategySharpe: real("strategy_sharpe"),
  strategySortino: real("strategy_sortino"),

  createdAt: text("created_at").notNull(),
});

// ==================== INFRASTRUCTURE ========================

// Polling state
export const pollingState = sqliteTable("polling_state", {
  id: integer("id").primaryKey({ autoIncrement: true }),
  lastPolledAt: text("last_polled_at").notNull(),
  lastAccessionNumber: text("last_accession_number"),
  totalFilingsProcessed: integer("total_filings_processed").default(0),
  status: text("status").default("active"),
});

// Schwab API configuration
export const schwabConfig = sqliteTable("schwab_config", {
  id: integer("id").primaryKey({ autoIncrement: true }),
  appKey: text("app_key"),
  appSecret: text("app_secret"),
  accessToken: text("access_token"),
  refreshToken: text("refresh_token"),
  tokenExpiresAt: text("token_expires_at"),
  accountHash: text("account_hash"),
  accountNumber: text("account_number"),
  isConnected: integer("is_connected", { mode: "boolean" }).default(false),
  lastSyncAt: text("last_sync_at"),
  status: text("status").default("disconnected"),
  createdAt: text("created_at").notNull(),
});

// Data pipeline status — track backfill and enrichment progress
export const pipelineStatus = sqliteTable("pipeline_status", {
  id: integer("id").primaryKey({ autoIncrement: true }),
  taskName: text("task_name").notNull(),                // sec_backfill, price_enrichment, factor_analysis, etc.
  status: text("status").default("pending"),            // pending, running, completed, error
  lastRunAt: text("last_run_at"),
  progress: real("progress"),                           // 0-100%
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
export const insertForwardReturnSchema = createInsertSchema(dailyForwardReturns).omit({ id: true });
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
export type DailyForwardReturn = typeof dailyForwardReturns.$inferSelect;
export type InsertForwardReturn = z.infer<typeof insertForwardReturnSchema>;
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
