"use strict";
var __create = Object.create;
var __defProp = Object.defineProperty;
var __getOwnPropDesc = Object.getOwnPropertyDescriptor;
var __getOwnPropNames = Object.getOwnPropertyNames;
var __getProtoOf = Object.getPrototypeOf;
var __hasOwnProp = Object.prototype.hasOwnProperty;
var __esm = (fn, res) => function __init() {
  return fn && (res = (0, fn[__getOwnPropNames(fn)[0]])(fn = 0)), res;
};
var __export = (target, all) => {
  for (var name in all)
    __defProp(target, name, { get: all[name], enumerable: true });
};
var __copyProps = (to, from, except, desc4) => {
  if (from && typeof from === "object" || typeof from === "function") {
    for (let key of __getOwnPropNames(from))
      if (!__hasOwnProp.call(to, key) && key !== except)
        __defProp(to, key, { get: () => from[key], enumerable: !(desc4 = __getOwnPropDesc(from, key)) || desc4.enumerable });
  }
  return to;
};
var __toESM = (mod, isNodeMode, target) => (target = mod != null ? __create(__getProtoOf(mod)) : {}, __copyProps(
  // If the importer is in node compatibility mode or this is not an ESM
  // file that has been converted to a CommonJS file using a Babel-
  // compatible transform (i.e. "__esModule" has not been set), then set
  // "default" to the CommonJS "module.exports" for node compatibility.
  isNodeMode || !mod || !mod.__esModule ? __defProp(target, "default", { value: mod, enumerable: true }) : target,
  mod
));
var __toCommonJS = (mod) => __copyProps(__defProp({}, "__esModule", { value: true }), mod);

// vite.config.ts
var import_vite, import_plugin_react, import_path2, import_meta, vite_config_default;
var init_vite_config = __esm({
  "vite.config.ts"() {
    "use strict";
    import_vite = require("vite");
    import_plugin_react = __toESM(require("@vitejs/plugin-react"), 1);
    import_path2 = __toESM(require("path"), 1);
    import_meta = {};
    vite_config_default = (0, import_vite.defineConfig)({
      plugins: [(0, import_plugin_react.default)()],
      resolve: {
        alias: {
          "@": import_path2.default.resolve(import_meta.dirname, "client", "src"),
          "@shared": import_path2.default.resolve(import_meta.dirname, "shared"),
          "@assets": import_path2.default.resolve(import_meta.dirname, "attached_assets")
        }
      },
      root: import_path2.default.resolve(import_meta.dirname, "client"),
      base: "./",
      build: {
        outDir: import_path2.default.resolve(import_meta.dirname, "dist/public"),
        emptyOutDir: true
      },
      server: {
        fs: {
          strict: true,
          deny: ["**/.*"]
        }
      }
    });
  }
});

// server/vite.ts
var vite_exports = {};
__export(vite_exports, {
  setupVite: () => setupVite
});
async function setupVite(server, app2) {
  const serverOptions = {
    middlewareMode: true,
    hmr: { server, path: "/vite-hmr" },
    allowedHosts: true
  };
  const vite = await (0, import_vite2.createServer)({
    ...vite_config_default,
    configFile: false,
    customLogger: {
      ...viteLogger,
      error: (msg, options) => {
        viteLogger.error(msg, options);
        process.exit(1);
      }
    },
    server: serverOptions,
    appType: "custom"
  });
  app2.use(vite.middlewares);
  app2.use("/{*path}", async (req, res, next) => {
    const url = req.originalUrl;
    try {
      const clientTemplate = import_path3.default.resolve(
        import_meta2.dirname,
        "..",
        "client",
        "index.html"
      );
      let template = await import_fs3.default.promises.readFile(clientTemplate, "utf-8");
      template = template.replace(
        `src="/src/main.tsx"`,
        `src="/src/main.tsx?v=${(0, import_nanoid.nanoid)()}"`
      );
      const page = await vite.transformIndexHtml(url, template);
      res.status(200).set({ "Content-Type": "text/html" }).end(page);
    } catch (e) {
      vite.ssrFixStacktrace(e);
      next(e);
    }
  });
}
var import_vite2, import_fs3, import_path3, import_nanoid, import_meta2, viteLogger;
var init_vite = __esm({
  "server/vite.ts"() {
    "use strict";
    import_vite2 = require("vite");
    init_vite_config();
    import_fs3 = __toESM(require("fs"), 1);
    import_path3 = __toESM(require("path"), 1);
    import_nanoid = require("nanoid");
    import_meta2 = {};
    viteLogger = (0, import_vite2.createLogger)();
  }
});

// server/index.ts
var index_exports = {};
__export(index_exports, {
  log: () => log
});
module.exports = __toCommonJS(index_exports);
var import_express2 = __toESM(require("express"), 1);

// shared/schema.ts
var import_sqlite_core = require("drizzle-orm/sqlite-core");
var import_drizzle_zod = require("drizzle-zod");
var insiderTransactions = (0, import_sqlite_core.sqliteTable)("insider_transactions", {
  id: (0, import_sqlite_core.integer)("id").primaryKey({ autoIncrement: true }),
  accessionNumber: (0, import_sqlite_core.text)("accession_number").notNull(),
  // Filing metadata
  filingDate: (0, import_sqlite_core.text)("filing_date").notNull(),
  // YYYY-MM-DD
  filingTimestamp: (0, import_sqlite_core.text)("filing_timestamp"),
  // YYYY-MM-DD HH:mm:ss (EDGAR acceptance time)
  filingMarketState: (0, import_sqlite_core.text)("filing_market_state"),
  // pre_market, regular, after_hours, weekend
  // Issuer
  issuerCik: (0, import_sqlite_core.text)("issuer_cik").notNull(),
  issuerName: (0, import_sqlite_core.text)("issuer_name").notNull(),
  issuerTicker: (0, import_sqlite_core.text)("issuer_ticker"),
  // Reporting person
  reportingPersonName: (0, import_sqlite_core.text)("reporting_person_name").notNull(),
  reportingPersonCik: (0, import_sqlite_core.text)("reporting_person_cik"),
  reportingPersonTitle: (0, import_sqlite_core.text)("reporting_person_title"),
  isDirector: (0, import_sqlite_core.integer)("is_director", { mode: "boolean" }).default(false),
  isOfficer: (0, import_sqlite_core.integer)("is_officer", { mode: "boolean" }).default(false),
  isTenPercentOwner: (0, import_sqlite_core.integer)("is_ten_percent_owner", { mode: "boolean" }).default(false),
  // Transaction core
  transactionType: (0, import_sqlite_core.text)("transaction_type").notNull(),
  // P, S, A, M, G, etc.
  transactionDate: (0, import_sqlite_core.text)("transaction_date"),
  // YYYY-MM-DD
  transactionCode: (0, import_sqlite_core.text)("transaction_code"),
  sharesTraded: (0, import_sqlite_core.real)("shares_traded"),
  pricePerShare: (0, import_sqlite_core.real)("price_per_share"),
  totalValue: (0, import_sqlite_core.real)("total_value"),
  sharesOwnedAfter: (0, import_sqlite_core.real)("shares_owned_after"),
  ownershipType: (0, import_sqlite_core.text)("ownership_type"),
  // D=Direct, I=Indirect
  ownershipNature: (0, import_sqlite_core.text)("ownership_nature"),
  // Nature text from Form 4 (By Spouse, By Trust, etc.)
  securityTitle: (0, import_sqlite_core.text)("security_title"),
  // V3: Computed filing attributes
  filingLagDays: (0, import_sqlite_core.integer)("filing_lag_days"),
  // Business days: filingDate - transactionDate
  ownershipChangePct: (0, import_sqlite_core.real)("ownership_change_pct"),
  // sharesTraded / (sharesOwnedAfter - sharesTraded) * 100
  indirectAccountType: (0, import_sqlite_core.text)("indirect_account_type"),
  // direct, family_spouse, family_child, family_other, trust, retirement, foundation
  // V3: Insider profile at signal time
  isOpportunistic: (0, import_sqlite_core.integer)("is_opportunistic"),
  // 1=opportunistic, 0=routine, null=insufficient data
  isFirstPurchase: (0, import_sqlite_core.integer)("is_first_purchase"),
  // 1 if first open-market purchase of this stock by this insider
  insiderHistoricalAlpha30d: (0, import_sqlite_core.real)("insider_historical_alpha_30d"),
  insiderHistoricalAlpha63d: (0, import_sqlite_core.real)("insider_historical_alpha_63d"),
  insiderPastTradeCount: (0, import_sqlite_core.integer)("insider_past_trade_count"),
  // V3: Company attributes at signal time
  marketCapAtFiling: (0, import_sqlite_core.real)("market_cap_at_filing"),
  sectorCode: (0, import_sqlite_core.text)("sector_code"),
  // GICS sector
  industryName: (0, import_sqlite_core.text)("industry_name"),
  securityType: (0, import_sqlite_core.text)("security_type"),
  // operating_company, reit, closed_end_fund, spac, bdc, other
  exchangeListing: (0, import_sqlite_core.text)("exchange_listing"),
  // NYSE, NASDAQ, OTC, etc.
  bookToMarket: (0, import_sqlite_core.real)("book_to_market"),
  hasActiveBuyback: (0, import_sqlite_core.integer)("has_active_buyback"),
  // 1 if company has active repurchase program
  // V3: Market context at signal time
  avgDailyVolume: (0, import_sqlite_core.real)("avg_daily_volume"),
  // 30-day ADV
  typicalSpreadPct: (0, import_sqlite_core.real)("typical_spread_pct"),
  // Typical bid-ask spread as % of price
  priceDriftFromTx: (0, import_sqlite_core.real)("price_drift_from_tx"),
  // (filing price - tx price) / tx price
  distanceFrom52wHigh: (0, import_sqlite_core.real)("distance_from_52w_high"),
  // current price / 52-week high
  priorReturn30d: (0, import_sqlite_core.real)("prior_return_30d"),
  priorReturn90d: (0, import_sqlite_core.real)("prior_return_90d"),
  analystConsensus: (0, import_sqlite_core.text)("analyst_consensus"),
  // buy, hold, sell, null
  recentDowngrade: (0, import_sqlite_core.integer)("recent_downgrade"),
  // 1 if downgrade within 30 days
  analystCount: (0, import_sqlite_core.integer)("analyst_count"),
  // Number of analysts covering
  recentVolumeSpike: (0, import_sqlite_core.real)("recent_volume_spike"),
  // 5d avg volume / 30d avg volume
  createdAt: (0, import_sqlite_core.text)("created_at").notNull()
});
var purchaseSignals = (0, import_sqlite_core.sqliteTable)("purchase_signals", {
  id: (0, import_sqlite_core.integer)("id").primaryKey({ autoIncrement: true }),
  issuerCik: (0, import_sqlite_core.text)("issuer_cik").notNull(),
  issuerName: (0, import_sqlite_core.text)("issuer_name").notNull(),
  issuerTicker: (0, import_sqlite_core.text)("issuer_ticker"),
  signalDate: (0, import_sqlite_core.text)("signal_date").notNull(),
  // V3: Multi-factor composite score (data-derived weights)
  signalScore: (0, import_sqlite_core.real)("signal_score").notNull(),
  // 0-100 composite
  scoreTier: (0, import_sqlite_core.integer)("score_tier"),
  // 1-4, based on score distribution percentiles
  factorBreakdown: (0, import_sqlite_core.text)("factor_breakdown"),
  // JSON: { factorName: { score, rawValue, weight }, ... }
  // Cluster attributes
  clusterSize: (0, import_sqlite_core.integer)("cluster_size").notNull(),
  totalPurchaseValue: (0, import_sqlite_core.real)("total_purchase_value").notNull(),
  avgPurchasePrice: (0, import_sqlite_core.real)("avg_purchase_price"),
  insiderNames: (0, import_sqlite_core.text)("insider_names").notNull(),
  // JSON array
  insiderTitles: (0, import_sqlite_core.text)("insider_titles").notNull(),
  // JSON array
  cSuiteCount: (0, import_sqlite_core.integer)("c_suite_count").default(0),
  directorCount: (0, import_sqlite_core.integer)("director_count").default(0),
  daysSpan: (0, import_sqlite_core.integer)("days_span"),
  // V3: Comparable signals
  comparableCount: (0, import_sqlite_core.integer)("comparable_count"),
  // How many similar historical signals exist
  comparableAvgReturn63d: (0, import_sqlite_core.real)("comparable_avg_return_63d"),
  comparableWinRate: (0, import_sqlite_core.real)("comparable_win_rate"),
  createdAt: (0, import_sqlite_core.text)("created_at").notNull()
});
var signalEntryPrices = (0, import_sqlite_core.sqliteTable)("signal_entry_prices", {
  id: (0, import_sqlite_core.integer)("id").primaryKey({ autoIncrement: true }),
  signalId: (0, import_sqlite_core.integer)("signal_id").notNull(),
  // FK to purchaseSignals
  filingTimestamp: (0, import_sqlite_core.text)("filing_timestamp"),
  priorClose: (0, import_sqlite_core.real)("prior_close"),
  // Last regular-session close before filing
  ahPrice: (0, import_sqlite_core.real)("ah_price"),
  // After-hours price at filing time
  ahSpreadPct: (0, import_sqlite_core.real)("ah_spread_pct"),
  // Estimated AH bid-ask spread %
  nextOpen: (0, import_sqlite_core.real)("next_open"),
  // Next regular-session open
  nextVwap: (0, import_sqlite_core.real)("next_vwap"),
  // Next regular-session VWAP
  overnightGap: (0, import_sqlite_core.real)("overnight_gap"),
  // (nextOpen / priorClose) - 1
  ahNetPremium: (0, import_sqlite_core.real)("ah_net_premium"),
  // AH entry advantage net of spread
  insiderTxPrice: (0, import_sqlite_core.real)("insider_tx_price"),
  // The insider's actual transaction price
  createdAt: (0, import_sqlite_core.text)("created_at").notNull()
});
var dailyForwardReturns = (0, import_sqlite_core.sqliteTable)("daily_forward_returns", {
  id: (0, import_sqlite_core.integer)("id").primaryKey({ autoIncrement: true }),
  signalId: (0, import_sqlite_core.integer)("signal_id").notNull(),
  tradingDay: (0, import_sqlite_core.integer)("trading_day").notNull(),
  // 0, 1, 2, ... 252
  calendarDate: (0, import_sqlite_core.text)("calendar_date").notNull(),
  closePrice: (0, import_sqlite_core.real)("close_price"),
  benchmarkClose: (0, import_sqlite_core.real)("benchmark_close"),
  // SPY close
  returnFromNextOpen: (0, import_sqlite_core.real)("return_from_next_open"),
  // Cumulative return from next-open entry
  returnFromAhEntry: (0, import_sqlite_core.real)("return_from_ah_entry"),
  // Cumulative return from AH entry
  excessFromNextOpen: (0, import_sqlite_core.real)("excess_from_next_open"),
  // Return minus benchmark
  excessFromAhEntry: (0, import_sqlite_core.real)("excess_from_ah_entry")
});
var factorAnalysis = (0, import_sqlite_core.sqliteTable)("factor_analysis", {
  id: (0, import_sqlite_core.integer)("id").primaryKey({ autoIncrement: true }),
  factorName: (0, import_sqlite_core.text)("factor_name").notNull(),
  // marketCap, sector, role, etc.
  sliceName: (0, import_sqlite_core.text)("slice_name").notNull(),
  // micro, small, mid, large (or sector name, etc.)
  horizon: (0, import_sqlite_core.integer)("horizon").notNull(),
  // Trading days: 1, 2, 3, 5, 10, 21, 63, 126, 252
  sampleSize: (0, import_sqlite_core.integer)("sample_size").notNull(),
  meanExcessReturn: (0, import_sqlite_core.real)("mean_excess_return"),
  medianExcessReturn: (0, import_sqlite_core.real)("median_excess_return"),
  stdDev: (0, import_sqlite_core.real)("std_dev"),
  tStat: (0, import_sqlite_core.real)("t_stat"),
  winRate: (0, import_sqlite_core.real)("win_rate"),
  // % of signals with positive excess return
  informationRatio: (0, import_sqlite_core.real)("information_ratio"),
  // mean / std
  windowStart: (0, import_sqlite_core.text)("window_start"),
  // Analysis window start (YYYY-MM-DD)
  windowEnd: (0, import_sqlite_core.text)("window_end"),
  computedAt: (0, import_sqlite_core.text)("computed_at").notNull()
});
var modelWeights = (0, import_sqlite_core.sqliteTable)("model_weights", {
  id: (0, import_sqlite_core.integer)("id").primaryKey({ autoIncrement: true }),
  factorName: (0, import_sqlite_core.text)("factor_name").notNull().unique(),
  dataWeight: (0, import_sqlite_core.real)("data_weight"),
  // Weight derived from information ratios
  priorWeight: (0, import_sqlite_core.real)("prior_weight"),
  // Academic prior weight
  effectiveWeight: (0, import_sqlite_core.real)("effective_weight"),
  // Bayesian blend
  sampleSize: (0, import_sqlite_core.integer)("sample_size"),
  // N used to derive weight
  informationRatio: (0, import_sqlite_core.real)("information_ratio"),
  // Factor's IR at optimal horizon
  optimalHorizon: (0, import_sqlite_core.integer)("optimal_horizon"),
  // Trading days where this factor's IR peaks
  confidenceLevel: (0, import_sqlite_core.real)("confidence_level"),
  // 0-1
  lastUpdated: (0, import_sqlite_core.text)("last_updated").notNull()
});
var insiderHistory = (0, import_sqlite_core.sqliteTable)("insider_history", {
  id: (0, import_sqlite_core.integer)("id").primaryKey({ autoIncrement: true }),
  reportingPersonCik: (0, import_sqlite_core.text)("reporting_person_cik").notNull().unique(),
  reportingPersonName: (0, import_sqlite_core.text)("reporting_person_name"),
  tradingMonths: (0, import_sqlite_core.text)("trading_months"),
  // JSON array of YYYY-MM months with purchases
  totalPurchaseCount: (0, import_sqlite_core.integer)("total_purchase_count").default(0),
  isRoutine: (0, import_sqlite_core.integer)("is_routine"),
  // 1 = same-month pattern 3+ years, 0 = no
  routineConfidence: (0, import_sqlite_core.real)("routine_confidence"),
  // 0-1
  avgAlpha30d: (0, import_sqlite_core.real)("avg_alpha_30d"),
  // Average 30d excess return of past purchases
  avgAlpha63d: (0, import_sqlite_core.real)("avg_alpha_63d"),
  winRate30d: (0, import_sqlite_core.real)("win_rate_30d"),
  lastUpdated: (0, import_sqlite_core.text)("last_updated").notNull()
});
var strategyRecommendations = (0, import_sqlite_core.sqliteTable)("strategy_recommendations", {
  id: (0, import_sqlite_core.integer)("id").primaryKey({ autoIncrement: true }),
  signalId: (0, import_sqlite_core.integer)("signal_id").notNull(),
  compositeScore: (0, import_sqlite_core.real)("composite_score"),
  scoreTier: (0, import_sqlite_core.integer)("score_tier"),
  // 1-4
  recommendedAction: (0, import_sqlite_core.text)("recommended_action"),
  // BUY, WATCHLIST, SKIP
  recommendedAllocationPct: (0, import_sqlite_core.real)("recommended_allocation_pct"),
  idealEntryDate: (0, import_sqlite_core.text)("ideal_entry_date"),
  idealEntryPrice: (0, import_sqlite_core.real)("ideal_entry_price"),
  recommendedHoldDays: (0, import_sqlite_core.integer)("recommended_hold_days"),
  trailingStopPct: (0, import_sqlite_core.real)("trailing_stop_pct"),
  currentStatus: (0, import_sqlite_core.text)("current_status").default("active"),
  // active, exited_time, exited_stop, expired
  theoreticalEntryPrice: (0, import_sqlite_core.real)("theoretical_entry_price"),
  theoreticalExitPrice: (0, import_sqlite_core.real)("theoretical_exit_price"),
  theoreticalExitDate: (0, import_sqlite_core.text)("theoretical_exit_date"),
  theoreticalPnl: (0, import_sqlite_core.real)("theoretical_pnl"),
  theoreticalPnlPct: (0, import_sqlite_core.real)("theoretical_pnl_pct"),
  factorBreakdown: (0, import_sqlite_core.text)("factor_breakdown"),
  // JSON
  createdAt: (0, import_sqlite_core.text)("created_at").notNull()
});
var tradeExecutions = (0, import_sqlite_core.sqliteTable)("trade_executions", {
  id: (0, import_sqlite_core.integer)("id").primaryKey({ autoIncrement: true }),
  ticker: (0, import_sqlite_core.text)("ticker").notNull(),
  companyName: (0, import_sqlite_core.text)("company_name"),
  side: (0, import_sqlite_core.text)("side").notNull(),
  // BUY, SELL
  quantity: (0, import_sqlite_core.real)("quantity").notNull(),
  avgPrice: (0, import_sqlite_core.real)("avg_price").notNull(),
  totalCost: (0, import_sqlite_core.real)("total_cost").notNull(),
  executionDate: (0, import_sqlite_core.text)("execution_date").notNull(),
  executionTime: (0, import_sqlite_core.text)("execution_time"),
  isAfterHours: (0, import_sqlite_core.integer)("is_after_hours"),
  // V3: was this an AH execution?
  signalId: (0, import_sqlite_core.integer)("signal_id"),
  signalScore: (0, import_sqlite_core.real)("signal_score"),
  source: (0, import_sqlite_core.text)("source").default("manual"),
  // manual, schwab_sync, api
  orderId: (0, import_sqlite_core.text)("order_id"),
  status: (0, import_sqlite_core.text)("status").default("filled"),
  notes: (0, import_sqlite_core.text)("notes"),
  createdAt: (0, import_sqlite_core.text)("created_at").notNull()
});
var executionDeviations = (0, import_sqlite_core.sqliteTable)("execution_deviations", {
  id: (0, import_sqlite_core.integer)("id").primaryKey({ autoIncrement: true }),
  userTradeId: (0, import_sqlite_core.integer)("user_trade_id").notNull(),
  // FK to tradeExecutions
  signalId: (0, import_sqlite_core.integer)("signal_id"),
  // FK to purchaseSignals (null for independent)
  classification: (0, import_sqlite_core.text)("classification").notNull(),
  // signal_aligned, signal_adjacent, independent, contra_signal
  entryDelayDays: (0, import_sqlite_core.integer)("entry_delay_days"),
  entryPriceGapPct: (0, import_sqlite_core.real)("entry_price_gap_pct"),
  sizingDeviationPct: (0, import_sqlite_core.real)("sizing_deviation_pct"),
  // (actual - recommended) / recommended
  holdDeviationDays: (0, import_sqlite_core.integer)("hold_deviation_days"),
  // actual - recommended (negative = early exit)
  exitType: (0, import_sqlite_core.text)("exit_type"),
  // time, stop, discretionary, refresh
  pnlDifference: (0, import_sqlite_core.real)("pnl_difference"),
  // your P&L minus strategy P&L
  alphaCost: (0, import_sqlite_core.real)("alpha_cost"),
  // alpha left on table (or added by discretion)
  createdAt: (0, import_sqlite_core.text)("created_at").notNull()
});
var portfolioPositions = (0, import_sqlite_core.sqliteTable)("portfolio_positions", {
  id: (0, import_sqlite_core.integer)("id").primaryKey({ autoIncrement: true }),
  ticker: (0, import_sqlite_core.text)("ticker").notNull().unique(),
  companyName: (0, import_sqlite_core.text)("company_name"),
  quantity: (0, import_sqlite_core.real)("quantity").notNull(),
  avgCostBasis: (0, import_sqlite_core.real)("avg_cost_basis").notNull(),
  currentPrice: (0, import_sqlite_core.real)("current_price"),
  marketValue: (0, import_sqlite_core.real)("market_value"),
  unrealizedPnl: (0, import_sqlite_core.real)("unrealized_pnl"),
  unrealizedPnlPct: (0, import_sqlite_core.real)("unrealized_pnl_pct"),
  dayChange: (0, import_sqlite_core.real)("day_change"),
  dayChangePct: (0, import_sqlite_core.real)("day_change_pct"),
  // V3: Signal relationship
  signalClassification: (0, import_sqlite_core.text)("signal_classification"),
  // signal_aligned, signal_adjacent, independent
  signalId: (0, import_sqlite_core.integer)("signal_id"),
  signalScoreAtEntry: (0, import_sqlite_core.real)("signal_score_at_entry"),
  recommendedHoldDays: (0, import_sqlite_core.integer)("recommended_hold_days"),
  recommendedExitDate: (0, import_sqlite_core.text)("recommended_exit_date"),
  entryDate: (0, import_sqlite_core.text)("entry_date"),
  holdingDays: (0, import_sqlite_core.integer)("holding_days"),
  lastSyncedAt: (0, import_sqlite_core.text)("last_synced_at"),
  source: (0, import_sqlite_core.text)("source").default("manual"),
  createdAt: (0, import_sqlite_core.text)("created_at").notNull()
});
var closedTrades = (0, import_sqlite_core.sqliteTable)("closed_trades", {
  id: (0, import_sqlite_core.integer)("id").primaryKey({ autoIncrement: true }),
  ticker: (0, import_sqlite_core.text)("ticker").notNull(),
  companyName: (0, import_sqlite_core.text)("company_name"),
  entryDate: (0, import_sqlite_core.text)("entry_date").notNull(),
  exitDate: (0, import_sqlite_core.text)("exit_date").notNull(),
  entryPrice: (0, import_sqlite_core.real)("entry_price").notNull(),
  exitPrice: (0, import_sqlite_core.real)("exit_price").notNull(),
  quantity: (0, import_sqlite_core.real)("quantity").notNull(),
  realizedPnl: (0, import_sqlite_core.real)("realized_pnl").notNull(),
  realizedPnlPct: (0, import_sqlite_core.real)("realized_pnl_pct").notNull(),
  holdingDays: (0, import_sqlite_core.integer)("holding_days"),
  // V3: Signal attribution
  signalClassification: (0, import_sqlite_core.text)("signal_classification"),
  signalId: (0, import_sqlite_core.integer)("signal_id"),
  signalScoreAtEntry: (0, import_sqlite_core.real)("signal_score_at_entry"),
  exitType: (0, import_sqlite_core.text)("exit_type"),
  // time, stop, discretionary, refresh
  theoreticalPnlPct: (0, import_sqlite_core.real)("theoretical_pnl_pct"),
  // What strategy would have made
  pnlDeviation: (0, import_sqlite_core.real)("pnl_deviation"),
  // Your P&L - strategy P&L
  createdAt: (0, import_sqlite_core.text)("created_at").notNull()
});
var strategySnapshots = (0, import_sqlite_core.sqliteTable)("strategy_snapshots", {
  id: (0, import_sqlite_core.integer)("id").primaryKey({ autoIncrement: true }),
  date: (0, import_sqlite_core.text)("date").notNull(),
  // Model portfolio (ideal strategy)
  strategyNav: (0, import_sqlite_core.real)("strategy_nav"),
  strategyDailyReturn: (0, import_sqlite_core.real)("strategy_daily_return"),
  strategyCumulativeReturn: (0, import_sqlite_core.real)("strategy_cumulative_return"),
  // Your actual portfolio
  portfolioValue: (0, import_sqlite_core.real)("portfolio_value"),
  cashBalance: (0, import_sqlite_core.real)("cash_balance").default(0),
  totalNav: (0, import_sqlite_core.real)("total_nav"),
  dailyReturn: (0, import_sqlite_core.real)("daily_return"),
  cumulativeReturn: (0, import_sqlite_core.real)("cumulative_return"),
  // Benchmark (SPY)
  benchmarkReturn: (0, import_sqlite_core.real)("benchmark_return"),
  benchmarkCumulative: (0, import_sqlite_core.real)("benchmark_cumulative"),
  // Derived
  alphaVsBenchmark: (0, import_sqlite_core.real)("alpha_vs_benchmark"),
  // Your return - SPY
  alphaVsStrategy: (0, import_sqlite_core.real)("alpha_vs_strategy"),
  // Your return - strategy
  deviationCost: (0, import_sqlite_core.real)("deviation_cost"),
  // Strategy return - your return
  // Risk metrics (rolling)
  openPositions: (0, import_sqlite_core.integer)("open_positions").default(0),
  sharpeRatio: (0, import_sqlite_core.real)("sharpe_ratio"),
  sortinoRatio: (0, import_sqlite_core.real)("sortino_ratio"),
  maxDrawdown: (0, import_sqlite_core.real)("max_drawdown"),
  currentDrawdown: (0, import_sqlite_core.real)("current_drawdown"),
  strategySharpe: (0, import_sqlite_core.real)("strategy_sharpe"),
  strategySortino: (0, import_sqlite_core.real)("strategy_sortino"),
  createdAt: (0, import_sqlite_core.text)("created_at").notNull()
});
var pollingState = (0, import_sqlite_core.sqliteTable)("polling_state", {
  id: (0, import_sqlite_core.integer)("id").primaryKey({ autoIncrement: true }),
  lastPolledAt: (0, import_sqlite_core.text)("last_polled_at").notNull(),
  lastAccessionNumber: (0, import_sqlite_core.text)("last_accession_number"),
  totalFilingsProcessed: (0, import_sqlite_core.integer)("total_filings_processed").default(0),
  status: (0, import_sqlite_core.text)("status").default("active")
});
var schwabConfig = (0, import_sqlite_core.sqliteTable)("schwab_config", {
  id: (0, import_sqlite_core.integer)("id").primaryKey({ autoIncrement: true }),
  appKey: (0, import_sqlite_core.text)("app_key"),
  appSecret: (0, import_sqlite_core.text)("app_secret"),
  accessToken: (0, import_sqlite_core.text)("access_token"),
  refreshToken: (0, import_sqlite_core.text)("refresh_token"),
  tokenExpiresAt: (0, import_sqlite_core.text)("token_expires_at"),
  accountHash: (0, import_sqlite_core.text)("account_hash"),
  accountNumber: (0, import_sqlite_core.text)("account_number"),
  isConnected: (0, import_sqlite_core.integer)("is_connected", { mode: "boolean" }).default(false),
  lastSyncAt: (0, import_sqlite_core.text)("last_sync_at"),
  status: (0, import_sqlite_core.text)("status").default("disconnected"),
  createdAt: (0, import_sqlite_core.text)("created_at").notNull()
});
var pipelineStatus = (0, import_sqlite_core.sqliteTable)("pipeline_status", {
  id: (0, import_sqlite_core.integer)("id").primaryKey({ autoIncrement: true }),
  taskName: (0, import_sqlite_core.text)("task_name").notNull(),
  // sec_backfill, price_enrichment, factor_analysis, etc.
  status: (0, import_sqlite_core.text)("status").default("pending"),
  // pending, running, completed, error
  lastRunAt: (0, import_sqlite_core.text)("last_run_at"),
  progress: (0, import_sqlite_core.real)("progress"),
  // 0-100%
  totalItems: (0, import_sqlite_core.integer)("total_items"),
  processedItems: (0, import_sqlite_core.integer)("processed_items"),
  errorMessage: (0, import_sqlite_core.text)("error_message"),
  metadata: (0, import_sqlite_core.text)("metadata")
  // JSON for task-specific data
});
var insertTransactionSchema = (0, import_drizzle_zod.createInsertSchema)(insiderTransactions).omit({ id: true });
var insertSignalSchema = (0, import_drizzle_zod.createInsertSchema)(purchaseSignals).omit({ id: true });
var insertPollingStateSchema = (0, import_drizzle_zod.createInsertSchema)(pollingState).omit({ id: true });
var insertTradeExecutionSchema = (0, import_drizzle_zod.createInsertSchema)(tradeExecutions).omit({ id: true });
var insertPositionSchema = (0, import_drizzle_zod.createInsertSchema)(portfolioPositions).omit({ id: true });
var insertSnapshotSchema = (0, import_drizzle_zod.createInsertSchema)(strategySnapshots).omit({ id: true });
var insertClosedTradeSchema = (0, import_drizzle_zod.createInsertSchema)(closedTrades).omit({ id: true });
var insertSchwabConfigSchema = (0, import_drizzle_zod.createInsertSchema)(schwabConfig).omit({ id: true });
var insertEntryPriceSchema = (0, import_drizzle_zod.createInsertSchema)(signalEntryPrices).omit({ id: true });
var insertForwardReturnSchema = (0, import_drizzle_zod.createInsertSchema)(dailyForwardReturns).omit({ id: true });
var insertFactorAnalysisSchema = (0, import_drizzle_zod.createInsertSchema)(factorAnalysis).omit({ id: true });
var insertModelWeightSchema = (0, import_drizzle_zod.createInsertSchema)(modelWeights).omit({ id: true });
var insertInsiderHistorySchema = (0, import_drizzle_zod.createInsertSchema)(insiderHistory).omit({ id: true });
var insertRecommendationSchema = (0, import_drizzle_zod.createInsertSchema)(strategyRecommendations).omit({ id: true });
var insertDeviationSchema = (0, import_drizzle_zod.createInsertSchema)(executionDeviations).omit({ id: true });
var insertPipelineStatusSchema = (0, import_drizzle_zod.createInsertSchema)(pipelineStatus).omit({ id: true });

// server/storage.ts
var import_drizzle_orm = require("drizzle-orm");

// server/db.ts
var import_better_sqlite3 = require("drizzle-orm/better-sqlite3");
var import_better_sqlite32 = __toESM(require("better-sqlite3"), 1);
var sqlite = new import_better_sqlite32.default("data.db");
sqlite.pragma("journal_mode = WAL");
sqlite.pragma("busy_timeout = 30000");
sqlite.pragma("cache_size = -20000");
var db = (0, import_better_sqlite3.drizzle)(sqlite);

// server/storage.ts
var DatabaseStorage = class {
  insertTransaction(tx) {
    return db.insert(insiderTransactions).values(tx).returning().get();
  }
  getTransactions(limit = 100, offset = 0) {
    return db.select().from(insiderTransactions).orderBy((0, import_drizzle_orm.desc)(insiderTransactions.filingDate)).limit(limit).offset(offset).all();
  }
  getPurchaseTransactions(limit = 100, days = 30) {
    const cutoff = /* @__PURE__ */ new Date();
    cutoff.setDate(cutoff.getDate() - days);
    const cutoffStr = cutoff.toISOString().split("T")[0];
    return db.select().from(insiderTransactions).where((0, import_drizzle_orm.and)(
      (0, import_drizzle_orm.eq)(insiderTransactions.transactionType, "P"),
      (0, import_drizzle_orm.gte)(insiderTransactions.filingDate, cutoffStr)
    )).orderBy((0, import_drizzle_orm.desc)(insiderTransactions.filingDate)).limit(limit).all();
  }
  getTransactionByAccession(accessionNumber) {
    return db.select().from(insiderTransactions).where((0, import_drizzle_orm.eq)(insiderTransactions.accessionNumber, accessionNumber)).get();
  }
  getTransactionCount() {
    const result = db.select({ count: (0, import_drizzle_orm.count)() }).from(insiderTransactions).get();
    return result?.count ?? 0;
  }
  getPurchaseCount(days = 30) {
    const cutoff = /* @__PURE__ */ new Date();
    cutoff.setDate(cutoff.getDate() - days);
    const cutoffStr = cutoff.toISOString().split("T")[0];
    const result = db.select({ count: (0, import_drizzle_orm.count)() }).from(insiderTransactions).where((0, import_drizzle_orm.and)(
      (0, import_drizzle_orm.eq)(insiderTransactions.transactionType, "P"),
      (0, import_drizzle_orm.gte)(insiderTransactions.filingDate, cutoffStr)
    )).get();
    return result?.count ?? 0;
  }
  getRecentPurchaseVolume(days) {
    const cutoff = /* @__PURE__ */ new Date();
    cutoff.setDate(cutoff.getDate() - days);
    const cutoffStr = cutoff.toISOString().split("T")[0];
    const result = db.select({
      total: import_drizzle_orm.sql`COALESCE(SUM(CASE WHEN ${insiderTransactions.totalValue} <= 500000000 THEN ${insiderTransactions.totalValue} ELSE 0 END), 0)`
    }).from(insiderTransactions).where((0, import_drizzle_orm.and)(
      (0, import_drizzle_orm.eq)(insiderTransactions.transactionType, "P"),
      (0, import_drizzle_orm.gte)(insiderTransactions.filingDate, cutoffStr)
    )).get();
    return result?.total ?? 0;
  }
  getTopBuyers(days = 30, limit = 10) {
    const cutoff = /* @__PURE__ */ new Date();
    cutoff.setDate(cutoff.getDate() - days);
    const cutoffStr = cutoff.toISOString().split("T")[0];
    return db.select({
      name: insiderTransactions.reportingPersonName,
      title: import_drizzle_orm.sql`MAX(${insiderTransactions.reportingPersonTitle})`,
      ticker: import_drizzle_orm.sql`MAX(${insiderTransactions.issuerTicker})`,
      totalValue: import_drizzle_orm.sql`COALESCE(SUM(${insiderTransactions.totalValue}), 0)`,
      count: (0, import_drizzle_orm.count)()
    }).from(insiderTransactions).where((0, import_drizzle_orm.and)(
      (0, import_drizzle_orm.eq)(insiderTransactions.transactionType, "P"),
      (0, import_drizzle_orm.gte)(insiderTransactions.filingDate, cutoffStr)
    )).groupBy(insiderTransactions.reportingPersonName).orderBy(import_drizzle_orm.sql`SUM(${insiderTransactions.totalValue}) DESC`).limit(limit).all();
  }
  getDailyPurchaseVolume(days) {
    const cutoff = /* @__PURE__ */ new Date();
    cutoff.setDate(cutoff.getDate() - days);
    const cutoffStr = cutoff.toISOString().split("T")[0];
    return db.select({
      date: insiderTransactions.filingDate,
      volume: import_drizzle_orm.sql`COALESCE(SUM(CASE WHEN ${insiderTransactions.totalValue} <= 500000000 THEN ${insiderTransactions.totalValue} ELSE 0 END), 0)`,
      count: (0, import_drizzle_orm.count)()
    }).from(insiderTransactions).where((0, import_drizzle_orm.and)(
      (0, import_drizzle_orm.eq)(insiderTransactions.transactionType, "P"),
      (0, import_drizzle_orm.gte)(insiderTransactions.filingDate, cutoffStr)
    )).groupBy(insiderTransactions.filingDate).orderBy(insiderTransactions.filingDate).all();
  }
  insertSignal(signal) {
    return db.insert(purchaseSignals).values(signal).returning().get();
  }
  getSignals(limit = 50) {
    return db.select().from(purchaseSignals).orderBy((0, import_drizzle_orm.desc)(purchaseSignals.signalDate)).limit(limit).all();
  }
  getTopSignals(limit = 20) {
    return db.select().from(purchaseSignals).orderBy((0, import_drizzle_orm.desc)(purchaseSignals.signalScore)).limit(limit).all();
  }
  getPollingState() {
    return db.select().from(pollingState).get();
  }
  updatePollingState(lastPolledAt, lastAccession, totalProcessed) {
    const existing = this.getPollingState();
    if (existing) {
      db.update(pollingState).set({
        lastPolledAt,
        ...lastAccession ? { lastAccessionNumber: lastAccession } : {},
        ...totalProcessed !== void 0 ? { totalFilingsProcessed: totalProcessed } : {}
      }).where((0, import_drizzle_orm.eq)(pollingState.id, existing.id)).run();
    } else {
      db.insert(pollingState).values({
        lastPolledAt,
        lastAccessionNumber: lastAccession ?? null,
        totalFilingsProcessed: totalProcessed ?? 0,
        status: "active"
      }).run();
    }
  }
  getSectorBreakdown(days = 30) {
    const cutoff = /* @__PURE__ */ new Date();
    cutoff.setDate(cutoff.getDate() - days);
    const cutoffStr = cutoff.toISOString().split("T")[0];
    return db.select({
      ticker: import_drizzle_orm.sql`COALESCE(${insiderTransactions.issuerTicker}, 'N/A')`,
      name: import_drizzle_orm.sql`MAX(${insiderTransactions.issuerName})`,
      totalValue: import_drizzle_orm.sql`COALESCE(SUM(${insiderTransactions.totalValue}), 0)`,
      count: (0, import_drizzle_orm.count)()
    }).from(insiderTransactions).where((0, import_drizzle_orm.and)(
      (0, import_drizzle_orm.eq)(insiderTransactions.transactionType, "P"),
      (0, import_drizzle_orm.gte)(insiderTransactions.filingDate, cutoffStr)
    )).groupBy(insiderTransactions.issuerTicker).orderBy(import_drizzle_orm.sql`SUM(${insiderTransactions.totalValue}) DESC`).limit(20).all();
  }
  getInsiderTypeBreakdown(days = 30) {
    const cutoff = /* @__PURE__ */ new Date();
    cutoff.setDate(cutoff.getDate() - days);
    const cutoffStr = cutoff.toISOString().split("T")[0];
    return db.select({
      type: import_drizzle_orm.sql`
        CASE 
          WHEN ${insiderTransactions.isOfficer} = 1 THEN 'Officer'
          WHEN ${insiderTransactions.isDirector} = 1 THEN 'Director'
          WHEN ${insiderTransactions.isTenPercentOwner} = 1 THEN '10%+ Owner'
          ELSE 'Other'
        END`,
      count: (0, import_drizzle_orm.count)(),
      totalValue: import_drizzle_orm.sql`COALESCE(SUM(${insiderTransactions.totalValue}), 0)`
    }).from(insiderTransactions).where((0, import_drizzle_orm.and)(
      (0, import_drizzle_orm.eq)(insiderTransactions.transactionType, "P"),
      (0, import_drizzle_orm.gte)(insiderTransactions.filingDate, cutoffStr)
    )).groupBy(import_drizzle_orm.sql`CASE 
        WHEN ${insiderTransactions.isOfficer} = 1 THEN 'Officer'
        WHEN ${insiderTransactions.isDirector} = 1 THEN 'Director'
        WHEN ${insiderTransactions.isTenPercentOwner} = 1 THEN '10%+ Owner'
        ELSE 'Other'
      END`).all();
  }
  getClusterBuys(days = 30) {
    const cutoff = /* @__PURE__ */ new Date();
    cutoff.setDate(cutoff.getDate() - days);
    const cutoffStr = cutoff.toISOString().split("T")[0];
    return db.select({
      ticker: import_drizzle_orm.sql`COALESCE(${insiderTransactions.issuerTicker}, 'N/A')`,
      name: import_drizzle_orm.sql`MAX(${insiderTransactions.issuerName})`,
      insiderCount: import_drizzle_orm.sql`COUNT(DISTINCT ${insiderTransactions.reportingPersonName})`,
      totalValue: import_drizzle_orm.sql`COALESCE(SUM(${insiderTransactions.totalValue}), 0)`,
      avgPrice: import_drizzle_orm.sql`COALESCE(AVG(${insiderTransactions.pricePerShare}), 0)`,
      dates: import_drizzle_orm.sql`MIN(${insiderTransactions.filingDate}) || ' to ' || MAX(${insiderTransactions.filingDate})`
    }).from(insiderTransactions).where((0, import_drizzle_orm.and)(
      (0, import_drizzle_orm.eq)(insiderTransactions.transactionType, "P"),
      (0, import_drizzle_orm.gte)(insiderTransactions.filingDate, cutoffStr)
    )).groupBy(insiderTransactions.issuerTicker).having(import_drizzle_orm.sql`COUNT(DISTINCT ${insiderTransactions.reportingPersonName}) >= 2`).orderBy(import_drizzle_orm.sql`COUNT(DISTINCT ${insiderTransactions.reportingPersonName}) DESC`).limit(20).all();
  }
  // ========== V2: Trade Executions ==========
  insertTradeExecution(trade) {
    return db.insert(tradeExecutions).values(trade).returning().get();
  }
  getTradeExecutions(limit = 100) {
    return db.select().from(tradeExecutions).orderBy((0, import_drizzle_orm.desc)(tradeExecutions.executionDate)).limit(limit).all();
  }
  getTradeExecutionsByTicker(ticker) {
    return db.select().from(tradeExecutions).where((0, import_drizzle_orm.eq)(tradeExecutions.ticker, ticker)).orderBy((0, import_drizzle_orm.desc)(tradeExecutions.executionDate)).all();
  }
  // ========== V2: Portfolio Positions ==========
  upsertPosition(pos) {
    const existing = this.getPositionByTicker(pos.ticker);
    if (existing) {
      db.update(portfolioPositions).set({
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
        source: pos.source ?? existing.source
      }).where((0, import_drizzle_orm.eq)(portfolioPositions.ticker, pos.ticker)).run();
      return this.getPositionByTicker(pos.ticker);
    }
    return db.insert(portfolioPositions).values(pos).returning().get();
  }
  getPositions() {
    return db.select().from(portfolioPositions).orderBy((0, import_drizzle_orm.desc)(portfolioPositions.marketValue)).all();
  }
  getPositionByTicker(ticker) {
    return db.select().from(portfolioPositions).where((0, import_drizzle_orm.eq)(portfolioPositions.ticker, ticker)).get();
  }
  deletePosition(ticker) {
    db.delete(portfolioPositions).where((0, import_drizzle_orm.eq)(portfolioPositions.ticker, ticker)).run();
  }
  // ========== V2: Strategy Snapshots ==========
  insertSnapshot(snap) {
    return db.insert(strategySnapshots).values(snap).returning().get();
  }
  getSnapshots(days = 90) {
    const cutoff = /* @__PURE__ */ new Date();
    cutoff.setDate(cutoff.getDate() - days);
    const cutoffStr = cutoff.toISOString().split("T")[0];
    return db.select().from(strategySnapshots).where((0, import_drizzle_orm.gte)(strategySnapshots.date, cutoffStr)).orderBy((0, import_drizzle_orm.asc)(strategySnapshots.date)).all();
  }
  getLatestSnapshot() {
    return db.select().from(strategySnapshots).orderBy((0, import_drizzle_orm.desc)(strategySnapshots.date)).limit(1).get();
  }
  // ========== V2: Closed Trades ==========
  insertClosedTrade(trade) {
    return db.insert(closedTrades).values(trade).returning().get();
  }
  getClosedTrades(limit = 100) {
    return db.select().from(closedTrades).orderBy((0, import_drizzle_orm.desc)(closedTrades.exitDate)).limit(limit).all();
  }
  // ========== V2: Execution Deviations ==========
  insertExecutionDeviation(dev) {
    return db.insert(executionDeviations).values(dev).returning().get();
  }
  getTradeExecutionByOrderId(orderId) {
    return db.select().from(tradeExecutions).where((0, import_drizzle_orm.eq)(tradeExecutions.orderId, orderId)).get();
  }
  // ========== V2: Schwab Config ==========
  getSchwabConfig() {
    return db.select().from(schwabConfig).get();
  }
  upsertSchwabConfig(config) {
    const existing = this.getSchwabConfig();
    if (existing) {
      db.update(schwabConfig).set(config).where((0, import_drizzle_orm.eq)(schwabConfig.id, existing.id)).run();
    } else {
      db.insert(schwabConfig).values({
        ...config,
        createdAt: (/* @__PURE__ */ new Date()).toISOString()
      }).run();
    }
  }
};
var storage = new DatabaseStorage();

// server/routes.ts
var import_drizzle_orm4 = require("drizzle-orm");
var import_fs = __toESM(require("fs"), 1);

// server/v3-strategy.ts
var import_drizzle_orm2 = require("drizzle-orm");
var CACHE_TTL = 30 * 60 * 1e3;
function getFactorResults(horizon) {
  let query = db.select().from(factorAnalysis);
  if (horizon) {
    return query.where((0, import_drizzle_orm2.eq)(factorAnalysis.horizon, horizon)).orderBy(factorAnalysis.factorName, (0, import_drizzle_orm2.desc)(factorAnalysis.meanExcessReturn)).all();
  }
  return query.orderBy(factorAnalysis.factorName, factorAnalysis.horizon).all();
}
function getFactorEffectiveness() {
  return db.all(import_drizzle_orm2.sql`
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
function getFactorHeatmap(factorName) {
  return db.select().from(factorAnalysis).where((0, import_drizzle_orm2.eq)(factorAnalysis.factorName, factorName)).orderBy(factorAnalysis.sliceName, factorAnalysis.horizon).all();
}
var ALPHA_DECAY_CACHE_PATH = "/opt/insider-signal-dash/alpha-decay-cache.json";
var fs = require("fs");
function getAlphaDecayCurve(options = {}) {
  try {
    if (fs.existsSync(ALPHA_DECAY_CACHE_PATH)) {
      return JSON.parse(fs.readFileSync(ALPHA_DECAY_CACHE_PATH, "utf-8"));
    }
  } catch (e) {
    console.error("[ALPHA DECAY] Failed to read cache file:", e.message);
  }
  return [];
}
function precomputeAlphaDecay() {
  const { exec } = require("child_process");
  console.log("[ALPHA DECAY] Pre-computing alpha decay curve via sqlite3 CLI...");
  return new Promise((resolve, reject) => {
    const sql5 = `SELECT trading_day, COUNT(*) as sample_size, ROUND(AVG(excess_from_next_open) * 100, 3) as avg_excess_pct, ROUND(AVG(return_from_next_open) * 100, 3) as avg_return_pct FROM daily_forward_returns WHERE excess_from_next_open IS NOT NULL GROUP BY trading_day ORDER BY trading_day`;
    const cmd = `nice -n 19 sqlite3 -csv -header -readonly /opt/insider-signal-dash/data.db "${sql5}"`;
    exec(
      cmd,
      { timeout: 6e5, maxBuffer: 50 * 1024 * 1024, shell: "/bin/bash" },
      (error, stdout, stderr) => {
        if (error) {
          console.error("[ALPHA DECAY] Failed:", error.message);
          return reject(error);
        }
        try {
          const lines = stdout.trim().split("\n");
          if (lines.length < 2) {
            fs.writeFileSync(ALPHA_DECAY_CACHE_PATH, JSON.stringify([]));
            console.log("[ALPHA DECAY] Done: 0 data points (no results)");
            return resolve();
          }
          const headers = lines[0].split(",");
          const data = lines.slice(1).map((line) => {
            const vals = line.split(",");
            const obj = {};
            headers.forEach((h, i) => {
              obj[h] = isNaN(Number(vals[i])) ? vals[i] : Number(vals[i]);
            });
            return obj;
          });
          fs.writeFileSync(ALPHA_DECAY_CACHE_PATH, JSON.stringify(data));
          console.log(`[ALPHA DECAY] Done: ${data.length} data points cached`);
          resolve();
        } catch (parseErr) {
          console.error("[ALPHA DECAY] CSV parse failed:", parseErr.message);
          reject(parseErr);
        }
      }
    );
  });
}
function getModelWeights() {
  return db.select().from(modelWeights).orderBy((0, import_drizzle_orm2.desc)(modelWeights.effectiveWeight)).all();
}
function getScoredSignals(limit = 50, minScore) {
  let query = db.select({
    signal: purchaseSignals,
    entryPrice: signalEntryPrices
  }).from(purchaseSignals).leftJoin(signalEntryPrices, (0, import_drizzle_orm2.eq)(purchaseSignals.id, signalEntryPrices.signalId));
  const results = query.orderBy((0, import_drizzle_orm2.desc)(purchaseSignals.signalScore), (0, import_drizzle_orm2.desc)(purchaseSignals.signalDate)).limit(limit * 3).all();
  const seen = /* @__PURE__ */ new Map();
  const deduped = [];
  for (const r of results) {
    const key = `${r.signal.issuerTicker}|${r.signal.signalDate}`;
    const existingId = seen.get(key);
    if (existingId === void 0 || r.signal.id > existingId) {
      if (existingId !== void 0) {
        const idx = deduped.findIndex((d) => d.signal.id === existingId);
        if (idx !== -1) deduped.splice(idx, 1);
      }
      seen.set(key, r.signal.id);
      deduped.push(r);
    }
  }
  return deduped.slice(0, limit).map((r) => ({
    ...r.signal,
    entryPrices: r.entryPrice
  }));
}
function getPerformanceSnapshots(days = 90) {
  return db.select().from(strategySnapshots).orderBy((0, import_drizzle_orm2.desc)(strategySnapshots.date)).limit(days).all().reverse();
}
function getPerformanceSummary() {
  const snapshots = db.select().from(strategySnapshots).orderBy((0, import_drizzle_orm2.desc)(strategySnapshots.date)).limit(252).all();
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
    deviationCost: latest.deviationCost
  };
}
function getExecutionSummary() {
  const deviations = db.select().from(executionDeviations).all();
  const signalAligned = deviations.filter((d) => d.classification === "signal_aligned");
  const independent = deviations.filter((d) => d.classification === "independent");
  const totalTradeCount = deviations.length;
  const tradedSignals = signalAligned.length;
  return {
    signalCoverage: totalTradeCount > 0 ? tradedSignals / totalTradeCount : 0,
    signalCoverageCount: `${tradedSignals}/${totalTradeCount}`,
    avgEntryDelay: signalAligned.length > 0 ? signalAligned.reduce((s, d) => s + (d.entryDelayDays || 0), 0) / signalAligned.length : 0,
    exitDiscipline: signalAligned.length > 0 ? signalAligned.filter((d) => d.exitType === "time" || d.exitType === "stop").length / signalAligned.length : 0,
    totalDeviationCost: deviations.reduce((s, d) => s + (d.alphaCost || 0), 0),
    independentAlpha: independent.reduce((s, d) => s + (d.pnlDifference || 0), 0),
    totalTrades: deviations.length,
    signalAlignedCount: signalAligned.length,
    independentCount: independent.length
  };
}
function getTradeDeviations(limit = 100) {
  return db.select({
    deviation: executionDeviations,
    trade: tradeExecutions,
    signal: purchaseSignals
  }).from(executionDeviations).leftJoin(tradeExecutions, (0, import_drizzle_orm2.eq)(executionDeviations.userTradeId, tradeExecutions.id)).leftJoin(purchaseSignals, (0, import_drizzle_orm2.eq)(executionDeviations.signalId, purchaseSignals.id)).orderBy((0, import_drizzle_orm2.desc)(executionDeviations.createdAt)).limit(limit).all();
}
function getMissedSignals(days = 90) {
  return db.all(import_drizzle_orm2.sql`
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
async function getPortfolioWithSignalHealth(getSchwabAccessToken) {
  let positions = [];
  if (getSchwabAccessToken) {
    try {
      const token = await getSchwabAccessToken();
      if (token) {
        const resp = await fetch("https://api.schwabapi.com/trader/v1/accounts?fields=positions", {
          headers: { "Authorization": `Bearer ${token}` }
        });
        if (resp.ok) {
          const accounts = await resp.json();
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
                unrealizedPnlPct: pos.averagePrice ? (currentPrice - pos.averagePrice) / pos.averagePrice * 100 : 0,
                dayChange: pos.currentDayProfitLoss || 0,
                dayChangePct: pos.currentDayProfitLossPercentage || 0,
                source: "schwab"
              });
            }
          }
        }
      }
    } catch (err) {
      console.error("[PORTFOLIO] Schwab API error, falling back to DB:", err.message);
    }
  }
  if (positions.length === 0) {
    positions = db.select().from(portfolioPositions).all();
  }
  const recentSignals = db.all(import_drizzle_orm2.sql`
    SELECT id, issuer_ticker, signal_date, signal_score, score_tier
    FROM purchase_signals
    WHERE signal_date >= date('now', '-90 days')
    ORDER BY signal_date DESC
  `);
  const signalByTicker = /* @__PURE__ */ new Map();
  for (const sig of recentSignals) {
    const ticker = sig.issuer_ticker;
    if (ticker && !signalByTicker.has(ticker)) {
      signalByTicker.set(ticker, sig);
    }
  }
  return positions.map((pos) => {
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
      const signalDate = new Date(matchingSignal.signal_date);
      const now = /* @__PURE__ */ new Date();
      const daysSinceSignal = Math.floor((now.getTime() - signalDate.getTime()) / (1e3 * 60 * 60 * 24));
      const optimalHold = 63;
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
      shouldExit
    };
  });
}
var _pipelineStatusCache = null;
var _pipelineStatusCacheTime = 0;
var PIPELINE_STATUS_CACHE_TTL = 6e4;
function invalidatePipelineStatusCache() {
  _pipelineStatusCache = null;
  _pipelineStatusCacheTime = 0;
}
function getDataPipelineStatus() {
  const now = Date.now();
  if (_pipelineStatusCache && now - _pipelineStatusCacheTime < PIPELINE_STATUS_CACHE_TTL) {
    return _pipelineStatusCache;
  }
  const txCount = db.select({ count: import_drizzle_orm2.sql`count(*)` }).from(insiderTransactions).where((0, import_drizzle_orm2.eq)(insiderTransactions.transactionType, "P")).get();
  const signalCount = db.select({ count: import_drizzle_orm2.sql`count(*)` }).from(purchaseSignals).get();
  const enrichedCount = db.select({ count: import_drizzle_orm2.sql`count(DISTINCT signal_id)` }).from(signalEntryPrices).get();
  const fwdReturnApprox = db.all(import_drizzle_orm2.sql`SELECT MAX(rowid) as count FROM daily_forward_returns`);
  const fwdReturnCount = fwdReturnApprox?.[0]?.count || 0;
  const factorCount = db.select({ count: import_drizzle_orm2.sql`count(*)` }).from(factorAnalysis).get();
  const weightCount = db.select({ count: import_drizzle_orm2.sql`count(*)` }).from(modelWeights).get();
  const insiderCount = db.select({ count: import_drizzle_orm2.sql`count(*)` }).from(insiderHistory).get();
  let failedTickerCount = 0;
  try {
    const ft = db.all(import_drizzle_orm2.sql`SELECT COUNT(*) as count FROM enrichment_failed_tickers`);
    failedTickerCount = ft?.[0]?.count || 0;
  } catch (e) {
  }
  const totalAll = signalCount?.count || 0;
  const enriched = enrichedCount?.count || 0;
  _pipelineStatusCache = {
    totalPurchases: txCount?.count || 0,
    totalSignals: totalAll,
    enrichedSignals: enriched,
    failedTickers: failedTickerCount,
    forwardReturnDataPoints: fwdReturnCount,
    factorAnalysisResults: factorCount?.count || 0,
    modelFactors: weightCount?.count || 0,
    insiderProfiles: insiderCount?.count || 0,
    enrichmentProgress: totalAll > 0 ? Math.round(enriched / totalAll * 100) : 0
  };
  _pipelineStatusCacheTime = now;
  return _pipelineStatusCache;
}

// server/trade-engine.ts
var import_drizzle_orm3 = require("drizzle-orm");
function runFifoMatching() {
  const errors = [];
  let matched = 0;
  sqlite.exec("DELETE FROM closed_trades");
  const allTrades = db.select().from(tradeExecutions).orderBy((0, import_drizzle_orm3.asc)(tradeExecutions.executionDate), (0, import_drizzle_orm3.asc)(tradeExecutions.id)).all();
  const byTicker = /* @__PURE__ */ new Map();
  for (const t of allTrades) {
    const key = t.ticker.toUpperCase();
    if (!byTicker.has(key)) byTicker.set(key, []);
    byTicker.get(key).push(t);
  }
  for (const [ticker, trades] of byTicker) {
    const openLots = [];
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
          signalScore: trade.signalScore
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
          const realizedPnlPct = entryPrice > 0 ? (exitPrice - entryPrice) / entryPrice * 100 : 0;
          const holdingDays = Math.max(0, Math.floor(
            (new Date(exitDate).getTime() - new Date(lot.entryDate).getTime()) / (1e3 * 60 * 60 * 24)
          ));
          const devRow = db.all(import_drizzle_orm3.sql`
            SELECT classification, signal_id FROM execution_deviations
            WHERE user_trade_id = ${lot.tradeId} LIMIT 1
          `);
          try {
            db.insert(closedTrades).values({
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
              createdAt: (/* @__PURE__ */ new Date()).toISOString()
            }).run();
            matched++;
          } catch (e) {
            errors.push(`${ticker}: ${e.message}`);
          }
          lot.remainingQty -= matchQty;
          sellQtyRemaining -= matchQty;
          if (lot.remainingQty <= 1e-3) {
            openLots.shift();
          }
        }
        if (sellQtyRemaining > 1e-3) {
          errors.push(`${ticker}: ${sellQtyRemaining.toFixed(2)} shares sold with no matching buy lot`);
        }
      }
    }
  }
  return { matched, errors };
}
function getPerformanceAnalytics() {
  const closed = db.select().from(closedTrades).orderBy((0, import_drizzle_orm3.asc)(closedTrades.exitDate)).all();
  if (closed.length === 0) {
    return null;
  }
  const positions = db.select().from(portfolioPositions).all();
  const totalUnrealizedPnl = positions.reduce((s, p) => s + (p.unrealizedPnl || 0), 0);
  const totalMarketValue = positions.reduce((s, p) => s + (p.marketValue || 0), 0);
  const totalRealizedPnl = closed.reduce((s, t) => s + t.realizedPnl, 0);
  const combinedPnl = totalRealizedPnl + totalUnrealizedPnl;
  const winners = closed.filter((t) => t.realizedPnl > 0);
  const losers = closed.filter((t) => t.realizedPnl < 0);
  const winRate = closed.length > 0 ? winners.length / closed.length * 100 : 0;
  const grossProfits = winners.reduce((s, t) => s + t.realizedPnl, 0);
  const grossLosses = Math.abs(losers.reduce((s, t) => s + t.realizedPnl, 0));
  const profitFactor = grossLosses > 0 ? grossProfits / grossLosses : grossProfits > 0 ? Infinity : 0;
  const avgWinDollar = winners.length > 0 ? grossProfits / winners.length : 0;
  const avgLossDollar = losers.length > 0 ? grossLosses / losers.length : 0;
  const avgWinPct = winners.length > 0 ? winners.reduce((s, t) => s + t.realizedPnlPct, 0) / winners.length : 0;
  const avgLossPct = losers.length > 0 ? losers.reduce((s, t) => s + t.realizedPnlPct, 0) / losers.length : 0;
  const bestTrade = closed.reduce((best, t) => t.realizedPnl > best.realizedPnl ? t : best, closed[0]);
  const worstTrade = closed.reduce((worst, t) => t.realizedPnl < worst.realizedPnl ? t : worst, closed[0]);
  const avgHoldingPeriod = closed.reduce((s, t) => s + (t.holdingDays || 0), 0) / closed.length;
  const expectancy = winRate / 100 * avgWinDollar - (1 - winRate / 100) * avgLossDollar;
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
      date: bestTrade.exitDate
    },
    worstTrade: {
      ticker: worstTrade.ticker,
      pnl: worstTrade.realizedPnl,
      pnlPct: worstTrade.realizedPnlPct,
      date: worstTrade.exitDate
    },
    avgHoldingPeriod,
    expectancy
  };
}
function getEquityCurve() {
  const closed = db.select().from(closedTrades).orderBy((0, import_drizzle_orm3.asc)(closedTrades.exitDate)).all();
  if (closed.length === 0) return [];
  let cumulativePnl = 0;
  let cumulativeInvested = 0;
  const curve = [];
  const byDate = /* @__PURE__ */ new Map();
  for (const t of closed) {
    const d = t.exitDate;
    if (!byDate.has(d)) byDate.set(d, []);
    byDate.get(d).push(t);
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
      cumulativePnlPct: cumulativeInvested > 0 ? cumulativePnl / cumulativeInvested * 100 : 0,
      tradeCount
    });
  }
  return curve;
}
function runSignalTradeMatching() {
  const errors = [];
  let matched = 0;
  let unmatched = 0;
  const deviations = db.all(import_drizzle_orm3.sql`
    SELECT ed.id as dev_id, ed.user_trade_id, ed.signal_id, ed.classification,
           te.ticker, te.execution_date, te.avg_price, te.side
    FROM execution_deviations ed
    JOIN trade_executions te ON te.id = ed.user_trade_id
    WHERE te.side = 'BUY'
  `);
  for (const dev of deviations) {
    const matchingSignals = db.all(import_drizzle_orm3.sql`
      SELECT ps.id, ps.signal_score, ps.signal_date, ps.score_tier,
             sep.next_open as entry_price, sep.prior_close
      FROM purchase_signals ps
      LEFT JOIN signal_entry_prices sep ON sep.signal_id = ps.id
      WHERE UPPER(ps.issuer_ticker) = UPPER(${dev.ticker})
        AND ps.signal_date <= ${dev.execution_date}
        AND ps.signal_date >= date(${dev.execution_date}, '-90 days')
      ORDER BY ps.signal_score DESC, ps.signal_date DESC
      LIMIT 1
    `);
    const bestSignal = matchingSignals[0];
    if (bestSignal) {
      const signalDate = new Date(bestSignal.signal_date);
      const tradeDate = new Date(dev.execution_date);
      const entryDelayDays = Math.floor(
        (tradeDate.getTime() - signalDate.getTime()) / (1e3 * 60 * 60 * 24)
      );
      let entryPriceGapPct = null;
      const signalPrice = bestSignal.entry_price || bestSignal.prior_close;
      if (signalPrice && dev.avg_price) {
        entryPriceGapPct = (dev.avg_price - signalPrice) / signalPrice * 100;
      }
      sqlite.exec(`
        UPDATE execution_deviations
        SET signal_id = ${bestSignal.id},
            classification = 'signal_aligned',
            entry_delay_days = ${entryDelayDays},
            entry_price_gap_pct = ${entryPriceGapPct !== null ? entryPriceGapPct : "NULL"}
        WHERE id = ${dev.dev_id}
      `);
      sqlite.exec(`
        UPDATE trade_executions
        SET signal_id = ${bestSignal.id},
            signal_score = ${bestSignal.signal_score}
        WHERE id = ${dev.user_trade_id}
      `);
      matched++;
    } else {
      unmatched++;
    }
  }
  return { matched, unmatched, errors };
}
function getEnhancedMissedSignals(days = 90, minScore = 70) {
  const tradedTickers = db.all(import_drizzle_orm3.sql`
    SELECT DISTINCT UPPER(ticker) as ticker FROM trade_executions
  `);
  const tradedSet = new Set(tradedTickers.map((t) => t.ticker));
  const signals = db.all(import_drizzle_orm3.sql`
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
    WHERE ps.signal_score >= ${minScore}
      AND ps.signal_date >= date('now', '-' || ${days} || ' days')
    ORDER BY ps.signal_score DESC
    LIMIT 100
  `);
  const missed = signals.filter((s) => {
    const ticker = (s.issuer_ticker || "").toUpperCase();
    return !tradedSet.has(ticker);
  });
  return missed.map((s) => ({
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
    estimatedAlphaMissed: s.excess_63d_pct != null ? s.excess_63d_pct : s.signal_score >= 80 ? 5 : s.signal_score >= 70 ? 3 : 1.5
  }));
}

// server/routes.ts
function getPollerStatus() {
  try {
    return JSON.parse((0, import_fs.readFileSync)("/tmp/poller-status.json", "utf-8"));
  } catch {
    return { active: false, mode: "separate process (status file not found)" };
  }
}
async function registerRoutes(httpServer2, app2) {
  try {
    const tradeCount = db.all(import_drizzle_orm4.sql`SELECT count(*) as c FROM trade_executions`)[0];
    const closedCount = db.all(import_drizzle_orm4.sql`SELECT count(*) as c FROM closed_trades`)[0];
    if (tradeCount?.c > 0 && closedCount?.c === 0) {
      console.log("[STARTUP] Running signal-trade matching and FIFO matching...");
      const sigResult = runSignalTradeMatching();
      console.log(`[STARTUP] Signal matching: ${sigResult.matched} matched, ${sigResult.unmatched} unmatched`);
      const fifoResult = runFifoMatching();
      console.log(`[STARTUP] FIFO matching: ${fifoResult.matched} closed trades created`);
    }
  } catch (e) {
    console.error("[STARTUP] Trade matching failed:", e.message);
  }
  app2.get("/api/dashboard", (_req, res) => {
    try {
      let purchaseCount30d = storage.getPurchaseCount(30);
      let purchaseCount7d = storage.getPurchaseCount(7);
      let purchaseCount1d = storage.getPurchaseCount(1);
      let volume30d = storage.getRecentPurchaseVolume(30);
      let volume7d = storage.getRecentPurchaseVolume(7);
      const totalTransactions = storage.getTransactionCount();
      let clusters = storage.getClusterBuys(30);
      const pollingStatus = getPollerStatus();
      if (purchaseCount30d === 0) {
        purchaseCount30d = storage.getPurchaseCount(90);
        purchaseCount7d = storage.getPurchaseCount(90);
        volume30d = storage.getRecentPurchaseVolume(90);
        volume7d = storage.getRecentPurchaseVolume(90);
        clusters = storage.getClusterBuys(90);
      }
      if (purchaseCount30d === 0) {
        purchaseCount30d = storage.getPurchaseCount(36500);
        volume30d = storage.getRecentPurchaseVolume(36500);
        clusters = storage.getClusterBuys(36500);
      }
      res.json({
        kpis: {
          purchaseCount30d,
          purchaseCount7d,
          purchaseCount1d,
          volume30d,
          volume7d,
          totalTransactions,
          clusterCount: clusters.length
        },
        pollingStatus
      });
    } catch (err) {
      console.error("[/api/dashboard ERROR]", err.message);
      res.status(500).json({ error: err.message });
    }
  });
  app2.get("/api/signals", (req, res) => {
    const limit = parseInt(req.query.limit) || 50;
    const minScore = req.query.minScore ? parseInt(req.query.minScore) : void 0;
    res.json(getScoredSignals(limit, minScore));
  });
  app2.get("/api/transactions", (req, res) => {
    const limit = parseInt(req.query.limit) || 100;
    const type = req.query.type;
    const days = parseInt(req.query.days) || 30;
    if (type === "P") {
      let result = storage.getPurchaseTransactions(limit, days);
      if (result.length === 0) result = storage.getPurchaseTransactions(limit, 90);
      if (result.length === 0) result = storage.getPurchaseTransactions(limit, 365);
      if (result.length === 0) result = storage.getPurchaseTransactions(limit, 36500);
      res.json(result);
    } else {
      res.json(storage.getTransactions(limit));
    }
  });
  const MIN_CHART_POINTS = 10;
  app2.get("/api/analytics/daily-volume", (req, res) => {
    const days = parseInt(req.query.days) || 30;
    for (const d of [days, 90, 365, 36500]) {
      const result = storage.getDailyPurchaseVolume(d);
      if (result.length >= MIN_CHART_POINTS || d === 36500) return res.json(result);
    }
  });
  app2.get("/api/analytics/cluster-buys", (req, res) => {
    const days = parseInt(req.query.days) || 30;
    for (const d of [days, 90, 365, 36500]) {
      const result = storage.getClusterBuys(d);
      if (result.length >= MIN_CHART_POINTS || d === 36500) return res.json(result);
    }
  });
  app2.get("/api/analytics/insider-types", (req, res) => {
    const days = parseInt(req.query.days) || 30;
    for (const d of [days, 90, 365, 36500]) {
      const result = storage.getInsiderTypeBreakdown(d);
      if (result.length >= MIN_CHART_POINTS || d === 36500) return res.json(result);
    }
  });
  app2.get("/api/factors/effectiveness", (_req, res) => {
    res.json(getFactorEffectiveness());
  });
  app2.get("/api/factors/analysis", (req, res) => {
    const horizon = req.query.horizon ? parseInt(req.query.horizon) : void 0;
    res.json(getFactorResults(horizon));
  });
  app2.get("/api/factors/heatmap/:factorName", (req, res) => {
    res.json(getFactorHeatmap(req.params.factorName));
  });
  app2.get("/api/factors/alpha-decay", (req, res) => {
    try {
      const scoreTier = req.query.tier ? parseInt(req.query.tier) : void 0;
      res.json(getAlphaDecayCurve({ scoreTier }));
    } catch (err) {
      console.error("[/api/factors/alpha-decay ERROR]", err.message);
      res.status(500).json({ error: "Alpha decay computation failed", detail: err.message });
    }
  });
  app2.get("/api/factors/model-weights", (_req, res) => {
    res.json(getModelWeights());
  });
  app2.get("/api/portfolio/positions", async (_req, res) => {
    try {
      const positions = await getPortfolioWithSignalHealth(getSchwabAccessToken);
      const totalValue = positions.reduce((s, p) => s + (p.marketValue || 0), 0);
      const totalPnl = positions.reduce((s, p) => s + (p.unrealizedPnl || 0), 0);
      const totalCost = positions.reduce((s, p) => s + p.avgCostBasis * p.quantity, 0);
      const totalDayChange = positions.reduce((s, p) => s + (p.dayChange || 0), 0);
      const signalAligned = positions.filter((p) => p.signalClassification === "signal_aligned").length;
      res.json({
        positions,
        summary: {
          totalValue,
          totalCost,
          totalPnl,
          totalPnlPct: totalCost > 0 ? totalPnl / totalCost * 100 : 0,
          totalDayChange,
          totalDayChangePct: totalValue > 0 ? totalDayChange / totalValue * 100 : 0,
          positionCount: positions.length,
          signalAlignedCount: signalAligned,
          independentCount: positions.length - signalAligned
        }
      });
    } catch (err) {
      console.error("[/api/portfolio/positions ERROR]", err.message);
      res.status(500).json({ error: err.message });
    }
  });
  app2.get("/api/portfolio/executions", (req, res) => {
    const limit = parseInt(req.query.limit) || 100;
    res.json(storage.getTradeExecutions(limit));
  });
  app2.get("/api/portfolio/closed-trades", (req, res) => {
    const limit = parseInt(req.query.limit) || 100;
    res.json(storage.getClosedTrades(limit));
  });
  app2.get("/api/performance/summary", async (_req, res) => {
    try {
      const analytics = getPerformanceAnalytics();
      if (analytics) {
        if (analytics.totalUnrealizedPnl === 0 && analytics.openPositionCount === 0) {
          try {
            const positions = await getPortfolioWithSignalHealth(getSchwabAccessToken);
            if (positions.length > 0) {
              const schwabUnrealized = positions.reduce((s, p) => s + (p.unrealizedPnl || 0), 0);
              const schwabMarketValue = positions.reduce((s, p) => s + (p.marketValue || 0), 0);
              analytics.totalUnrealizedPnl = schwabUnrealized;
              analytics.totalMarketValue = schwabMarketValue;
              analytics.openPositionCount = positions.length;
              analytics.combinedPnl = analytics.totalRealizedPnl + schwabUnrealized;
            }
          } catch (e) {
            console.error("[PERF SUMMARY] Schwab position fetch failed:", e.message);
          }
        }
        return res.json(analytics);
      }
      res.json(getPerformanceSummary());
    } catch (err) {
      console.error("[/api/performance/summary ERROR]", err.message);
      res.status(500).json({ error: err.message });
    }
  });
  app2.get("/api/performance/chart", (req, res) => {
    const days = parseInt(req.query.days) || 90;
    res.json(getPerformanceSnapshots(days));
  });
  app2.get("/api/performance/equity-curve", (_req, res) => {
    res.json(getEquityCurve());
  });
  app2.get("/api/execution/summary", (_req, res) => {
    res.json(getExecutionSummary());
  });
  app2.get("/api/execution/deviations", (req, res) => {
    const limit = parseInt(req.query.limit) || 100;
    res.json(getTradeDeviations(limit));
  });
  app2.get("/api/execution/missed-signals", (req, res) => {
    const days = parseInt(req.query.days) || 90;
    const minScore = parseInt(req.query.minScore) || 70;
    const enhanced = getEnhancedMissedSignals(days, minScore);
    if (enhanced.length > 0) {
      return res.json(enhanced);
    }
    res.json(getMissedSignals(days));
  });
  app2.get("/api/settings/pipeline-status", (_req, res) => {
    res.json(getDataPipelineStatus());
  });
  app2.get("/api/schwab/status", (_req, res) => {
    const config = storage.getSchwabConfig();
    res.json({
      isConnected: config?.isConnected || false,
      status: config?.status || "disconnected",
      lastSyncAt: config?.lastSyncAt || null,
      accountNumber: config?.accountNumber ? `****${config.accountNumber.slice(-4)}` : null
    });
  });
  app2.post("/api/schwab/configure", (req, res) => {
    const { appKey, appSecret, callbackUrl } = req.body;
    if (!appKey || !appSecret) {
      return res.status(400).json({ error: "appKey and appSecret are required" });
    }
    storage.upsertSchwabConfig({ appKey, appSecret, status: "pending_auth" });
    const authUrl = `https://api.schwabapi.com/v1/oauth/authorize?client_id=${encodeURIComponent(appKey)}&redirect_uri=${encodeURIComponent(callbackUrl || "https://127.0.0.1")}&response_type=code`;
    res.json({ authUrl, message: "Visit the authorization URL to connect your Schwab account." });
  });
  app2.post("/api/schwab/callback", async (req, res) => {
    const { code, callbackUrl } = req.body;
    const config = storage.getSchwabConfig();
    if (!config?.appKey || !config?.appSecret) {
      return res.status(400).json({ error: "Schwab not configured." });
    }
    try {
      const basicAuth = Buffer.from(`${config.appKey}:${config.appSecret}`).toString("base64");
      const tokenResp = await fetch("https://api.schwabapi.com/v1/oauth/token", {
        method: "POST",
        headers: { "Authorization": `Basic ${basicAuth}`, "Content-Type": "application/x-www-form-urlencoded" },
        body: new URLSearchParams({ grant_type: "authorization_code", code, redirect_uri: callbackUrl || "https://127.0.0.1" })
      });
      if (!tokenResp.ok) return res.status(400).json({ error: `Token exchange failed: ${await tokenResp.text()}` });
      const tokens = await tokenResp.json();
      storage.upsertSchwabConfig({
        accessToken: tokens.access_token,
        refreshToken: tokens.refresh_token,
        tokenExpiresAt: new Date(Date.now() + tokens.expires_in * 1e3).toISOString(),
        isConnected: true,
        status: "connected",
        lastSyncAt: (/* @__PURE__ */ new Date()).toISOString()
      });
      res.json({ success: true, message: "Schwab account connected." });
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  });
  async function refreshSchwabToken() {
    const config = storage.getSchwabConfig();
    if (!config?.refreshToken || !config?.appKey || !config?.appSecret) return false;
    try {
      const basicAuth = Buffer.from(`${config.appKey}:${config.appSecret}`).toString("base64");
      const resp = await fetch("https://api.schwabapi.com/v1/oauth/token", {
        method: "POST",
        headers: { "Authorization": `Basic ${basicAuth}`, "Content-Type": "application/x-www-form-urlencoded" },
        body: new URLSearchParams({ grant_type: "refresh_token", refresh_token: config.refreshToken })
      });
      if (!resp.ok) {
        console.error("[SCHWAB] Token refresh failed:", await resp.text());
        return false;
      }
      const tokens = await resp.json();
      storage.upsertSchwabConfig({
        accessToken: tokens.access_token,
        refreshToken: tokens.refresh_token || config.refreshToken,
        tokenExpiresAt: new Date(Date.now() + tokens.expires_in * 1e3).toISOString()
      });
      return true;
    } catch (err) {
      console.error("[SCHWAB] Token refresh error:", err.message);
      return false;
    }
  }
  async function getSchwabAccessToken() {
    const config = storage.getSchwabConfig();
    if (!config?.accessToken) return null;
    if (config.tokenExpiresAt && new Date(config.tokenExpiresAt).getTime() < Date.now() + 6e4) {
      const refreshed = await refreshSchwabToken();
      if (!refreshed) return null;
      return storage.getSchwabConfig()?.accessToken || null;
    }
    return config.accessToken;
  }
  app2.get("/api/schwab/accounts", async (_req, res) => {
    try {
      const token = await getSchwabAccessToken();
      if (!token) return res.status(401).json({ error: "Schwab not connected or token expired" });
      const resp = await fetch("https://api.schwabapi.com/trader/v1/accounts?fields=positions", {
        headers: { "Authorization": `Bearer ${token}` }
      });
      if (!resp.ok) return res.status(resp.status).json({ error: `Schwab API error: ${await resp.text()}` });
      const accounts = await resp.json();
      if (accounts?.length > 0) {
        const acct = accounts[0];
        const acctNum = acct.securitiesAccount?.accountNumber || acct.accountNumber;
        if (acctNum) {
          storage.upsertSchwabConfig({ accountNumber: acctNum, lastSyncAt: (/* @__PURE__ */ new Date()).toISOString() });
        }
      }
      res.json(accounts);
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  });
  app2.get("/api/schwab/positions", async (_req, res) => {
    try {
      const token = await getSchwabAccessToken();
      if (!token) return res.status(401).json({ error: "Schwab not connected or token expired" });
      const resp = await fetch("https://api.schwabapi.com/trader/v1/accounts?fields=positions", {
        headers: { "Authorization": `Bearer ${token}` }
      });
      if (!resp.ok) return res.status(resp.status).json({ error: `Schwab API error: ${await resp.text()}` });
      const accounts = await resp.json();
      const positions = [];
      for (const acct of accounts || []) {
        const acctPositions = acct.securitiesAccount?.positions || [];
        for (const pos of acctPositions) {
          positions.push({
            ticker: pos.instrument?.symbol || "?",
            description: pos.instrument?.description || "",
            assetType: pos.instrument?.assetType || "",
            quantity: pos.longQuantity - (pos.shortQuantity || 0),
            marketValue: pos.marketValue || 0,
            averagePrice: pos.averagePrice || 0,
            currentDayPnl: pos.currentDayProfitLoss || 0,
            currentDayPnlPct: pos.currentDayProfitLossPercentage || 0
          });
        }
      }
      storage.upsertSchwabConfig({ lastSyncAt: (/* @__PURE__ */ new Date()).toISOString() });
      res.json(positions);
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  });
  app2.get("/api/schwab/orders", async (req, res) => {
    try {
      const token = await getSchwabAccessToken();
      if (!token) return res.status(401).json({ error: "Schwab not connected or token expired" });
      const fromDate = /* @__PURE__ */ new Date();
      fromDate.setDate(fromDate.getDate() - 60);
      const toDate = /* @__PURE__ */ new Date();
      const params = new URLSearchParams({
        fromEnteredTime: fromDate.toISOString(),
        toEnteredTime: toDate.toISOString()
      });
      const resp = await fetch(`https://api.schwabapi.com/trader/v1/orders?${params}`, {
        headers: { "Authorization": `Bearer ${token}` }
      });
      if (!resp.ok) return res.status(resp.status).json({ error: `Schwab API error: ${await resp.text()}` });
      const orders = await resp.json();
      res.json(orders || []);
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  });
  app2.post("/api/schwab/sync", async (_req, res) => {
    try {
      const token = await getSchwabAccessToken();
      if (!token) return res.status(401).json({ error: "Schwab not connected or token expired" });
      const resp = await fetch("https://api.schwabapi.com/trader/v1/accounts?fields=positions", {
        headers: { "Authorization": `Bearer ${token}` }
      });
      if (!resp.ok) return res.status(resp.status).json({ error: `Schwab API error: ${await resp.text()}` });
      const accounts = await resp.json();
      let syncedCount = 0;
      for (const acct of accounts || []) {
        const acctPositions = acct.securitiesAccount?.positions || [];
        for (const pos of acctPositions) {
          const ticker = pos.instrument?.symbol;
          if (!ticker || pos.instrument?.assetType !== "EQUITY") continue;
          storage.upsertPosition({
            ticker,
            quantity: pos.longQuantity - (pos.shortQuantity || 0),
            avgCostBasis: pos.averagePrice || 0,
            currentPrice: pos.marketValue ? pos.marketValue / (pos.longQuantity || 1) : 0,
            marketValue: pos.marketValue || 0,
            unrealizedPnl: pos.longQuantity * (pos.marketValue / (pos.longQuantity || 1) - (pos.averagePrice || 0)),
            unrealizedPnlPct: pos.averagePrice ? (pos.marketValue / (pos.longQuantity || 1) - pos.averagePrice) / pos.averagePrice * 100 : 0,
            dayChange: pos.currentDayProfitLoss || 0,
            dayChangePct: pos.currentDayProfitLossPercentage || 0,
            source: "schwab",
            lastSyncedAt: (/* @__PURE__ */ new Date()).toISOString()
          });
          syncedCount++;
        }
      }
      storage.upsertSchwabConfig({ lastSyncAt: (/* @__PURE__ */ new Date()).toISOString() });
      try {
        const signalResult = runSignalTradeMatching();
        const fifoResult = runFifoMatching();
        console.log(`[PIPELINE] Post-sync trade compute: ${signalResult.matched} signal matches, ${fifoResult.matched} FIFO matches`);
      } catch (e) {
        console.error("[PIPELINE] Post-sync trade compute failed:", e.message);
      }
      res.json({ success: true, syncedPositions: syncedCount });
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  });
  app2.post("/api/schwab/sync-orders", async (_req, res) => {
    try {
      const token = await getSchwabAccessToken();
      if (!token) return res.status(401).json({ error: "Schwab not connected or token expired" });
      const fromDate = /* @__PURE__ */ new Date();
      fromDate.setDate(fromDate.getDate() - 60);
      const toDate = /* @__PURE__ */ new Date();
      const params = new URLSearchParams({
        fromEnteredTime: fromDate.toISOString(),
        toEnteredTime: toDate.toISOString()
      });
      const resp = await fetch(`https://api.schwabapi.com/trader/v1/orders?${params}`, {
        headers: { "Authorization": `Bearer ${token}` }
      });
      if (!resp.ok) return res.status(resp.status).json({ error: `Schwab API error: ${await resp.text()}` });
      const orders = await resp.json();
      let createdExecutions = 0;
      let createdDeviations = 0;
      let createdClosedTrades = 0;
      for (const order of orders || []) {
        if (order.status !== "FILLED") continue;
        const legs = order.orderLegCollection || [];
        for (const leg of legs) {
          const ticker = leg.instrument?.symbol;
          if (!ticker || leg.instrument?.assetType !== "EQUITY") continue;
          const side = leg.instruction;
          const quantity = leg.quantity || order.filledQuantity || 0;
          const avgPrice = order.price || order.stopPrice || 0;
          const orderId = String(order.orderId || "");
          if (orderId && storage.getTradeExecutionByOrderId(orderId)) continue;
          const executionDate = order.closeTime ? new Date(order.closeTime).toISOString().split("T")[0] : new Date(order.enteredTime).toISOString().split("T")[0];
          const executionTime = order.closeTime ? new Date(order.closeTime).toISOString().split("T")[1]?.split(".")[0] : void 0;
          const trade = storage.insertTradeExecution({
            ticker,
            companyName: leg.instrument?.description || ticker,
            side: side === "BUY" ? "BUY" : "SELL",
            quantity,
            avgPrice,
            totalCost: quantity * avgPrice,
            executionDate,
            executionTime,
            source: "schwab_sync",
            orderId,
            status: "filled",
            createdAt: (/* @__PURE__ */ new Date()).toISOString()
          });
          createdExecutions++;
          if (side === "BUY") {
            const matchingSignals = db.all(import_drizzle_orm4.sql`
              SELECT id, signal_score, signal_date FROM purchase_signals
              WHERE issuer_ticker = ${ticker}
                AND signal_date >= date(${executionDate}, '-14 days')
                AND signal_date <= ${executionDate}
              ORDER BY signal_date DESC
              LIMIT 1
            `);
            const matchingSignal = matchingSignals[0];
            if (matchingSignal) {
              const signalDate = new Date(matchingSignal.signal_date);
              const tradeDate = new Date(executionDate);
              const entryDelayDays = Math.floor((tradeDate.getTime() - signalDate.getTime()) / (1e3 * 60 * 60 * 24));
              storage.insertExecutionDeviation({
                userTradeId: trade.id,
                signalId: matchingSignal.id,
                classification: "signal_aligned",
                entryDelayDays,
                entryPriceGapPct: null,
                sizingDeviationPct: null,
                holdDeviationDays: null,
                exitType: null,
                pnlDifference: null,
                alphaCost: null,
                createdAt: (/* @__PURE__ */ new Date()).toISOString()
              });
            } else {
              storage.insertExecutionDeviation({
                userTradeId: trade.id,
                signalId: null,
                classification: "independent",
                entryDelayDays: null,
                entryPriceGapPct: null,
                sizingDeviationPct: null,
                holdDeviationDays: null,
                exitType: null,
                pnlDifference: null,
                alphaCost: null,
                createdAt: (/* @__PURE__ */ new Date()).toISOString()
              });
            }
            createdDeviations++;
          } else if (side === "SELL") {
            const openBuys = db.all(import_drizzle_orm4.sql`
              SELECT te.* FROM trade_executions te
              WHERE te.ticker = ${ticker}
                AND te.side = 'BUY'
                AND te.id NOT IN (SELECT ct.id FROM closed_trades ct WHERE ct.ticker = ${ticker})
              ORDER BY te.execution_date ASC
              LIMIT 1
            `);
            const matchingBuy = openBuys[0];
            if (matchingBuy) {
              const entryDate = matchingBuy.execution_date;
              const entryPrice = matchingBuy.avg_price;
              const exitPrice = avgPrice;
              const holdingDays = Math.floor((new Date(executionDate).getTime() - new Date(entryDate).getTime()) / (1e3 * 60 * 60 * 24));
              const realizedPnl = (exitPrice - entryPrice) * quantity;
              const realizedPnlPct = entryPrice > 0 ? (exitPrice - entryPrice) / entryPrice * 100 : 0;
              const buyDeviation = db.all(import_drizzle_orm4.sql`
                SELECT ed.classification, ed.signal_id FROM execution_deviations ed
                WHERE ed.user_trade_id = ${matchingBuy.id}
                LIMIT 1
              `);
              storage.insertClosedTrade({
                ticker,
                companyName: leg.instrument?.description || ticker,
                entryDate,
                exitDate: executionDate,
                entryPrice,
                exitPrice,
                quantity,
                realizedPnl,
                realizedPnlPct,
                holdingDays,
                signalClassification: buyDeviation[0]?.classification || "independent",
                signalId: buyDeviation[0]?.signal_id || null,
                exitType: "discretionary",
                createdAt: (/* @__PURE__ */ new Date()).toISOString()
              });
              createdClosedTrades++;
            }
          }
        }
      }
      res.json({
        success: true,
        createdExecutions,
        createdDeviations,
        createdClosedTrades,
        totalOrders: orders?.length || 0
      });
    } catch (err) {
      console.error("[SCHWAB SYNC-ORDERS] Error:", err.message);
      res.status(500).json({ error: err.message });
    }
  });
  app2.get("/api/status", (_req, res) => {
    res.json(getPollerStatus());
  });
  const DEPLOY_SECRET = process.env.DEPLOY_SECRET || "9092e955d3811673c357dbd1e9205b36d96a85e76a782c4f8e8d4cc5be9cdb49";
  app2.post("/api/admin/deploy", async (req, res) => {
    const secret = req.headers["x-deploy-secret"] || req.query.secret;
    if (secret !== process.env.DEPLOY_SECRET && secret !== DEPLOY_SECRET) {
      return res.status(401).json({ error: "unauthorized" });
    }
    res.json({ status: "deploying", started: (/* @__PURE__ */ new Date()).toISOString() });
    const { exec } = require("child_process");
    exec(
      "cd /opt/insider-signal-dash && git pull origin master && bash script/setup-services.sh 2>&1 || true && sleep 1 && echo RESTARTING_POLLER && systemctl restart insider-signal-poller 2>&1 && echo POLLER_RESTARTED || echo POLLER_RESTART_FAILED && echo RESTARTING_WEB && systemctl restart insider-signal",
      { timeout: 3e5 },
      (error, stdout, _stderr) => {
        if (error) console.error("[DEPLOY] Failed:", error.message);
        else console.log("[DEPLOY] Success:", stdout.slice(-200));
      }
    );
  });
  app2.get("/api/admin/health", (req, res) => {
    const secret = req.headers["x-deploy-secret"] || req.query.secret;
    if (secret !== process.env.DEPLOY_SECRET && secret !== DEPLOY_SECRET) {
      return res.status(401).json({ error: "unauthorized" });
    }
    const { execSync } = require("child_process");
    try {
      const gitLog = execSync("cd /opt/insider-signal-dash && git log --oneline -5 2>/dev/null || echo 'no git'").toString();
      const svcStatus = execSync("systemctl is-active insider-signal 2>/dev/null || echo 'unknown'").toString().trim();
      res.json({ service: svcStatus, gitLog: gitLog.trim().split("\n") });
    } catch (err) {
      res.json({ error: err.message });
    }
  });
  app2.post("/api/admin/create-indexes", (req, res) => {
    const secret = req.headers["x-deploy-secret"] || req.query.secret;
    if (secret !== process.env.DEPLOY_SECRET && secret !== DEPLOY_SECRET) {
      return res.status(401).json({ error: "unauthorized" });
    }
    const { exec, execSync } = require("child_process");
    try {
      const DB = "/opt/insider-signal-dash/data.db";
      execSync(`sqlite3 ${DB} "
        CREATE INDEX IF NOT EXISTS idx_tx_type_filing_date ON insider_transactions(transaction_type, filing_date);
        CREATE INDEX IF NOT EXISTS idx_tx_accession ON insider_transactions(accession_number);
        CREATE INDEX IF NOT EXISTS idx_tx_ticker ON insider_transactions(issuer_ticker);
        CREATE INDEX IF NOT EXISTS idx_signals_date ON purchase_signals(signal_date);
        CREATE INDEX IF NOT EXISTS idx_signals_score ON purchase_signals(signal_score);
        CREATE INDEX IF NOT EXISTS idx_signals_tier ON purchase_signals(score_tier);
        CREATE INDEX IF NOT EXISTS idx_entry_prices_signal ON signal_entry_prices(signal_id);
        CREATE INDEX IF NOT EXISTS idx_factor_analysis_factor ON factor_analysis(factor_name, horizon);
        CREATE INDEX IF NOT EXISTS idx_exec_deviations_signal ON execution_deviations(signal_id);
        CREATE INDEX IF NOT EXISTS idx_exec_deviations_trade ON execution_deviations(user_trade_id);
      "`, { timeout: 12e4 });
      const indexCount = execSync(`sqlite3 ${DB} "SELECT count(*) FROM sqlite_master WHERE type='index';"`, { timeout: 1e4 }).toString().trim();
      exec(`sqlite3 ${DB} "
        CREATE INDEX IF NOT EXISTS idx_fwd_returns_signal_day ON daily_forward_returns(signal_id, trading_day);
        CREATE INDEX IF NOT EXISTS idx_fwd_returns_day ON daily_forward_returns(trading_day);
      "`, { timeout: 6e5 }, (err) => {
        if (err) console.error("[INDEXES] Forward return indexes failed:", err.message);
        else console.log("[INDEXES] Forward return indexes created");
      });
      res.json({ status: "core_indexes_created", totalIndexes: indexCount, note: "Forward return indexes creating in background" });
    } catch (err) {
      res.json({ error: err.message });
    }
  });
  let enrichmentRunning = false;
  let enrichmentContinuous = false;
  let lastEnrichedCount = 0;
  let enrichBatchCount = 0;
  function walCheckpoint() {
    try {
      const { execSync } = require("child_process");
      execSync('sqlite3 /opt/insider-signal-dash/data.db "PRAGMA wal_checkpoint(PASSIVE);"', { timeout: 12e4 });
      console.log("[DB] WAL checkpoint completed");
    } catch (e) {
      console.error("[DB] WAL checkpoint failed:", e.message);
    }
  }
  function runEnrichmentBatch() {
    const { exec } = require("child_process");
    enrichmentRunning = true;
    console.log("[ENRICH] Starting batch...");
    exec(
      "nice -n 10 python3 scripts/enrich-prices.py 2000 2010",
      { cwd: "/opt/insider-signal-dash", timeout: 6e5, env: { ...process.env, PYTHONUNBUFFERED: "1" } },
      (error, stdout, stderr) => {
        enrichmentRunning = false;
        if (error) console.error("[ENRICH] Batch failed:", error.message);
        if (stdout) console.log("[ENRICH] stdout:", stdout.slice(-500));
        if (stderr) console.error("[ENRICH] stderr:", stderr.slice(-500));
        enrichBatchCount++;
        if (enrichBatchCount % 3 === 0) walCheckpoint();
        if (enrichmentContinuous) {
          invalidatePipelineStatusCache();
          const status = getDataPipelineStatus();
          const currentEnriched = status.enrichedSignals || 0;
          const noSignals = stdout && stdout.includes("0 signals to enrich");
          const noProgress = currentEnriched === lastEnrichedCount && lastEnrichedCount > 0;
          if (noSignals || noProgress && !error) {
            console.log(`[ENRICH] No more enrichable signals (${currentEnriched} total enriched, ${status.enrichmentProgress}%). Stopping continuous mode.`);
            enrichmentContinuous = false;
            walCheckpoint();
            console.log("[PIPELINE] Enrichment complete. Auto-starting factor research...");
            const { exec: execFR } = require("child_process");
            execFR(
              "nice -n 10 python3 scripts/factor-research.py",
              { cwd: "/opt/insider-signal-dash", timeout: 6e5, env: { ...process.env, PYTHONUNBUFFERED: "1" } },
              (frError, frStdout, frStderr) => {
                if (frError) console.error("[PIPELINE] Factor research failed:", frError.message);
                if (frStdout) console.log("[PIPELINE] Factor research stdout:", frStdout.slice(-2e3));
                if (frStderr) console.error("[PIPELINE] Factor research stderr:", frStderr.slice(-2e3));
                console.log("[PIPELINE] Factor research complete. Auto-starting alpha decay...");
                precomputeAlphaDecay().then(() => {
                  console.log("[PIPELINE] Full pipeline complete: enrichment \u2192 factor research \u2192 alpha decay");
                }).catch((e) => console.error("[PIPELINE] Alpha decay failed:", e.message));
              }
            );
          } else {
            lastEnrichedCount = currentEnriched;
            const delay = error ? 3e4 : 5e3;
            console.log(`[ENRICH] Continuous mode: ${status.enrichmentProgress}% (${currentEnriched}/${status.totalSignals}). Next batch in ${delay / 1e3}s...${error ? " (retrying after error)" : ""}`);
            setTimeout(runEnrichmentBatch, delay);
          }
        }
      }
    );
  }
  app2.post("/api/admin/enrich", (req, res) => {
    const secret = req.headers["x-deploy-secret"] || req.query.secret;
    if (secret !== process.env.DEPLOY_SECRET && secret !== DEPLOY_SECRET) {
      return res.status(401).json({ error: "unauthorized" });
    }
    const continuous = req.query.continuous === "true" || req.body?.continuous === true;
    if (continuous) {
      enrichmentContinuous = true;
      if (!enrichmentRunning) {
        runEnrichmentBatch();
      }
      return res.json({ status: "enrichment_started", mode: "continuous", message: "Will auto-continue until all signals are enriched" });
    }
    if (enrichmentContinuous && enrichmentRunning) {
      enrichmentContinuous = false;
      return res.json({ status: "continuous_stopped", message: "Continuous enrichment will stop after current batch completes" });
    }
    enrichmentContinuous = false;
    if (!enrichmentRunning) {
      runEnrichmentBatch();
    }
    res.json({ status: "enrichment_started", mode: "single_batch" });
  });
  app2.post("/api/admin/backfill", (req, res) => {
    const secret = req.headers["x-deploy-secret"] || req.query.secret;
    if (secret !== process.env.DEPLOY_SECRET && secret !== DEPLOY_SECRET) {
      return res.status(401).json({ error: "unauthorized" });
    }
    const startYear = req.body?.startYear || 2026;
    const { exec } = require("child_process");
    exec(
      `nice -n 10 npx tsx server/sec-backfill.ts ${startYear}`,
      { cwd: "/opt/insider-signal-dash", timeout: 6e5, env: { ...process.env, NODE_OPTIONS: "--max-old-space-size=512" } },
      (error, stdout, stderr) => {
        if (error) console.error("[BACKFILL] Failed:", error.message);
        if (stdout) console.log("[BACKFILL] stdout:", stdout.slice(-500));
        if (stderr) console.error("[BACKFILL] stderr:", stderr.slice(-500));
      }
    );
    res.json({ status: "backfill_started", startYear });
  });
  app2.post("/api/admin/compute-trades", (req, res) => {
    const secret = req.headers["x-deploy-secret"] || req.query.secret;
    if (secret !== process.env.DEPLOY_SECRET && secret !== DEPLOY_SECRET) {
      return res.status(401).json({ error: "unauthorized" });
    }
    try {
      const signalResult = runSignalTradeMatching();
      console.log(`[COMPUTE] Signal matching: ${signalResult.matched} matched, ${signalResult.unmatched} unmatched`);
      const fifoResult = runFifoMatching();
      console.log(`[COMPUTE] FIFO matching: ${fifoResult.matched} closed trades created`);
      const perf = getPerformanceAnalytics();
      res.json({
        status: "completed",
        signalMatching: signalResult,
        fifoMatching: fifoResult,
        performance: perf ? {
          totalRealizedPnl: perf.totalRealizedPnl,
          winRate: perf.winRate,
          closedTradeCount: perf.closedTradeCount,
          profitFactor: perf.profitFactor
        } : null
      });
    } catch (err) {
      console.error("[COMPUTE] Error:", err.message);
      res.status(500).json({ error: err.message });
    }
  });
  app2.post("/api/admin/factor-research", (req, res) => {
    const secret = req.headers["x-deploy-secret"] || req.query.secret;
    if (secret !== process.env.DEPLOY_SECRET && secret !== DEPLOY_SECRET) {
      return res.status(401).json({ error: "unauthorized" });
    }
    const { exec } = require("child_process");
    exec(
      "nice -n 10 python3 scripts/factor-research.py",
      { cwd: "/opt/insider-signal-dash", timeout: 6e5, env: { ...process.env, PYTHONUNBUFFERED: "1" } },
      (error, stdout, stderr) => {
        if (error) console.error("[FACTOR-RESEARCH] Failed:", error.message);
        if (stdout) console.log("[FACTOR-RESEARCH] stdout:", stdout.slice(-500));
        if (stderr) console.error("[FACTOR-RESEARCH] stderr:", stderr.slice(-500));
        console.log("[PIPELINE] Factor research complete. Auto-starting alpha decay...");
        precomputeAlphaDecay().then(() => {
          console.log("[PIPELINE] Alpha decay complete after factor research.");
        }).catch((e) => console.error("[PIPELINE] Alpha decay failed:", e.message));
      }
    );
    res.json({ status: "factor_research_started" });
  });
  app2.post("/api/admin/precompute-alpha-decay", async (req, res) => {
    const secret = req.headers["x-deploy-secret"] || req.query.secret;
    if (secret !== process.env.DEPLOY_SECRET && secret !== DEPLOY_SECRET) {
      return res.status(401).json({ error: "unauthorized" });
    }
    precomputeAlphaDecay().catch((e) => console.error("[ADMIN] Alpha decay failed:", e.message));
    res.json({ status: "started", message: "Alpha decay pre-computation running in background" });
  });
  app2.post("/api/admin/backup", (req, res) => {
    const secret = req.headers["x-deploy-secret"] || req.query.secret;
    if (secret !== process.env.DEPLOY_SECRET && secret !== DEPLOY_SECRET) {
      return res.status(401).json({ error: "unauthorized" });
    }
    const { exec } = require("child_process");
    const backupScript = [
      "set -e",
      'sqlite3 /opt/insider-signal-dash/data.db ".backup /tmp/insider-signal-backup.db"',
      "FSIZE=$(stat -c%s /tmp/insider-signal-backup.db)",
      'echo "STEP1: SQLite backup done, bytes=$FSIZE"',
      `TOKEN=$(curl -s -H "Metadata-Flavor: Google" "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token" | python3 -c "import sys,json;print(json.load(sys.stdin)['access_token'])")`,
      'echo "STEP2: Got token"',
      'DEST="backups/data-$(date +%Y%m%d-%H%M%S).db"',
      'HTTP_CODE=$(curl -s -o /tmp/gcs-response.json -w "%{http_code}" -X POST -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/octet-stream" --data-binary @/tmp/insider-signal-backup.db "https://storage.googleapis.com/upload/storage/v1/b/insider-signal-deploys/o?uploadType=media&name=$DEST")',
      'echo "STEP3: HTTP $HTTP_CODE response: $(cat /tmp/gcs-response.json)"',
      "rm -f /tmp/insider-signal-backup.db /tmp/gcs-response.json",
      'if [ "$HTTP_CODE" != "200" ]; then echo "FAILED: Upload returned HTTP $HTTP_CODE"; exit 1; fi',
      'echo "DONE: Backed up to gs://insider-signal-deploys/$DEST"'
    ].join("\n");
    exec(
      backupScript,
      { timeout: 3e5, shell: "/bin/bash" },
      (error, stdout, stderr) => {
        if (error) console.error("[BACKUP] Failed:", error.message);
        if (stdout) console.log("[BACKUP] stdout:", stdout.slice(-1e3));
        if (stderr) console.error("[BACKUP] stderr:", stderr.slice(-1e3));
      }
    );
    res.json({ status: "backup_started" });
  });
  app2.get("/api/admin/test-gcs", (req, res) => {
    const secret = req.headers["x-deploy-secret"] || req.query.secret;
    if (secret !== process.env.DEPLOY_SECRET && secret !== DEPLOY_SECRET) {
      return res.status(401).json({ error: "unauthorized" });
    }
    const { exec } = require("child_process");
    const testCmd = [
      `TOKEN=$(curl -s -H "Metadata-Flavor: Google" "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token" | python3 -c "import sys,json;print(json.load(sys.stdin)['access_token'])")`,
      'curl -s -H "Authorization: Bearer $TOKEN" "https://storage.googleapis.com/storage/v1/b/insider-signal-deploys?fields=name,timeCreated"'
    ].join(" && ");
    exec(
      testCmd,
      { timeout: 3e4, shell: "/bin/bash" },
      (error, stdout, stderr) => {
        if (error) return res.json({ error: error.message, stderr });
        try {
          res.json({ gcsAccess: JSON.parse(stdout), status: "ok" });
        } catch {
          res.json({ rawResponse: stdout, stderr, status: "parse_error" });
        }
      }
    );
  });
  app2.get("/api/admin/logs", (req, res) => {
    const secret = req.headers["x-deploy-secret"] || req.query.secret;
    if (secret !== process.env.DEPLOY_SECRET && secret !== DEPLOY_SECRET) {
      return res.status(401).json({ error: "unauthorized" });
    }
    const { exec } = require("child_process");
    const lines = parseInt(req.query.lines) || 50;
    const grep = req.query.grep ? `| grep -i '${req.query.grep.replace(/'/g, "")}'` : "";
    const service = req.query.service === "poller" ? "insider-signal-poller" : "insider-signal";
    exec(
      `journalctl -u ${service} --no-pager -n ${lines} ${grep}`,
      { timeout: 1e4 },
      (error, stdout, stderr) => {
        if (error) return res.json({ error: error.message, stderr });
        res.json({ logs: stdout.split("\n").slice(-lines) });
      }
    );
  });
  app2.post("/api/admin/setup-systemd", (req, res) => {
    const secret = req.headers["x-deploy-secret"] || req.query.secret;
    if (secret !== process.env.DEPLOY_SECRET && secret !== DEPLOY_SECRET) {
      return res.status(401).json({ error: "unauthorized" });
    }
    const { exec } = require("child_process");
    exec(
      `mkdir -p /etc/systemd/system/insider-signal.service.d && cat > /etc/systemd/system/insider-signal.service.d/indexes.conf << 'CONF'
[Service]
ExecStartPost=/bin/bash /opt/insider-signal-dash/script/create-indexes.sh
CONF
systemctl daemon-reload`,
      { timeout: 3e4 },
      (error, stdout, stderr) => {
        if (error) console.error("[SETUP-SYSTEMD] Failed:", error.message);
        if (stdout) console.log("[SETUP-SYSTEMD] stdout:", stdout);
        if (stderr) console.error("[SETUP-SYSTEMD] stderr:", stderr);
      }
    );
    res.json({ status: "systemd_setup_started" });
  });
  app2.post("/api/admin/fix-deploy-cron", (req, res) => {
    const secret = req.headers["x-deploy-secret"] || req.query.secret;
    if (secret !== process.env.DEPLOY_SECRET && secret !== DEPLOY_SECRET) {
      return res.status(401).json({ error: "unauthorized" });
    }
    const { exec } = require("child_process");
    const deployScript = `#!/bin/bash
cd /opt/insider-signal-dash
git fetch origin master 2>/dev/null
LOCAL=$(git rev-parse HEAD)
REMOTE=$(git rev-parse origin/master)
if [ "$LOCAL" != "$REMOTE" ]; then
  echo "$(date) - Deploying: $LOCAL -> $REMOTE"
  git pull origin master
  systemctl restart insider-signal
  echo "$(date) - Deploy complete"
fi
`;
    exec(
      `cat > /opt/deploy.sh << 'DEPLOYSCRIPT'
${deployScript.trim()}
DEPLOYSCRIPT
chmod +x /opt/deploy.sh`,
      { timeout: 1e4 },
      (error, stdout, stderr) => {
        if (error) console.error("[FIX-DEPLOY-CRON] Failed:", error.message);
        if (stdout) console.log("[FIX-DEPLOY-CRON] stdout:", stdout);
        if (stderr) console.error("[FIX-DEPLOY-CRON] stderr:", stderr);
      }
    );
    res.json({ status: "deploy_cron_fixed" });
  });
  app2.post("/api/admin/cleanup", (req, res) => {
    const secret = req.headers["x-deploy-secret"] || req.query.secret;
    if (secret !== process.env.DEPLOY_SECRET && secret !== DEPLOY_SECRET) {
      return res.status(401).json({ error: "unauthorized" });
    }
    const { execSync } = require("child_process");
    try {
      const before = execSync(`sqlite3 /opt/insider-signal-dash/data.db "SELECT count(*) FROM insider_transactions WHERE transaction_type='P';"`).toString().trim();
      execSync('sqlite3 /opt/insider-signal-dash/data.db "DELETE FROM insider_transactions WHERE rowid NOT IN (SELECT MIN(rowid) FROM insider_transactions GROUP BY accession_number, reporting_person_cik, transaction_date, shares_traded);"', { timeout: 12e4 });
      execSync('sqlite3 /opt/insider-signal-dash/data.db "DELETE FROM purchase_signals WHERE rowid NOT IN (SELECT MIN(rowid) FROM purchase_signals GROUP BY issuer_ticker, signal_date);"', { timeout: 12e4 });
      const after = execSync(`sqlite3 /opt/insider-signal-dash/data.db "SELECT count(*) FROM insider_transactions WHERE transaction_type='P';"`).toString().trim();
      res.json({ before, after, status: "cleaned" });
    } catch (err) {
      res.json({ error: err.message });
    }
  });
  let lastHeartbeat = Date.now();
  setInterval(() => {
    lastHeartbeat = Date.now();
  }, 5e3);
  setInterval(() => {
    const lag = Date.now() - lastHeartbeat;
    if (lag > 3e4) {
      console.error(`[WATCHDOG] Event loop blocked for ${lag}ms, killing child processes`);
      const { execSync } = require("child_process");
      try {
        execSync("pkill -f 'enrich-prices.py' || true");
      } catch {
      }
      try {
        execSync("pkill -f 'sec-backfill.ts' || true");
      } catch {
      }
      try {
        execSync("pkill -f 'factor-research.py' || true");
      } catch {
      }
    }
  }, 1e4);
  app2.get("/api/ping", (_req, res) => {
    res.json({ ok: true, uptime: process.uptime(), lag: Date.now() - lastHeartbeat });
  });
  setTimeout(() => {
    try {
      invalidatePipelineStatusCache();
      const status = getDataPipelineStatus();
      const progress = status.enrichmentProgress ?? 0;
      if (progress < 100) {
        console.log(`[ENRICH] Auto-resuming continuous enrichment on startup (current: ${progress}%)`);
        enrichmentContinuous = true;
        if (!enrichmentRunning) {
          runEnrichmentBatch();
        }
      } else {
        console.log(`[ENRICH] Enrichment complete (100%), no auto-resume needed`);
      }
    } catch (e) {
      console.error("[ENRICH] Auto-resume check failed:", e.message);
    }
  }, 3e4);
  const BACKUP_INTERVAL = 24 * 60 * 60 * 1e3;
  function runScheduledBackup() {
    const { exec } = require("child_process");
    const backupScript = [
      "set -e",
      'sqlite3 /opt/insider-signal-dash/data.db ".backup /tmp/insider-signal-backup.db"',
      `TOKEN=$(curl -s -H "Metadata-Flavor: Google" "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token" | python3 -c "import sys,json;print(json.load(sys.stdin)['access_token'])")`,
      'DEST="backups/data-$(date +%Y%m%d-%H%M%S).db"',
      'curl -s -o /dev/null -w "%{http_code}" -X POST -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/octet-stream" --data-binary @/tmp/insider-signal-backup.db "https://storage.googleapis.com/upload/storage/v1/b/insider-signal-deploys/o?uploadType=media&name=$DEST" | grep -q 200 && echo "[BACKUP] Success: $DEST" || echo "[BACKUP] Failed"',
      "rm -f /tmp/insider-signal-backup.db"
    ].join("\n");
    exec(backupScript, { timeout: 3e5, shell: "/bin/bash" }, (error, stdout, stderr) => {
      if (error) console.error("[BACKUP] Scheduled backup failed:", error.message);
      if (stdout) console.log("[BACKUP]", stdout.trim());
    });
  }
  setTimeout(() => {
    runScheduledBackup();
    setInterval(runScheduledBackup, BACKUP_INTERVAL);
  }, 5 * 60 * 1e3);
  console.log("[BACKUP] Daily backup scheduled (first run in 5 minutes)");
  app2.get("/api/admin/system-health", (req, res) => {
    const secret = req.headers["authorization"]?.replace("Bearer ", "") || req.query.secret;
    if (secret !== process.env.DEPLOY_SECRET && secret !== DEPLOY_SECRET) {
      return res.status(401).json({ error: "unauthorized" });
    }
    const status = getDataPipelineStatus();
    let pollerStatus = { status: "unknown" };
    try {
      pollerStatus = JSON.parse(import_fs.default.readFileSync("/tmp/poller-status.json", "utf-8"));
    } catch {
    }
    let alphaDecayStatus = "missing";
    try {
      const stat = import_fs.default.statSync(ALPHA_DECAY_CACHE_PATH);
      const ageHours = (Date.now() - stat.mtimeMs) / (1e3 * 60 * 60);
      alphaDecayStatus = ageHours < 24 ? "fresh" : `stale (${Math.round(ageHours)}h old)`;
    } catch {
    }
    res.json({
      server: {
        uptime: process.uptime(),
        commit: "see gitLog"
      },
      poller: pollerStatus,
      enrichment: {
        running: enrichmentRunning,
        continuous: enrichmentContinuous,
        progress: status.enrichmentProgress,
        enrichedSignals: status.enrichedSignals,
        totalSignals: status.totalSignals
      },
      factorResearch: {
        factorAnalysisResults: status.factorAnalysisResults,
        modelFactors: status.modelFactors
      },
      alphaDecay: {
        cacheStatus: alphaDecayStatus
      },
      data: {
        totalPurchases: status.totalPurchases,
        forwardReturnDataPoints: status.forwardReturnDataPoints,
        insiderProfiles: status.insiderProfiles,
        failedTickers: status.failedTickers
      },
      disk: (() => {
        try {
          const { execSync } = require("child_process");
          const df = execSync("df -h / | tail -1").toString().trim().split(/\s+/);
          const du = execSync("du -sh /opt/insider-signal-dash/data/ /opt/insider-signal-dash/data/sec-raw/ 2>/dev/null || echo 'N/A'").toString().trim();
          return { filesystem: df[0], size: df[1], used: df[2], available: df[3], usePct: df[4], dataDirSizes: du };
        } catch {
          return { error: "could not check" };
        }
      })()
    });
  });
  app2.post("/api/admin/disk-cleanup", (req, res) => {
    const secret = req.headers["authorization"]?.replace("Bearer ", "") || req.headers["x-deploy-secret"] || req.query.secret;
    if (secret !== process.env.DEPLOY_SECRET && secret !== DEPLOY_SECRET) {
      return res.status(401).json({ error: "unauthorized" });
    }
    const { execSync } = require("child_process");
    try {
      const before = execSync("df / | tail -1").toString().trim().split(/\s+/);
      execSync("rm -rf /opt/insider-signal-dash/data/sec-raw/*", { timeout: 3e4 });
      execSync('sqlite3 /opt/insider-signal-dash/data.db "PRAGMA wal_checkpoint(TRUNCATE);"', { timeout: 18e4 });
      const after = execSync("df / | tail -1").toString().trim().split(/\s+/);
      const freedKB = parseInt(after[3]) - parseInt(before[3]);
      res.json({ status: "cleanup_complete", freedMB: Math.round(freedKB / 1024), diskBefore: before[4], diskAfter: after[4], availableAfter: after[3] + "K" });
    } catch (e) {
      res.status(500).json({ error: e.message });
    }
  });
  app2.post("/api/admin/shell", (req, res) => {
    const secret = req.headers["authorization"]?.replace("Bearer ", "") || req.headers["x-deploy-secret"] || req.query.secret;
    if (secret !== process.env.DEPLOY_SECRET && secret !== DEPLOY_SECRET) {
      return res.status(401).json({ error: "unauthorized" });
    }
    const cmd = req.body?.command;
    if (!cmd || typeof cmd !== "string") {
      return res.status(400).json({ error: "missing command" });
    }
    const { execSync } = require("child_process");
    try {
      const output = execSync(cmd, { cwd: "/opt/insider-signal-dash", timeout: 12e4, maxBuffer: 1024 * 1024 }).toString();
      res.json({ output });
    } catch (e) {
      res.status(500).json({ error: e.message, stderr: e.stderr?.toString()?.slice(-500) });
    }
  });
  return httpServer2;
}

// server/static.ts
var import_express = __toESM(require("express"), 1);
var import_fs2 = __toESM(require("fs"), 1);
var import_path = __toESM(require("path"), 1);
function serveStatic(app2) {
  const distPath = import_path.default.resolve(__dirname, "public");
  if (!import_fs2.default.existsSync(distPath)) {
    throw new Error(
      `Could not find the build directory: ${distPath}, make sure to build the client first`
    );
  }
  app2.use(import_express.default.static(distPath));
  app2.use("/{*path}", (_req, res) => {
    res.sendFile(import_path.default.resolve(distPath, "index.html"));
  });
}

// server/index.ts
var import_http = require("http");
var app = (0, import_express2.default)();
var httpServer = (0, import_http.createServer)(app);
app.use(
  import_express2.default.json({
    verify: (req, _res, buf) => {
      req.rawBody = buf;
    }
  })
);
app.use(import_express2.default.urlencoded({ extended: false }));
function log(message, source = "express") {
  const formattedTime = (/* @__PURE__ */ new Date()).toLocaleTimeString("en-US", {
    hour: "numeric",
    minute: "2-digit",
    second: "2-digit",
    hour12: true
  });
  console.log(`${formattedTime} [${source}] ${message}`);
}
app.use((req, res, next) => {
  const start = Date.now();
  const path4 = req.path;
  let capturedJsonResponse = void 0;
  const originalResJson = res.json;
  res.json = function(bodyJson, ...args) {
    capturedJsonResponse = bodyJson;
    return originalResJson.apply(res, [bodyJson, ...args]);
  };
  res.on("finish", () => {
    const duration = Date.now() - start;
    if (path4.startsWith("/api")) {
      let logLine = `${req.method} ${path4} ${res.statusCode} in ${duration}ms`;
      if (capturedJsonResponse) {
        logLine += ` :: ${JSON.stringify(capturedJsonResponse)}`;
      }
      log(logLine);
    }
  });
  next();
});
(async () => {
  await registerRoutes(httpServer, app);
  app.use((err, _req, res, next) => {
    const status = err.status || err.statusCode || 500;
    const message = err.message || "Internal Server Error";
    console.error("Internal Server Error:", err);
    if (res.headersSent) {
      return next(err);
    }
    return res.status(status).json({ message });
  });
  if (process.env.NODE_ENV === "production") {
    serveStatic(app);
  } else {
    const { setupVite: setupVite2 } = await Promise.resolve().then(() => (init_vite(), vite_exports));
    await setupVite2(httpServer, app);
  }
  const port = parseInt(process.env.PORT || "5000", 10);
  httpServer.listen(
    {
      port,
      host: "0.0.0.0",
      reusePort: true
    },
    () => {
      log(`serving on port ${port}`);
    }
  );
})();
// Annotate the CommonJS export names for ESM import in node:
0 && (module.exports = {
  log
});
