import type { Express } from "express";
import { createServer, type Server } from "http";
import { storage } from "./storage";
import { startV3Polling, getV3PollingStatus } from "./edgar-poller-v3";
import {
  getFactorResults, getFactorEffectiveness, getFactorHeatmap,
  getAlphaDecayCurve, getModelWeights, getScoredSignals,
  getPerformanceSnapshots, getPerformanceSummary,
  getExecutionSummary, getTradeDeviations, getMissedSignals,
  getPortfolioWithSignalHealth, getDataPipelineStatus,
} from "./v3-strategy";

export async function registerRoutes(
  httpServer: Server,
  app: Express
): Promise<Server> {

  // Start the V3 dual-mode EDGAR polling engine
  startV3Polling();

  // ============================================================
  // TAB 1: SIGNALS
  // ============================================================

  /** Dashboard summary KPIs */
  app.get("/api/dashboard", (_req, res) => {
    try {
    // Try recent data first, fall back to all-time for historical-only mode
    let purchaseCount30d = storage.getPurchaseCount(30);
    let purchaseCount7d = storage.getPurchaseCount(7);
    let purchaseCount1d = storage.getPurchaseCount(1);
    let volume30d = storage.getRecentPurchaseVolume(30);
    let volume7d = storage.getRecentPurchaseVolume(7);
    const totalTransactions = storage.getTransactionCount();
    let clusters = storage.getClusterBuys(30);
    const pollingStatus = getV3PollingStatus();

    // If no recent data, show last 90 days of historical data
    if (purchaseCount30d === 0) {
      purchaseCount30d = storage.getPurchaseCount(90);
      purchaseCount7d = storage.getPurchaseCount(90);
      volume30d = storage.getRecentPurchaseVolume(90);
      volume7d = storage.getRecentPurchaseVolume(90);
      clusters = storage.getClusterBuys(90);
    }
    // If still no data, use all-time
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
        clusterCount: clusters.length,
      },
      pollingStatus,
    });
    } catch (err: any) {
      console.error("[/api/dashboard ERROR]", err.message);
      res.status(500).json({ error: err.message });
    }
  });

  /** Scored signals feed with factor breakdowns */
  app.get("/api/signals", (req, res) => {
    const limit = parseInt(req.query.limit as string) || 50;
    const minScore = req.query.minScore ? parseInt(req.query.minScore as string) : undefined;
    res.json(getScoredSignals(limit, minScore));
  });

  /** Transactions feed — with auto-widen fallback */
  app.get("/api/transactions", (req, res) => {
    const limit = parseInt(req.query.limit as string) || 100;
    const type = req.query.type as string;
    const days = parseInt(req.query.days as string) || 30;
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

  /** Analytics endpoints — with auto-widen fallback for historical-only data */
  app.get("/api/analytics/daily-volume", (req, res) => {
    let days = parseInt(req.query.days as string) || 30;
    let result = storage.getDailyPurchaseVolume(days);
    if (result.length === 0) result = storage.getDailyPurchaseVolume(90);
    if (result.length === 0) result = storage.getDailyPurchaseVolume(365);
    if (result.length === 0) result = storage.getDailyPurchaseVolume(36500);
    res.json(result);
  });

  app.get("/api/analytics/cluster-buys", (req, res) => {
    let days = parseInt(req.query.days as string) || 30;
    let result = storage.getClusterBuys(days);
    if (result.length === 0) result = storage.getClusterBuys(90);
    if (result.length === 0) result = storage.getClusterBuys(365);
    if (result.length === 0) result = storage.getClusterBuys(36500);
    res.json(result);
  });

  app.get("/api/analytics/insider-types", (req, res) => {
    let days = parseInt(req.query.days as string) || 30;
    let result = storage.getInsiderTypeBreakdown(days);
    if (result.length === 0) result = storage.getInsiderTypeBreakdown(90);
    if (result.length === 0) result = storage.getInsiderTypeBreakdown(365);
    if (result.length === 0) result = storage.getInsiderTypeBreakdown(36500);
    res.json(result);
  });

  // ============================================================
  // TAB 2: FACTOR RESEARCH
  // ============================================================

  /** Factor effectiveness summary — all factors ranked by IR */
  app.get("/api/factors/effectiveness", (_req, res) => {
    res.json(getFactorEffectiveness());
  });

  /** Factor analysis results — optionally filtered by horizon */
  app.get("/api/factors/analysis", (req, res) => {
    const horizon = req.query.horizon ? parseInt(req.query.horizon as string) : undefined;
    res.json(getFactorResults(horizon));
  });

  /** Factor heatmap — slices × horizons for a specific factor */
  app.get("/api/factors/heatmap/:factorName", (req, res) => {
    res.json(getFactorHeatmap(req.params.factorName));
  });

  /** Alpha decay curve — average excess return at each trading day */
  app.get("/api/factors/alpha-decay", (req, res) => {
    const scoreTier = req.query.tier ? parseInt(req.query.tier as string) : undefined;
    res.json(getAlphaDecayCurve({ scoreTier }));
  });

  /** Model weights — data-derived scoring weights */
  app.get("/api/factors/model-weights", (_req, res) => {
    res.json(getModelWeights());
  });

  // ============================================================
  // TAB 3: PORTFOLIO
  // ============================================================

  /** Portfolio positions with signal health */
  app.get("/api/portfolio/positions", (_req, res) => {
    const positions = getPortfolioWithSignalHealth();
    const totalValue = positions.reduce((s, p) => s + (p.marketValue || 0), 0);
    const totalPnl = positions.reduce((s, p) => s + (p.unrealizedPnl || 0), 0);
    const totalCost = positions.reduce((s, p) => s + (p.avgCostBasis * p.quantity), 0);
    const signalAligned = positions.filter(p => p.signalClassification === "signal_aligned").length;

    res.json({
      positions,
      summary: {
        totalValue,
        totalCost,
        totalPnl,
        totalPnlPct: totalCost > 0 ? (totalPnl / totalCost) * 100 : 0,
        positionCount: positions.length,
        signalAlignedCount: signalAligned,
        independentCount: positions.length - signalAligned,
      },
    });
  });

  app.get("/api/portfolio/executions", (req, res) => {
    const limit = parseInt(req.query.limit as string) || 100;
    res.json(storage.getTradeExecutions(limit));
  });

  app.get("/api/portfolio/closed-trades", (req, res) => {
    const limit = parseInt(req.query.limit as string) || 100;
    res.json(storage.getClosedTrades(limit));
  });

  // ============================================================
  // TAB 4: PERFORMANCE
  // ============================================================

  /** Performance summary — three-way comparison KPIs */
  app.get("/api/performance/summary", (_req, res) => {
    res.json(getPerformanceSummary());
  });

  /** Performance chart data — equity curves for strategy, user, benchmark */
  app.get("/api/performance/chart", (req, res) => {
    const days = parseInt(req.query.days as string) || 90;
    res.json(getPerformanceSnapshots(days));
  });

  // ============================================================
  // TAB 5: EXECUTION ANALYSIS
  // ============================================================

  /** Execution analysis summary KPIs */
  app.get("/api/execution/summary", (_req, res) => {
    res.json(getExecutionSummary());
  });

  /** Trade-level deviations */
  app.get("/api/execution/deviations", (req, res) => {
    const limit = parseInt(req.query.limit as string) || 100;
    res.json(getTradeDeviations(limit));
  });

  /** Missed signals — tier 1-2 signals the user didn't trade */
  app.get("/api/execution/missed-signals", (req, res) => {
    const days = parseInt(req.query.days as string) || 90;
    res.json(getMissedSignals(days));
  });

  // ============================================================
  // TAB 6: SETTINGS
  // ============================================================

  /** Data pipeline status */
  app.get("/api/settings/pipeline-status", (_req, res) => {
    res.json(getDataPipelineStatus());
  });

  /** Schwab integration status */
  app.get("/api/schwab/status", (_req, res) => {
    const config = storage.getSchwabConfig();
    res.json({
      isConnected: config?.isConnected || false,
      status: config?.status || "disconnected",
      lastSyncAt: config?.lastSyncAt || null,
      accountNumber: config?.accountNumber ? `****${config.accountNumber.slice(-4)}` : null,
    });
  });

  /** Schwab OAuth setup */
  app.post("/api/schwab/configure", (req, res) => {
    const { appKey, appSecret, callbackUrl } = req.body;
    if (!appKey || !appSecret) {
      return res.status(400).json({ error: "appKey and appSecret are required" });
    }
    storage.upsertSchwabConfig({ appKey, appSecret, status: "pending_auth" });
    const authUrl = `https://api.schwabapi.com/v1/oauth/authorize?client_id=${encodeURIComponent(appKey)}&redirect_uri=${encodeURIComponent(callbackUrl || "https://127.0.0.1")}&response_type=code`;
    res.json({ authUrl, message: "Visit the authorization URL to connect your Schwab account." });
  });

  /** Schwab OAuth callback */
  app.post("/api/schwab/callback", async (req, res) => {
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
        body: new URLSearchParams({ grant_type: "authorization_code", code, redirect_uri: callbackUrl || "https://127.0.0.1" }),
      });
      if (!tokenResp.ok) return res.status(400).json({ error: `Token exchange failed: ${await tokenResp.text()}` });
      const tokens = await tokenResp.json() as any;
      storage.upsertSchwabConfig({
        accessToken: tokens.access_token, refreshToken: tokens.refresh_token,
        tokenExpiresAt: new Date(Date.now() + tokens.expires_in * 1000).toISOString(),
        isConnected: true, status: "connected", lastSyncAt: new Date().toISOString(),
      });
      res.json({ success: true, message: "Schwab account connected." });
    } catch (err: any) {
      res.status(500).json({ error: err.message });
    }
  });

  /** Polling status */
  app.get("/api/status", (_req, res) => {
    res.json(getV3PollingStatus());
  });

  // ============================================================
  // ADMIN: Remote Deploy & Health
  // ============================================================

  const DEPLOY_SECRET = "9092e955d3811673c357dbd1e9205b36d96a85e76a782c4f8e8d4cc5be9cdb49";

  app.post("/api/admin/deploy", async (req, res) => {
    const secret = req.headers["x-deploy-secret"] || req.query.secret;
    if (secret !== process.env.DEPLOY_SECRET && secret !== DEPLOY_SECRET) {
      return res.status(401).json({ error: "unauthorized" });
    }
    res.json({ status: "deploying", started: new Date().toISOString() });
    const { exec } = require("child_process");
    exec("cd /opt/insider-signal-dash && git pull origin master && npm install --production=false && sleep 1 && systemctl restart insider-signal",
      { timeout: 300000 },
      (error: any, stdout: string, _stderr: string) => {
        if (error) console.error("[DEPLOY] Failed:", error.message);
        else console.log("[DEPLOY] Success:", stdout.slice(-200));
      }
    );
  });

  app.get("/api/admin/health", (req, res) => {
    const secret = req.headers["x-deploy-secret"] || req.query.secret;
    if (secret !== process.env.DEPLOY_SECRET && secret !== DEPLOY_SECRET) {
      return res.status(401).json({ error: "unauthorized" });
    }
    const { execSync } = require("child_process");
    try {
      const gitLog = execSync("cd /opt/insider-signal-dash && git log --oneline -5 2>/dev/null || echo 'no git'").toString();
      const svcStatus = execSync("systemctl is-active insider-signal 2>/dev/null || echo 'unknown'").toString().trim();
      res.json({ service: svcStatus, gitLog: gitLog.trim().split("\n") });
    } catch (err: any) {
      res.json({ error: err.message });
    }
  });


  // Admin: create database indexes for performance
  app.post("/api/admin/create-indexes", (req, res) => {
    const secret = req.headers["x-deploy-secret"] || req.query.secret;
    if (secret !== process.env.DEPLOY_SECRET && secret !== DEPLOY_SECRET) {
      return res.status(401).json({ error: "unauthorized" });
    }
    const { exec, execSync } = require("child_process");
    try {
      const DB = "/opt/insider-signal-dash/data.db";
      // Phase 1: Critical indexes (fast, sync)
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
      "`, { timeout: 120000 });
      const indexCount = execSync(`sqlite3 ${DB} "SELECT count(*) FROM sqlite_master WHERE type='index';"`, { timeout: 10000 }).toString().trim();
      
      // Phase 2: Expensive indexes (async, non-blocking)
      exec(`sqlite3 ${DB} "
        CREATE INDEX IF NOT EXISTS idx_fwd_returns_signal_day ON daily_forward_returns(signal_id, trading_day);
        CREATE INDEX IF NOT EXISTS idx_fwd_returns_day ON daily_forward_returns(trading_day);
      "`, { timeout: 600000 }, (err: any) => {
        if (err) console.error("[INDEXES] Forward return indexes failed:", err.message);
        else console.log("[INDEXES] Forward return indexes created");
      });
      
      res.json({ status: "core_indexes_created", totalIndexes: indexCount, note: "Forward return indexes creating in background" });
    } catch (err: any) {
      res.json({ error: err.message });
    }
  });

  // Admin: database cleanup
  app.post("/api/admin/cleanup", (req, res) => {
    const secret = req.headers["x-deploy-secret"] || req.query.secret;
    if (secret !== process.env.DEPLOY_SECRET && secret !== DEPLOY_SECRET) {
      return res.status(401).json({ error: "unauthorized" });
    }
    const { execSync } = require("child_process");
    try {
      const before = execSync("sqlite3 /opt/insider-signal-dash/data.db \"SELECT count(*) FROM insider_transactions WHERE transaction_type='P';\"").toString().trim();
      execSync("sqlite3 /opt/insider-signal-dash/data.db \"DELETE FROM insider_transactions WHERE rowid NOT IN (SELECT MIN(rowid) FROM insider_transactions GROUP BY accession_number, reporting_person_cik, transaction_date, shares_traded);\"", { timeout: 120000 });
      execSync("sqlite3 /opt/insider-signal-dash/data.db \"DELETE FROM purchase_signals WHERE rowid NOT IN (SELECT MIN(rowid) FROM purchase_signals GROUP BY issuer_ticker, signal_date);\"", { timeout: 120000 });
      const after = execSync("sqlite3 /opt/insider-signal-dash/data.db \"SELECT count(*) FROM insider_transactions WHERE transaction_type='P';\"").toString().trim();
      res.json({ before, after, status: "cleaned" });
    } catch (err: any) {
      res.json({ error: err.message });
    }
  });

  return httpServer;
}
