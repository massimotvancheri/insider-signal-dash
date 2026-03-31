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

  /** Transactions feed */
  app.get("/api/transactions", (req, res) => {
    const limit = parseInt(req.query.limit as string) || 100;
    const type = req.query.type as string;
    const days = parseInt(req.query.days as string) || 30;
    if (type === "P") {
      res.json(storage.getPurchaseTransactions(limit, days));
    } else {
      res.json(storage.getTransactions(limit));
    }
  });

  /** Analytics endpoints */
  app.get("/api/analytics/daily-volume", (req, res) => {
    const days = parseInt(req.query.days as string) || 30;
    res.json(storage.getDailyPurchaseVolume(days));
  });

  app.get("/api/analytics/cluster-buys", (req, res) => {
    const days = parseInt(req.query.days as string) || 30;
    res.json(storage.getClusterBuys(days));
  });

  app.get("/api/analytics/insider-types", (req, res) => {
    const days = parseInt(req.query.days as string) || 30;
    res.json(storage.getInsiderTypeBreakdown(days));
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

  return httpServer;
}
