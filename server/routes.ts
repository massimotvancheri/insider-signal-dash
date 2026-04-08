import type { Express } from "express";
import { createServer, type Server } from "http";
import { storage } from "./storage";
import { db } from "./db";
import { sql } from "drizzle-orm";
import fs, { readFileSync } from "fs";
import {
  getFactorResults, getFactorEffectiveness, getFactorHeatmap,
  getAlphaDecayCurve, getModelWeights, getScoredSignals,
  getPerformanceSnapshots, getPerformanceSummary,
  getExecutionSummary, getTradeDeviations, getMissedSignals,
  getPortfolioWithSignalHealth, getDataPipelineStatus, invalidatePipelineStatusCache,
  precomputeAlphaDecay, ALPHA_DECAY_CACHE_PATH,
} from "./v3-strategy";
import {
  runFifoMatching, getPerformanceAnalytics, getEquityCurve,
  runSignalTradeMatching, getEnhancedMissedSignals,
} from "./trade-engine";

/** Read poller status from the file written by the standalone poller process */
function getPollerStatus(): Record<string, unknown> {
  try {
    return JSON.parse(readFileSync("/tmp/poller-status.json", "utf-8"));
  } catch {
    return { active: false, mode: "separate process (status file not found)" };
  }
}

export async function registerRoutes(
  httpServer: Server,
  app: Express
): Promise<Server> {

  // Poller runs as a separate process (server/poller.ts / dist/poller.cjs)
  // Status is read from /tmp/poller-status.json written by the poller process

  // Auto-run trade matching on startup (if trades exist but no closed trades)
  try {
    const tradeCount = db.all(sql`SELECT count(*) as c FROM trade_executions`)[0] as any;
    const closedCount = db.all(sql`SELECT count(*) as c FROM closed_trades`)[0] as any;
    if (tradeCount?.c > 0 && closedCount?.c === 0) {
      console.log("[STARTUP] Running signal-trade matching and FIFO matching...");
      const sigResult = runSignalTradeMatching();
      console.log(`[STARTUP] Signal matching: ${sigResult.matched} matched, ${sigResult.unmatched} unmatched`);
      const fifoResult = runFifoMatching();
      console.log(`[STARTUP] FIFO matching: ${fifoResult.matched} closed trades created`);
    }
  } catch (e: any) {
    console.error("[STARTUP] Trade matching failed:", e.message);
  }

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
    const pollingStatus = getPollerStatus();

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

  /** Analytics endpoints — auto-widen fallback (needs ≥10 points for useful charts) */
  const MIN_CHART_POINTS = 10;
  app.get("/api/analytics/daily-volume", (req, res) => {
    const days = parseInt(req.query.days as string) || 30;
    for (const d of [days, 90, 365, 36500]) {
      const result = storage.getDailyPurchaseVolume(d);
      if (result.length >= MIN_CHART_POINTS || d === 36500) return res.json(result);
    }
  });

  app.get("/api/analytics/cluster-buys", (req, res) => {
    const days = parseInt(req.query.days as string) || 30;
    for (const d of [days, 90, 365, 36500]) {
      const result = storage.getClusterBuys(d);
      if (result.length >= MIN_CHART_POINTS || d === 36500) return res.json(result);
    }
  });

  app.get("/api/analytics/insider-types", (req, res) => {
    const days = parseInt(req.query.days as string) || 30;
    for (const d of [days, 90, 365, 36500]) {
      const result = storage.getInsiderTypeBreakdown(d);
      if (result.length >= MIN_CHART_POINTS || d === 36500) return res.json(result);
    }
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

  /** Alpha decay curve — average excess return at each trading day (cached, heavy query) */
  app.get("/api/factors/alpha-decay", (req, res) => {
    try {
      const scoreTier = req.query.tier ? parseInt(req.query.tier as string) : undefined;
      res.json(getAlphaDecayCurve({ scoreTier }));
    } catch (err: any) {
      console.error("[/api/factors/alpha-decay ERROR]", err.message);
      res.status(500).json({ error: "Alpha decay computation failed", detail: err.message });
    }
  });

  /** Model weights — data-derived scoring weights */
  app.get("/api/factors/model-weights", (_req, res) => {
    res.json(getModelWeights());
  });

  // ============================================================
  // TAB 3: PORTFOLIO
  // ============================================================

  /** Portfolio positions with signal health — pulls live Schwab data */
  app.get("/api/portfolio/positions", async (_req, res) => {
    try {
      const positions = await getPortfolioWithSignalHealth(getSchwabAccessToken);
      const totalValue = positions.reduce((s: number, p: any) => s + (p.marketValue || 0), 0);
      const totalPnl = positions.reduce((s: number, p: any) => s + (p.unrealizedPnl || 0), 0);
      const totalCost = positions.reduce((s: number, p: any) => s + (p.avgCostBasis * p.quantity), 0);
      const totalDayChange = positions.reduce((s: number, p: any) => s + (p.dayChange || 0), 0);
      const signalAligned = positions.filter((p: any) => p.signalClassification === "signal_aligned").length;

      res.json({
        positions,
        summary: {
          totalValue,
          totalCost,
          totalPnl,
          totalPnlPct: totalCost > 0 ? (totalPnl / totalCost) * 100 : 0,
          totalDayChange,
          totalDayChangePct: totalValue > 0 ? (totalDayChange / totalValue) * 100 : 0,
          positionCount: positions.length,
          signalAlignedCount: signalAligned,
          independentCount: positions.length - signalAligned,
        },
      });
    } catch (err: any) {
      console.error("[/api/portfolio/positions ERROR]", err.message);
      res.status(500).json({ error: err.message });
    }
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

  /** Performance summary — trade-based analytics with realized/unrealized P&L */
  app.get("/api/performance/summary", async (_req, res) => {
    try {
      // Try trade-engine analytics first (based on actual closed trades)
      const analytics = getPerformanceAnalytics();
      if (analytics) {
        // If DB positions show zero unrealized P&L, try Schwab live positions
        if (analytics.totalUnrealizedPnl === 0 && analytics.openPositionCount === 0) {
          try {
            const positions = await getPortfolioWithSignalHealth(getSchwabAccessToken);
            if (positions.length > 0) {
              const schwabUnrealized = positions.reduce((s: number, p: any) => s + (p.unrealizedPnl || 0), 0);
              const schwabMarketValue = positions.reduce((s: number, p: any) => s + (p.marketValue || 0), 0);
              analytics.totalUnrealizedPnl = schwabUnrealized;
              analytics.totalMarketValue = schwabMarketValue;
              analytics.openPositionCount = positions.length;
              analytics.combinedPnl = analytics.totalRealizedPnl + schwabUnrealized;
            }
          } catch (e: any) {
            console.error("[PERF SUMMARY] Schwab position fetch failed:", e.message);
          }
        }
        return res.json(analytics);
      }
      // Fall back to snapshot-based summary
      res.json(getPerformanceSummary());
    } catch (err: any) {
      console.error("[/api/performance/summary ERROR]", err.message);
      res.status(500).json({ error: err.message });
    }
  });

  /** Performance chart data — equity curves for strategy, user, benchmark */
  app.get("/api/performance/chart", (req, res) => {
    const days = parseInt(req.query.days as string) || 90;
    res.json(getPerformanceSnapshots(days));
  });

  /** Equity curve — cumulative P&L over time from closed trades */
  app.get("/api/performance/equity-curve", (_req, res) => {
    res.json(getEquityCurve());
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

  /** Missed signals — high-scoring signals the user didn't trade */
  app.get("/api/execution/missed-signals", (req, res) => {
    const days = parseInt(req.query.days as string) || 90;
    const minScore = parseInt(req.query.minScore as string) || 70;
    // Use enhanced version that filters by traded tickers and estimates alpha missed
    const enhanced = getEnhancedMissedSignals(days, minScore);
    if (enhanced.length > 0) {
      return res.json(enhanced);
    }
    // Fall back to original query
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

  /** Refresh Schwab access token using refresh token */
  async function refreshSchwabToken(): Promise<boolean> {
    const config = storage.getSchwabConfig();
    if (!config?.refreshToken || !config?.appKey || !config?.appSecret) return false;
    try {
      const basicAuth = Buffer.from(`${config.appKey}:${config.appSecret}`).toString("base64");
      const resp = await fetch("https://api.schwabapi.com/v1/oauth/token", {
        method: "POST",
        headers: { "Authorization": `Basic ${basicAuth}`, "Content-Type": "application/x-www-form-urlencoded" },
        body: new URLSearchParams({ grant_type: "refresh_token", refresh_token: config.refreshToken }),
      });
      if (!resp.ok) {
        console.error("[SCHWAB] Token refresh failed:", await resp.text());
        return false;
      }
      const tokens = await resp.json() as any;
      storage.upsertSchwabConfig({
        accessToken: tokens.access_token,
        refreshToken: tokens.refresh_token || config.refreshToken,
        tokenExpiresAt: new Date(Date.now() + tokens.expires_in * 1000).toISOString(),
      });
      return true;
    } catch (err: any) {
      console.error("[SCHWAB] Token refresh error:", err.message);
      return false;
    }
  }

  /** Get a valid Schwab access token, refreshing if expired */
  async function getSchwabAccessToken(): Promise<string | null> {
    const config = storage.getSchwabConfig();
    if (!config?.accessToken) return null;
    // Check if token is expired (with 60s buffer)
    if (config.tokenExpiresAt && new Date(config.tokenExpiresAt).getTime() < Date.now() + 60000) {
      const refreshed = await refreshSchwabToken();
      if (!refreshed) return null;
      return storage.getSchwabConfig()?.accessToken || null;
    }
    return config.accessToken;
  }

  /** Schwab accounts — fetch account numbers and hashes */
  app.get("/api/schwab/accounts", async (_req, res) => {
    try {
      const token = await getSchwabAccessToken();
      if (!token) return res.status(401).json({ error: "Schwab not connected or token expired" });
      const resp = await fetch("https://api.schwabapi.com/trader/v1/accounts?fields=positions", {
        headers: { "Authorization": `Bearer ${token}` },
      });
      if (!resp.ok) return res.status(resp.status).json({ error: `Schwab API error: ${await resp.text()}` });
      const accounts = await resp.json() as any[];
      // Store account number from first account
      if (accounts?.length > 0) {
        const acct = accounts[0];
        const acctNum = acct.securitiesAccount?.accountNumber || acct.accountNumber;
        if (acctNum) {
          storage.upsertSchwabConfig({ accountNumber: acctNum, lastSyncAt: new Date().toISOString() });
        }
      }
      res.json(accounts);
    } catch (err: any) {
      res.status(500).json({ error: err.message });
    }
  });

  /** Schwab positions — fetch positions for the connected account */
  app.get("/api/schwab/positions", async (_req, res) => {
    try {
      const token = await getSchwabAccessToken();
      if (!token) return res.status(401).json({ error: "Schwab not connected or token expired" });
      const resp = await fetch("https://api.schwabapi.com/trader/v1/accounts?fields=positions", {
        headers: { "Authorization": `Bearer ${token}` },
      });
      if (!resp.ok) return res.status(resp.status).json({ error: `Schwab API error: ${await resp.text()}` });
      const accounts = await resp.json() as any[];
      const positions: any[] = [];
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
            currentDayPnlPct: pos.currentDayProfitLossPercentage || 0,
          });
        }
      }
      storage.upsertSchwabConfig({ lastSyncAt: new Date().toISOString() });
      res.json(positions);
    } catch (err: any) {
      res.status(500).json({ error: err.message });
    }
  });

  /** Schwab orders — fetch order history */
  app.get("/api/schwab/orders", async (req, res) => {
    try {
      const token = await getSchwabAccessToken();
      if (!token) return res.status(401).json({ error: "Schwab not connected or token expired" });
      // Default to past 60 days of orders
      const fromDate = new Date();
      fromDate.setDate(fromDate.getDate() - 60);
      const toDate = new Date();
      const params = new URLSearchParams({
        fromEnteredTime: fromDate.toISOString(),
        toEnteredTime: toDate.toISOString(),
      });
      const resp = await fetch(`https://api.schwabapi.com/trader/v1/orders?${params}`, {
        headers: { "Authorization": `Bearer ${token}` },
      });
      if (!resp.ok) return res.status(resp.status).json({ error: `Schwab API error: ${await resp.text()}` });
      const orders = await resp.json() as any[];
      res.json(orders || []);
    } catch (err: any) {
      res.status(500).json({ error: err.message });
    }
  });

  /** Schwab sync — refresh positions and sync to local DB */
  app.post("/api/schwab/sync", async (_req, res) => {
    try {
      const token = await getSchwabAccessToken();
      if (!token) return res.status(401).json({ error: "Schwab not connected or token expired" });
      const resp = await fetch("https://api.schwabapi.com/trader/v1/accounts?fields=positions", {
        headers: { "Authorization": `Bearer ${token}` },
      });
      if (!resp.ok) return res.status(resp.status).json({ error: `Schwab API error: ${await resp.text()}` });
      const accounts = await resp.json() as any[];
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
            unrealizedPnl: pos.longQuantity * ((pos.marketValue / (pos.longQuantity || 1)) - (pos.averagePrice || 0)),
            unrealizedPnlPct: pos.averagePrice ? (((pos.marketValue / (pos.longQuantity || 1)) - pos.averagePrice) / pos.averagePrice * 100) : 0,
            dayChange: pos.currentDayProfitLoss || 0,
            dayChangePct: pos.currentDayProfitLossPercentage || 0,
            source: "schwab",
            lastSyncedAt: new Date().toISOString(),
          });
          syncedCount++;
        }
      }
      storage.upsertSchwabConfig({ lastSyncAt: new Date().toISOString() });
      // Auto-compute trades after successful sync
      try {
        const signalResult = runSignalTradeMatching();
        const fifoResult = runFifoMatching();
        console.log(`[PIPELINE] Post-sync trade compute: ${signalResult.matched} signal matches, ${fifoResult.matched} FIFO matches`);
      } catch (e: any) {
        console.error("[PIPELINE] Post-sync trade compute failed:", e.message);
      }
      res.json({ success: true, syncedPositions: syncedCount });
    } catch (err: any) {
      res.status(500).json({ error: err.message });
    }
  });

  /** Schwab orders sync — fetch filled orders and create trade executions & deviations */
  app.post("/api/schwab/sync-orders", async (_req, res) => {
    try {
      const token = await getSchwabAccessToken();
      if (!token) return res.status(401).json({ error: "Schwab not connected or token expired" });

      // Fetch orders from past 60 days
      const fromDate = new Date();
      fromDate.setDate(fromDate.getDate() - 60);
      const toDate = new Date();
      const params = new URLSearchParams({
        fromEnteredTime: fromDate.toISOString(),
        toEnteredTime: toDate.toISOString(),
      });
      const resp = await fetch(`https://api.schwabapi.com/trader/v1/orders?${params}`, {
        headers: { "Authorization": `Bearer ${token}` },
      });
      if (!resp.ok) return res.status(resp.status).json({ error: `Schwab API error: ${await resp.text()}` });
      const orders = (await resp.json()) as any[];

      let createdExecutions = 0;
      let createdDeviations = 0;
      let createdClosedTrades = 0;

      for (const order of (orders || [])) {
        // Only process FILLED orders
        if (order.status !== "FILLED") continue;

        const legs = order.orderLegCollection || [];
        for (const leg of legs) {
          const ticker = leg.instrument?.symbol;
          if (!ticker || leg.instrument?.assetType !== "EQUITY") continue;

          const side = leg.instruction; // BUY or SELL
          const quantity = leg.quantity || order.filledQuantity || 0;
          const avgPrice = order.price || order.stopPrice || 0;
          const orderId = String(order.orderId || "");

          // Skip if already synced
          if (orderId && storage.getTradeExecutionByOrderId(orderId)) continue;

          const executionDate = order.closeTime
            ? new Date(order.closeTime).toISOString().split("T")[0]
            : new Date(order.enteredTime).toISOString().split("T")[0];
          const executionTime = order.closeTime
            ? new Date(order.closeTime).toISOString().split("T")[1]?.split(".")[0]
            : undefined;

          // Create trade execution
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
            createdAt: new Date().toISOString(),
          });
          createdExecutions++;

          if (side === "BUY") {
            // For BUY orders, check for matching purchase signal (same ticker, signal date within 14 days before trade)
            const matchingSignals = db.all(sql`
              SELECT id, signal_score, signal_date FROM purchase_signals
              WHERE issuer_ticker = ${ticker}
                AND signal_date >= date(${executionDate}, '-14 days')
                AND signal_date <= ${executionDate}
              ORDER BY signal_date DESC
              LIMIT 1
            `) as any[];

            const matchingSignal = matchingSignals[0];

            if (matchingSignal) {
              const signalDate = new Date(matchingSignal.signal_date);
              const tradeDate = new Date(executionDate);
              const entryDelayDays = Math.floor((tradeDate.getTime() - signalDate.getTime()) / (1000 * 60 * 60 * 24));

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
                createdAt: new Date().toISOString(),
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
                createdAt: new Date().toISOString(),
              });
            }
            createdDeviations++;
          } else if (side === "SELL") {
            // For SELL orders, try to match against open BUY executions to create closed trades
            const openBuys = db.all(sql`
              SELECT te.* FROM trade_executions te
              WHERE te.ticker = ${ticker}
                AND te.side = 'BUY'
                AND te.id NOT IN (SELECT ct.id FROM closed_trades ct WHERE ct.ticker = ${ticker})
              ORDER BY te.execution_date ASC
              LIMIT 1
            `) as any[];

            const matchingBuy = openBuys[0];
            if (matchingBuy) {
              const entryDate = matchingBuy.execution_date;
              const entryPrice = matchingBuy.avg_price;
              const exitPrice = avgPrice;
              const holdingDays = Math.floor((new Date(executionDate).getTime() - new Date(entryDate).getTime()) / (1000 * 60 * 60 * 24));
              const realizedPnl = (exitPrice - entryPrice) * quantity;
              const realizedPnlPct = entryPrice > 0 ? ((exitPrice - entryPrice) / entryPrice) * 100 : 0;

              // Check if the buy had a signal
              const buyDeviation = db.all(sql`
                SELECT ed.classification, ed.signal_id FROM execution_deviations ed
                WHERE ed.user_trade_id = ${matchingBuy.id}
                LIMIT 1
              `) as any[];

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
                createdAt: new Date().toISOString(),
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
        totalOrders: orders?.length || 0,
      });
    } catch (err: any) {
      console.error("[SCHWAB SYNC-ORDERS] Error:", err.message);
      res.status(500).json({ error: err.message });
    }
  });

  /** Polling status */
  app.get("/api/status", (_req, res) => {
    res.json(getPollerStatus());
  });

  // ============================================================
  // ADMIN: Remote Deploy & Health
  // ============================================================

  const DEPLOY_SECRET = process.env.DEPLOY_SECRET || "9092e955d3811673c357dbd1e9205b36d96a85e76a782c4f8e8d4cc5be9cdb49";

  app.post("/api/admin/deploy", async (req, res) => {
    const secret = req.headers["x-deploy-secret"] || req.query.secret;
    if (secret !== process.env.DEPLOY_SECRET && secret !== DEPLOY_SECRET) {
      return res.status(401).json({ error: "unauthorized" });
    }
    res.json({ status: "deploying", started: new Date().toISOString() });
    const { exec } = require("child_process");
    exec("cd /opt/insider-signal-dash && git pull origin master && bash script/setup-services.sh 2>&1 || true && sleep 1 && echo RESTARTING_POLLER && systemctl restart insider-signal-poller 2>&1 && echo POLLER_RESTARTED || echo POLLER_RESTART_FAILED && echo RESTARTING_WEB && systemctl restart insider-signal",
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

  // Admin: enrich prices — supports continuous=true for auto-loop
  let enrichmentRunning = false;
  let enrichmentContinuous = false;

  let lastEnrichedCount = 0;

  function runEnrichmentBatch() {
    const { exec } = require("child_process");
    enrichmentRunning = true;
    console.log("[ENRICH] Starting batch...");
    exec("nice -n 10 python3 scripts/enrich-prices.py 2000 2010", { cwd: "/opt/insider-signal-dash", timeout: 600000, env: { ...process.env, PYTHONUNBUFFERED: '1' } },
      (error: any, stdout: string, stderr: string) => {
        enrichmentRunning = false;
        if (error) console.error("[ENRICH] Batch failed:", error.message);
        if (stdout) console.log("[ENRICH] stdout:", stdout.slice(-500));
        if (stderr) console.error("[ENRICH] stderr:", stderr.slice(-500));

        // If continuous mode is on, schedule next batch (retry even on error)
        if (enrichmentContinuous) {
          // Invalidate pipeline status cache to get fresh counts after enrichment
          invalidatePipelineStatusCache();
          const status = getDataPipelineStatus();
          const currentEnriched = status.enrichedSignals || 0;

          // Stop if script found 0 signals to enrich, or no progress since last batch
          const noSignals = stdout && stdout.includes("0 signals to enrich");
          const noProgress = currentEnriched === lastEnrichedCount && lastEnrichedCount > 0;

          if (noSignals || (noProgress && !error)) {
            console.log(`[ENRICH] No more enrichable signals (${currentEnriched} total enriched, ${status.enrichmentProgress}%). Stopping continuous mode.`);
            enrichmentContinuous = false;
            // Auto-chain: run factor research after enrichment completes
            console.log("[PIPELINE] Enrichment complete. Auto-starting factor research...");
            const { exec: execFR } = require("child_process");
            execFR("nice -n 10 python3 scripts/factor-research.py", { cwd: "/opt/insider-signal-dash", timeout: 600000, env: { ...process.env, PYTHONUNBUFFERED: '1' } },
              (frError: any, frStdout: string, frStderr: string) => {
                if (frError) console.error("[PIPELINE] Factor research failed:", frError.message);
                if (frStdout) console.log("[PIPELINE] Factor research stdout:", frStdout.slice(-2000));
                if (frStderr) console.error("[PIPELINE] Factor research stderr:", frStderr.slice(-2000));
                console.log("[PIPELINE] Factor research complete. Auto-starting alpha decay...");
                precomputeAlphaDecay().then(() => {
                  console.log("[PIPELINE] Full pipeline complete: enrichment → factor research → alpha decay");
                }).catch((e) => console.error("[PIPELINE] Alpha decay failed:", e.message));
              }
            );
          } else {
            lastEnrichedCount = currentEnriched;
            const delay = error ? 30000 : 5000; // 30s retry on error, 5s on success
            console.log(`[ENRICH] Continuous mode: ${status.enrichmentProgress}% (${currentEnriched}/${status.totalSignals}). Next batch in ${delay/1000}s...${error ? ' (retrying after error)' : ''}`);
            setTimeout(runEnrichmentBatch, delay);
          }
        }
      }
    );
  }

  app.post("/api/admin/enrich", (req, res) => {
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

    // Stop continuous mode if explicitly called without continuous flag while running
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

  // Admin: backfill SEC data
  app.post("/api/admin/backfill", (req, res) => {
    const secret = req.headers["x-deploy-secret"] || req.query.secret;
    if (secret !== process.env.DEPLOY_SECRET && secret !== DEPLOY_SECRET) {
      return res.status(401).json({ error: "unauthorized" });
    }
    const startYear = req.body?.startYear || 2026;
    const { exec } = require("child_process");
    exec(`nice -n 10 npx tsx server/sec-backfill.ts ${startYear}`, { cwd: "/opt/insider-signal-dash", timeout: 600000, env: { ...process.env, NODE_OPTIONS: '--max-old-space-size=512' } },
      (error: any, stdout: string, stderr: string) => {
        if (error) console.error("[BACKFILL] Failed:", error.message);
        if (stdout) console.log("[BACKFILL] stdout:", stdout.slice(-500));
        if (stderr) console.error("[BACKFILL] stderr:", stderr.slice(-500));
      }
    );
    res.json({ status: "backfill_started", startYear });
  });

  // Admin: run FIFO trade matching + signal matching
  app.post("/api/admin/compute-trades", (req, res) => {
    const secret = req.headers["x-deploy-secret"] || req.query.secret;
    if (secret !== process.env.DEPLOY_SECRET && secret !== DEPLOY_SECRET) {
      return res.status(401).json({ error: "unauthorized" });
    }
    try {
      // Step 1: Run signal-trade matching (update execution_deviations)
      const signalResult = runSignalTradeMatching();
      console.log(`[COMPUTE] Signal matching: ${signalResult.matched} matched, ${signalResult.unmatched} unmatched`);

      // Step 2: Run FIFO closed trade matching
      const fifoResult = runFifoMatching();
      console.log(`[COMPUTE] FIFO matching: ${fifoResult.matched} closed trades created`);

      // Step 3: Get performance summary
      const perf = getPerformanceAnalytics();

      res.json({
        status: "completed",
        signalMatching: signalResult,
        fifoMatching: fifoResult,
        performance: perf ? {
          totalRealizedPnl: perf.totalRealizedPnl,
          winRate: perf.winRate,
          closedTradeCount: perf.closedTradeCount,
          profitFactor: perf.profitFactor,
        } : null,
      });
    } catch (err: any) {
      console.error("[COMPUTE] Error:", err.message);
      res.status(500).json({ error: err.message });
    }
  });

  // Admin: run factor research
  app.post("/api/admin/factor-research", (req, res) => {
    const secret = req.headers["x-deploy-secret"] || req.query.secret;
    if (secret !== process.env.DEPLOY_SECRET && secret !== DEPLOY_SECRET) {
      return res.status(401).json({ error: "unauthorized" });
    }
    const { exec } = require("child_process");
    exec("nice -n 10 python3 scripts/factor-research.py", { cwd: "/opt/insider-signal-dash", timeout: 600000, env: { ...process.env, PYTHONUNBUFFERED: '1' } },
      (error: any, stdout: string, stderr: string) => {
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

  // Admin: pre-compute alpha decay cache (heavy 23M row query, runs in background sqlite3 process)
  app.post("/api/admin/precompute-alpha-decay", async (req, res) => {
    const secret = req.headers["x-deploy-secret"] || req.query.secret;
    if (secret !== process.env.DEPLOY_SECRET && secret !== DEPLOY_SECRET) {
      return res.status(401).json({ error: "unauthorized" });
    }
    // Start in background and respond immediately
    precomputeAlphaDecay().catch((e: any) => console.error("[ADMIN] Alpha decay failed:", e.message));
    res.json({ status: "started", message: "Alpha decay pre-computation running in background" });
  });

  // Admin: backup database to GCS
  app.post("/api/admin/backup", (req, res) => {
    const secret = req.headers["x-deploy-secret"] || req.query.secret;
    if (secret !== process.env.DEPLOY_SECRET && secret !== DEPLOY_SECRET) {
      return res.status(401).json({ error: "unauthorized" });
    }
    const { exec } = require("child_process");
    const backupScript = [
      'set -e',
      'sqlite3 /opt/insider-signal-dash/data.db ".backup /tmp/insider-signal-backup.db"',
      'FSIZE=$(stat -c%s /tmp/insider-signal-backup.db)',
      'echo "STEP1: SQLite backup done, bytes=$FSIZE"',
      'TOKEN=$(curl -s -H "Metadata-Flavor: Google" "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token" | python3 -c "import sys,json;print(json.load(sys.stdin)[\'access_token\'])")',
      'echo "STEP2: Got token"',
      'DEST="backups/data-$(date +%Y%m%d-%H%M%S).db"',
      'HTTP_CODE=$(curl -s -o /tmp/gcs-response.json -w "%{http_code}" -X POST -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/octet-stream" --data-binary @/tmp/insider-signal-backup.db "https://storage.googleapis.com/upload/storage/v1/b/insider-signal-deploys/o?uploadType=media&name=$DEST")',
      'echo "STEP3: HTTP $HTTP_CODE response: $(cat /tmp/gcs-response.json)"',
      'rm -f /tmp/insider-signal-backup.db /tmp/gcs-response.json',
      'if [ "$HTTP_CODE" != "200" ]; then echo "FAILED: Upload returned HTTP $HTTP_CODE"; exit 1; fi',
      'echo "DONE: Backed up to gs://insider-signal-deploys/$DEST"',
    ].join('\n');
    exec(backupScript,
      { timeout: 300000, shell: "/bin/bash" },
      (error: any, stdout: string, stderr: string) => {
        if (error) console.error("[BACKUP] Failed:", error.message);
        if (stdout) console.log("[BACKUP] stdout:", stdout.slice(-1000));
        if (stderr) console.error("[BACKUP] stderr:", stderr.slice(-1000));
      }
    );
    res.json({ status: "backup_started" });
  });

  // Admin: test GCS access (diagnostic)
  app.get("/api/admin/test-gcs", (req, res) => {
    const secret = req.headers["x-deploy-secret"] || req.query.secret;
    if (secret !== process.env.DEPLOY_SECRET && secret !== DEPLOY_SECRET) {
      return res.status(401).json({ error: "unauthorized" });
    }
    const { exec } = require("child_process");
    const testCmd = [
      'TOKEN=$(curl -s -H "Metadata-Flavor: Google" "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token" | python3 -c "import sys,json;print(json.load(sys.stdin)[\'access_token\'])")',
      'curl -s -H "Authorization: Bearer $TOKEN" "https://storage.googleapis.com/storage/v1/b/insider-signal-deploys?fields=name,timeCreated"',
    ].join(' && ');
    exec(testCmd,
      { timeout: 30000, shell: "/bin/bash" },
      (error: any, stdout: string, stderr: string) => {
        if (error) return res.json({ error: error.message, stderr });
        try {
          res.json({ gcsAccess: JSON.parse(stdout), status: "ok" });
        } catch {
          res.json({ rawResponse: stdout, stderr, status: "parse_error" });
        }
      }
    );
  });

  // Admin: read recent service logs
  app.get("/api/admin/logs", (req, res) => {
    const secret = req.headers["x-deploy-secret"] || req.query.secret;
    if (secret !== process.env.DEPLOY_SECRET && secret !== DEPLOY_SECRET) {
      return res.status(401).json({ error: "unauthorized" });
    }
    const { exec } = require("child_process");
    const lines = parseInt(req.query.lines as string) || 50;
    const grep = req.query.grep ? `| grep -i '${(req.query.grep as string).replace(/'/g, '')}'` : '';
    const service = req.query.service === 'poller' ? 'insider-signal-poller' : 'insider-signal';
    exec(`journalctl -u ${service} --no-pager -n ${lines} ${grep}`,
      { timeout: 10000 },
      (error: any, stdout: string, stderr: string) => {
        if (error) return res.json({ error: error.message, stderr });
        res.json({ logs: stdout.split('\n').slice(-lines) });
      }
    );
  });

  // Admin: setup systemd override for index creation on restart
  app.post("/api/admin/setup-systemd", (req, res) => {
    const secret = req.headers["x-deploy-secret"] || req.query.secret;
    if (secret !== process.env.DEPLOY_SECRET && secret !== DEPLOY_SECRET) {
      return res.status(401).json({ error: "unauthorized" });
    }
    const { exec } = require("child_process");
    exec(`mkdir -p /etc/systemd/system/insider-signal.service.d && cat > /etc/systemd/system/insider-signal.service.d/indexes.conf << 'CONF'
[Service]
ExecStartPost=/bin/bash /opt/insider-signal-dash/script/create-indexes.sh
CONF
systemctl daemon-reload`,
      { timeout: 30000 },
      (error: any, stdout: string, stderr: string) => {
        if (error) console.error("[SETUP-SYSTEMD] Failed:", error.message);
        if (stdout) console.log("[SETUP-SYSTEMD] stdout:", stdout);
        if (stderr) console.error("[SETUP-SYSTEMD] stderr:", stderr);
      }
    );
    res.json({ status: "systemd_setup_started" });
  });

  // Admin: fix deploy cron script
  app.post("/api/admin/fix-deploy-cron", (req, res) => {
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
    exec(`cat > /opt/deploy.sh << 'DEPLOYSCRIPT'
${deployScript.trim()}
DEPLOYSCRIPT
chmod +x /opt/deploy.sh`,
      { timeout: 10000 },
      (error: any, stdout: string, stderr: string) => {
        if (error) console.error("[FIX-DEPLOY-CRON] Failed:", error.message);
        if (stdout) console.log("[FIX-DEPLOY-CRON] stdout:", stdout);
        if (stderr) console.error("[FIX-DEPLOY-CRON] stderr:", stderr);
      }
    );
    res.json({ status: "deploy_cron_fixed" });
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

  // Self-healing watchdog: if event loop is blocked >30s, kill stale child processes
  let lastHeartbeat = Date.now();
  setInterval(() => { lastHeartbeat = Date.now(); }, 5000);
  setInterval(() => {
    const lag = Date.now() - lastHeartbeat;
    if (lag > 30000) {
      console.error(`[WATCHDOG] Event loop blocked for ${lag}ms, killing child processes`);
      const { execSync } = require("child_process");
      try { execSync("pkill -f 'enrich-prices.py' || true"); } catch {}
      try { execSync("pkill -f 'sec-backfill.ts' || true"); } catch {}
      try { execSync("pkill -f 'factor-research.py' || true"); } catch {}
    }
  }, 10000);

  // Health check endpoint for external monitoring (no auth required)
  app.get("/api/ping", (_req, res) => {
    res.json({ ok: true, uptime: process.uptime(), lag: Date.now() - lastHeartbeat });
  });

  // Alpha decay pre-computation is too heavy for this VM (23M rows).
  // Run manually via POST /api/admin/precompute-alpha-decay when needed.
  // The API endpoint returns [] if the cache file doesn't exist yet.

  // Auto-resume continuous enrichment on server startup after a 30s delay
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
    } catch (e: any) {
      console.error("[ENRICH] Auto-resume check failed:", e.message);
    }
  }, 30000);

  // Schedule daily backup at startup, then every 24 hours
  const BACKUP_INTERVAL = 24 * 60 * 60 * 1000;
  function runScheduledBackup() {
    const { exec } = require("child_process");
    const backupScript = [
      'set -e',
      'sqlite3 /opt/insider-signal-dash/data.db ".backup /tmp/insider-signal-backup.db"',
      'TOKEN=$(curl -s -H "Metadata-Flavor: Google" "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token" | python3 -c "import sys,json;print(json.load(sys.stdin)[\'access_token\'])")',
      'DEST="backups/data-$(date +%Y%m%d-%H%M%S).db"',
      'curl -s -o /dev/null -w "%{http_code}" -X POST -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/octet-stream" --data-binary @/tmp/insider-signal-backup.db "https://storage.googleapis.com/upload/storage/v1/b/insider-signal-deploys/o?uploadType=media&name=$DEST" | grep -q 200 && echo "[BACKUP] Success: $DEST" || echo "[BACKUP] Failed"',
      'rm -f /tmp/insider-signal-backup.db',
    ].join('\n');
    exec(backupScript, { timeout: 300000, shell: "/bin/bash" }, (error: any, stdout: string, stderr: string) => {
      if (error) console.error("[BACKUP] Scheduled backup failed:", error.message);
      if (stdout) console.log("[BACKUP]", stdout.trim());
    });
  }
  // First backup 5 minutes after startup, then every 24 hours
  setTimeout(() => {
    runScheduledBackup();
    setInterval(runScheduledBackup, BACKUP_INTERVAL);
  }, 5 * 60 * 1000);
  console.log("[BACKUP] Daily backup scheduled (first run in 5 minutes)");

  // Comprehensive system health endpoint
  app.get("/api/admin/system-health", (req, res) => {
    const secret = req.headers["authorization"]?.replace("Bearer ", "") || req.query.secret;
    if (secret !== process.env.DEPLOY_SECRET && secret !== DEPLOY_SECRET) {
      return res.status(401).json({ error: "unauthorized" });
    }

    const status = getDataPipelineStatus();

    let pollerStatus: any = { status: "unknown" };
    try {
      pollerStatus = JSON.parse(fs.readFileSync("/tmp/poller-status.json", "utf-8"));
    } catch {}

    let alphaDecayStatus = "missing";
    try {
      const stat = fs.statSync(ALPHA_DECAY_CACHE_PATH);
      const ageHours = (Date.now() - stat.mtimeMs) / (1000 * 60 * 60);
      alphaDecayStatus = ageHours < 24 ? "fresh" : `stale (${Math.round(ageHours)}h old)`;
    } catch {}

    res.json({
      server: {
        uptime: process.uptime(),
        commit: "see gitLog",
      },
      poller: pollerStatus,
      enrichment: {
        running: enrichmentRunning,
        continuous: enrichmentContinuous,
        progress: status.enrichmentProgress,
        enrichedSignals: status.enrichedSignals,
        totalSignals: status.totalSignals,
      },
      factorResearch: {
        factorAnalysisResults: status.factorAnalysisResults,
        modelFactors: status.modelFactors,
      },
      alphaDecay: {
        cacheStatus: alphaDecayStatus,
      },
      data: {
        totalPurchases: status.totalPurchases,
        forwardReturnDataPoints: status.forwardReturnDataPoints,
        insiderProfiles: status.insiderProfiles,
        failedTickers: status.failedTickers,
      },
      disk: (() => {
        try {
          const { execSync } = require("child_process");
          const df = execSync("df -h / | tail -1").toString().trim().split(/\s+/);
          const du = execSync("du -sh /opt/insider-signal-dash/data/ /opt/insider-signal-dash/data/sec-raw/ 2>/dev/null || echo 'N/A'").toString().trim();
          return { filesystem: df[0], size: df[1], used: df[2], available: df[3], usePct: df[4], dataDirSizes: du };
        } catch { return { error: "could not check" }; }
      })(),
    });
  });

  // Admin: disk cleanup — remove SEC raw data files (already imported into DB)
  app.post("/api/admin/disk-cleanup", (req, res) => {
    const secret = req.headers["authorization"]?.replace("Bearer ", "") || req.headers["x-deploy-secret"] || req.query.secret;
    if (secret !== process.env.DEPLOY_SECRET && secret !== DEPLOY_SECRET) {
      return res.status(401).json({ error: "unauthorized" });
    }
    const { execSync } = require("child_process");
    try {
      const before = execSync("df / | tail -1").toString().trim().split(/\s+/);
      // Remove SEC raw data (zip files and extracted TSVs)
      execSync("rm -rf /opt/insider-signal-dash/data/sec-raw/*", { timeout: 30000 });
      // VACUUM the database to reclaim space from deleted rows
      execSync('sqlite3 /opt/insider-signal-dash/data.db "PRAGMA wal_checkpoint(TRUNCATE);"', { timeout: 60000 });
      const after = execSync("df / | tail -1").toString().trim().split(/\s+/);
      const freedKB = parseInt(after[3]) - parseInt(before[3]);
      res.json({ status: "cleanup_complete", freedMB: Math.round(freedKB / 1024), diskBefore: before[4], diskAfter: after[4], availableAfter: after[3] + "K" });
    } catch (e: any) {
      res.status(500).json({ error: e.message });
    }
  });

  // Admin: run arbitrary shell command (DANGEROUS — admin only)
  app.post("/api/admin/shell", (req, res) => {
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
      const output = execSync(cmd, { cwd: "/opt/insider-signal-dash", timeout: 30000, maxBuffer: 1024 * 1024 }).toString();
      res.json({ output });
    } catch (e: any) {
      res.status(500).json({ error: e.message, stderr: e.stderr?.toString()?.slice(-500) });
    }
  });

  return httpServer;
}
