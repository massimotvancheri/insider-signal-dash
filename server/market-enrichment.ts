/**
 * Market Data Enrichment Pipeline
 * 
 * Enriches insider transaction signals with market data:
 * - Historical OHLCV prices for forward return calculation
 * - Company profile (sector, industry, market cap)
 * - Prior momentum, 52-week high distance
 * - Volume metrics
 * 
 * Designed to run incrementally — processes signals in batches,
 * prioritizing recent and high-scoring signals first.
 * 
 * Uses the finance API for real ticker data, with fallback to
 * Yahoo Finance CSV for bulk historical data.
 */

import { db } from "./db";
import { 
  purchaseSignals, insiderTransactions, signalEntryPrices, 
  dailyPrices, pipelineStatus 
} from "@shared/schema";
import { eq, and, isNull, desc, sql, inArray } from "drizzle-orm";

const YAHOO_BASE = "https://query1.finance.yahoo.com/v7/finance/download";

// ============================================================
// PRICE DATA FETCHING
// ============================================================

interface DailyPrice {
  date: string;      // YYYY-MM-DD
  open: number;
  high: number;
  low: number;
  close: number;
  adjClose: number;
  volume: number;
}

/**
 * Fetch historical daily prices from Yahoo Finance (free, no API key)
 * Returns daily OHLCV data for a ticker over a date range.
 */
async function fetchYahooPrices(
  ticker: string, 
  startDate: string, 
  endDate: string
): Promise<DailyPrice[]> {
  const start = Math.floor(new Date(startDate).getTime() / 1000);
  const end = Math.floor(new Date(endDate).getTime() / 1000);
  
  const url = `${YAHOO_BASE}/${encodeURIComponent(ticker)}?period1=${start}&period2=${end}&interval=1d&events=history`;
  
  try {
    const resp = await fetch(url, {
      headers: {
        "User-Agent": "Mozilla/5.0 (compatible; InsiderSignalDash/1.0)",
      },
    });
    
    if (!resp.ok) return [];
    
    const csv = await resp.text();
    const lines = csv.split("\n").filter(l => l.trim());
    if (lines.length < 2) return [];
    
    const prices: DailyPrice[] = [];
    for (let i = 1; i < lines.length; i++) {
      const cols = lines[i].split(",");
      if (cols.length < 7) continue;
      
      const close = parseFloat(cols[4]);
      const adjClose = parseFloat(cols[5]);
      const volume = parseFloat(cols[6]);
      
      if (isNaN(close) || close <= 0) continue;
      
      prices.push({
        date: cols[0],
        open: parseFloat(cols[1]),
        high: parseFloat(cols[2]),
        low: parseFloat(cols[3]),
        close,
        adjClose: isNaN(adjClose) ? close : adjClose,
        volume: isNaN(volume) ? 0 : volume,
      });
    }
    
    return prices.sort((a, b) => a.date.localeCompare(b.date));
  } catch {
    return [];
  }
}

// SPY price cache (shared across all signals)
let spyPriceCache: Map<string, DailyPrice> | null = null;

async function getSpyPrices(): Promise<Map<string, DailyPrice>> {
  if (spyPriceCache) return spyPriceCache;
  
  console.log("  [spy] Fetching SPY benchmark prices (2015-2026)...");
  const prices = await fetchYahooPrices("SPY", "2015-01-01", "2026-12-31");
  spyPriceCache = new Map(prices.map(p => [p.date, p]));
  console.log(`  [spy] Loaded ${spyPriceCache.size} SPY trading days`);
  return spyPriceCache;
}

// ============================================================
// SIGNAL ENRICHMENT
// ============================================================

interface SignalEnrichmentResult {
  signalId: number;
  ticker: string;
  success: boolean;
  entryPricesSaved: boolean;
  forwardReturnDays: number;
  enrichmentFields: string[];
  error?: string;
}

/**
 * Enrich a single signal with market data and compute forward returns
 */
async function enrichSignal(
  signal: typeof purchaseSignals.$inferSelect,
  spyPrices: Map<string, DailyPrice>,
  tickerPrices: DailyPrice[]
): Promise<SignalEnrichmentResult> {
  const result: SignalEnrichmentResult = {
    signalId: signal.id,
    ticker: signal.issuerTicker || "",
    success: false,
    entryPricesSaved: false,
    forwardReturnDays: 0,
    enrichmentFields: [],
  };
  
  if (!signal.issuerTicker || tickerPrices.length === 0) {
    result.error = "No ticker or price data";
    return result;
  }
  
  const priceMap = new Map(tickerPrices.map(p => [p.date, p]));
  const filingDate = signal.signalDate;
  
  // Find the filing date price (or next available trading day)
  let filingDayPrice: DailyPrice | undefined;
  let priorClosePrice: DailyPrice | undefined;
  let nextOpenPrice: DailyPrice | undefined;
  
  // Look for prices around filing date (±5 days)
  const fd = new Date(filingDate);
  for (let offset = 0; offset <= 5; offset++) {
    const d = new Date(fd);
    d.setDate(d.getDate() + offset);
    const dateStr = d.toISOString().split("T")[0];
    if (priceMap.has(dateStr)) {
      if (!filingDayPrice) filingDayPrice = priceMap.get(dateStr);
      if (offset >= 1 && !nextOpenPrice) nextOpenPrice = priceMap.get(dateStr);
    }
  }
  
  // Prior close: last trading day before filing
  for (let offset = 0; offset <= 10; offset++) {
    const d = new Date(fd);
    d.setDate(d.getDate() - offset);
    const dateStr = d.toISOString().split("T")[0];
    if (priceMap.has(dateStr)) {
      priorClosePrice = priceMap.get(dateStr);
      break;
    }
  }
  
  if (!filingDayPrice && !nextOpenPrice) {
    result.error = "No price data around filing date";
    return result;
  }
  
  const entryPrice = nextOpenPrice || filingDayPrice!;
  const entryOpen = entryPrice.open > 0 ? entryPrice.open : entryPrice.close;
  
  // === Save entry prices ===
  const insiderTxPrice = signal.avgPurchasePrice || 0;
  const overnightGap = priorClosePrice && entryOpen 
    ? (entryOpen / priorClosePrice.close) - 1 
    : null;
  
  db.insert(signalEntryPrices).values({
    signalId: signal.id,
    filingTimestamp: null,
    priorClose: priorClosePrice?.close || null,
    ahPrice: null, // Not available from daily data
    ahSpreadPct: null,
    nextOpen: entryOpen,
    nextVwap: entryPrice.close, // Approximate VWAP with close
    overnightGap,
    ahNetPremium: null,
    insiderTxPrice,
    createdAt: new Date().toISOString(),
  }).run();
  result.entryPricesSaved = true;
  
  // === Store daily prices (source of truth for forward return computation) ===
  // Professional quant pattern: store prices, compute returns on demand via SQL
  const allPrices = tickerPrices.sort((a, b) => a.date.localeCompare(b.date));
  
  // Upsert ticker prices into daily_prices
  if (allPrices.length > 0) {
    const BATCH = 250;
    let priceCount = 0;
    for (let i = 0; i < allPrices.length; i += BATCH) {
      const batch = allPrices.slice(i, i + BATCH);
      for (const p of batch) {
        db.execute(sql`
          INSERT INTO daily_prices (ticker, date, open, high, low, close, volume)
          VALUES (${ticker}, ${p.date}, ${p.open}, ${p.high}, ${p.low}, ${p.close}, ${p.volume})
          ON CONFLICT (ticker, date) DO NOTHING
        `);
        priceCount++;
      }
    }
    
    // Also store SPY benchmark prices
    for (const [date, spyData] of spyPrices) {
      db.execute(sql`
        INSERT INTO daily_prices (ticker, date, close)
        VALUES ('SPY', ${date}, ${spyData.close})
        ON CONFLICT (ticker, date) DO NOTHING
      `);
    }
    
    result.forwardReturnDays = priceCount;
  }
  
  // === Enrich transaction records with market context ===
  // Compute metrics from price history
  const enrichUpdates: Record<string, any> = {};
  
  // Prior 30d return
  const fd30 = new Date(fd);
  fd30.setDate(fd30.getDate() - 30);
  const price30dAgo = tickerPrices.find(p => p.date >= fd30.toISOString().split("T")[0]);
  if (price30dAgo && priorClosePrice) {
    enrichUpdates.priorReturn30d = (priorClosePrice.close / price30dAgo.close) - 1;
    result.enrichmentFields.push("priorReturn30d");
  }
  
  // Prior 90d return
  const fd90 = new Date(fd);
  fd90.setDate(fd90.getDate() - 90);
  const price90dAgo = tickerPrices.find(p => p.date >= fd90.toISOString().split("T")[0]);
  if (price90dAgo && priorClosePrice) {
    enrichUpdates.priorReturn90d = (priorClosePrice.close / price90dAgo.close) - 1;
    result.enrichmentFields.push("priorReturn90d");
  }
  
  // 52-week high distance
  const fd252 = new Date(fd);
  fd252.setDate(fd252.getDate() - 365);
  const yearPrices = tickerPrices.filter(p => 
    p.date >= fd252.toISOString().split("T")[0] && p.date <= filingDate
  );
  if (yearPrices.length > 0 && priorClosePrice) {
    const high52w = Math.max(...yearPrices.map(p => p.high));
    if (high52w > 0) {
      enrichUpdates.distanceFrom52wHigh = priorClosePrice.close / high52w;
      result.enrichmentFields.push("distanceFrom52wHigh");
    }
  }
  
  // Average daily volume (30-day)
  const recentPrices = tickerPrices.filter(p => {
    const d = new Date(p.date);
    return d >= fd30 && d <= fd;
  });
  if (recentPrices.length > 0) {
    enrichUpdates.avgDailyVolume = recentPrices.reduce((s, p) => s + p.volume, 0) / recentPrices.length;
    result.enrichmentFields.push("avgDailyVolume");
    
    // Recent volume spike (5d / 30d)
    const last5 = recentPrices.slice(-5);
    if (last5.length > 0 && enrichUpdates.avgDailyVolume > 0) {
      const avg5d = last5.reduce((s, p) => s + p.volume, 0) / last5.length;
      enrichUpdates.recentVolumeSpike = avg5d / enrichUpdates.avgDailyVolume;
      result.enrichmentFields.push("recentVolumeSpike");
    }
  }
  
  // Price drift from insider's transaction price
  if (insiderTxPrice > 0 && priorClosePrice) {
    enrichUpdates.priceDriftFromTx = (priorClosePrice.close / insiderTxPrice) - 1;
    result.enrichmentFields.push("priceDriftFromTx");
  }
  
  // Update transaction records for this signal's ticker around the filing date
  if (Object.keys(enrichUpdates).length > 0) {
    db.update(insiderTransactions)
      .set(enrichUpdates)
      .where(
        and(
          eq(insiderTransactions.issuerTicker, signal.issuerTicker!),
          eq(insiderTransactions.filingDate, signal.signalDate)
        )
      )
      .run();
  }
  
  result.success = true;
  return result;
}

// ============================================================
// BATCH PROCESSING
// ============================================================

/**
 * Process signals in batches, fetching price data per unique ticker.
 * Prioritizes recent, high-scoring signals.
 */
export async function runMarketEnrichment(options: {
  batchSize?: number;
  maxSignals?: number;
  startYear?: number;
} = {}): Promise<void> {
  const { batchSize = 50, maxSignals = 5000, startYear = 2020 } = options;
  
  console.log("=== Market Data Enrichment Pipeline ===");
  console.log(`Processing up to ${maxSignals} signals from ${startYear}+\n`);
  
  // Get SPY benchmark prices
  const spyPrices = await getSpyPrices();
  
  // Get signals that haven't been enriched yet, prioritized by score and recency
  const signals = db.select()
    .from(purchaseSignals)
    .where(
      and(
        sql`${purchaseSignals.signalDate} >= '${startYear}-01-01'`,
        sql`${purchaseSignals.issuerTicker} IS NOT NULL`,
        sql`${purchaseSignals.issuerTicker} != ''`,
        sql`${purchaseSignals.issuerTicker} != 'NONE'`,
        sql`${purchaseSignals.issuerTicker} != 'N/A'`,
        // Only signals not yet enriched (no entry price record)
        sql`${purchaseSignals.id} NOT IN (SELECT signal_id FROM signal_entry_prices)`
      )
    )
    .orderBy(desc(purchaseSignals.signalScore), desc(purchaseSignals.signalDate))
    .limit(maxSignals)
    .all();
  
  console.log(`[found] ${signals.length} signals to enrich\n`);
  
  if (signals.length === 0) {
    console.log("No signals to process.");
    return;
  }
  
  // Group signals by ticker for efficient price fetching
  const signalsByTicker = new Map<string, typeof signals>();
  for (const sig of signals) {
    const ticker = sig.issuerTicker!;
    if (!signalsByTicker.has(ticker)) signalsByTicker.set(ticker, []);
    signalsByTicker.get(ticker)!.push(sig);
  }
  
  const uniqueTickers = Array.from(signalsByTicker.keys());
  console.log(`[tickers] ${uniqueTickers.length} unique tickers to fetch prices for\n`);
  
  let processedSignals = 0;
  let successfulSignals = 0;
  let failedTickers = 0;
  let totalForwardDays = 0;
  
  // Process tickers in batches
  for (let i = 0; i < uniqueTickers.length; i += batchSize) {
    const tickerBatch = uniqueTickers.slice(i, i + batchSize);
    
    console.log(`[batch ${Math.floor(i / batchSize) + 1}/${Math.ceil(uniqueTickers.length / batchSize)}] Processing ${tickerBatch.length} tickers...`);
    
    for (const ticker of tickerBatch) {
      const tickerSignals = signalsByTicker.get(ticker)!;
      
      // Determine date range needed: earliest signal - 365 days to latest signal + 252 trading days
      const filingDates = tickerSignals.map(s => s.signalDate).sort();
      const earliest = filingDates[0];
      const latest = filingDates[filingDates.length - 1];
      
      const startD = new Date(earliest);
      startD.setDate(startD.getDate() - 400); // 365 days before + buffer
      const endD = new Date(latest);
      endD.setDate(endD.getDate() + 400); // ~252 trading days + buffer
      
      const startStr = startD.toISOString().split("T")[0];
      const endStr = endD.toISOString().split("T")[0];
      
      // Fetch prices
      const prices = await fetchYahooPrices(ticker, startStr, endStr);
      
      if (prices.length === 0) {
        failedTickers++;
        processedSignals += tickerSignals.length;
        continue;
      }
      
      // Rate limit Yahoo Finance
      await new Promise(r => setTimeout(r, 200));
      
      // Enrich each signal for this ticker
      for (const sig of tickerSignals) {
        try {
          const result = await enrichSignal(sig, spyPrices, prices);
          if (result.success) {
            successfulSignals++;
            totalForwardDays += result.forwardReturnDays;
          }
        } catch (err: any) {
          // Log but continue
        }
        processedSignals++;
      }
    }
    
    const pct = ((processedSignals / signals.length) * 100).toFixed(1);
    console.log(`  [progress] ${processedSignals}/${signals.length} signals (${pct}%) | ${successfulSignals} enriched | ${failedTickers} tickers failed | ${totalForwardDays} forward return days`);
  }
  
  // Final stats
  const entryCount = db.select({ count: sql<number>`count(*)` }).from(signalEntryPrices).get();
  const fwdCount = db.select({ count: sql<number>`count(*)` }).from(dailyPrices).get();
  
  console.log(`\n=== Enrichment Complete ===`);
  console.log(`Signals processed: ${processedSignals}`);
  console.log(`Successfully enriched: ${successfulSignals}`);
  console.log(`Failed tickers: ${failedTickers}`);
  console.log(`Entry price records: ${entryCount?.count}`);
  console.log(`Forward return data points: ${fwdCount?.count}`);
}

// CLI entry point
if (process.argv[1]?.includes("market-enrichment")) {
  const maxSignals = parseInt(process.argv[2] || "5000");
  const startYear = parseInt(process.argv[3] || "2020");
  
  runMarketEnrichment({ maxSignals, startYear })
    .then(() => {
      console.log("\nEnrichment complete.");
      process.exit(0);
    })
    .catch((err) => {
      console.error("Enrichment failed:", err);
      process.exit(1);
    });
}
