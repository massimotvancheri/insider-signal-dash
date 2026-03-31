/**
 * SEC Historical Data Backfill Pipeline
 * 
 * Downloads quarterly Insider Transaction datasets from SEC EDGAR (2006-2026),
 * parses SUBMISSION + REPORTINGOWNER + NONDERIV_TRANS tables,
 * filters to open-market purchases (TRANS_CODE = 'P'),
 * computes derived fields, and loads into the database.
 * 
 * SEC Data: https://www.sec.gov/data-research/sec-markets-data/insider-transactions-data-sets
 * Format: Tab-delimited .tsv files in quarterly ZIP archives
 */

import { db } from "./db";
import { insiderTransactions, purchaseSignals, insiderHistory, pollingState, pipelineStatus } from "@shared/schema";
import { eq, and, sql } from "drizzle-orm";
import { createWriteStream, existsSync, mkdirSync, readFileSync, readdirSync, unlinkSync } from "fs";
import { join } from "path";
import { execSync } from "child_process";

const SEC_USER_AGENT = "InsiderSignalDash research@insidersignal.app";
const DATA_DIR = join(process.cwd(), "data", "sec-raw");
const BASE_URL = "https://www.sec.gov/files/structureddata/data/insider-transactions-data-sets";

// Generate list of all quarters from 2016 Q1 to current
// Starting from 2016 to keep dataset manageable (10 years) while still
// having enough history for routine/opportunistic classification (needs 3+ years)
function getQuarterList(startYear = 2016): { year: number; quarter: number; filename: string }[] {
  const now = new Date();
  const currentYear = now.getFullYear();
  const currentQuarter = Math.ceil((now.getMonth() + 1) / 3);
  const quarters: { year: number; quarter: number; filename: string }[] = [];
  
  for (let year = startYear; year <= currentYear; year++) {
    for (let q = 1; q <= 4; q++) {
      if (year === currentYear && q > currentQuarter) break;
      quarters.push({
        year,
        quarter: q,
        filename: `${year}q${q}_form345.zip`,
      });
    }
  }
  return quarters;
}

// Download a quarterly ZIP file
async function downloadQuarter(filename: string): Promise<string> {
  const url = `${BASE_URL}/${filename}`;
  const outPath = join(DATA_DIR, filename);
  
  if (existsSync(outPath)) {
    const { statSync } = await import("fs");
    const fstat = statSync(outPath);
    if (fstat.size > 10000) {
      console.log(`  [skip] ${filename} already downloaded (${(fstat.size / 1024 / 1024).toFixed(1)} MB)`);
      return outPath;
    }
  }
  
  console.log(`  [download] ${url}`);
  const resp = await fetch(url, {
    headers: { "User-Agent": SEC_USER_AGENT },
  });
  
  if (!resp.ok) {
    throw new Error(`Failed to download ${filename}: ${resp.status} ${resp.statusText}`);
  }
  
  const buffer = Buffer.from(await resp.arrayBuffer());
  const { writeFileSync } = await import("fs");
  writeFileSync(outPath, buffer);
  console.log(`  [saved] ${filename} (${(buffer.length / 1024 / 1024).toFixed(1)} MB)`);
  
  // Rate limit: SEC allows 10 req/s, but be conservative
  await new Promise(r => setTimeout(r, 500));
  return outPath;
}

// Extract a ZIP file to a directory
function extractZip(zipPath: string, outDir: string): void {
  if (!existsSync(outDir)) mkdirSync(outDir, { recursive: true });
  execSync(`unzip -o "${zipPath}" -d "${outDir}"`, { stdio: "pipe" });
}

// Parse a TSV file into array of objects
function parseTsv(filePath: string): Record<string, string>[] {
  const content = readFileSync(filePath, "utf-8");
  const lines = content.split("\n").filter(l => l.trim());
  if (lines.length < 2) return [];
  
  const headers = lines[0].split("\t").map(h => h.trim());
  const rows: Record<string, string>[] = [];
  
  for (let i = 1; i < lines.length; i++) {
    const values = lines[i].split("\t");
    const row: Record<string, string> = {};
    for (let j = 0; j < headers.length; j++) {
      row[headers[j]] = (values[j] || "").trim();
    }
    rows.push(row);
  }
  return rows;
}

// Parse SEC date format (DD-MON-YYYY) to YYYY-MM-DD
function parseSecDate(dateStr: string): string {
  if (!dateStr) return "";
  const months: Record<string, string> = {
    JAN: "01", FEB: "02", MAR: "03", APR: "04", MAY: "05", JUN: "06",
    JUL: "07", AUG: "08", SEP: "09", OCT: "10", NOV: "11", DEC: "12",
  };
  const parts = dateStr.split("-");
  if (parts.length !== 3) return dateStr;
  const [day, mon, year] = parts;
  const monthNum = months[mon?.toUpperCase()] || "01";
  return `${year}-${monthNum}-${day.padStart(2, "0")}`;
}

// Compute business days between two dates
function businessDaysBetween(startStr: string, endStr: string): number {
  if (!startStr || !endStr) return -1;
  const start = new Date(startStr);
  const end = new Date(endStr);
  if (isNaN(start.getTime()) || isNaN(end.getTime())) return -1;
  
  let count = 0;
  const current = new Date(start);
  while (current < end) {
    current.setDate(current.getDate() + 1);
    const day = current.getDay();
    if (day !== 0 && day !== 6) count++;
  }
  return count;
}

// Classify indirect ownership nature
function classifyIndirectNature(nature: string): string {
  if (!nature) return "direct";
  const lower = nature.toLowerCase();
  if (lower.includes("spouse") || lower.includes("wife") || lower.includes("husband")) return "family_spouse";
  if (lower.includes("child") || lower.includes("son") || lower.includes("daughter") || lower.includes("minor")) return "family_child";
  if (lower.includes("family") || lower.includes("parent") || lower.includes("sibling")) return "family_other";
  if (lower.includes("trust")) return "trust";
  if (lower.includes("401") || lower.includes("ira") || lower.includes("retirement") || lower.includes("pension") || lower.includes("esop")) return "retirement";
  if (lower.includes("foundation") || lower.includes("charity") || lower.includes("philanthropic")) return "foundation";
  if (lower.includes("llc") || lower.includes("lp") || lower.includes("partnership") || lower.includes("fund") || lower.includes("corp")) return "entity";
  return "indirect_other";
}

// Determine insider role hierarchy from relationship and title
function classifyRole(relationship: string, title: string): string {
  const rel = (relationship || "").toLowerCase();
  const t = (title || "").toUpperCase();
  
  // Check for dual roles
  const isDirector = rel.includes("director");
  const isOfficer = rel.includes("officer");
  const isTenPct = rel.includes("tenpercentowner") || rel.includes("ten percent");
  
  if (t.includes("CEO") || t.includes("CHIEF EXECUTIVE")) {
    return isDirector ? "ceo_director" : "ceo";
  }
  if (t.includes("CFO") || t.includes("CHIEF FINANCIAL")) return "cfo";
  if (t.includes("COO") || t.includes("CHIEF OPERATING")) return "coo";
  if (t.includes("CTO") || t.includes("CHIEF TECHNOLOGY")) return "cto";
  if (t.includes("CIO") || t.includes("CHIEF INVESTMENT") || t.includes("CHIEF INFORMATION")) return "cio";
  if (t.includes("PRESIDENT")) return isDirector ? "president_director" : "president";
  if (t.includes("CHAIRMAN") || t.includes("CHAIR")) return "chairman";
  if (t.includes("CHIEF") || t.includes("EVP") || t.includes("SVP")) return "c_suite_other";
  if (isOfficer && isDirector) return "officer_director";
  if (isOfficer) return "officer";
  if (isDirector) return "director";
  if (isTenPct) return "ten_pct_owner";
  return "other";
}

// C-Suite keywords for quick check
const C_SUITE_KEYWORDS = ["CEO", "CFO", "COO", "CTO", "CIO", "Chief", "President", "Chairman"];

interface JoinedPurchase {
  // From SUBMISSION
  accessionNumber: string;
  filingDate: string;
  periodOfReport: string;
  documentType: string;
  issuerCik: string;
  issuerName: string;
  issuerTicker: string;
  // From REPORTINGOWNER
  reportingPersonCik: string;
  reportingPersonName: string;
  relationship: string;
  title: string;
  // From NONDERIV_TRANS
  transactionDate: string;
  transactionCode: string;
  shares: number;
  pricePerShare: number;
  acquiredDisposed: string;
  sharesOwnedAfter: number;
  directIndirect: string;
  natureOfOwnership: string;
  securityTitle: string;
  transTimeliness: string;
}

// Process a single quarter's data
async function processQuarter(quarterDir: string, yearQuarter: string): Promise<number> {
  const submissionPath = join(quarterDir, "SUBMISSION.tsv");
  const ownerPath = join(quarterDir, "REPORTINGOWNER.tsv");
  const transPath = join(quarterDir, "NONDERIV_TRANS.tsv");
  
  if (!existsSync(submissionPath) || !existsSync(ownerPath) || !existsSync(transPath)) {
    console.log(`  [skip] Missing files in ${quarterDir}`);
    return 0;
  }
  
  console.log(`  [parse] Loading TSV files for ${yearQuarter}...`);
  
  // Build lookup maps
  const submissions = parseTsv(submissionPath);
  const owners = parseTsv(ownerPath);
  const transactions = parseTsv(transPath);
  
  // Index submissions and owners by accession number
  const subMap = new Map<string, Record<string, string>>();
  for (const sub of submissions) {
    if (sub.DOCUMENT_TYPE === "4" || sub.DOCUMENT_TYPE === "4/A") {
      subMap.set(sub.ACCESSION_NUMBER, sub);
    }
  }
  
  const ownerMap = new Map<string, Record<string, string>[]>();
  for (const own of owners) {
    const key = own.ACCESSION_NUMBER;
    if (!ownerMap.has(key)) ownerMap.set(key, []);
    ownerMap.get(key)!.push(own);
  }
  
  // Filter to open-market purchases (P code) and join
  const purchases: JoinedPurchase[] = [];
  for (const tx of transactions) {
    if (tx.TRANS_CODE !== "P") continue;
    if (tx.TRANS_ACQUIRED_DISP_CD !== "A") continue; // Must be acquisition
    
    const sub = subMap.get(tx.ACCESSION_NUMBER);
    if (!sub) continue;
    
    const ownersForFiling = ownerMap.get(tx.ACCESSION_NUMBER) || [];
    const owner = ownersForFiling[0]; // Primary reporting owner
    if (!owner) continue;
    
    const shares = parseFloat(tx.TRANS_SHARES || "0");
    const price = parseFloat(tx.TRANS_PRICEPERSHARE || "0");
    if (shares <= 0 || price <= 0) continue; // Skip zero-value transactions
    
    purchases.push({
      accessionNumber: tx.ACCESSION_NUMBER,
      filingDate: parseSecDate(sub.FILING_DATE),
      periodOfReport: parseSecDate(sub.PERIOD_OF_REPORT),
      documentType: sub.DOCUMENT_TYPE,
      issuerCik: sub.ISSUERCIK,
      issuerName: sub.ISSUERNAME || "",
      issuerTicker: sub.ISSUERTRADINGSYMBOL || "",
      reportingPersonCik: owner.RPTOWNERCIK,
      reportingPersonName: owner.RPTOWNERNAME || "",
      relationship: owner.RPTOWNER_RELATIONSHIP || "",
      title: owner.RPTOWNER_TITLE || "",
      transactionDate: parseSecDate(tx.TRANS_DATE),
      transactionCode: tx.TRANS_CODE,
      shares,
      pricePerShare: price,
      acquiredDisposed: tx.TRANS_ACQUIRED_DISP_CD,
      sharesOwnedAfter: parseFloat(tx.SHRS_OWND_FOLWNG_TRANS || "0"),
      directIndirect: tx.DIRECT_INDIRECT_OWNERSHIP || "D",
      natureOfOwnership: tx.NATURE_OF_OWNERSHIP || "",
      securityTitle: tx.SECURITY_TITLE || "Common Stock",
      transTimeliness: tx.TRANS_TIMELINESS || "",
    });
  }
  
  console.log(`  [found] ${purchases.length} open-market purchases in ${yearQuarter} (from ${transactions.length} total transactions)`);
  
  // Insert in batches
  const BATCH_SIZE = 500;
  let inserted = 0;
  
  for (let i = 0; i < purchases.length; i += BATCH_SIZE) {
    const batch = purchases.slice(i, i + BATCH_SIZE);
    const records = batch.map(p => {
      const sharesOwnedBefore = p.sharesOwnedAfter - p.shares;
      const ownershipChangePct = sharesOwnedBefore > 0 
        ? (p.shares / sharesOwnedBefore) * 100 
        : 100; // First purchase = 100% increase
      
      const filingLagDays = businessDaysBetween(p.transactionDate, p.filingDate);
      const role = classifyRole(p.relationship, p.title);
      const isCSuite = C_SUITE_KEYWORDS.some(k => 
        (p.title || "").toUpperCase().includes(k.toUpperCase())
      );
      
      return {
        accessionNumber: p.accessionNumber,
        filingDate: p.filingDate,
        filingTimestamp: null as string | null, // Will be enriched later from EDGAR
        filingMarketState: null as string | null,
        issuerCik: p.issuerCik,
        issuerName: p.issuerName,
        issuerTicker: p.issuerTicker.toUpperCase(),
        reportingPersonName: p.reportingPersonName,
        reportingPersonCik: p.reportingPersonCik,
        reportingPersonTitle: p.title || (p.relationship.includes("Director") ? "Director" : ""),
        isDirector: p.relationship.toLowerCase().includes("director"),
        isOfficer: p.relationship.toLowerCase().includes("officer"),
        isTenPercentOwner: p.relationship.toLowerCase().includes("tenpercentowner"),
        transactionType: "P" as const,
        transactionDate: p.transactionDate,
        transactionCode: p.transactionCode,
        sharesTraded: p.shares,
        pricePerShare: p.pricePerShare,
        totalValue: p.shares * p.pricePerShare,
        sharesOwnedAfter: p.sharesOwnedAfter,
        ownershipType: p.directIndirect,
        ownershipNature: p.natureOfOwnership || null,
        securityTitle: p.securityTitle,
        // V3 computed fields
        filingLagDays,
        ownershipChangePct: Math.min(ownershipChangePct, 10000), // Cap at 10000%
        indirectAccountType: p.directIndirect === "D" ? "direct" : classifyIndirectNature(p.natureOfOwnership),
        // These will be enriched later
        isOpportunistic: null as number | null,
        isFirstPurchase: null as number | null,
        insiderHistoricalAlpha30d: null as number | null,
        insiderHistoricalAlpha63d: null as number | null,
        insiderPastTradeCount: null as number | null,
        marketCapAtFiling: null as number | null,
        sectorCode: null as string | null,
        industryName: null as string | null,
        securityType: null as string | null,
        exchangeListing: null as string | null,
        bookToMarket: null as number | null,
        hasActiveBuyback: null as number | null,
        avgDailyVolume: null as number | null,
        typicalSpreadPct: null as number | null,
        priceDriftFromTx: null as number | null,
        distanceFrom52wHigh: null as number | null,
        priorReturn30d: null as number | null,
        priorReturn90d: null as number | null,
        analystConsensus: null as string | null,
        recentDowngrade: null as number | null,
        analystCount: null as number | null,
        recentVolumeSpike: null as number | null,
        createdAt: new Date().toISOString(),
      };
    });
    
    // Use a transaction for batch insert
    db.transaction((tx) => {
      for (const record of records) {
        tx.insert(insiderTransactions).values(record).run();
      }
    });
    
    inserted += batch.length;
  }
  
  return inserted;
}

// Build insider history table for routine/opportunistic classification
async function buildInsiderHistory(): Promise<void> {
  console.log("\n[insiderHistory] Building insider trading history for routine/opportunistic classification...");
  
  // Get all unique insiders with their purchase months
  const allTrades = db.select({
    personCik: insiderTransactions.reportingPersonCik,
    personName: insiderTransactions.reportingPersonName,
    txDate: insiderTransactions.transactionDate,
  })
    .from(insiderTransactions)
    .where(eq(insiderTransactions.transactionType, "P"))
    .all();
  
  // Group by insider CIK
  const insiderMap = new Map<string, { name: string; months: Set<string>; count: number }>();
  
  for (const trade of allTrades) {
    if (!trade.personCik) continue;
    if (!insiderMap.has(trade.personCik)) {
      insiderMap.set(trade.personCik, { name: trade.personName || "", months: new Set(), count: 0 });
    }
    const entry = insiderMap.get(trade.personCik)!;
    entry.count++;
    if (trade.txDate) {
      const month = trade.txDate.substring(0, 7); // YYYY-MM
      entry.months.add(month);
    }
  }
  
  console.log(`  [found] ${insiderMap.size} unique insiders with purchase history`);
  
  // Classify routine vs opportunistic
  // Routine = same calendar month purchases for 3+ consecutive years
  let routineCount = 0;
  let opportunisticCount = 0;
  
  const records: { personCik: string; name: string; months: string[]; isRoutine: boolean; count: number }[] = [];
  
  for (const [cik, data] of insiderMap) {
    const sortedMonths = Array.from(data.months).sort();
    
    // Check for same-calendar-month pattern across 3+ consecutive years
    let isRoutine = false;
    const monthsByCalendarMonth = new Map<number, number[]>(); // month (1-12) -> list of years
    
    for (const m of sortedMonths) {
      const [yearStr, monthStr] = m.split("-");
      const year = parseInt(yearStr);
      const month = parseInt(monthStr);
      if (!monthsByCalendarMonth.has(month)) monthsByCalendarMonth.set(month, []);
      monthsByCalendarMonth.get(month)!.push(year);
    }
    
    // Check if any calendar month has 3+ consecutive years
    for (const [, years] of monthsByCalendarMonth) {
      years.sort();
      let consecutive = 1;
      for (let i = 1; i < years.length; i++) {
        if (years[i] === years[i - 1] + 1) {
          consecutive++;
          if (consecutive >= 3) {
            isRoutine = true;
            break;
          }
        } else {
          consecutive = 1;
        }
      }
      if (isRoutine) break;
    }
    
    if (isRoutine) routineCount++;
    else opportunisticCount++;
    
    records.push({
      personCik: cik,
      name: data.name,
      months: sortedMonths,
      isRoutine,
      count: data.count,
    });
  }
  
  console.log(`  [classified] ${routineCount} routine, ${opportunisticCount} opportunistic insiders`);
  
  // Insert into insiderHistory
  const BATCH = 500;
  for (let i = 0; i < records.length; i += BATCH) {
    const batch = records.slice(i, i + BATCH);
    db.transaction((tx) => {
      for (const r of batch) {
        tx.insert(insiderHistory).values({
          reportingPersonCik: r.personCik,
          reportingPersonName: r.name,
          tradingMonths: JSON.stringify(r.months),
          totalPurchaseCount: r.count,
          isRoutine: r.isRoutine ? 1 : 0,
          routineConfidence: r.count >= 10 ? 0.9 : r.count >= 5 ? 0.7 : 0.5,
          avgAlpha30d: null,
          avgAlpha63d: null,
          winRate30d: null,
          lastUpdated: new Date().toISOString(),
        }).onConflictDoUpdate({
          target: insiderHistory.reportingPersonCik,
          set: {
            tradingMonths: JSON.stringify(r.months),
            totalPurchaseCount: r.count,
            isRoutine: r.isRoutine ? 1 : 0,
            routineConfidence: r.count >= 10 ? 0.9 : r.count >= 5 ? 0.7 : 0.5,
            lastUpdated: new Date().toISOString(),
          },
        }).run();
      }
    });
  }
  
  // Now update insiderTransactions with isOpportunistic flag
  console.log("  [updating] Setting isOpportunistic flags on transactions...");
  const historyRecords = db.select().from(insiderHistory).all();
  const histMap = new Map(historyRecords.map(h => [h.reportingPersonCik, h.isRoutine]));
  
  db.run(sql`UPDATE insider_transactions SET is_opportunistic = 1 WHERE reporting_person_cik IS NOT NULL`);
  
  for (const [cik, isRoutine] of histMap) {
    if (isRoutine) {
      db.update(insiderTransactions)
        .set({ isOpportunistic: 0 })
        .where(eq(insiderTransactions.reportingPersonCik, cik))
        .run();
    }
  }
  
  console.log("  [done] Insider history built and opportunistic flags set");
}

// Generate purchase signal clusters from transactions
async function generateSignalClusters(): Promise<void> {
  console.log("\n[signals] Generating purchase signal clusters...");
  
  // Group purchases by ticker + 14-day window
  const allPurchases = db.select()
    .from(insiderTransactions)
    .where(eq(insiderTransactions.transactionType, "P"))
    .orderBy(insiderTransactions.issuerTicker, insiderTransactions.filingDate)
    .all();
  
  console.log(`  [loaded] ${allPurchases.length} purchase transactions`);
  
  // Cluster: group by ticker, within 14-day filing date windows
  const clusters: Map<string, typeof allPurchases> = new Map();
  
  let currentKey = "";
  let currentCluster: typeof allPurchases = [];
  let clusterEndDate = "";
  
  for (const tx of allPurchases) {
    if (!tx.issuerTicker || !tx.filingDate) continue;
    
    const key = tx.issuerTicker;
    
    if (key !== currentKey || (clusterEndDate && tx.filingDate > clusterEndDate)) {
      // Save current cluster if it has data
      if (currentCluster.length > 0) {
        const clusterKey = `${currentKey}_${currentCluster[0].filingDate}`;
        clusters.set(clusterKey, [...currentCluster]);
      }
      currentKey = key;
      currentCluster = [tx];
      // Window: 14 days from first filing in cluster
      const d = new Date(tx.filingDate);
      d.setDate(d.getDate() + 14);
      clusterEndDate = d.toISOString().split("T")[0];
    } else {
      currentCluster.push(tx);
    }
  }
  // Don't forget the last cluster
  if (currentCluster.length > 0) {
    const clusterKey = `${currentKey}_${currentCluster[0].filingDate}`;
    clusters.set(clusterKey, [...currentCluster]);
  }
  
  console.log(`  [clustered] ${clusters.size} signal clusters from ${allPurchases.length} transactions`);
  
  // Insert signals
  let signalCount = 0;
  const BATCH = 500;
  const signalBatch: any[] = [];
  
  for (const [, txs] of clusters) {
    const uniqueInsiders = new Set(txs.map(t => t.reportingPersonName));
    const totalValue = txs.reduce((sum, t) => sum + (t.totalValue || 0), 0);
    const avgPrice = txs.reduce((sum, t) => sum + (t.pricePerShare || 0), 0) / txs.length;
    const cSuiteCount = txs.filter(t => 
      C_SUITE_KEYWORDS.some(k => (t.reportingPersonTitle || "").toUpperCase().includes(k.toUpperCase()))
    ).length;
    const directorCount = txs.filter(t => t.isDirector).length;
    
    const filingDates = txs.map(t => t.filingDate).filter(Boolean).sort();
    const firstDate = filingDates[0] || "";
    const lastDate = filingDates[filingDates.length - 1] || "";
    const daysSpan = firstDate && lastDate
      ? Math.ceil((new Date(lastDate).getTime() - new Date(firstDate).getTime()) / (1000 * 60 * 60 * 24))
      : 0;
    
    // Preliminary score (will be replaced by data-derived model)
    // For now, use a simple version so the signal table has something
    let score = 0;
    score += Math.min(uniqueInsiders.size * 8, 25); // cluster
    if (totalValue >= 10_000_000) score += 25;
    else if (totalValue >= 1_000_000) score += 20;
    else if (totalValue >= 500_000) score += 15;
    else if (totalValue >= 100_000) score += 10;
    else if (totalValue >= 50_000) score += 5;
    score += Math.min(cSuiteCount * 10, 25);
    const directCount = txs.filter(t => t.ownershipType === "D").length;
    score += Math.min((directCount / txs.length) * 15, 15);
    score += Math.min((txs.filter(t => t.isOpportunistic === 1).length / txs.length) * 10, 10);
    score = Math.round(Math.min(score, 100));
    
    signalBatch.push({
      issuerCik: txs[0].issuerCik,
      issuerName: txs[0].issuerName,
      issuerTicker: txs[0].issuerTicker,
      signalDate: firstDate,
      signalScore: score,
      scoreTier: score >= 80 ? 1 : score >= 60 ? 2 : score >= 40 ? 3 : 4,
      factorBreakdown: null,
      clusterSize: uniqueInsiders.size,
      totalPurchaseValue: totalValue,
      avgPurchasePrice: avgPrice,
      insiderNames: JSON.stringify(Array.from(uniqueInsiders)),
      insiderTitles: JSON.stringify(txs.map(t => t.reportingPersonTitle || "").filter(Boolean)),
      cSuiteCount,
      directorCount,
      daysSpan,
      comparableCount: null,
      comparableAvgReturn63d: null,
      comparableWinRate: null,
      createdAt: new Date().toISOString(),
    });
    
    if (signalBatch.length >= BATCH) {
      db.transaction((tx) => {
        for (const record of signalBatch) {
          tx.insert(purchaseSignals).values(record).run();
        }
      });
      signalCount += signalBatch.length;
      signalBatch.length = 0;
    }
  }
  
  // Insert remaining
  if (signalBatch.length > 0) {
    db.transaction((tx) => {
      for (const record of signalBatch) {
        tx.insert(purchaseSignals).values(record).run();
      }
    });
    signalCount += signalBatch.length;
  }
  
  console.log(`  [done] Generated ${signalCount} purchase signal clusters`);
}

// Main pipeline entry point
export async function runSecBackfill(startYear = 2016): Promise<void> {
  console.log("=== SEC Historical Data Backfill Pipeline ===");
  console.log(`Starting from ${startYear} Q1\n`);
  
  // Ensure data directory exists
  if (!existsSync(DATA_DIR)) mkdirSync(DATA_DIR, { recursive: true });
  
  const quarters = getQuarterList(startYear);
  console.log(`[plan] ${quarters.length} quarters to process\n`);
  
  // Update pipeline status
  db.insert(pipelineStatus).values({
    taskName: "sec_backfill",
    status: "running",
    lastRunAt: new Date().toISOString(),
    totalItems: quarters.length,
    processedItems: 0,
    metadata: JSON.stringify({ startYear }),
  }).run();
  
  let totalInserted = 0;
  
  for (let i = 0; i < quarters.length; i++) {
    const q = quarters[i];
    const yq = `${q.year}Q${q.quarter}`;
    console.log(`\n[${i + 1}/${quarters.length}] Processing ${yq}`);
    
    try {
      // Download
      const zipPath = await downloadQuarter(q.filename);
      
      // Extract
      const extractDir = join(DATA_DIR, `${q.year}q${q.quarter}`);
      if (!existsSync(join(extractDir, "NONDERIV_TRANS.tsv"))) {
        extractZip(zipPath, extractDir);
      } else {
        console.log(`  [skip] Already extracted`);
      }
      
      // Process
      const count = await processQuarter(extractDir, yq);
      totalInserted += count;
      
      // Update progress
      db.update(pipelineStatus)
        .set({ 
          processedItems: i + 1,
          progress: ((i + 1) / quarters.length) * 100,
        })
        .where(eq(pipelineStatus.taskName, "sec_backfill"))
        .run();
        
    } catch (err: any) {
      console.error(`  [error] Failed to process ${yq}: ${err.message}`);
    }
  }
  
  console.log(`\n[complete] Inserted ${totalInserted} total purchase transactions`);
  
  // Build insider history
  await buildInsiderHistory();
  
  // Generate signal clusters
  await generateSignalClusters();
  
  // Final stats
  const txCount = db.select({ count: sql<number>`count(*)` }).from(insiderTransactions).get();
  const sigCount = db.select({ count: sql<number>`count(*)` }).from(purchaseSignals).get();
  const histCount = db.select({ count: sql<number>`count(*)` }).from(insiderHistory).get();
  
  console.log(`\n=== Final Database Stats ===`);
  console.log(`Transactions: ${txCount?.count}`);
  console.log(`Signal clusters: ${sigCount?.count}`);
  console.log(`Insider profiles: ${histCount?.count}`);
  
  // Update pipeline status
  db.update(pipelineStatus)
    .set({ 
      status: "completed",
      processedItems: quarters.length,
      progress: 100,
      metadata: JSON.stringify({ 
        startYear, 
        totalTransactions: txCount?.count,
        totalSignals: sigCount?.count,
        totalInsiders: histCount?.count,
      }),
    })
    .where(eq(pipelineStatus.taskName, "sec_backfill"))
    .run();
}

// CLI entry point
if (process.argv[1]?.includes("sec-backfill")) {
  const startYear = parseInt(process.argv[2] || "2016");
  runSecBackfill(startYear)
    .then(() => {
      console.log("\nBackfill complete.");
      process.exit(0);
    })
    .catch((err) => {
      console.error("Backfill failed:", err);
      process.exit(1);
    });
}
