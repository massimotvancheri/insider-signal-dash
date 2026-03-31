/**
 * SEC EDGAR Form 4 Poller
 * 
 * Polls the EDGAR RSS feed for new Form 4 filings (insider transactions).
 * Parses the XML filings to extract purchase signals.
 * 
 * SEC Rate Limit: 10 requests/second with proper User-Agent.
 * We poll every 60 seconds during market hours, every 5 minutes otherwise.
 */

import { storage } from "./storage";
import type { InsertTransaction, InsertSignal } from "@shared/schema";

const SEC_USER_AGENT = "InsiderSignalDash research@insidersignal.app";
const EDGAR_RSS_URL = "https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=4&dateb=&owner=include&count=40&search_text=&start=0&output=atom";
const EDGAR_FULL_INDEX_URL = "https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=4&dateb=&owner=include&count=100&search_text=&start=0&output=atom";

// Transaction codes that indicate open-market purchases
const PURCHASE_CODES = new Set(["P"]);
// C-Suite title keywords
const C_SUITE_KEYWORDS = ["CEO", "CFO", "COO", "CTO", "CIO", "Chief", "President", "Chairman"];

let pollingInterval: ReturnType<typeof setInterval> | null = null;
let isPolling = false;
let pollCount = 0;
let lastPollTime: Date | null = null;
let lastError: string | null = null;

interface FilingEntry {
  accessionNumber: string;
  filingHref: string;
  companyName: string;
  cik: string;
  dateFiled: string;
  formType: string;
}

/**
 * Parse EDGAR Atom feed to extract Form 4 filing entries
 */
function parseAtomFeed(xml: string): FilingEntry[] {
  const entries: FilingEntry[] = [];
  
  // Simple regex-based XML parsing for EDGAR Atom feed
  const entryRegex = /<entry>([\s\S]*?)<\/entry>/g;
  let match;
  
  while ((match = entryRegex.exec(xml)) !== null) {
    const entry = match[1];
    
    const accessionMatch = entry.match(/accession-number[^>]*>([^<]+)/i) 
      || entry.match(/accession-nunber[^>]*>([^<]+)/i)
      || entry.match(/<id[^>]*>urn:tag:sec\.gov,2008:accession-number=([^<]+)/i);
    const titleMatch = entry.match(/<title[^>]*>([^<]+)/);
    const linkMatch = entry.match(/<link[^>]*href="([^"]+)"/);
    const updatedMatch = entry.match(/<updated>([^<]+)/);
    
    // Extract CIK from the link URL
    const cikMatch = entry.match(/CIK=(\d+)/i) || entry.match(/data\/(\d+)\//);
    
    if (titleMatch && linkMatch) {
      const title = titleMatch[1];
      const formTypeMatch = title.match(/^4\s/);
      
      entries.push({
        accessionNumber: accessionMatch ? accessionMatch[1].trim() : "",
        filingHref: linkMatch[1],
        companyName: title.replace(/^4\s*-\s*/, "").trim(),
        cik: cikMatch ? cikMatch[1] : "",
        dateFiled: updatedMatch ? updatedMatch[1].split("T")[0] : new Date().toISOString().split("T")[0],
        formType: "4",
      });
    }
  }
  
  return entries;
}

/**
 * Fetch and parse a Form 4 XML filing from EDGAR
 */
async function fetchForm4Details(filingUrl: string): Promise<Partial<InsertTransaction>[]> {
  const transactions: Partial<InsertTransaction>[] = [];
  
  try {
    // The filing URL typically goes to the index page. We need the XML.
    let xmlUrl = filingUrl;
    
    // If it's an index page, find the XML document
    if (!xmlUrl.endsWith(".xml")) {
      const indexResp = await fetch(xmlUrl, {
        headers: { "User-Agent": SEC_USER_AGENT, "Accept-Encoding": "gzip, deflate" }
      });
      if (!indexResp.ok) return transactions;
      const indexHtml = await indexResp.text();
      
      // Find the XML filing link
      const xmlMatch = indexHtml.match(/href="([^"]*\.xml)"/i);
      if (xmlMatch) {
        xmlUrl = xmlMatch[1];
        if (!xmlUrl.startsWith("http")) {
          xmlUrl = "https://www.sec.gov" + xmlUrl;
        }
      } else {
        return transactions;
      }
    }
    
    // Rate limit: wait briefly
    await new Promise(r => setTimeout(r, 150));
    
    const resp = await fetch(xmlUrl, {
      headers: { "User-Agent": SEC_USER_AGENT, "Accept-Encoding": "gzip, deflate" }
    });
    if (!resp.ok) return transactions;
    const xml = await resp.text();
    
    // Parse issuer info
    const issuerCik = extractXmlValue(xml, "issuerCik") || "";
    const issuerName = extractXmlValue(xml, "issuerName") || "";
    const issuerTicker = extractXmlValue(xml, "issuerTradingSymbol") || "";
    
    // Parse reporting person info
    const personName = extractXmlValue(xml, "rptOwnerName") || "";
    const personCik = extractXmlValue(xml, "rptOwnerCik") || "";
    
    // Parse relationship
    const isDirector = xml.includes("<isDirector>true</isDirector>") || xml.includes("<isDirector>1</isDirector>");
    const isOfficer = xml.includes("<isOfficer>true</isOfficer>") || xml.includes("<isOfficer>1</isOfficer>");
    const isTenPercent = xml.includes("<isTenPercentOwner>true</isTenPercentOwner>") || xml.includes("<isTenPercentOwner>1</isTenPercentOwner>");
    const officerTitle = extractXmlValue(xml, "officerTitle") || "";
    
    // Parse non-derivative transactions
    const txRegex = /<nonDerivativeTransaction>([\s\S]*?)<\/nonDerivativeTransaction>/g;
    let txMatch;
    
    while ((txMatch = txRegex.exec(xml)) !== null) {
      const txXml = txMatch[1];
      
      const securityTitle = extractXmlValue(txXml, "securityTitle>.*?<value") || 
                           extractNestedValue(txXml, "securityTitle");
      const txDate = extractXmlValue(txXml, "transactionDate>.*?<value") ||
                    extractNestedValue(txXml, "transactionDate");
      const txCode = extractXmlValue(txXml, "transactionCode") || "";
      const shares = parseFloat(extractXmlValue(txXml, "transactionShares>.*?<value") || 
                               extractNestedValue(txXml, "transactionShares") || "0");
      const price = parseFloat(extractXmlValue(txXml, "transactionPricePerShare>.*?<value") ||
                              extractNestedValue(txXml, "transactionPricePerShare") || "0");
      const sharesAfter = parseFloat(extractXmlValue(txXml, "sharesOwnedFollowingTransaction>.*?<value") ||
                                    extractNestedValue(txXml, "sharesOwnedFollowingTransaction") || "0");
      const ownershipNature = extractXmlValue(txXml, "directOrIndirectOwnership>.*?<value") ||
                             extractNestedValue(txXml, "directOrIndirectOwnership") || "D";
      
      // Determine transaction type
      const acquisitionDisposition = extractXmlValue(txXml, "transactionAcquiredDisposedCode>.*?<value") ||
                                    extractNestedValue(txXml, "transactionAcquiredDisposedCode") || "";
      
      let transactionType = "O"; // Other
      if (txCode === "P" || (acquisitionDisposition === "A" && txCode === "P")) {
        transactionType = "P"; // Purchase
      } else if (txCode === "S" || (acquisitionDisposition === "D" && txCode === "S")) {
        transactionType = "S"; // Sale
      } else if (txCode === "A") {
        transactionType = "A"; // Award/Grant
      } else if (txCode === "M") {
        transactionType = "M"; // Option exercise
      } else if (txCode === "G") {
        transactionType = "G"; // Gift
      }
      
      transactions.push({
        issuerCik,
        issuerName: cleanCompanyName(issuerName),
        issuerTicker: issuerTicker.toUpperCase(),
        reportingPersonName: cleanPersonName(personName),
        reportingPersonCik: personCik,
        reportingPersonTitle: officerTitle || (isDirector ? "Director" : ""),
        isDirector,
        isOfficer,
        isTenPercentOwner: isTenPercent,
        transactionType,
        transactionDate: txDate || "",
        sharesTraded: shares,
        pricePerShare: price,
        totalValue: shares * price,
        sharesOwnedAfter: sharesAfter,
        ownershipType: ownershipNature,
        securityTitle: securityTitle || "Common Stock",
        transactionCode: txCode,
      });
    }
  } catch (err) {
    // Silently continue — individual filing failures are expected
  }
  
  return transactions;
}

function extractXmlValue(xml: string, tag: string): string {
  const regex = new RegExp(`<${tag}>([^<]+)`, "i");
  const match = xml.match(regex);
  return match ? match[1].trim() : "";
}

function extractNestedValue(xml: string, tag: string): string {
  const regex = new RegExp(`<${tag}[^>]*>[\\s\\S]*?<value>([^<]+)`, "i");
  const match = xml.match(regex);
  return match ? match[1].trim() : "";
}

function cleanCompanyName(name: string): string {
  return name.replace(/\s+/g, " ").replace(/[\/\\]/g, "").trim();
}

function cleanPersonName(name: string): string {
  return name.replace(/\s+/g, " ").trim();
}

/**
 * Calculate signal score for a cluster of insider purchases at the same company
 */
function calculateSignalScore(transactions: InsertTransaction[]): number {
  if (transactions.length === 0) return 0;
  
  let score = 0;
  
  // 1. Cluster size (0-25 points): More insiders buying = stronger signal
  const uniqueInsiders = new Set(transactions.map(t => t.reportingPersonName)).size;
  score += Math.min(uniqueInsiders * 8, 25);
  
  // 2. Total purchase value (0-25 points): Larger $ amount = more conviction
  const totalValue = transactions.reduce((sum, t) => sum + (t.totalValue || 0), 0);
  if (totalValue >= 10_000_000) score += 25;
  else if (totalValue >= 1_000_000) score += 20;
  else if (totalValue >= 500_000) score += 15;
  else if (totalValue >= 100_000) score += 10;
  else if (totalValue >= 50_000) score += 5;
  
  // 3. C-Suite involvement (0-25 points): CEO/CFO purchases are strongest
  const cSuiteCount = transactions.filter(t => {
    const title = (t.reportingPersonTitle || "").toUpperCase();
    return C_SUITE_KEYWORDS.some(k => title.includes(k.toUpperCase()));
  }).length;
  score += Math.min(cSuiteCount * 10, 25);
  
  // 4. Transaction characteristics (0-25 points)
  // Direct ownership (stronger signal than indirect)
  const directCount = transactions.filter(t => t.ownershipType === "D").length;
  score += Math.min((directCount / transactions.length) * 15, 15);
  
  // Multiple transactions from same person (conviction)
  const txPerPerson = transactions.length / uniqueInsiders;
  if (txPerPerson >= 3) score += 10;
  else if (txPerPerson >= 2) score += 5;
  
  return Math.min(Math.round(score), 100);
}

/**
 * Process new filings and generate signals
 */
async function processNewFilings(): Promise<{ newTransactions: number; newSignals: number }> {
  let newTransactions = 0;
  let newSignals = 0;
  
  try {
    // Fetch the EDGAR RSS feed
    const resp = await fetch(EDGAR_FULL_INDEX_URL, {
      headers: { 
        "User-Agent": SEC_USER_AGENT,
        "Accept": "application/atom+xml, application/xml, text/xml",
      }
    });
    
    if (!resp.ok) {
      lastError = `EDGAR RSS returned ${resp.status}`;
      return { newTransactions: 0, newSignals: 0 };
    }
    
    const feedXml = await resp.text();
    const entries = parseAtomFeed(feedXml);
    
    // Process each filing entry
    const companyPurchases = new Map<string, InsertTransaction[]>();
    
    for (const entry of entries) {
      // Skip if we already have this filing
      if (entry.accessionNumber) {
        const existing = storage.getTransactionByAccession(entry.accessionNumber);
        if (existing) continue;
      }
      
      // Fetch and parse the Form 4 XML
      const details = await fetchForm4Details(entry.filingHref);
      
      for (const detail of details) {
        const tx: InsertTransaction = {
          accessionNumber: entry.accessionNumber || `gen-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`,
          filingDate: entry.dateFiled,
          filingTime: null,
          issuerCik: detail.issuerCik || entry.cik,
          issuerName: detail.issuerName || entry.companyName,
          issuerTicker: detail.issuerTicker || null,
          reportingPersonName: detail.reportingPersonName || "",
          reportingPersonCik: detail.reportingPersonCik || null,
          reportingPersonTitle: detail.reportingPersonTitle || null,
          isDirector: detail.isDirector ?? false,
          isOfficer: detail.isOfficer ?? false,
          isTenPercentOwner: detail.isTenPercentOwner ?? false,
          transactionType: detail.transactionType || "O",
          transactionDate: detail.transactionDate || null,
          sharesTraded: detail.sharesTraded || null,
          pricePerShare: detail.pricePerShare || null,
          totalValue: detail.totalValue || null,
          sharesOwnedAfter: detail.sharesOwnedAfter || null,
          ownershipType: detail.ownershipType || null,
          securityTitle: detail.securityTitle || null,
          transactionCode: detail.transactionCode || null,
          createdAt: new Date().toISOString(),
        };
        
        try {
          storage.insertTransaction(tx);
          newTransactions++;
          
          // Track purchases by company for cluster detection
          if (tx.transactionType === "P" && tx.totalValue && tx.totalValue > 0) {
            const key = tx.issuerTicker || tx.issuerCik;
            if (!companyPurchases.has(key)) {
              companyPurchases.set(key, []);
            }
            companyPurchases.get(key)!.push(tx);
          }
        } catch (err) {
          // Skip duplicates
        }
      }
    }
    
    // Generate signals for companies with cluster purchases
    for (const [key, purchases] of companyPurchases) {
      if (purchases.length >= 1) { // Generate signal for any purchase
        const score = calculateSignalScore(purchases);
        const uniqueInsiders = [...new Set(purchases.map(p => p.reportingPersonName))];
        const uniqueTitles = [...new Set(purchases.map(p => p.reportingPersonTitle || "Unknown"))];
        
        const cSuiteCount = purchases.filter(p => {
          const title = (p.reportingPersonTitle || "").toUpperCase();
          return C_SUITE_KEYWORDS.some(k => title.includes(k.toUpperCase()));
        }).length;
        
        const directorCount = purchases.filter(p => p.isDirector).length;
        
        const signal: InsertSignal = {
          issuerCik: purchases[0].issuerCik,
          issuerName: purchases[0].issuerName,
          issuerTicker: purchases[0].issuerTicker || null,
          signalDate: new Date().toISOString().split("T")[0],
          signalScore: score,
          clusterSize: uniqueInsiders.length,
          totalPurchaseValue: purchases.reduce((s, p) => s + (p.totalValue || 0), 0),
          avgPurchasePrice: purchases.reduce((s, p) => s + (p.pricePerShare || 0), 0) / purchases.length,
          insiderNames: JSON.stringify(uniqueInsiders),
          insiderTitles: JSON.stringify(uniqueTitles),
          purchaseValueRatio: null,
          unanimityScore: null,
          cSuiteCount,
          directorCount,
          daysSpan: 1,
          createdAt: new Date().toISOString(),
        };
        
        try {
          storage.insertSignal(signal);
          newSignals++;
        } catch (err) {
          // Skip
        }
      }
    }
    
    // Update polling state
    const state = storage.getPollingState();
    const totalProcessed = (state?.totalFilingsProcessed || 0) + newTransactions;
    storage.updatePollingState(
      new Date().toISOString(),
      entries[0]?.accessionNumber,
      totalProcessed
    );
    
    lastError = null;
    
  } catch (err: any) {
    lastError = err.message || "Unknown polling error";
  }
  
  return { newTransactions, newSignals };
}

/**
 * Seed the database with realistic sample data for demonstration
 */
export function seedDemoData() {
  const count = storage.getTransactionCount();
  if (count > 0) return; // Already seeded
  
  const now = new Date();
  const companies = [
    { cik: "0000320193", name: "Apple Inc", ticker: "AAPL" },
    { cik: "0000789019", name: "Microsoft Corp", ticker: "MSFT" },
    { cik: "0001318605", name: "Tesla Inc", ticker: "TSLA" },
    { cik: "0001652044", name: "Alphabet Inc", ticker: "GOOGL" },
    { cik: "0001018724", name: "Amazon.com Inc", ticker: "AMZN" },
    { cik: "0000886982", name: "Goldman Sachs Group Inc", ticker: "GS" },
    { cik: "0000070858", name: "Bank of America Corp", ticker: "BAC" },
    { cik: "0000019617", name: "JPMorgan Chase & Co", ticker: "JPM" },
    { cik: "0000804328", name: "Procter & Gamble Co", ticker: "PG" },
    { cik: "0000200406", name: "Johnson & Johnson", ticker: "JNJ" },
    { cik: "0001326801", name: "Meta Platforms Inc", ticker: "META" },
    { cik: "0001045810", name: "NVIDIA Corp", ticker: "NVDA" },
    { cik: "0000858877", name: "Berkshire Hathaway Inc", ticker: "BRK.B" },
    { cik: "0000051143", name: "Intl Business Machines Corp", ticker: "IBM" },
    { cik: "0000078003", name: "Pfizer Inc", ticker: "PFE" },
    { cik: "0000831001", name: "Citigroup Inc", ticker: "C" },
    { cik: "0000732717", name: "AT&T Inc", ticker: "T" },
    { cik: "0000092122", name: "Starbucks Corp", ticker: "SBUX" },
    { cik: "0000021344", name: "Coca-Cola Co", ticker: "KO" },
    { cik: "0000077476", name: "PepsiCo Inc", ticker: "PEP" },
  ];
  
  const insiders = [
    { name: "Tim Cook", title: "Chief Executive Officer", isOfficer: true, isDirector: true },
    { name: "Satya Nadella", title: "Chief Executive Officer", isOfficer: true, isDirector: false },
    { name: "Luca Maestri", title: "Chief Financial Officer", isOfficer: true, isDirector: false },
    { name: "Amy Hood", title: "Chief Financial Officer", isOfficer: true, isDirector: false },
    { name: "James Gorman", title: "Chairman of the Board", isOfficer: false, isDirector: true },
    { name: "Ruth Porat", title: "President & CIO", isOfficer: true, isDirector: false },
    { name: "Jamie Dimon", title: "Chairman & CEO", isOfficer: true, isDirector: true },
    { name: "David Solomon", title: "Chairman & CEO", isOfficer: true, isDirector: true },
    { name: "Warren Buffett", title: "Chairman of the Board", isOfficer: false, isDirector: true },
    { name: "Andrea Jung", title: "Director", isOfficer: false, isDirector: true },
    { name: "John Thompson", title: "Director", isOfficer: false, isDirector: true },
    { name: "Susan Wagner", title: "Director", isOfficer: false, isDirector: true },
    { name: "Alex Gorsky", title: "Director", isOfficer: false, isDirector: true },
    { name: "Brian Moynihan", title: "Chief Executive Officer", isOfficer: true, isDirector: true },
    { name: "Jensen Huang", title: "President & CEO", isOfficer: true, isDirector: true },
    { name: "Mark Zuckerberg", title: "Chairman & CEO", isOfficer: true, isDirector: true },
    { name: "Mary Barra", title: "Director", isOfficer: false, isDirector: true },
    { name: "James Quincey", title: "Chairman & CEO", isOfficer: true, isDirector: true },
    { name: "Ramon Laguarta", title: "Chairman & CEO", isOfficer: true, isDirector: true },
    { name: "Albert Bourla", title: "Chairman & CEO", isOfficer: true, isDirector: true },
  ];
  
  const txTypes = ["P", "P", "P", "P", "S", "A", "M"]; // Weight towards purchases
  
  // Generate 300+ transactions over the past 60 days
  for (let i = 0; i < 340; i++) {
    const daysAgo = Math.floor(Math.random() * 60);
    const date = new Date(now);
    date.setDate(date.getDate() - daysAgo);
    const dateStr = date.toISOString().split("T")[0];
    
    const company = companies[Math.floor(Math.random() * companies.length)];
    const insider = insiders[Math.floor(Math.random() * insiders.length)];
    const txType = txTypes[Math.floor(Math.random() * txTypes.length)];
    
    // Realistic price ranges per company
    const basePrices: Record<string, number> = {
      AAPL: 188, MSFT: 425, TSLA: 178, GOOGL: 156, AMZN: 185,
      GS: 525, BAC: 39, JPM: 205, PG: 168, JNJ: 156,
      META: 510, NVDA: 875, "BRK.B": 420, IBM: 195, PFE: 28,
      C: 65, T: 22, SBUX: 98, KO: 63, PEP: 172
    };
    const basePrice = basePrices[company.ticker] || 100;
    const price = basePrice * (0.95 + Math.random() * 0.1); // ±5% variation
    
    // Realistic share quantities
    const shareMultiplier = txType === "P" ? 
      (insider.isOfficer ? (1000 + Math.floor(Math.random() * 50000)) : (500 + Math.floor(Math.random() * 20000))) :
      (100 + Math.floor(Math.random() * 10000));
    
    const shares = shareMultiplier;
    const totalVal = shares * price;
    
    const tx: InsertTransaction = {
      accessionNumber: `0001-${(26000000 + i).toString()}-${dateStr.replace(/-/g, "")}`,
      filingDate: dateStr,
      filingTime: `${8 + Math.floor(Math.random() * 10)}:${String(Math.floor(Math.random() * 60)).padStart(2, "0")}:00`,
      issuerCik: company.cik,
      issuerName: company.name,
      issuerTicker: company.ticker,
      reportingPersonName: insider.name,
      reportingPersonCik: `000${1000000 + i}`,
      reportingPersonTitle: insider.title,
      isDirector: insider.isDirector,
      isOfficer: insider.isOfficer,
      isTenPercentOwner: Math.random() < 0.05,
      transactionType: txType,
      transactionDate: dateStr,
      sharesTraded: shares,
      pricePerShare: Math.round(price * 100) / 100,
      totalValue: Math.round(totalVal * 100) / 100,
      sharesOwnedAfter: shares * (2 + Math.floor(Math.random() * 10)),
      ownershipType: Math.random() < 0.85 ? "D" : "I",
      securityTitle: "Common Stock",
      transactionCode: txType,
      createdAt: date.toISOString(),
    };
    
    try {
      storage.insertTransaction(tx);
    } catch (e) {
      // Skip duplicates
    }
  }
  
  // Generate cluster signals for companies with multiple insider purchases
  const clusterCompanies = companies.slice(0, 12); // Top companies
  for (const company of clusterCompanies) {
    const daysAgo = Math.floor(Math.random() * 30);
    const date = new Date(now);
    date.setDate(date.getDate() - daysAgo);
    
    const numInsiders = 2 + Math.floor(Math.random() * 4);
    const selectedInsiders = insiders.sort(() => Math.random() - 0.5).slice(0, numInsiders);
    const totalVal = (100000 + Math.random() * 5000000);
    const cSuiteCount = selectedInsiders.filter(i => 
      C_SUITE_KEYWORDS.some(k => i.title.toUpperCase().includes(k.toUpperCase()))
    ).length;
    const dirCount = selectedInsiders.filter(i => i.isDirector).length;
    
    // Calculate a realistic signal score
    let score = 0;
    score += Math.min(numInsiders * 8, 25);
    if (totalVal >= 1_000_000) score += 20;
    else if (totalVal >= 500_000) score += 15;
    else if (totalVal >= 100_000) score += 10;
    score += Math.min(cSuiteCount * 10, 25);
    score += 12; // Direct ownership bonus
    score = Math.min(score, 100);
    
    const signal: InsertSignal = {
      issuerCik: company.cik,
      issuerName: company.name,
      issuerTicker: company.ticker,
      signalDate: date.toISOString().split("T")[0],
      signalScore: score,
      clusterSize: numInsiders,
      totalPurchaseValue: Math.round(totalVal * 100) / 100,
      avgPurchasePrice: Math.round((totalVal / (numInsiders * 5000)) * 100) / 100,
      insiderNames: JSON.stringify(selectedInsiders.map(i => i.name)),
      insiderTitles: JSON.stringify(selectedInsiders.map(i => i.title)),
      purchaseValueRatio: null,
      unanimityScore: Math.round((numInsiders / 8) * 100),
      cSuiteCount,
      directorCount: dirCount,
      daysSpan: 1 + Math.floor(Math.random() * 14),
      createdAt: new Date().toISOString(),
    };
    
    try {
      storage.insertSignal(signal);
    } catch (e) {
      // Skip
    }
  }
  
  // Initialize polling state
  storage.updatePollingState(new Date().toISOString(), undefined, 340);
}

/**
 * Start the polling loop
 */
export function startPolling(intervalMs = 60_000) {
  if (pollingInterval) return;
  
  // Seed demo data on first run
  seedDemoData();
  
  const poll = async () => {
    if (isPolling) return;
    isPolling = true;
    pollCount++;
    lastPollTime = new Date();
    
    try {
      await processNewFilings();
    } catch (err: any) {
      lastError = err.message;
    } finally {
      isPolling = false;
    }
  };
  
  // Initial poll
  poll();
  
  // Set interval
  pollingInterval = setInterval(poll, intervalMs);
}

/**
 * Stop polling
 */
export function stopPolling() {
  if (pollingInterval) {
    clearInterval(pollingInterval);
    pollingInterval = null;
  }
}

/**
 * Get polling status for the dashboard
 */
export function getPollingStatus() {
  const state = storage.getPollingState();
  return {
    isActive: !!pollingInterval,
    isCurrentlyPolling: isPolling,
    pollCount,
    lastPollTime: lastPollTime?.toISOString() || null,
    lastError,
    totalFilingsProcessed: state?.totalFilingsProcessed || 0,
    lastAccessionNumber: state?.lastAccessionNumber || null,
    status: state?.status || "initializing",
  };
}
