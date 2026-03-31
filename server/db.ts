import { drizzle } from "drizzle-orm/better-sqlite3";
import Database from "better-sqlite3";

const sqlite = new Database("data.db");
sqlite.pragma("journal_mode = WAL");
sqlite.pragma("busy_timeout = 30000");
sqlite.pragma("cache_size = -20000"); // 20MB cache

// Create indexes for performance (idempotent — IF NOT EXISTS)
try {
  sqlite.exec(`
    CREATE INDEX IF NOT EXISTS idx_tx_type_filing_date ON insider_transactions(transaction_type, filing_date);
    CREATE INDEX IF NOT EXISTS idx_tx_accession ON insider_transactions(accession_number);
    CREATE INDEX IF NOT EXISTS idx_tx_ticker ON insider_transactions(issuer_ticker);
    CREATE INDEX IF NOT EXISTS idx_tx_person_cik ON insider_transactions(reporting_person_cik);
    CREATE INDEX IF NOT EXISTS idx_signals_date ON purchase_signals(signal_date);
    CREATE INDEX IF NOT EXISTS idx_signals_score ON purchase_signals(signal_score);
    CREATE INDEX IF NOT EXISTS idx_signals_tier ON purchase_signals(score_tier);
    CREATE INDEX IF NOT EXISTS idx_entry_prices_signal ON signal_entry_prices(signal_id);
    CREATE INDEX IF NOT EXISTS idx_fwd_returns_signal_day ON daily_forward_returns(signal_id, trading_day);
    CREATE INDEX IF NOT EXISTS idx_fwd_returns_day ON daily_forward_returns(trading_day);
    CREATE INDEX IF NOT EXISTS idx_factor_analysis_factor ON factor_analysis(factor_name, horizon);
    CREATE INDEX IF NOT EXISTS idx_exec_deviations_signal ON execution_deviations(signal_id);
    CREATE INDEX IF NOT EXISTS idx_exec_deviations_trade ON execution_deviations(user_trade_id);
  `);
  console.log("[DB] Indexes verified/created");
} catch (e: any) {
  console.error("[DB] Index creation warning:", e.message);
}

export const db = drizzle(sqlite);
export { sqlite };
