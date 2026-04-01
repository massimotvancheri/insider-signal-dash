#!/bin/bash
# Create database indexes after service starts
DB="/opt/insider-signal-dash/data.db"
sleep 5  # Wait for service to be ready
sqlite3 "$DB" "
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
  CREATE INDEX IF NOT EXISTS idx_fwd_returns_signal_day ON daily_forward_returns(signal_id, trading_day);
  CREATE INDEX IF NOT EXISTS idx_fwd_returns_day ON daily_forward_returns(trading_day);
"
echo "$(date) - Indexes created: $(sqlite3 $DB "SELECT count(*) FROM sqlite_master WHERE type='index';")"
