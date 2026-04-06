/**
 * Standalone EDGAR Poller Process
 *
 * Runs separately from the web server to avoid blocking the event loop.
 * Connects to the same data.db with WAL mode for concurrent read/write.
 */

import Database from "better-sqlite3";
import { drizzle } from "drizzle-orm/better-sqlite3";
import { startV3Polling, stopV3Polling } from "./edgar-poller-v3";

// Create own SQLite connection (same settings as db.ts)
const sqlite = new Database("data.db");
sqlite.pragma("journal_mode = WAL");
sqlite.pragma("busy_timeout = 30000");
sqlite.pragma("cache_size = -20000"); // 20MB cache

const db = drizzle(sqlite);

console.log("[POLLER] Standalone EDGAR poller starting...");
console.log("[POLLER] Connected to data.db (WAL mode, busy_timeout=30s)");

// Start polling with our own db connection
startV3Polling(db);

// Graceful shutdown
function shutdown() {
  console.log("[POLLER] Shutting down...");
  stopV3Polling();
  sqlite.close();
  process.exit(0);
}

process.on("SIGTERM", shutdown);
process.on("SIGINT", shutdown);
