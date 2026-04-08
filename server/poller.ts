/**
 * Standalone EDGAR Poller Process
 *
 * Runs separately from the web server to avoid blocking the event loop.
 * Connects to the same PostgreSQL database for concurrent read/write.
 */

import { drizzle } from "drizzle-orm/node-postgres";
import pg from "pg";
import { startV3Polling, stopV3Polling } from "./edgar-poller-v3";

// Create own PostgreSQL connection pool (same settings as db.ts)
const pool = new pg.Pool({
  connectionString: process.env.DATABASE_URL || "postgresql://postgres@localhost:5432/insider_signal",
  max: 5, // Poller needs fewer connections than main server
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 10000,
});

pool.on("error", (err) => {
  console.error("[POLLER] Unexpected pool error:", err.message);
});

const db = drizzle(pool);

console.log("[POLLER] Standalone EDGAR poller starting...");
console.log("[POLLER] Connected to PostgreSQL (insider_signal)");

// Start polling with our own db connection
startV3Polling(db);

// Graceful shutdown
function shutdown() {
  console.log("[POLLER] Shutting down...");
  stopV3Polling();
  pool.end().then(() => process.exit(0));
}

process.on("SIGTERM", shutdown);
process.on("SIGINT", shutdown);
