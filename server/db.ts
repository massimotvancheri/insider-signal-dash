import { drizzle } from "drizzle-orm/better-sqlite3";
import Database from "better-sqlite3";

const sqlite = new Database("data.db");
sqlite.pragma("journal_mode = WAL");
sqlite.pragma("busy_timeout = 30000");
sqlite.pragma("cache_size = -20000"); // 20MB cache

// Indexes are created via /api/admin/create-indexes endpoint (non-blocking, uses sqlite3 CLI)
// This avoids blocking the Node.js event loop on startup with large tables

export const db = drizzle(sqlite);
export { sqlite };
