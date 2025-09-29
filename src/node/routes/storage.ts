import { field, logger } from "@coder/logger"
import * as express from "express"
import * as path from "path"
import { promises as fs } from "fs"
import { DatabaseSync } from "node:sqlite"
import { paths } from "../util"
import { Router as WsRouter, type WebsocketRequest } from "../wsRouter"
import { ensureAuthenticated } from "../http"
import * as stream from "stream"

interface StorageBatchRequest {
  insert?: Record<string, string>
  delete?: string[]
  lastSeenRevision?: number
}

interface StorageBatchResponse {
  ok: boolean
  newRevision: number
  changedKeys: string[]
}

class SQLiteBucketStore {
  private db: DatabaseSync;

  constructor(filePath: string) {
    this.db = new DatabaseSync(filePath);
    this.db.exec('CREATE TABLE IF NOT EXISTS ItemTable (key TEXT PRIMARY KEY, value BLOB)');
  }

  getAll(): [string, string][] {
    const stmt = this.db.prepare('SELECT key, value FROM ItemTable');
    const rows = stmt.all() as { key: string; value: Uint8Array }[];
    // No need to finalize() in Node.js 22 SQLite - statements are automatically managed
    return rows.map(row => [row.key, new TextDecoder().decode(row.value)]);
  }

  applyBatch(insert: Map<string, string>, deleteSet: Set<string>): void {
    this.db.exec('BEGIN IMMEDIATE');
    try {
      // Inserts
      if (insert.size > 0) {
        const insertStmt = this.db.prepare('INSERT INTO ItemTable (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value');
        for (const [key, value] of insert) {
          insertStmt.run(key, value);
        }
        // No need to finalize() in Node.js 22 SQLite - statements are automatically managed
      }
      // Deletes
      if (deleteSet.size > 0) {
        const placeholders = Array(deleteSet.size).fill('?').join(',');
        const deleteStmt = this.db.prepare(`DELETE FROM ItemTable WHERE key IN (${placeholders})`);
        deleteStmt.run(...Array.from(deleteSet));
        // No need to finalize() in Node.js 22 SQLite - statements are automatically managed
      }
      this.db.exec('COMMIT');
    } catch (error) {
      this.db.exec('ROLLBACK');
      throw error;
    }
  }

  close(): void {
    this.db.close();
  }
}

export const router = express.Router()
export const wsRouter = WsRouter()

// In-memory map of bucket to SQLiteBucketStore instances
const storageDatabases = new Map<string, SQLiteBucketStore>()

// In-memory map of bucket to revision
const bucketRevisions = new Map<string, number>()

// In-memory map of bucket to connected WS clients (using Duplex instead of WebSocket)
const bucketClients = new Map<string, Set<stream.Duplex>>()

/**
 * Get or create SQLiteBucketStore for a bucket
 */
async function getBucketStore(bucket: string): Promise<SQLiteBucketStore> {
  if (!storageDatabases.has(bucket)) {
    let dbPath: string;

    if (bucket.startsWith("workspace-")) {
      // Workspace storage: $DATA/User/workspaceStorage/<workspaceId>/state.vscdb
      const workspaceId = bucket.replace("workspace-", "");
      dbPath = path.join(paths.data, "User", "workspaceStorage", workspaceId, "state.vscdb");
    } else {
      // Global/profile storage: $DATA/User/globalStorage/profiles/<profileId>/state.vscdb
      const profileId = bucket.replace("global-", ""); // Remove "global-" prefix if present
      dbPath = path.join(paths.data, "User", "globalStorage", "profiles", profileId, "state.vscdb");
    }

    await fs.mkdir(path.dirname(dbPath), { recursive: true });
    const store = new SQLiteBucketStore(dbPath);
    storageDatabases.set(bucket, store);
    bucketRevisions.set(bucket, 0); // Initialize revision
  }
  return storageDatabases.get(bucket)!;
}

/**
 * Broadcast change to all WS clients for a bucket
 */
function broadcastChange(bucket: string, changedKeys: string[], newRevision: number) {
  const clients = bucketClients.get(bucket)
  if (clients) {
    const message = JSON.stringify({ bucket, changedKeys, newRevision })
    clients.forEach(client => {
      // For Duplex streams, we need to write the message instead of using WebSocket.send()
      client.write(message + '\n');
    })
  }
}

// GET /storage/{bucket} → { items, revision }
router.get("/storage/:bucket", ensureAuthenticated, async (req: express.Request, res: express.Response) => {
  try {
    const { bucket } = req.params
    const store = await getBucketStore(bucket)
    const items = store.getAll()
    const revision = bucketRevisions.get(bucket) || 0
    res.json({ items: Object.fromEntries(items), revision })
  } catch (error) {
    logger.error(`Storage GET error for bucket ${req.params.bucket}:`, field("error", error))
    res.status(500).json({ error: "Internal server error" })
  }
})

// POST /storage/{bucket}/batch { insert, delete, lastSeenRevision } → { ok, newRevision, changedKeys }
router.post("/storage/:bucket/batch", ensureAuthenticated, async (req: express.Request, res: express.Response) => {
  try {
    const { bucket } = req.params
    const body: StorageBatchRequest = req.body
    const { insert, delete: deleteKeys } = body
    const store = await getBucketStore(bucket)
    const currentRevision = bucketRevisions.get(bucket) || 0

    // Simple last-writer-wins: ignore lastSeenRevision for now, but could add conflict detection later
    const insertMap = new Map(Object.entries(insert || {}))
    const deleteSet = new Set(deleteKeys || [])

    store.applyBatch(insertMap, deleteSet)

    const newRevision = currentRevision + 1
    bucketRevisions.set(bucket, newRevision)

    const changedKeys = [...insertMap.keys(), ...Array.from(deleteSet)]
    broadcastChange(bucket, changedKeys, newRevision)

    const response: StorageBatchResponse = { ok: true, newRevision, changedKeys }
    res.json(response)
  } catch (error) {
    logger.error(`Storage POST batch error for bucket ${req.params.bucket}:`, field("error", error))
    res.status(500).json({ error: "Internal server error" })
  }
})

// WS /storage/changes/:bucket
wsRouter.ws("/storage/changes/:bucket", ensureAuthenticated, (req: WebsocketRequest) => {
  const ws = req.ws
  const bucket = (req as any).params.bucket

  if (!bucket) {
    // For Duplex streams, we end the connection instead of using WebSocket.close()
    ws.end("Bucket parameter required\n");
    return
  }

  if (!bucketClients.has(bucket)) {
    bucketClients.set(bucket, new Set())
  }
  bucketClients.get(bucket)!.add(ws)

  ws.on("close", () => {
    bucketClients.get(bucket)?.delete(ws)
  })

  ws.on("error", (error: Error) => {
    logger.error(`WS error for bucket ${bucket}:`, field("error", error))
    bucketClients.get(bucket)?.delete(ws)
  })
})

export function dispose() {
  // Close all databases
  for (const store of storageDatabases.values()) {
    try {
      store.close()
    } catch (error) {
      logger.error("Error closing storage DB:", field("error", error))
    }
  }
  storageDatabases.clear()
  bucketRevisions.clear()
  bucketClients.clear()
}