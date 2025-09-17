import { logger } from "@coder/logger"
import * as express from "express"
import * as path from "path"
import { promises as fs } from "fs"
import { SQLiteStorageDatabase } from "../../../lib/vscode/src/vs/base/parts/storage/node/storage"
import { paths } from "../util"
import { Router as WsRouter, type WebsocketRequest } from "../wsRouter"
import { ensureAuthenticated } from "../http"

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

export const router = express.Router()
export const wsRouter = WsRouter()

// In-memory map of bucket to SQLiteStorageDatabase instances
const storageDatabases = new Map<string, SQLiteStorageDatabase>()

// In-memory map of bucket to revision
const bucketRevisions = new Map<string, number>()

// In-memory map of bucket to connected WS clients
const bucketClients = new Map<string, Set<WebSocket>>()

/**
 * Get or create SQLiteStorageDatabase for a bucket
 */
async function getStorageDatabase(bucket: string): Promise<SQLiteStorageDatabase> {
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
    const db = new SQLiteStorageDatabase(dbPath);
    storageDatabases.set(bucket, db);
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
      if (client.readyState === WebSocket.OPEN) {
        client.send(message)
      }
    })
  }
}

// GET /storage/{bucket} → { items, revision }
router.get("/storage/:bucket", ensureAuthenticated, async (req: express.Request, res: express.Response) => {
  try {
    const { bucket } = req.params
    const db = await getStorageDatabase(bucket)
    const items = await db.getItems()
    const revision = bucketRevisions.get(bucket) || 0
    res.json({ items: Object.fromEntries(items), revision })
  } catch (error) {
    logger.error(`Storage GET error for bucket ${req.params.bucket}:`, error)
    res.status(500).json({ error: "Internal server error" })
  }
})

// POST /storage/{bucket}/batch { insert, delete, lastSeenRevision } → { ok, newRevision, changedKeys }
router.post("/storage/:bucket/batch", ensureAuthenticated, async (req: express.Request, res: express.Response) => {
  try {
    const { bucket } = req.params
    const body: StorageBatchRequest = req.body
    const { insert, delete: deleteKeys, lastSeenRevision } = body
    const db = await getStorageDatabase(bucket)
    const currentRevision = bucketRevisions.get(bucket) || 0

    // Simple last-writer-wins: ignore lastSeenRevision for now, but could add conflict detection later
    const insertMap = new Map(Object.entries(insert || {}))
    const deleteSet = new Set(deleteKeys || [])

    await db.updateItems({ insert: insertMap, delete: deleteSet })

    const newRevision = currentRevision + 1
    bucketRevisions.set(bucket, newRevision)

    const changedKeys = [...insertMap.keys(), ...Array.from(deleteSet)]
    broadcastChange(bucket, changedKeys, newRevision)

    const response: StorageBatchResponse = { ok: true, newRevision, changedKeys }
    res.json(response)
  } catch (error) {
    logger.error(`Storage POST batch error for bucket ${req.params.bucket}:`, error)
    res.status(500).json({ error: "Internal server error" })
  }
})

// WS /storage/changes/:bucket
wsRouter.ws("/storage/changes/:bucket", ensureAuthenticated, (req: WebsocketRequest) => {
  const ws = req.ws
  const bucket = (req as any).params.bucket

  if (!bucket) {
    ws.close(1008, "Bucket parameter required")
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
    logger.error(`WS error for bucket ${bucket}:`, error)
    bucketClients.get(bucket)?.delete(ws)
  })
})

export function dispose() {
  // Close all databases
  for (const db of storageDatabases.values()) {
    db.close().catch((error: Error) => logger.error("Error closing storage DB:", error))
  }
  storageDatabases.clear()
  bucketRevisions.clear()
  bucketClients.clear()
}