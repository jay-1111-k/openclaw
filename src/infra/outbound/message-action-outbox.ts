import { randomUUID } from "node:crypto";
import fs from "node:fs/promises";
import path from "node:path";

import type { OpenClawConfig } from "../../config/config.js";
import { resolveStateDir } from "../../config/paths.js";
import type {
  ChannelMessageActionName,
  ChannelThreadingToolContext,
} from "../../channels/plugins/types.js";
import type { MessageActionRunnerGateway } from "./message-action-runner.js";

export type OutboxMessageActionPayloadV1 = {
  action: ChannelMessageActionName;
  params: Record<string, unknown>;
  defaultAccountId?: string;
  toolContext?: ChannelThreadingToolContext;
  gateway?: MessageActionRunnerGateway;
  sessionKey?: string;
  agentId?: string;
  dryRun?: boolean;
  cfgSnapshot?: OpenClawConfig;
};

export type MessageActionOutboxEntry = {
  id: string;
  status: "pending" | "leased" | "done" | "failed" | "dead";
  attempts: number;
  availableAt: number;
  leaseOwner?: string;
  leaseToken?: string;
  leaseUntil?: number;
  createdAt: number;
  updatedAt: number;
  lastError?: string;
  payload: OutboxMessageActionPayloadV1;
};

type MessageActionOutboxState = {
  entriesById: Record<string, MessageActionOutboxEntry>;
};

const DEFAULT_MAX_ATTEMPTS = 10;
const MAX_BACKOFF_MS = 60_000;
const BASE_BACKOFF_MS = 1_000;
const LOCK_STALE_MS = 30_000;
const LOCK_RETRY_LIMIT = 24;
const LOCK_RETRY_BASE_MS = 25;
const LOCK_RETRY_MAX_MS = 500;

function resolveOutboxPath(baseDir?: string) {
  const root = baseDir ?? resolveStateDir();
  const dir = path.join(root, "outbound");
  return {
    dir,
    outboxPath: path.join(dir, "message-action-outbox.json"),
    lockPath: path.join(dir, "message-action-outbox.json.lock"),
  };
}

async function readJSON<T>(filePath: string): Promise<T | null> {
  try {
    const raw = await fs.readFile(filePath, "utf8");
    return JSON.parse(raw) as T;
  } catch {
    return null;
  }
}

async function writeJSONAtomic(filePath: string, value: unknown) {
  const dir = path.dirname(filePath);
  await fs.mkdir(dir, { recursive: true });
  const tmp = `${filePath}.${randomUUID()}.tmp`;
  await fs.writeFile(tmp, JSON.stringify(value, null, 2), "utf8");
  try {
    await fs.chmod(tmp, 0o600);
  } catch {
    // best-effort; ignore on platforms without chmod
  }
  await fs.rename(tmp, filePath);
  try {
    await fs.chmod(filePath, 0o600);
  } catch {
    // best-effort; ignore on platforms without chmod
  }
}

let lock: Promise<void> = Promise.resolve();
async function withLock<T>(fn: () => Promise<T>): Promise<T> {
  const prev = lock;
  let release: (() => void) | undefined;
  lock = new Promise<void>((resolve) => {
    release = resolve;
  });
  await prev;
  const { lockPath } = resolveOutboxPath();
  let acquired = false;
  try {
    await acquireOutboxLock(lockPath);
    acquired = true;
    return await fn();
  } finally {
    if (acquired) {
      await releaseOutboxLock(lockPath);
    }
    release?.();
  }
}

function isProcessAlive(pid: number): boolean {
  try {
    process.kill(pid, 0);
    return true;
  } catch (err) {
    if (err instanceof Error && "code" in err && err.code === "ESRCH") {
      return false;
    }
    return true;
  }
}

async function readLockInfo(
  lockPath: string,
): Promise<{ pid?: number; createdAt?: number } | null> {
  try {
    const raw = await fs.readFile(lockPath, "utf8");
    const parsed = JSON.parse(raw) as { pid?: number; createdAt?: number };
    return parsed;
  } catch {
    return null;
  }
}

async function isLockStale(lockPath: string): Promise<boolean> {
  try {
    const stat = await fs.stat(lockPath);
    const now = Date.now();
    if (now - stat.mtimeMs > LOCK_STALE_MS) {
      return true;
    }
    const info = await readLockInfo(lockPath);
    if (info?.pid && !isProcessAlive(info.pid)) {
      return true;
    }
    return false;
  } catch {
    return true;
  }
}

async function acquireOutboxLock(lockPath: string): Promise<void> {
  for (let attempt = 0; attempt < LOCK_RETRY_LIMIT; attempt += 1) {
    try {
      await fs.mkdir(path.dirname(lockPath), { recursive: true });
      const handle = await fs.open(lockPath, "wx");
      try {
        await handle.writeFile(
          JSON.stringify({ pid: process.pid, createdAt: Date.now() }, null, 2),
          "utf8",
        );
      } finally {
        await handle.close();
      }
      return;
    } catch (err) {
      if (!(err instanceof Error) || !("code" in err) || err.code !== "EEXIST") {
        throw err;
      }
      if (await isLockStale(lockPath)) {
        await fs.unlink(lockPath).catch(() => undefined);
        continue;
      }
      const backoff = Math.min(LOCK_RETRY_MAX_MS, LOCK_RETRY_BASE_MS * 2 ** attempt);
      const jitter = Math.floor(Math.random() * LOCK_RETRY_BASE_MS);
      await new Promise((resolve) => setTimeout(resolve, backoff + jitter));
    }
  }
  throw new Error("Timed out acquiring message action outbox lock.");
}

async function releaseOutboxLock(lockPath: string): Promise<void> {
  await fs.unlink(lockPath).catch(() => undefined);
}

async function loadState(baseDir?: string): Promise<MessageActionOutboxState> {
  const { outboxPath } = resolveOutboxPath(baseDir);
  const state = await readJSON<MessageActionOutboxState>(outboxPath);
  return {
    entriesById: state?.entriesById ?? {},
  };
}

async function persistState(state: MessageActionOutboxState, baseDir?: string): Promise<void> {
  const { outboxPath } = resolveOutboxPath(baseDir);
  await writeJSONAtomic(outboxPath, state);
}

function formatError(err: unknown): string {
  if (err instanceof Error) {
    return err.message || err.name;
  }
  return String(err);
}

function computeBackoffMs(attempts: number): number {
  const factor = Math.max(0, attempts - 1);
  return Math.min(MAX_BACKOFF_MS, BASE_BACKOFF_MS * 2 ** factor);
}

export async function enqueueMessageActionOutbox(
  payload: OutboxMessageActionPayloadV1,
): Promise<{ id: string }> {
  return await withLock(async () => {
    const state = await loadState();
    const now = Date.now();
    const id = randomUUID();
    state.entriesById[id] = {
      id,
      status: "pending",
      attempts: 0,
      availableAt: now,
      createdAt: now,
      updatedAt: now,
      payload,
    };
    await persistState(state);
    return { id };
  });
}

export async function leaseMessageActionOutbox(opts: {
  limit: number;
  leaseMs: number;
  workerId: string;
  now?: number;
}): Promise<MessageActionOutboxEntry[]> {
  return await withLock(async () => {
    const state = await loadState();
    const now = opts.now ?? Date.now();
    const eligible = Object.values(state.entriesById)
      .filter((entry) => {
        if (entry.status === "done" || entry.status === "dead") {
          return false;
        }
        if (entry.availableAt > now) {
          return false;
        }
        if (entry.status === "leased") {
          const leaseUntil = entry.leaseUntil ?? 0;
          return leaseUntil <= now;
        }
        return true;
      })
      .sort((a, b) => {
        if (a.availableAt !== b.availableAt) {
          return a.availableAt - b.availableAt;
        }
        return a.createdAt - b.createdAt;
      })
      .slice(0, Math.max(0, opts.limit));

    if (eligible.length === 0) {
      return [];
    }

    for (const entry of eligible) {
      entry.status = "leased";
      entry.leaseOwner = opts.workerId;
      entry.leaseToken = randomUUID();
      entry.leaseUntil = now + opts.leaseMs;
      entry.updatedAt = now;
      state.entriesById[entry.id] = entry;
    }

    await persistState(state);
    return eligible;
  });
}

export async function extendMessageActionOutboxLease(opts: {
  id: string;
  leaseToken: string;
  leaseMs: number;
  now?: number;
}): Promise<boolean> {
  return await withLock(async () => {
    const state = await loadState();
    const entry = state.entriesById[opts.id];
    if (!entry || entry.status !== "leased") {
      return false;
    }
    if (entry.leaseToken !== opts.leaseToken) {
      return false;
    }
    const now = opts.now ?? Date.now();
    entry.leaseUntil = now + opts.leaseMs;
    entry.updatedAt = now;
    state.entriesById[entry.id] = entry;
    await persistState(state);
    return true;
  });
}

export async function markMessageActionOutboxDone(
  id: string,
  opts?: { leaseToken?: string; now?: number },
): Promise<void> {
  return await withLock(async () => {
    const state = await loadState();
    const entry = state.entriesById[id];
    if (!entry) {
      return;
    }
    if (entry.leaseToken && entry.leaseToken !== opts?.leaseToken) {
      return;
    }
    const now = opts?.now ?? Date.now();
    entry.status = "done";
    entry.updatedAt = now;
    entry.leaseOwner = undefined;
    entry.leaseToken = undefined;
    entry.leaseUntil = undefined;
    entry.lastError = undefined;
    state.entriesById[id] = entry;
    await persistState(state);
  });
}

export async function markMessageActionOutboxFailed(
  id: string,
  err: unknown,
  opts?: { now?: number; maxAttempts?: number; leaseToken?: string },
): Promise<void> {
  return await withLock(async () => {
    const state = await loadState();
    const entry = state.entriesById[id];
    if (!entry) {
      return;
    }
    if (entry.leaseToken && entry.leaseToken !== opts?.leaseToken) {
      return;
    }
    const now = opts?.now ?? Date.now();
    const maxAttempts = opts?.maxAttempts ?? DEFAULT_MAX_ATTEMPTS;
    const attempts = entry.attempts + 1;
    entry.attempts = attempts;
    entry.lastError = formatError(err);
    entry.leaseOwner = undefined;
    entry.leaseToken = undefined;
    entry.leaseUntil = undefined;
    if (attempts >= maxAttempts) {
      entry.status = "dead";
      entry.availableAt = now;
    } else {
      entry.status = "failed";
      entry.availableAt = now + computeBackoffMs(attempts);
    }
    entry.updatedAt = now;
    state.entriesById[id] = entry;
    await persistState(state);
  });
}
