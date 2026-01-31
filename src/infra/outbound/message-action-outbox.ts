import { randomUUID } from "node:crypto";
import fs from "node:fs/promises";
import path from "node:path";

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
};

export type MessageActionOutboxEntry = {
  id: string;
  status: "pending" | "leased" | "done" | "failed" | "dead";
  attempts: number;
  availableAt: number;
  leaseOwner?: string;
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

function resolveOutboxPath(baseDir?: string) {
  const root = baseDir ?? resolveStateDir();
  const dir = path.join(root, "outbound");
  return {
    dir,
    outboxPath: path.join(dir, "message-action-outbox.json"),
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
  try {
    return await fn();
  } finally {
    release?.();
  }
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
      entry.leaseUntil = now + opts.leaseMs;
      entry.updatedAt = now;
      state.entriesById[entry.id] = entry;
    }

    await persistState(state);
    return eligible;
  });
}

export async function markMessageActionOutboxDone(id: string): Promise<void> {
  return await withLock(async () => {
    const state = await loadState();
    const entry = state.entriesById[id];
    if (!entry) {
      return;
    }
    const now = Date.now();
    entry.status = "done";
    entry.updatedAt = now;
    entry.leaseOwner = undefined;
    entry.leaseUntil = undefined;
    entry.lastError = undefined;
    state.entriesById[id] = entry;
    await persistState(state);
  });
}

export async function markMessageActionOutboxFailed(
  id: string,
  err: unknown,
  opts?: { now?: number; maxAttempts?: number },
): Promise<void> {
  return await withLock(async () => {
    const state = await loadState();
    const entry = state.entriesById[id];
    if (!entry) {
      return;
    }
    const now = opts?.now ?? Date.now();
    const maxAttempts = opts?.maxAttempts ?? DEFAULT_MAX_ATTEMPTS;
    const attempts = entry.attempts + 1;
    entry.attempts = attempts;
    entry.lastError = formatError(err);
    entry.leaseOwner = undefined;
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
