import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import {
  enqueueMessageActionOutbox,
  leaseMessageActionOutbox,
  markMessageActionOutboxDone,
  markMessageActionOutboxFailed,
  type OutboxMessageActionPayloadV1,
} from "./message-action-outbox.js";

const payload: OutboxMessageActionPayloadV1 = {
  action: "send",
  params: { target: "telegram:123", message: "hi" },
};

function resolveOutboxPath(root: string): string {
  return path.join(root, "outbound", "message-action-outbox.json");
}

function resolveOutboxLockPath(root: string): string {
  return path.join(root, "outbound", "message-action-outbox.json.lock");
}

async function readOutbox(root: string) {
  const raw = await fs.readFile(resolveOutboxPath(root), "utf8");
  return JSON.parse(raw) as { entriesById: Record<string, unknown> };
}

describe("message-action outbox", () => {
  let stateDir: string;
  let prevStateDir: string | undefined;

  beforeEach(async () => {
    prevStateDir = process.env.OPENCLAW_STATE_DIR;
    stateDir = await fs.mkdtemp(path.join(os.tmpdir(), "openclaw-outbox-"));
    process.env.OPENCLAW_STATE_DIR = stateDir;
    vi.useFakeTimers();
  });

  afterEach(async () => {
    vi.useRealTimers();
    if (prevStateDir === undefined) {
      delete process.env.OPENCLAW_STATE_DIR;
    } else {
      process.env.OPENCLAW_STATE_DIR = prevStateDir;
    }
    await fs.rm(stateDir, { recursive: true, force: true });
  });

  it("enqueue -> lease -> done", async () => {
    vi.setSystemTime(new Date("2024-01-01T00:00:00Z"));
    const { id } = await enqueueMessageActionOutbox(payload);
    const leased = await leaseMessageActionOutbox({
      limit: 1,
      leaseMs: 5_000,
      workerId: "worker-a",
      now: Date.now(),
    });
    expect(leased).toHaveLength(1);
    expect(leased[0]?.id).toBe(id);

    await markMessageActionOutboxDone(id, { leaseToken: leased[0]?.leaseToken });

    const followUp = await leaseMessageActionOutbox({
      limit: 1,
      leaseMs: 5_000,
      workerId: "worker-a",
      now: Date.now(),
    });
    expect(followUp).toHaveLength(0);

    const state = await readOutbox(stateDir);
    const entry = state.entriesById[id] as { status?: string };
    expect(entry?.status).toBe("done");
  });

  it("enqueue -> lease -> fail -> backoff -> re-lease after backoff", async () => {
    vi.setSystemTime(1_000);
    const { id } = await enqueueMessageActionOutbox(payload);

    const leased = await leaseMessageActionOutbox({
      limit: 1,
      leaseMs: 5_000,
      workerId: "worker-a",
      now: 1_000,
    });
    expect(leased).toHaveLength(1);

    await markMessageActionOutboxFailed(id, new Error("nope"), {
      now: 1_000,
      leaseToken: leased[0]?.leaseToken,
    });

    const retryTooSoon = await leaseMessageActionOutbox({
      limit: 1,
      leaseMs: 5_000,
      workerId: "worker-b",
      now: 1_500,
    });
    expect(retryTooSoon).toHaveLength(0);

    const retryReady = await leaseMessageActionOutbox({
      limit: 1,
      leaseMs: 5_000,
      workerId: "worker-b",
      now: 2_000,
    });
    expect(retryReady).toHaveLength(1);
    expect(retryReady[0]?.id).toBe(id);
  });

  it("lease expiration allows re-lease", async () => {
    vi.setSystemTime(1_000);
    const { id } = await enqueueMessageActionOutbox(payload);

    const leased = await leaseMessageActionOutbox({
      limit: 1,
      leaseMs: 200,
      workerId: "worker-a",
      now: 1_000,
    });
    expect(leased).toHaveLength(1);
    expect(leased[0]?.id).toBe(id);

    const reLeased = await leaseMessageActionOutbox({
      limit: 1,
      leaseMs: 200,
      workerId: "worker-b",
      now: 1_300,
    });
    expect(reLeased).toHaveLength(1);
    expect(reLeased[0]?.leaseOwner).toBe("worker-b");
  });

  it("waits for the outbox lock to clear", async () => {
    const lockPath = resolveOutboxLockPath(stateDir);
    await fs.mkdir(path.dirname(lockPath), { recursive: true });
    await fs.writeFile(
      lockPath,
      JSON.stringify({ pid: process.pid, createdAt: Date.now() }),
      "utf8",
    );

    const enqueuePromise = enqueueMessageActionOutbox(payload);
    await vi.advanceTimersByTimeAsync(100);
    await fs.unlink(lockPath);
    await vi.advanceTimersByTimeAsync(200);

    const { id } = await enqueuePromise;
    const state = await readOutbox(stateDir);
    expect(state.entriesById[id]).toBeDefined();
  });
});
