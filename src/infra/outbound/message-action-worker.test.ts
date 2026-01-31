import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import type { OpenClawConfig } from "../../config/config.js";
import type { MessageActionRunResult } from "./message-action-runner.js";
import { enqueueMessageActionOutbox } from "./message-action-outbox.js";
import { runMessageActionOutboxWorker } from "./message-action-worker.js";

const mocks = vi.hoisted(() => ({
  runMessageAction: vi.fn(),
}));

vi.mock("./message-action-runner.js", async () => {
  const actual = await vi.importActual<typeof import("./message-action-runner.js")>(
    "./message-action-runner.js",
  );
  return {
    ...actual,
    runMessageAction: mocks.runMessageAction,
  };
});

const payload = {
  action: "send",
  params: { target: "telegram:123", message: "hi" },
} as const;

function resolveOutboxPath(root: string): string {
  return path.join(root, "outbound", "message-action-outbox.json");
}

async function readOutbox(root: string) {
  const raw = await fs.readFile(resolveOutboxPath(root), "utf8");
  return JSON.parse(raw) as {
    entriesById: Record<string, { status: string; attempts: number; availableAt: number }>;
  };
}

describe("message-action outbox worker", () => {
  let stateDir: string;
  let prevStateDir: string | undefined;
  let prevWorkerEnabled: string | undefined;

  beforeEach(async () => {
    prevStateDir = process.env.OPENCLAW_STATE_DIR;
    prevWorkerEnabled = process.env.WORKER_OUTBOX_ENABLED;
    stateDir = await fs.mkdtemp(path.join(os.tmpdir(), "openclaw-worker-"));
    process.env.OPENCLAW_STATE_DIR = stateDir;
    process.env.WORKER_OUTBOX_ENABLED = "1";
    vi.useFakeTimers();
  });

  afterEach(async () => {
    vi.useRealTimers();
    if (prevStateDir === undefined) {
      delete process.env.OPENCLAW_STATE_DIR;
    } else {
      process.env.OPENCLAW_STATE_DIR = prevStateDir;
    }
    if (prevWorkerEnabled === undefined) {
      delete process.env.WORKER_OUTBOX_ENABLED;
    } else {
      process.env.WORKER_OUTBOX_ENABLED = prevWorkerEnabled;
    }
    await fs.rm(stateDir, { recursive: true, force: true });
  });

  it("executes leased jobs and marks them done", async () => {
    vi.setSystemTime(1_000);
    const { id } = await enqueueMessageActionOutbox(payload);

    const controller = new AbortController();
    mocks.runMessageAction.mockImplementationOnce(async () => {
      controller.abort();
      return {
        kind: "send",
        action: "send",
        channel: "telegram",
        to: "telegram:123",
        handledBy: "plugin",
        payload: {},
        dryRun: false,
      } satisfies MessageActionRunResult;
    });

    await runMessageActionOutboxWorker({
      cfg: {} as OpenClawConfig,
      workerId: "worker-a",
      stopSignal: controller.signal,
      pollIntervalMs: 5,
      leaseMs: 5_000,
      limit: 1,
    });

    const state = await readOutbox(stateDir);
    expect(state.entriesById[id]?.status).toBe("done");
  });

  it("marks failures with backoff", async () => {
    vi.setSystemTime(1_000);
    const { id } = await enqueueMessageActionOutbox(payload);

    const controller = new AbortController();
    mocks.runMessageAction.mockImplementationOnce(async () => {
      controller.abort();
      throw new Error("boom");
    });

    await runMessageActionOutboxWorker({
      cfg: {} as OpenClawConfig,
      workerId: "worker-a",
      stopSignal: controller.signal,
      pollIntervalMs: 5,
      leaseMs: 5_000,
      limit: 1,
    });

    const state = await readOutbox(stateDir);
    const entry = state.entriesById[id];
    expect(entry?.status).toBe("failed");
    expect(entry?.attempts).toBe(1);
    expect(entry?.availableAt).toBe(2_000);
  });

  it("keeps leases alive during long-running actions", async () => {
    vi.setSystemTime(1_000);
    await enqueueMessageActionOutbox(payload);

    const controllerA = new AbortController();
    const controllerB = new AbortController();

    mocks.runMessageAction.mockImplementationOnce(
      () =>
        new Promise((resolve) => {
          setTimeout(() => {
            controllerA.abort();
            controllerB.abort();
            resolve({
              kind: "send",
              action: "send",
              channel: "telegram",
              to: "telegram:123",
              handledBy: "plugin",
              payload: {},
              dryRun: false,
            } satisfies MessageActionRunResult);
          }, 200);
        }),
    );

    const workerA = runMessageActionOutboxWorker({
      cfg: {} as OpenClawConfig,
      workerId: "worker-a",
      stopSignal: controllerA.signal,
      pollIntervalMs: 10,
      leaseMs: 50,
      limit: 1,
    });
    const workerB = runMessageActionOutboxWorker({
      cfg: {} as OpenClawConfig,
      workerId: "worker-b",
      stopSignal: controllerB.signal,
      pollIntervalMs: 10,
      leaseMs: 50,
      limit: 1,
    });

    await vi.advanceTimersByTimeAsync(250);
    await Promise.all([workerA, workerB]);

    expect(mocks.runMessageAction).toHaveBeenCalledTimes(1);
  });

  it("uses cfg snapshot for queued actions and merges secrets from runtime", async () => {
    vi.setSystemTime(1_000);
    await enqueueMessageActionOutbox({
      ...payload,
      cfgSnapshot: {
        channels: { telegram: { dmPolicy: "open" } },
      } as OpenClawConfig,
    });

    const controller = new AbortController();
    mocks.runMessageAction.mockImplementationOnce(async ({ cfg }) => {
      controller.abort();
      expect((cfg as OpenClawConfig).channels?.telegram?.dmPolicy).toBe("open");
      expect((cfg as OpenClawConfig).channels?.telegram?.token).toBe("secret");
      return {
        kind: "send",
        action: "send",
        channel: "telegram",
        to: "telegram:123",
        handledBy: "plugin",
        payload: {},
        dryRun: false,
      } satisfies MessageActionRunResult;
    });

    await runMessageActionOutboxWorker({
      cfg: {
        channels: { telegram: { dmPolicy: "closed", token: "secret" } },
      } as OpenClawConfig,
      workerId: "worker-a",
      stopSignal: controller.signal,
      pollIntervalMs: 5,
      leaseMs: 5_000,
      limit: 1,
    });

    expect(mocks.runMessageAction).toHaveBeenCalledTimes(1);
  });
});
