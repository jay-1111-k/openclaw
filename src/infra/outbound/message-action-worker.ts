import type { OpenClawConfig } from "../../config/config.js";
import type { OutboundSendDeps } from "./deliver.js";
import {
  leaseMessageActionOutbox,
  markMessageActionOutboxDone,
  markMessageActionOutboxFailed,
} from "./message-action-outbox.js";
import { runMessageAction } from "./message-action-runner.js";

function isOutboxWorkerEnabled(): boolean {
  const raw = process.env.WORKER_OUTBOX_ENABLED;
  return raw === "1" || raw === "true";
}

function waitForStopOrTimeout(ms: number, stopSignal?: AbortSignal): Promise<void> {
  if (ms <= 0) {
    return Promise.resolve();
  }
  return new Promise((resolve) => {
    const timer = setTimeout(resolve, ms);
    if (!stopSignal) {
      return;
    }
    if (stopSignal.aborted) {
      clearTimeout(timer);
      resolve();
      return;
    }
    const onAbort = () => {
      clearTimeout(timer);
      stopSignal.removeEventListener("abort", onAbort);
      resolve();
    };
    stopSignal.addEventListener("abort", onAbort, { once: true });
  });
}

export async function runMessageActionOutboxWorker(params: {
  cfg: OpenClawConfig;
  deps?: OutboundSendDeps;
  workerId: string;
  pollIntervalMs?: number;
  leaseMs?: number;
  limit?: number;
  stopSignal?: AbortSignal;
}): Promise<void> {
  if (!isOutboxWorkerEnabled()) {
    return;
  }

  const pollIntervalMs = params.pollIntervalMs ?? 1_000;
  const leaseMs = params.leaseMs ?? 30_000;
  const limit = params.limit ?? 10;

  while (!params.stopSignal?.aborted) {
    const leased = await leaseMessageActionOutbox({
      limit,
      leaseMs,
      workerId: params.workerId,
      now: Date.now(),
    });

    if (leased.length === 0) {
      await waitForStopOrTimeout(pollIntervalMs, params.stopSignal);
      continue;
    }

    for (const entry of leased) {
      try {
        await runMessageAction({
          cfg: params.cfg,
          action: entry.payload.action,
          params: entry.payload.params,
          defaultAccountId: entry.payload.defaultAccountId,
          toolContext: entry.payload.toolContext,
          gateway: entry.payload.gateway,
          deps: params.deps,
          sessionKey: entry.payload.sessionKey,
          agentId: entry.payload.agentId,
          dryRun: entry.payload.dryRun,
        });
        await markMessageActionOutboxDone(entry.id);
      } catch (err) {
        await markMessageActionOutboxFailed(entry.id, err, { now: Date.now() });
      }
    }
  }
}
