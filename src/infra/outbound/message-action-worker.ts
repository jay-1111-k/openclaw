import type { OpenClawConfig } from "../../config/config.js";
import type { OutboundSendDeps } from "./deliver.js";
import {
  leaseMessageActionOutbox,
  extendMessageActionOutboxLease,
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

const OUTBOX_SENSITIVE_KEY_PATTERN = /token|password|secret|api.?key/i;

function mergeOutboxSnapshotWithSecrets(
  snapshot: OpenClawConfig,
  runtime: OpenClawConfig,
): OpenClawConfig {
  const merged = structuredClone(snapshot) as OpenClawConfig;
  applyRuntimeSecrets(merged, runtime);
  return merged;
}

function applyRuntimeSecrets(target: unknown, source: unknown): void {
  if (!source || typeof source !== "object") {
    return;
  }
  if (Array.isArray(source)) {
    if (!Array.isArray(target)) {
      return;
    }
    for (let index = 0; index < target.length; index += 1) {
      applyRuntimeSecrets(target[index], source[index]);
    }
    return;
  }
  if (!target || typeof target !== "object" || Array.isArray(target)) {
    return;
  }
  const targetRecord = target as Record<string, unknown>;
  const sourceRecord = source as Record<string, unknown>;
  for (const [key, value] of Object.entries(sourceRecord)) {
    if (OUTBOX_SENSITIVE_KEY_PATTERN.test(key)) {
      targetRecord[key] = value;
      continue;
    }
    if (Object.prototype.hasOwnProperty.call(targetRecord, key)) {
      applyRuntimeSecrets(targetRecord[key], value);
    }
  }
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
  const heartbeatMs = Math.max(1_000, Math.floor(leaseMs / 2));

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
      const leaseToken = entry.leaseToken;
      const cfg = entry.payload.cfgSnapshot
        ? mergeOutboxSnapshotWithSecrets(entry.payload.cfgSnapshot, params.cfg)
        : params.cfg;
      const heartbeat =
        leaseToken === undefined
          ? undefined
          : setInterval(() => {
              void extendMessageActionOutboxLease({
                id: entry.id,
                leaseToken,
                leaseMs,
                now: Date.now(),
              });
            }, heartbeatMs);
      try {
        await runMessageAction({
          cfg,
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
        await markMessageActionOutboxDone(entry.id, { leaseToken, now: Date.now() });
      } catch (err) {
        await markMessageActionOutboxFailed(entry.id, err, { now: Date.now(), leaseToken });
      } finally {
        if (heartbeat) {
          clearInterval(heartbeat);
        }
      }
    }
  }
}
