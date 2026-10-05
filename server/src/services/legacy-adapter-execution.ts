import { prepareLegacyAdapterFinalizationInputs, type LegacyAdapterFinalizationInputs } from "./legacy-adapter-finalization-inputs.js";
import fs from "node:fs/promises";
import path from "node:path";
import type { AdapterExecutionContext, AdapterInvocationMeta, AdapterRuntimeEvent, RuntimeStatusSink } from "@paperclipai/adapter-utils";
import type { Db } from "@paperclipai/db";
import { reserveLegacyAdapterCustody, commitLegacyAdapterEvents } from "./legacy-adapter-custody.js";
import { readLegacyAdapterEventBatch, type LegacyAdapterWorkerEvent } from "./legacy-adapter-events.js";
import { startLegacyAdapterWorker, cancelLegacyAdapterWorker } from "./legacy-adapter-supervisor.js";
import type { AppendHeartbeatRunEventInput } from "./heartbeat-run-events.js";

export class LegacyAdapterControllerDetachedError extends Error {
  constructor(cause?: unknown) { super("Supervised legacy adapter controller detached", { cause }); this.name = "LegacyAdapterControllerDetachedError"; }
}

/** Current-controller integration. Restart reattachment remains disabled until
 * prepared host finalization inputs and restart routing are implemented.
 */
export async function executeSupervisedLegacyAdapter(input: {
  db: Db;
  instanceRoot: string;
  context: AdapterExecutionContext;
  secretValues: string[];
  workerEnv?: NodeJS.ProcessEnv;
  preparedFinalization: LegacyAdapterFinalizationInputs;
  assertOwned: () => Promise<void>;
  cancellationRequested: () => Promise<boolean>;
  project: (event: LegacyAdapterWorkerEvent) => Pick<AppendHeartbeatRunEventInput,
    "eventType" | "stream" | "level" | "color" | "message" | "payload">;
}) {
  const context = input.context;
  context.signal?.throwIfAborted();
  await context.onCancellationReady?.();
  context.signal?.throwIfAborted();
  await input.assertOwned();
  const binding = { runId: context.runId, companyId: context.agent.companyId,
    agentId: context.agent.id, adapterType: context.agent.adapterType! };
  const custody = await reserveLegacyAdapterCustody(input.db, binding, undefined,
    prepareLegacyAdapterFinalizationInputs(input.preparedFinalization));
  const root = path.join(input.instanceRoot, "data", "legacy-adapter-workers", binding.companyId, custody.custodyId);
  await fs.mkdir(root, { recursive: true, mode: 0o700 });
  await input.assertOwned();
  await startLegacyAdapterWorker({ root, context, secretValues: input.secretValues, workerEnv: input.workerEnv });
  return consumeSupervisedLegacyAdapter({
    db: input.db, instanceRoot: input.instanceRoot, binding, custodyId: custody.custodyId,
    cursor: custody.eventCursor, signal: context.signal,
    cancellationRequested: input.cancellationRequested, project: input.project,
    onCommittedEvents: async events => {
      for (const event of events) {
        switch (event.kind) {
          case "log": await context.onLog(event.stream, event.chunk); break;
          case "meta": await context.onMeta?.(event.meta as unknown as AdapterInvocationMeta); break;
          case "event": await context.onEvent?.(event.event as unknown as AdapterRuntimeEvent); break;
          case "progress": await context.onRuntimeProgress?.(event.progress as unknown as Parameters<RuntimeStatusSink>[0]); break;
          // Spawn metadata already committed inside the ownership transaction.
          // Do not repeat the unfenced direct-adapter metadata callback.
          case "spawn": context.onDispatch?.(); break;
          case "dispatch": context.onDispatch?.(); break;
        }
      }
    },
  });
}

/** Consume an existing worker without adapter configuration, credentials or a
 * launch operation. The caller must first acquire custody; every committed
 * frame remains fenced by its controller lease. This alone is not heartbeat
 * recovery: prepared host finalization and log reconstruction are still needed.
 */
export async function consumeSupervisedLegacyAdapter(input: {
  db: Db;
  instanceRoot: string;
  binding: { runId: string; companyId: string; agentId: string; adapterType: string };
  custodyId: string;
  cursor: number;
  controllerBootId?: string;
  signal?: AbortSignal;
  cancellationRequested: () => Promise<boolean>;
  project: (event: LegacyAdapterWorkerEvent) => Pick<AppendHeartbeatRunEventInput,
    "eventType" | "stream" | "level" | "color" | "message" | "payload">;
  onCommittedEvents?: (events: LegacyAdapterWorkerEvent[]) => Promise<void>;
}) {
  const binding = input.binding;
  try {
    // Fence before touching a spool, even when it has no new output yet.
    const owned = await commitLegacyAdapterEvents(input.db, { ...binding,
      custodyId: input.custodyId, expectedCursor: input.cursor, nextCursor: input.cursor,
      events: [], project: input.project,
    }, input.controllerBootId);
    if (!owned && !await input.cancellationRequested()) throw new LegacyAdapterControllerDetachedError();
    const directory = path.join(input.instanceRoot, "data", "legacy-adapter-workers",
      binding.companyId, input.custodyId, binding.runId);
    let cursor = input.cursor;
    let cancelling = !owned;
    if (cancelling) await cancelLegacyAdapterWorker(directory);
    for (;;) {
      if (input.signal?.aborted) {
        if (!await input.cancellationRequested()) throw new LegacyAdapterControllerDetachedError();
        if (!cancelling) { await cancelLegacyAdapterWorker(directory); cancelling = true; }
      }
      const batch = await readLegacyAdapterEventBatch({ directory, binding, cursor });
      if (!cancelling) {
        const committed = await commitLegacyAdapterEvents(input.db, { ...binding, custodyId: input.custodyId,
          expectedCursor: cursor, nextCursor: batch.nextCursor, events: batch.events, project: input.project }, input.controllerBootId);
        if (!committed) {
          if (await input.cancellationRequested()) {
            await cancelLegacyAdapterWorker(directory); cancelling = true;
          } else throw new LegacyAdapterControllerDetachedError();
        }
      }
      if (!cancelling) await input.onCommittedEvents?.(batch.events);
      cursor = batch.nextCursor;
      if (batch.terminal) return batch.terminal.adapterResult;
      await new Promise(resolve => setTimeout(resolve, 25));
    }
  } catch (error) {
    // After launch, callback/database/transport failure cannot certify executor
    // exit or authorize another attempt. Preserve custody and the worker even
    // when the failure occurred after it published its durable result.
    throw error instanceof LegacyAdapterControllerDetachedError ? error : new LegacyAdapterControllerDetachedError(error);
  }
}
