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

/** Launch once under host custody, then consume through the same fenced path
 * used by the replacement controller.
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
 * frame remains fenced by its controller identity and cursor. Running frames
 * require a live lease; cancelled exit tails require the unchanged Stop owner.
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
  onReady?: () => void;
}) {
  const binding = input.binding;
  try {
    // Fence before touching a spool, even when it has no new output yet.
    const owned = await commitLegacyAdapterEvents(input.db, { ...binding,
      custodyId: input.custodyId, expectedCursor: input.cursor, nextCursor: input.cursor,
      events: [], project: input.project,
    }, input.controllerBootId);
    if (!owned && !await input.cancellationRequested()) throw new LegacyAdapterControllerDetachedError();
    if (owned) input.onReady?.();
    const directory = path.join(input.instanceRoot, "data", "legacy-adapter-workers",
      binding.companyId, input.custodyId, binding.runId);
    let cursor = input.cursor;
    let cancelling = !owned;
    if (cancelling) {
      await cancelLegacyAdapterWorker(directory);
      const cancelledOwner = await commitLegacyAdapterEvents(input.db, { ...binding,
        custodyId: input.custodyId, expectedCursor: cursor, nextCursor: cursor, events: [],
        project: input.project, allowCancelled: true }, input.controllerBootId);
      if (!cancelledOwner) throw new LegacyAdapterControllerDetachedError();
      input.onReady?.();
    }
    for (;;) {
      const stopRequested = await input.cancellationRequested();
      if (input.signal?.aborted || stopRequested) {
        if (!stopRequested) throw new LegacyAdapterControllerDetachedError();
        if (!cancelling) { await cancelLegacyAdapterWorker(directory); cancelling = true; }
      }
      const batch = await readLegacyAdapterEventBatch({ directory, binding, cursor });
      const commit = () => commitLegacyAdapterEvents(input.db, { ...binding, custodyId: input.custodyId,
        expectedCursor: cursor, nextCursor: batch.nextCursor, events: batch.events, project: input.project,
        allowCancelled: cancelling }, input.controllerBootId);
      let committed = await commit();
      if (!committed && !cancelling && await input.cancellationRequested()) {
        await cancelLegacyAdapterWorker(directory);
        cancelling = true;
        committed = await commit();
      }
      if (!committed) throw new LegacyAdapterControllerDetachedError();
      // Stop already owns presentation. Retain its exit tail in the database;
      // do not call running-only host progress or live-output callbacks.
      if (!cancelling) {
        try { await input.onCommittedEvents?.(batch.events); }
        catch (error) {
          if (!input.signal?.aborted || !await input.cancellationRequested()) throw error;
          await cancelLegacyAdapterWorker(directory);
          cancelling = true;
        }
      }
      cursor = batch.nextCursor;
      if (batch.terminal) {
        const result = batch.terminal.adapterResult;
        if (!cancelling) return result;
        // The worker publishes only after parser/cleanup settlement and child
        // join; the framing reader also requires the supervisor's close receipt.
        return { ...result, resultJson: { ...result.resultJson,
          executionCancellation: { state: "acknowledged", acknowledgedAt: batch.terminal.completedAt,
            proof: "durable_adapter_worker_join", custodyId: input.custodyId } } };
      }
      await new Promise(resolve => setTimeout(resolve, 25));
    }
  } catch (error) {
    // After launch, callback/database/transport failure cannot certify executor
    // exit or authorize another attempt. Preserve custody and the worker even
    // when the failure occurred after it published its durable result.
    throw error instanceof LegacyAdapterControllerDetachedError ? error : new LegacyAdapterControllerDetachedError(error);
  }
}
