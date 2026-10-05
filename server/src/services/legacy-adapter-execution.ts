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
 * prepared host finalization inputs and lease transfer are implemented.
 */
export async function executeSupervisedLegacyAdapter(input: {
  db: Db;
  instanceRoot: string;
  context: AdapterExecutionContext;
  secretValues: string[];
  workerEnv?: NodeJS.ProcessEnv;
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
  const custody = await reserveLegacyAdapterCustody(input.db, binding);
  const root = path.join(input.instanceRoot, "data", "legacy-adapter-workers", binding.companyId, custody.custodyId);
  await fs.mkdir(root, { recursive: true, mode: 0o700 });
  await input.assertOwned();
  const handle = await startLegacyAdapterWorker({ root, context, secretValues: input.secretValues, workerEnv: input.workerEnv });
  let cursor = custody.eventCursor;
  let cancelling = false;
  try {
  for (;;) {
    if (context.signal?.aborted) {
      if (!await input.cancellationRequested()) throw new LegacyAdapterControllerDetachedError();
      if (!cancelling) { await cancelLegacyAdapterWorker(handle.directory); cancelling = true; }
    }
    const batch = await readLegacyAdapterEventBatch({ directory: handle.directory, binding, cursor });
    if (!cancelling) {
      const committed = await commitLegacyAdapterEvents(input.db, { ...binding, custodyId: custody.custodyId,
        expectedCursor: cursor, nextCursor: batch.nextCursor, events: batch.events, project: input.project });
      if (!committed) {
        if (await input.cancellationRequested()) {
          await cancelLegacyAdapterWorker(handle.directory); cancelling = true;
        } else throw new LegacyAdapterControllerDetachedError();
      }
    }
    // Forward projections only on this original controller. They remain normal
    // live log/runtime callbacks. Replacement controllers must reconstruct from
    // committed events rather than replay these file/callback side effects.
    if (!cancelling) for (const event of batch.events) {
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
