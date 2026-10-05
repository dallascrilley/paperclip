import { prepareLegacyAdapterFinalizationInputs, type LegacyAdapterFinalizationInputs } from "./legacy-adapter-finalization-inputs.js";
import { isDeepStrictEqual } from "node:util";
import { randomUUID } from "node:crypto";
import { and, eq, lte, sql } from "drizzle-orm";
import { agents, heartbeatRuns, legacyAdapterTransports, type Db } from "@paperclipai/db";
import { legacyControllerBootId } from "./legacy-controller-lease.js";
import { appendHeartbeatRunEvent, type AppendHeartbeatRunEventInput } from "./heartbeat-run-events.js";
import type { LegacyAdapterWorkerEvent } from "./legacy-adapter-events.js";
import { SUPERVISED_LEGACY_ADAPTERS } from "./legacy-adapter-supervisor.js";

type Binding = { companyId: string; runId: string; agentId: string; adapterType: string };
type Transaction = Parameters<Parameters<Db["transaction"]>[0]>[0];

async function lockOwnedRun(tx: Transaction, binding: Binding, bootId: string, allowCancelled = false) {
  const [run] = await tx.select().from(heartbeatRuns).where(and(
    eq(heartbeatRuns.id, binding.runId), eq(heartbeatRuns.companyId, binding.companyId),
    eq(heartbeatRuns.agentId, binding.agentId), eq(heartbeatRuns.runtimeMode, "legacy"),
    eq(heartbeatRuns.controllerBootId, bootId),
    sql`((${heartbeatRuns.status} = 'running' and ${heartbeatRuns.controllerLeaseExpiresAt} > clock_timestamp())
      or (${allowCancelled} and ${heartbeatRuns.status} = 'cancelled'))`,
  )).for("update");
  if (!run) return null;
  const [agent] = await tx.select({ adapterType: agents.adapterType }).from(agents).where(and(
    eq(agents.id, binding.agentId), eq(agents.companyId, binding.companyId),
  ));
  return agent?.adapterType === binding.adapterType ? run : null;
}

/** Before any supervised spawn. Repeated reservation reuses custody, never
 * permits another launch; the spool's exclusive directory is the launch gate.
 */
export async function reserveLegacyAdapterCustody(db: Db, binding: Binding, bootId: string = legacyControllerBootId,
  preparedFinalizationJson?: LegacyAdapterFinalizationInputs) {
  const preparation = preparedFinalizationJson ? prepareLegacyAdapterFinalizationInputs(preparedFinalizationJson) : undefined;
  if (!SUPERVISED_LEGACY_ADAPTERS.has(binding.adapterType)) throw new Error("Unsupported legacy adapter custody");
  return db.transaction(async tx => {
    if (!await lockOwnedRun(tx, binding, bootId)) throw new Error("Legacy adapter custody ownership lost");
    const [existing] = await tx.select().from(legacyAdapterTransports).where(eq(legacyAdapterTransports.runId, binding.runId));
    if (existing) {
      if (existing.companyId !== binding.companyId || existing.agentId !== binding.agentId || existing.adapterType !== binding.adapterType) {
        throw new Error("Legacy adapter custody binding mismatch");
      }
      if (preparation && !isDeepStrictEqual(existing.preparedFinalizationJson, preparation)) {
        throw new Error("Legacy adapter finalization preparation mismatch");
      }
      return existing;
    }
    const [created] = await tx.insert(legacyAdapterTransports).values({ ...binding, custodyId: randomUUID(), preparedFinalizationJson: preparation }).returning();
    return created;
  });
}

/** Transfer consumption authority for an existing spool after lease expiry.
 * This grants neither dispatch nor an adoption classification. The caller must
 * validate the bound spool and recover prepared finalization inputs separately.
 * Lock order matches ingestion: run, then custody. A live controller cannot be
 * displaced, and concurrent replacements cannot both acquire the same run.
 */
export async function claimExpiredLegacyAdapterCustody(db: Db, input: Binding & {
  custodyId: string;
  previousBootId: string;
}, replacementBootId: string = legacyControllerBootId) {
  if (!SUPERVISED_LEGACY_ADAPTERS.has(input.adapterType) ||
      !input.previousBootId || !replacementBootId || input.previousBootId === replacementBootId) return null;
  return db.transaction(async tx => {
    const [run] = await tx.select().from(heartbeatRuns).where(and(
      eq(heartbeatRuns.id, input.runId), eq(heartbeatRuns.companyId, input.companyId),
      eq(heartbeatRuns.agentId, input.agentId), eq(heartbeatRuns.runtimeMode, "legacy"),
      eq(heartbeatRuns.status, "running"), eq(heartbeatRuns.controllerBootId, input.previousBootId),
      lte(heartbeatRuns.controllerLeaseExpiresAt, sql`clock_timestamp()`),
    )).for("update");
    if (!run) return null;
    const [agent] = await tx.select({ adapterType: agents.adapterType }).from(agents).where(and(
      eq(agents.id, input.agentId), eq(agents.companyId, input.companyId),
    ));
    if (agent?.adapterType !== input.adapterType) return null;
    const [custody] = await tx.select().from(legacyAdapterTransports).where(and(
      eq(legacyAdapterTransports.runId, input.runId), eq(legacyAdapterTransports.companyId, input.companyId),
      eq(legacyAdapterTransports.agentId, input.agentId), eq(legacyAdapterTransports.adapterType, input.adapterType),
      eq(legacyAdapterTransports.custodyId, input.custodyId),
    )).for("update");
    if (!custody) return null;
    const [claimed] = await tx.update(heartbeatRuns).set({
      controllerBootId: replacementBootId,
      controllerLeaseExpiresAt: sql`clock_timestamp() + interval '60 seconds'`,
    }).where(and(eq(heartbeatRuns.id, input.runId),
      eq(heartbeatRuns.controllerBootId, input.previousBootId),
      lte(heartbeatRuns.controllerLeaseExpiresAt, sql`clock_timestamp()`),
    )).returning();
    if (!claimed) return null;
    const event = await appendHeartbeatRunEvent(tx as unknown as Db, {
      runId: input.runId, companyId: input.companyId, agentId: input.agentId,
      eventType: "legacy_adapter_custody_claimed", level: "info",
      message: "Replacement controller acquired expired transport custody",
      payload: { previousBootId: input.previousBootId, replacementBootId,
        custodyId: input.custodyId, eventCursor: custody.eventCursor },
    });
    return { run: { ...claimed, nextEventSeq: event.row.seq + 1 }, custody };
  });
}

/** SQL-only ingestion. The projection must be pure and apply host redaction /
 * payload limits before insertion. The caller publishes committed rows after
 * this transaction; no live callback, file append or executor runs inside it.
 * Holding the run lock serializes lease transfer, event allocation and cursor.
 */
export async function commitLegacyAdapterEvents(db: Db, input: Binding & {
  custodyId: string;
  // Terminal Stop does not transfer custody. Its exact controller can consume
  // the bound exit tail, but cannot change the cancellation or dispatch stage.
  allowCancelled?: boolean;
  expectedCursor: number;
  nextCursor: number;
  events: LegacyAdapterWorkerEvent[];
  project: (event: LegacyAdapterWorkerEvent) => Pick<AppendHeartbeatRunEventInput,
    "eventType" | "stream" | "level" | "color" | "message" | "payload">;
}, bootId: string = legacyControllerBootId) {
  if (![input.expectedCursor, input.nextCursor].every(value => Number.isSafeInteger(value) && value >= 0) ||
      input.nextCursor < input.expectedCursor || (input.events.length > 0 && input.nextCursor === input.expectedCursor) ||
      (input.events.length === 0 && input.nextCursor !== input.expectedCursor)) {
    throw new Error("Invalid legacy adapter custody cursor");
  }
  return db.transaction(async tx => {
    const run = await lockOwnedRun(tx, input, bootId, input.allowCancelled);
    if (!run) return null;
    const [custody] = await tx.select().from(legacyAdapterTransports).where(and(
      eq(legacyAdapterTransports.runId, input.runId), eq(legacyAdapterTransports.companyId, input.companyId),
      eq(legacyAdapterTransports.agentId, input.agentId), eq(legacyAdapterTransports.adapterType, input.adapterType),
      eq(legacyAdapterTransports.custodyId, input.custodyId), eq(legacyAdapterTransports.eventCursor, input.expectedCursor),
    )).for("update");
    if (!custody) return null;
    const rows = [];
    for (const event of input.events) {
      const projected = input.project(event);
      const appended = await appendHeartbeatRunEvent(tx as unknown as Db, {
        // The host binding wins even if a projection carries extra properties.
        eventType: projected.eventType, stream: projected.stream, level: projected.level,
        color: projected.color, message: projected.message, payload: projected.payload,
        runId: input.runId, companyId: input.companyId, agentId: input.agentId,
      });
      rows.push(appended.row);
      if (run.status === "running" && event.kind === "spawn") await tx.update(heartbeatRuns).set({
        processPid: event.meta.pid, processGroupId: event.meta.processGroupId,
        processStartedAt: new Date(event.meta.startedAt), executionStage: "dispatching",
      }).where(eq(heartbeatRuns.id, input.runId));
      if (run.status === "running" && event.kind === "dispatch") await tx.update(heartbeatRuns).set({ executionStage: "dispatching" }).where(eq(heartbeatRuns.id, input.runId));
    }
    // The database clock can cross expiry during a long batch. Roll back all
    // inserted events and sequence increments if authority expired meanwhile.
    const [advanced] = await tx.update(legacyAdapterTransports).set({
      eventCursor: input.nextCursor, updatedAt: new Date(),
    }).where(and(eq(legacyAdapterTransports.runId, input.runId), sql`exists (
      select 1 from ${heartbeatRuns} where ${heartbeatRuns.id} = ${input.runId}
      and ${heartbeatRuns.controllerBootId} = ${bootId}
      and ((${heartbeatRuns.controllerLeaseExpiresAt} > clock_timestamp() and ${heartbeatRuns.status} = 'running')
        or (${Boolean(input.allowCancelled)} and ${heartbeatRuns.status} = 'cancelled'))
    )`)).returning();
    if (!advanced) throw new Error("Legacy adapter custody lease expired during ingestion");
    return { custody: advanced, rows };
  });
}
