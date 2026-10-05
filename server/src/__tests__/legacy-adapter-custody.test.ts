import { preparedFinalizationFixture } from "./legacy-adapter-finalization-inputs.fixture.js";
import { randomUUID } from "node:crypto";
import { beforeAll, afterAll, describe, expect, it } from "vitest";
import { eq, sql } from "drizzle-orm";
import { agents, companies, heartbeatRuns, heartbeatRunEvents, legacyAdapterTransports, createDb } from "@paperclipai/db";
import { startEmbeddedPostgresTestDatabase } from "./helpers/embedded-postgres.js";
import { reserveLegacyAdapterCustody, commitLegacyAdapterEvents, claimExpiredLegacyAdapterCustody } from "../services/legacy-adapter-custody.js";
import { legacyControllerBootId } from "../services/legacy-controller-lease.js";
import type { LegacyAdapterWorkerEvent } from "../services/legacy-adapter-events.js";
import { terminalizeLegacyExecution } from "../services/legacy-execution-recovery.js";

describe("legacy transport database custody", () => {
  let temporary: Awaited<ReturnType<typeof startEmbeddedPostgresTestDatabase>>;
  let db: ReturnType<typeof createDb>;
  const companyId = randomUUID(), agentId = randomUUID();
  beforeAll(async () => {
    temporary = await startEmbeddedPostgresTestDatabase("dal47-custody-");
    db = createDb(temporary.connectionString);
    await db.insert(companies).values({ id: companyId, name: "Custody fixture", issuePrefix: "CST" });
    await db.insert(agents).values({ id: agentId, companyId, name: "Fixture", adapterType: "codex_local" });
  }, 90000);
  afterAll(async () => { if (temporary) await temporary.cleanup(); });
  async function fixture(preparation?: ReturnType<typeof preparedFinalizationFixture>) {
    const binding = { companyId, agentId, runId: randomUUID(), adapterType: "codex_local" };
    await db.insert(heartbeatRuns).values({ id: binding.runId, companyId, agentId, status: "running", runtimeMode: "legacy",
      controllerBootId: legacyControllerBootId, controllerLeaseExpiresAt: sql`clock_timestamp() + interval '60 seconds'` });
    const custody = await reserveLegacyAdapterCustody(db, binding, undefined, preparation);
    return { ...binding, custodyId: custody.custodyId, expectedCursor: 0, nextCursor: 20,
      events: [{ kind: "dispatch" }] as LegacyAdapterWorkerEvent[],
      project: (event: LegacyAdapterWorkerEvent) => ({ eventType: "legacy_adapter_worker", payload: event }) };
  }

  it("commits immutable host preparation before launch and preserves it through custody transfer", async () => {
    const preparation = preparedFinalizationFixture();
    const f = await fixture(preparation);
    expect((await reserveLegacyAdapterCustody(db, f, undefined, preparation)).preparedFinalizationJson).toEqual(preparation);
    await expect(reserveLegacyAdapterCustody(db, f, undefined, { ...preparation, configuredModel: "changed" }))
      .rejects.toThrow("preparation mismatch");
    const [before] = await db.select().from(heartbeatRuns).where(eq(heartbeatRuns.id, f.runId));
    expect(before.processPid).toBeNull();
    await db.update(heartbeatRuns).set({ controllerLeaseExpiresAt: sql`clock_timestamp() - interval '1 second'` }).where(eq(heartbeatRuns.id, f.runId));
    const replacement = await claimExpiredLegacyAdapterCustody(db, { ...f, previousBootId: legacyControllerBootId }, randomUUID());
    expect(replacement?.custody.preparedFinalizationJson).toEqual(preparation);
  });

  it("does not retrofit prepared finalization into previously reserved custody", async () => {
    const f = await fixture();
    await expect(reserveLegacyAdapterCustody(db, f, undefined, preparedFinalizationFixture())).rejects.toThrow("preparation mismatch");
    const [custody] = await db.select().from(legacyAdapterTransports).where(eq(legacyAdapterTransports.runId, f.runId));
    expect(custody.preparedFinalizationJson).toBeNull();
  });

  it("serializes two consumers and commits one event/cursor through the shared sequence allocator", async () => {
    const f = await fixture();
    expect((await reserveLegacyAdapterCustody(db, f)).custodyId).toBe(f.custodyId);
    const results = await Promise.all([commitLegacyAdapterEvents(db, f), commitLegacyAdapterEvents(db, f)]);
    expect(results.filter(Boolean)).toHaveLength(1);
    const rows = await db.select().from(heartbeatRunEvents).where(eq(heartbeatRunEvents.runId, f.runId));
    expect(rows).toHaveLength(1);
    expect(rows[0]).toMatchObject({ seq: 1, payload: { kind: "dispatch" } });
    const [custody] = await db.select().from(legacyAdapterTransports).where(eq(legacyAdapterTransports.runId, f.runId));
    const [run] = await db.select().from(heartbeatRuns).where(eq(heartbeatRuns.id, f.runId));
    expect(custody.eventCursor).toBe(20);
    expect(run).toMatchObject({ nextEventSeq: 2, executionStage: "dispatching" });
    console.info("legacy-custody-db-proof", JSON.stringify({ runId: f.runId, custodyId: f.custodyId, concurrentCommits: results.filter(Boolean).length, rows: rows.length, cursor: custody.eventCursor }));
  });

  it("rolls back event rows, sequence and spawn metadata when a later projection fails", async () => {
    const f = await fixture();
    const before = (await db.select().from(heartbeatRuns).where(eq(heartbeatRuns.id, f.runId)))[0];
    await expect(commitLegacyAdapterEvents(db, { ...f, nextCursor: 200,
      events: [{ kind: "spawn", meta: { pid: 1234, processGroupId: 1234, startedAt: new Date().toISOString() } }, { kind: "dispatch" }],
      project: event => { if (event.kind === "dispatch") throw new Error("Projection failed"); return f.project(event); },
    })).rejects.toThrow("Projection failed");
    expect(await db.select().from(heartbeatRunEvents).where(eq(heartbeatRunEvents.runId, f.runId))).toHaveLength(0);
    const [run] = await db.select().from(heartbeatRuns).where(eq(heartbeatRuns.id, f.runId));
    const [custody] = await db.select().from(legacyAdapterTransports).where(eq(legacyAdapterTransports.runId, f.runId));
    expect(run).toMatchObject({ nextEventSeq: before.nextEventSeq, processPid: null, executionStage: before.executionStage });
    expect(custody.eventCursor).toBe(0);
  });

  it("rejects cross-company binding, expired lease, and an old controller after transfer", async () => {
    const f = await fixture();
    expect(await commitLegacyAdapterEvents(db, { ...f, companyId: randomUUID() })).toBeNull();
    await db.update(heartbeatRuns).set({ controllerLeaseExpiresAt: sql`clock_timestamp() - interval '1 second'` }).where(eq(heartbeatRuns.id, f.runId));
    expect(await commitLegacyAdapterEvents(db, f)).toBeNull();
    await expect(reserveLegacyAdapterCustody(db, f)).rejects.toThrow("ownership lost");
    const replacementBoot = randomUUID();
    expect(await claimExpiredLegacyAdapterCustody(db, { ...f, previousBootId: legacyControllerBootId }, replacementBoot)).not.toBeNull();
    expect(await commitLegacyAdapterEvents(db, f)).toBeNull();
    expect(await commitLegacyAdapterEvents(db, f, replacementBoot)).not.toBeNull();
    const events = await db.select().from(heartbeatRunEvents).where(eq(heartbeatRunEvents.runId, f.runId));
    expect(events).toHaveLength(2);
    expect(events.filter(event => event.eventType === "legacy_adapter_custody_claimed")).toHaveLength(1);
  });

  it("retains the cursor and executor identity with exactly one concurrent replacement", async () => {
    const f = await fixture();
    const spawn = { pid: 1234, processGroupId: 1234, startedAt: new Date().toISOString() };
    await commitLegacyAdapterEvents(db, { ...f, events: [{ kind: "spawn", meta: spawn }] });
    const input = { ...f, previousBootId: legacyControllerBootId };
    const boots = [randomUUID(), randomUUID()];
    expect(await claimExpiredLegacyAdapterCustody(db, input, boots[0])).toBeNull();
    await db.update(heartbeatRuns).set({ controllerLeaseExpiresAt: sql`clock_timestamp() - interval '1 second'` }).where(eq(heartbeatRuns.id, f.runId));
    const results = await Promise.all(boots.map(boot => claimExpiredLegacyAdapterCustody(db, input, boot)));
    expect(results.filter(Boolean)).toHaveLength(1);
    const winner = results.findIndex(Boolean);
    expect(results[winner]).toMatchObject({
      run: { controllerBootId: boots[winner], processPid: spawn.pid, processGroupId: spawn.processGroupId, status: "running" },
      custody: { custodyId: f.custodyId, eventCursor: 20 },
    });
    expect(await commitLegacyAdapterEvents(db, { ...f, expectedCursor: 20, nextCursor: 40 })).toBeNull();
    expect(await commitLegacyAdapterEvents(db, { ...f, expectedCursor: 20, nextCursor: 40 }, boots[1 - winner])).toBeNull();
    expect(await commitLegacyAdapterEvents(db, { ...f, expectedCursor: 20, nextCursor: 40 }, boots[winner])).not.toBeNull();
    console.info("legacy-custody-transfer-proof", JSON.stringify({ runId: f.runId, custodyId: f.custodyId,
      previousBootId: legacyControllerBootId, replacementBootId: boots[winner], successfulClaims: 1, cursor: 40 }));
  });

  it("rejects missing, mismatched, terminal and native custody without changing ownership", async () => {
    const f = await fixture();
    const input = { ...f, previousBootId: legacyControllerBootId };
    const boot = randomUUID();
    await db.update(heartbeatRuns).set({ controllerLeaseExpiresAt: sql`clock_timestamp() - interval '1 second'` }).where(eq(heartbeatRuns.id, f.runId));
    for (const mismatch of [{ companyId: randomUUID() }, { agentId: randomUUID() },
      { custodyId: randomUUID() }, { previousBootId: randomUUID() }, { adapterType: "claude_local" }]) {
      expect(await claimExpiredLegacyAdapterCustody(db, { ...input, ...mismatch }, boot)).toBeNull();
    }
    expect(await claimExpiredLegacyAdapterCustody(db, input, legacyControllerBootId)).toBeNull();
    await db.update(heartbeatRuns).set({ runtimeMode: "native" }).where(eq(heartbeatRuns.id, f.runId));
    expect(await claimExpiredLegacyAdapterCustody(db, input, boot)).toBeNull();
    await db.update(heartbeatRuns).set({ runtimeMode: "legacy", status: "succeeded" }).where(eq(heartbeatRuns.id, f.runId));
    expect(await claimExpiredLegacyAdapterCustody(db, input, boot)).toBeNull();
    await db.update(heartbeatRuns).set({ status: "running" }).where(eq(heartbeatRuns.id, f.runId));
    await db.delete(legacyAdapterTransports).where(eq(legacyAdapterTransports.runId, f.runId));
    expect(await claimExpiredLegacyAdapterCustody(db, input, boot)).toBeNull();
    expect((await db.select().from(heartbeatRuns).where(eq(heartbeatRuns.id, f.runId)))[0].controllerBootId).toBe(legacyControllerBootId);
  });

  it("rolls back the entire batch if database-clock authority expires during projection", async () => {
    const f = await fixture();
    await db.update(heartbeatRuns).set({ controllerLeaseExpiresAt: sql`clock_timestamp() + interval '0.3 seconds'` }).where(eq(heartbeatRuns.id, f.runId));
    await expect(commitLegacyAdapterEvents(db, { ...f, project: event => {
      // Intentional bounded synchronous stall to cross the SQL lease deadline.
      const deadline = Date.now() + 500; while (Date.now() < deadline) {}
      return f.project(event);
    } })).rejects.toThrow("lease expired during ingestion");
    expect(await db.select().from(heartbeatRunEvents).where(eq(heartbeatRunEvents.runId, f.runId))).toHaveLength(0);
    expect((await db.select().from(legacyAdapterTransports).where(eq(legacyAdapterTransports.runId, f.runId)))[0].eventCursor).toBe(0);
  });

  it("fences common legacy terminalization against stale or expired controllers", async () => {
    const f = await fixture();
    const [run] = await db.select().from(heartbeatRuns).where(eq(heartbeatRuns.id, f.runId));
    const input = { db, run, status: "failed", fromStatuses: ["running"] };
    expect(await terminalizeLegacyExecution({ ...input, controllerBootFence: randomUUID() })).toBeNull();
    await db.update(heartbeatRuns).set({ controllerLeaseExpiresAt: sql`clock_timestamp() - interval '1 second'` }).where(eq(heartbeatRuns.id, f.runId));
    expect(await terminalizeLegacyExecution({ ...input, controllerBootFence: legacyControllerBootId })).toBeNull();
    expect((await db.select().from(heartbeatRuns).where(eq(heartbeatRuns.id, f.runId)))[0].status).toBe("running");
    await db.update(heartbeatRuns).set({ controllerLeaseExpiresAt: sql`clock_timestamp() + interval '60 seconds'` }).where(eq(heartbeatRuns.id, f.runId));
    expect(await terminalizeLegacyExecution({ ...input, controllerBootFence: legacyControllerBootId })).toMatchObject({ status: "failed" });
  });
});
