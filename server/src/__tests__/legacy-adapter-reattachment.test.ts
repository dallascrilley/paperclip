import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { randomUUID } from "node:crypto";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { eq, sql } from "drizzle-orm";
import { agents, companies, createDb, heartbeatRuns, heartbeatRunEvents, legacyAdapterTransports } from "@paperclipai/db";
import { readDurableChildResult } from "@paperclipai/adapter-utils/durable-child-process";
import { startEmbeddedPostgresTestDatabase } from "./helpers/embedded-postgres.js";
import { reserveLegacyAdapterCustody, claimExpiredLegacyAdapterCustody } from "../services/legacy-adapter-custody.js";
import { consumeSupervisedLegacyAdapter, LegacyAdapterControllerDetachedError } from "../services/legacy-adapter-execution.js";
import { startLegacyAdapterWorker, cancelLegacyAdapterWorker } from "../services/legacy-adapter-supervisor.js";

async function wait<T>(read: () => Promise<T>, ready: (value: T) => boolean) {
  const deadline = Date.now() + 20000;
  while (Date.now() < deadline) {
    const value = await read();
    if (ready(value)) return value;
    await new Promise(resolve => setTimeout(resolve, 25));
  }
  throw new Error("Reattachment fixture did not become ready");
}

describe("legacy worker database reattachment", () => {
  let temporary: Awaited<ReturnType<typeof startEmbeddedPostgresTestDatabase>>;
  let db: ReturnType<typeof createDb>;
  let root: string;
  const companyId = randomUUID(), agentId = randomUUID();
  beforeAll(async () => {
    root = await fs.mkdtemp(path.join(process.env.PAPERCLIP_RUN_SCRATCH_DIR ?? os.tmpdir(), "legacy-reattach-"));
    temporary = await startEmbeddedPostgresTestDatabase("dal47-reattach-");
    db = createDb(temporary.connectionString);
    await db.insert(companies).values({ id: companyId, name: "Reattachment fixture", issuePrefix: "RAT" });
    await db.insert(agents).values({ id: agentId, companyId, name: "Fixture", adapterType: "codex_local" });
  }, 90000);
  afterAll(async () => {
    if (temporary) await temporary.cleanup();
    if (root) await fs.rm(root, { recursive: true, force: true });
  });

  it.each(["sleeping", "logging"] as const)("consumes the same %s Codex worker after fenced lease transfer without launch inputs", async mode => {
    const runId = randomUUID(), previousBootId = randomUUID(), replacementBootId = randomUUID();
    const binding = { runId, companyId, agentId, adapterType: "codex_local" };
    await db.insert(heartbeatRuns).values({ id: runId, companyId, agentId, status: "running", runtimeMode: "legacy",
      controllerBootId: previousBootId, controllerLeaseExpiresAt: sql`clock_timestamp() + interval '60 seconds'` });
    const custody = await reserveLegacyAdapterCustody(db, binding, previousBootId);
    const work = path.join(root, runId);
    await fs.mkdir(work);
    const home = path.join(work, "home"), codexHome = path.join(work, "codex");
    await fs.mkdir(home); await fs.mkdir(codexHome);
    const effects = path.join(work, "effects"), release = path.join(work, "release");
    const command = path.join(work, "codex-fixture");
    await fs.writeFile(command, `#!${process.execPath}
      const fs = require('node:fs');
      fs.appendFileSync(${JSON.stringify(effects)}, 'started\\n');
      process.stdin.resume();
      process.stdout.write(JSON.stringify({type:'thread.started',thread_id:'reattached-session'})+'\\n');
      const timer = setInterval(() => {
        ${mode === "logging" ? "process.stdout.write(JSON.stringify({type:'item.started',item:{type:'command_execution',command:'logging fixture'}})+'\\n');" : ""}
        if (fs.existsSync(${JSON.stringify(release)})) {
          clearInterval(timer);
          process.stdout.write(JSON.stringify({type:'item.completed',item:{type:'agent_message',text:'reattached result'}})+'\\n');
          process.stdout.write(JSON.stringify({type:'turn.completed',usage:{input_tokens:3,output_tokens:5}})+'\\n');
          process.exit(0);
        }
      }, 50);
    `, { mode: 0o700 });
    const workerRoot = path.join(root, "data", "legacy-adapter-workers", companyId, custody.custodyId);
    await fs.mkdir(workerRoot, { recursive: true, mode: 0o700 });
    const handle = await startLegacyAdapterWorker({
      root: workerRoot,
      context: { runId, agent: { id: agentId, companyId, name: "Fixture", adapterType: "codex_local", adapterConfig: {} },
        runtime: { sessionId: null, sessionParams: null, sessionDisplayId: null, taskKey: null },
        config: { engine: "cli", command, cwd: work, model: "fixture", timeoutSec: 30, graceSec: 1,
          managedAiConnection: true, env: { CODEX_HOME: codexHome, OPENAI_API_KEY: "" } }, context: {}, onLog: async () => {} },
      secretValues: [], workerEnv: { HOME: home, PATH: process.env.PATH, CODEX_HOME: codexHome,
        PAPERCLIP_HOME: path.join(work, "paperclip"), PAPERCLIP_CODEX_AUTH_CACHE: "0", OPENAI_API_KEY: "" },
    });
    try {
      const signal = new AbortController();
      const project = (event: Parameters<Parameters<typeof consumeSupervisedLegacyAdapter>[0]["project"]>[0]) =>
        ({ eventType: "legacy_adapter_worker", payload: event });
      await expect(consumeSupervisedLegacyAdapter({ db, instanceRoot: root, binding, custodyId: custody.custodyId,
        cursor: 0, controllerBootId: previousBootId, signal: signal.signal,
        cancellationRequested: async () => false, project,
        onCommittedEvents: async events => {
          if (events.some(event => event.kind === "log" && event.chunk.includes("thread.started"))) signal.abort();
        },
      })).rejects.toBeInstanceOf(LegacyAdapterControllerDetachedError);
      const [before] = await db.select().from(heartbeatRuns).where(eq(heartbeatRuns.id, runId));
      expect(before.processPid).toBeGreaterThan(0);
      process.kill(before.processPid!, 0);
      expect(await fs.readFile(effects, "utf8")).toBe("started\n");
      await db.update(heartbeatRuns).set({ controllerLeaseExpiresAt: sql`clock_timestamp() - interval '1 second'` }).where(eq(heartbeatRuns.id, runId));
      const claimed = await claimExpiredLegacyAdapterCustody(db, { ...binding, custodyId: custody.custodyId, previousBootId }, replacementBootId);
      expect(claimed).not.toBeNull();
      await fs.writeFile(release, "");
      const result = await consumeSupervisedLegacyAdapter({ db, instanceRoot: root, binding, custodyId: custody.custodyId,
        cursor: claimed!.custody.eventCursor, controllerBootId: replacementBootId,
        cancellationRequested: async () => false, project });
      expect(result).toMatchObject({ exitCode: 0, signal: null, timedOut: false, sessionId: "reattached-session",
        summary: "reattached result", usage: { inputTokens: 3, outputTokens: 5 } });
      const [after] = await db.select().from(heartbeatRuns).where(eq(heartbeatRuns.id, runId));
      expect(after).toMatchObject({ status: "running", processPid: before.processPid, processGroupId: before.processGroupId,
        controllerBootId: replacementBootId });
      const [tail] = await db.select().from(legacyAdapterTransports).where(eq(legacyAdapterTransports.runId, runId));
      expect(tail.eventCursor).toBeGreaterThan(claimed!.custody.eventCursor);
      const rows = await db.select().from(heartbeatRunEvents).where(eq(heartbeatRunEvents.runId, runId));
      expect(rows.some(row => JSON.stringify(row.payload).includes("turn.completed"))).toBe(true);
      expect(new Set(rows.map(row => row.seq)).size).toBe(rows.length);
      expect(await fs.readFile(effects, "utf8")).toBe("started\n");
      // This is source-level transport custody, not replacement heartbeat
      // finalization or an installed controller-restart acceptance receipt.
      const proof = { mode, runId, previousBootId, replacementBootId,
        observingProcessPid: process.pid, supervisorPid: handle.supervisorPid,
        processPid: before.processPid, processGroupId: before.processGroupId,
        beforeCursor: claimed!.custody.eventCursor, afterCursor: tail.eventCursor,
        terminalResult: result, runStatus: after.status, startEffects: 1 };
      console.info("legacy-reattachment-source-proof", JSON.stringify(proof));
      if (process.env.PAPERCLIP_RUN_SCRATCH_DIR) await fs.writeFile(
        path.join(process.env.PAPERCLIP_RUN_SCRATCH_DIR, `reattachment-${mode}-proof.json`),
        JSON.stringify(proof, null, 2), { mode: 0o600 });
    } finally {
      await cancelLegacyAdapterWorker(handle.directory);
      await wait(() => readDurableChildResult(handle.directory), receipt => receipt !== null);
    }
  }, 90000);
});
