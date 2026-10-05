const restart = vi.hoisted(() => ({ readIntent: vi.fn(), writeReport: vi.fn(async (input: unknown) => input), removeIntent: vi.fn(async () => {}) }));
import { heartbeatService } from "../services/heartbeat.js";
import { legacyControllerBootId } from "../services/legacy-controller-lease.js";
import { preparedFinalizationFixture } from "./legacy-adapter-finalization-inputs.fixture.js";
import fs from "node:fs/promises";
import { execFileSync } from "node:child_process";
import os from "node:os";
import path from "node:path";
import { randomUUID } from "node:crypto";
import { afterAll, beforeAll, describe, expect, it, vi } from "vitest";
import { eq, sql } from "drizzle-orm";
import { agents, companies, createDb, heartbeatRuns, heartbeatRunEvents, legacyAdapterTransports, workspaceOperations, projects, executionWorkspaces } from "@paperclipai/db";
import { readDurableChildResult } from "@paperclipai/adapter-utils/durable-child-process";
import { startEmbeddedPostgresTestDatabase } from "./helpers/embedded-postgres.js";
import { reserveLegacyAdapterCustody } from "../services/legacy-adapter-custody.js";
import { consumeSupervisedLegacyAdapter, LegacyAdapterControllerDetachedError } from "../services/legacy-adapter-execution.js";
import { startLegacyAdapterWorker, cancelLegacyAdapterWorker } from "../services/legacy-adapter-supervisor.js";

vi.mock("../services/hot-restart.js", async importOriginal => {
  const actual = await importOriginal<typeof import("../services/hot-restart.js")>();
  return { ...actual, readHotRestartIntent: restart.readIntent,
    writeHotRestartReport: restart.writeReport, removeHotRestartIntent: restart.removeIntent };
});

async function wait<T>(read: () => Promise<T>, ready: (value: T) => boolean) {
  const deadline = Date.now() + 20000;
  while (Date.now() < deadline) {
    const value = await read();
    if (ready(value)) return value;
    await new Promise(resolve => setTimeout(resolve, 25));
  }
  throw new Error("Reattachment fixture did not become ready");
}

describe("replacement legacy host finalization", () => {
  let temporary: Awaited<ReturnType<typeof startEmbeddedPostgresTestDatabase>>;
  let db: ReturnType<typeof createDb>;
  let root: string;
  const originalLogPath = process.env.RUN_LOG_BASE_PATH;
  const companyId = randomUUID(), agentId = randomUUID();
  beforeAll(async () => {
    root = await fs.mkdtemp(path.join(process.env.PAPERCLIP_RUN_SCRATCH_DIR ?? os.tmpdir(), "legacy-reattach-"));
    process.env.RUN_LOG_BASE_PATH = path.join(root, "run-logs");
    temporary = await startEmbeddedPostgresTestDatabase("dal47-reattach-");
    db = createDb(temporary.connectionString);
    await db.insert(companies).values({ id: companyId, name: "Reattachment fixture", issuePrefix: "RAT" });
    await db.insert(agents).values({ id: agentId, companyId, name: "Fixture", adapterType: "codex_local" });
  }, 90000);
  afterAll(async () => {
    if (originalLogPath === undefined) delete process.env.RUN_LOG_BASE_PATH;
    else process.env.RUN_LOG_BASE_PATH = originalLogPath;
    if (temporary) await temporary.cleanup();
    if (root) await fs.rm(root, { recursive: true, force: true });
  });

  it("holds historical or invalid preparation without claiming its controller or launching", async () => {
    const runId = randomUUID(), previousBootId = randomUUID();
    const binding = { runId, companyId, agentId, adapterType: "codex_local" };
    await db.insert(heartbeatRuns).values({ id: runId, companyId, agentId, status: "running", runtimeMode: "legacy",
      controllerBootId: previousBootId, controllerLeaseExpiresAt: sql`clock_timestamp() - interval '1 second'` });
    // Reservation itself requires authority; create the historical row directly.
    await db.insert(legacyAdapterTransports).values({ ...binding, custodyId: randomUUID() });
    const heartbeat = heartbeatService(db, { legacyAdapterWorkerInstanceRoot: root });
    expect(await heartbeat.resumeSupervisedLegacyRun(runId)).toBe(false);
    await db.update(legacyAdapterTransports).set({ preparedFinalizationJson: { version: 2 } }).where(eq(legacyAdapterTransports.runId, runId));
    await expect(heartbeat.resumeSupervisedLegacyRun(runId)).rejects.toThrow();
    expect((await db.select().from(heartbeatRuns).where(eq(heartbeatRuns.id, runId)))[0]).toMatchObject({
      controllerBootId: previousBootId, status: "running", processPid: null });
    expect(await db.select().from(heartbeatRunEvents).where(eq(heartbeatRunEvents.runId, runId))).toHaveLength(0);
  });

  it.each(["sleeping", "logging", "cancelled", "branch_mismatch"] as const)("finalizes the same %s Codex executor through common host session and usage projection", async mode => {
    const runId = randomUUID(), previousBootId = randomUUID(), replacementBootId = legacyControllerBootId;
    const binding = { runId, companyId, agentId, adapterType: "codex_local" };
    await db.insert(heartbeatRuns).values({ id: runId, companyId, agentId, status: "running", runtimeMode: "legacy",
      controllerBootId: previousBootId, controllerLeaseExpiresAt: sql`clock_timestamp() + interval '60 seconds'` });
    const preparation = { ...preparedFinalizationFixture(), taskKey: `fixture-task-${runId}`,
      previousSessionParams: null, previousSessionId: null, previousSessionDisplayId: null, taskSessionReused: false };
    const work = path.join(root, runId);
    preparation.workspace.cwd = work;
    if (mode === "branch_mismatch") {
      const repo = path.join(root, `${runId}-repo`);
      await fs.mkdir(repo);
      const git = (args: string[], cwd = repo) => execFileSync("/usr/bin/git", args, { cwd, stdio: "pipe" });
      git(["init", "-b", "base"]);
      git(["-c", "user.name=Fixture", "-c", "user.email=fixture@localhost", "commit", "--allow-empty", "-m", "fixture"]);
      git(["worktree", "add", "-b", "expected", work]);
      git(["checkout", "-b", "wrong"], work);
      const projectId = randomUUID(), workspaceId = randomUUID();
      await db.insert(projects).values({ id: projectId, companyId, name: "Recovery branch fixture" });
      await db.insert(executionWorkspaces).values({ id: workspaceId, companyId, projectId,
        mode: "isolated_workspace", strategyType: "git_worktree", name: "Owned fixture",
        cwd: work, providerRef: work, branchName: "expected", status: "active" });
      preparation.workspace = { executionWorkspaceId: workspaceId, cwd: work, worktreePath: work, branchName: "expected" };
    } else await fs.mkdir(work);
    const custody = await reserveLegacyAdapterCustody(db, binding, previousBootId, preparation);
    const home = path.join(work, "home"), codexHome = path.join(work, "codex");
    await fs.mkdir(home); await fs.mkdir(codexHome);
    const effects = path.join(work, "effects"), release = path.join(work, "release");
    const command = path.join(work, "codex-fixture");
    await fs.writeFile(command, `#!${process.execPath}
      const fs = require('node:fs');
      fs.appendFileSync(${JSON.stringify(effects)}, 'started\\n');
      process.stdin.resume();
      const stop = () => {
        process.stdout.write(JSON.stringify({type:'item.completed',item:{type:'agent_message',text:'cancelled fixture tail'}})+'\\n');
        process.exit(130);
      };
      process.on('SIGINT', stop); process.on('SIGTERM', stop);
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
      const heartbeat = heartbeatService(db, { legacyAdapterWorkerInstanceRoot: root });
      expect(await heartbeat.resumeSupervisedLegacyRun(runId)).toBe(false);
      await db.update(heartbeatRuns).set({ controllerLeaseExpiresAt: sql`clock_timestamp() - interval '1 second'` }).where(eq(heartbeatRuns.id, runId));
      const [previousCustody] = await db.select().from(legacyAdapterTransports).where(eq(legacyAdapterTransports.runId, runId));
      // Marker I/O is mocked. The real recognized-adapter restart router,
      // database custody claim and output consumer produce the classification.
      restart.readIntent.mockResolvedValue({ version: 1, requestedAt: new Date().toISOString(),
        drainRequired: false, drainReason: null, previousServerPid: process.pid,
        previousServerVersion: "source-fixture", preflightActiveRunIds: [runId],
        shutdownSnapshot: { capturedAt: new Date().toISOString(), signal: "SIGTERM", activeRuns: [{
          runId, companyId, agentId, adapterType: "codex_local", status: "running",
          processPid: before.processPid, processGroupId: before.processGroupId,
        }] },
      });
      const adoption = await heartbeat.reconcileHotRestartAdoption();
      expect(adoption).toMatchObject({ mode: "reported", adoptedRunIds: [runId], lostRunIds: [], skippedRunIds: [] });
      expect(await heartbeat.resumeSupervisedLegacyRun(runId)).toBe(false);
      if (mode === "cancelled") await heartbeat.cancelRun(runId, "Stop recovered fixture");
      else await fs.writeFile(release, "");
      await heartbeat.drainActiveRunExecutions();
      const [after] = await db.select().from(heartbeatRuns).where(eq(heartbeatRuns.id, runId));
      expect(after).toMatchObject({ status: mode === "cancelled" ? "cancelled" : mode === "branch_mismatch" ? "failed" : "succeeded", processPid: before.processPid, processGroupId: before.processGroupId,
        controllerBootId: replacementBootId });
      const [tail] = await db.select().from(legacyAdapterTransports).where(eq(legacyAdapterTransports.runId, runId));
      expect(tail.eventCursor).toBeGreaterThan(previousCustody.eventCursor);
      const rows = await db.select().from(heartbeatRunEvents).where(eq(heartbeatRunEvents.runId, runId));
      expect(new Set(rows.map(row => row.seq)).size).toBe(rows.length);
      expect(await fs.readFile(effects, "utf8")).toBe("started\n");
      const log = await heartbeat.readLog(runId, { limitBytes: 1024 * 1024 });
      expect(log.content).toContain("thread.started");
      expect(after.stdoutExcerpt).toContain("thread.started");
      if (mode === "cancelled") {
        expect(after).toMatchObject({ status: "cancelled", sessionIdAfter: "reattached-session",
          resultJson: { executionCancellation: { state: "acknowledged", proof: "durable_adapter_worker_join",
            custodyId: custody.custodyId } } });
        expect(after.exitCode !== null || after.signal !== null).toBe(true);
        expect(after.logBytes).toBeGreaterThan(0);
        expect(after.logSha256).toMatch(/^[a-f0-9]{64}$/);
        expect(await readDurableChildResult(handle.directory)).not.toBeNull();
      } else {
        expect(rows.some(row => JSON.stringify(row.payload).includes("turn.completed"))).toBe(true);
        expect(after).toMatchObject({ exitCode: 0, signal: null, sessionIdAfter: "reattached-session",
          resultJson: { summary: "reattached result" }, usageJson: { inputTokens: 3, outputTokens: 5 } });
        expect(log.content).toContain("turn.completed");
        expect(after.stdoutExcerpt).toContain("turn.completed");
      }
      const operations = await db.select().from(workspaceOperations).where(eq(workspaceOperations.heartbeatRunId, runId));
      expect(operations.some(row => row.phase === "workspace_finalize" && row.status === (mode === "branch_mismatch" ? "failed" : "succeeded"))).toBe(true);
      if (mode === "branch_mismatch") {
        expect(after.errorCode).toBe("workspace_validation_failed");
        expect(execFileSync("/usr/bin/git", ["branch", "--show-current"], { cwd: work, encoding: "utf8" }).trim()).toBe("wrong");
      }
      expect(rows.filter(row => row.eventType === "legacy_adapter_custody_claimed")).toHaveLength(1);
      // Explicit lease expiry in one process: source host finalization proof,
      // not installed hot restart or physical replacement-controller proof.
      const proof = { mode, runId, previousBootId, replacementBootId,
        observingProcessPid: process.pid, supervisorPid: handle.supervisorPid,
        processPid: before.processPid, processGroupId: before.processGroupId,
        beforeCursor: previousCustody.eventCursor, afterCursor: tail.eventCursor,
        restartMarkerIo: "mocked", adoption,
        terminalResult: { exitCode: after.exitCode, result: after.resultJson, usage: after.usageJson, sessionId: after.sessionIdAfter }, runStatus: after.status, startEffects: 1 };
      console.info("legacy-host-recovery-source-proof", JSON.stringify(proof));
      if (process.env.PAPERCLIP_RUN_SCRATCH_DIR) await fs.writeFile(
        path.join(process.env.PAPERCLIP_RUN_SCRATCH_DIR, `host-recovery-${mode}-proof.json`),
        JSON.stringify(proof, null, 2), { mode: 0o600 });
    } finally {
      await cancelLegacyAdapterWorker(handle.directory);
      await wait(() => readDurableChildResult(handle.directory), receipt => receipt !== null);
    }
  }, 90000);
});
