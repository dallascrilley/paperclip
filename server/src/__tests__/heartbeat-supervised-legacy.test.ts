import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { randomUUID } from "node:crypto";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { afterAll, afterEach, beforeAll, describe, expect, it, vi } from "vitest";
import { eq } from "drizzle-orm";
import { agents, companies, createDb, heartbeatRuns, heartbeatRunEvents, legacyAdapterTransports, projects, projectWorkspaces, issues, toolMcpGateways, toolMcpGatewayTokens, toolProfiles } from "@paperclipai/db";
import { startEmbeddedPostgresTestDatabase } from "./helpers/embedded-postgres.js";
import { drainHeartbeatRunsToQuiescence } from "./helpers/drain-heartbeat-runs.js";

const directExecute = vi.hoisted(() => vi.fn(async () => { throw new Error("Direct adapter must not execute"); }));
vi.mock("../adapters/index.js", () => ({
  getServerAdapter: () => ({ type: "codex_local", execute: directExecute, supportsLocalAgentJwt: false }),
  findActiveServerAdapter: () => ({ type: "codex_local", execute: directExecute, supportsLocalAgentJwt: false }),
  runningProcesses: new Map(),
}));
const logHooks = vi.hoisted(() => ({ begin: null as null | ((runId: string) => Promise<void>), finalize: null as null | ((runId: string) => Promise<void>) }));
vi.mock("../services/run-log-store.js", async importOriginal => {
  const actual = await importOriginal<typeof import("../services/run-log-store.js")>();
  return { ...actual, getRunLogStore: () => {
    const store = actual.getRunLogStore();
    return { ...store,
      begin: async (input: Parameters<typeof store.begin>[0]) => {
        const handle = await store.begin(input);
        await logHooks.begin?.(input.runId);
        return handle;
      },
      finalize: async (handle: Parameters<typeof store.finalize>[0]) => {
        await logHooks.finalize?.(path.basename(handle.logRef, ".ndjson"));
        return store.finalize(handle);
      },
    };
  } };
});
const runtimeRelease = vi.hoisted(() => vi.fn());
vi.mock("../services/workspace-runtime.js", async importOriginal => {
  const actual = await importOriginal<typeof import("../services/workspace-runtime.js")>();
  return { ...actual, releaseRuntimeServicesForRun: runtimeRelease };
});
import { heartbeatService } from "../services/heartbeat.js";
import { instanceSettingsService } from "../services/instance-settings.js";

describe("opt-in supervised legacy heartbeat", () => {
  let temporary: Awaited<ReturnType<typeof startEmbeddedPostgresTestDatabase>>;
  let db: ReturnType<typeof createDb>;
  let root: string;
  const companyId = randomUUID(), agentId = randomUUID(), projectId = randomUUID(), workspaceId = randomUUID(), issueId = randomUUID();
  beforeAll(async () => {
    root = await fs.mkdtemp(path.join(process.env.PAPERCLIP_RUN_SCRATCH_DIR ?? os.tmpdir(), "supervised-heartbeat-"));
    await promisify(execFile)("/usr/bin/git", ["init", "-b", "main", root]);
    await fs.mkdir(path.join(root, "codex"));
    await fs.mkdir(path.join(root, "home"));
    const command = path.join(root, "codex-fixture");
    await fs.writeFile(command, `#!${process.execPath}
      const fs = require('node:fs');
      fs.appendFileSync(${JSON.stringify(path.join(root, "effects"))}, 'started\\n');
      process.stdin.resume();
      process.stdout.write(JSON.stringify({type:'thread.started',thread_id:'heartbeat-fixture-session'})+'\\n');
      setTimeout(() => {
        process.stdout.write(JSON.stringify({type:'item.completed',item:{type:'agent_message',text:'supervised heartbeat completed'}})+'\\n');
        process.stdout.write(JSON.stringify({type:'turn.completed',usage:{input_tokens:3,output_tokens:5}})+'\\n');
        process.exit(0);
      }, 150);
    `, { mode: 0o700 });
    temporary = await startEmbeddedPostgresTestDatabase("dal47-heartbeat-");
    db = createDb(temporary.connectionString);
    await instanceSettingsService(db).updateExperimental({ enableNativeRunner: false });
    await db.insert(companies).values({ id: companyId, name: "Supervised fixture", issuePrefix: "SUP", defaultResponsibleUserId: "fixture-user" });
    await db.insert(projects).values({ id: projectId, companyId, name: "Fixture project", status: "active" });
    await db.insert(projectWorkspaces).values({ id: workspaceId, companyId, projectId, name: "Fixture", cwd: root, isPrimary: true });
    await db.insert(agents).values({ id: agentId, companyId, name: "Fixture", adapterType: "codex_local",
      adapterConfig: { engine: "cli", command, cwd: root, model: "fixture", timeoutSec: 10, graceSec: 1,
        managedAiConnection: true, env: { CODEX_HOME: path.join(root, "codex"), HOME: path.join(root, "home"), OPENAI_API_KEY: "", PAPERCLIP_CODEX_AUTH_CACHE: "0" } },
      runtimeConfig: { heartbeat: { wakeOnDemand: true, maxConcurrentRuns: 1 } } });
    await db.insert(issues).values({ id: issueId, companyId, projectId, projectWorkspaceId: workspaceId,
      title: "Complete supervised fixture", status: "in_progress", assigneeAgentId: agentId });
  }, 90000);
  afterEach(() => { logHooks.begin = null; logHooks.finalize = null; });
  afterAll(async () => {
    if (temporary) await temporary.cleanup();
    if (root) await fs.rm(root, { recursive: true, force: true });
  });

  it("feeds the real worker/parser result through common heartbeat finalization with one executor", async () => {
    const heartbeat = heartbeatService(db, { runtimeEnv: { PAPERCLIP_ENABLE_LEGACY_SUPERVISION: "1" }, legacyAdapterWorkerInstanceRoot: root });
    const queued = await heartbeat.wakeup(agentId, { source: "automation", triggerDetail: "system", reason: "issue_commented",
      payload: { issueId }, contextSnapshot: { issueId, taskId: issueId, skipIssueComment: true } });
    expect(queued).not.toBeNull();
    await drainHeartbeatRunsToQuiescence(db, heartbeat);
    const [run] = await db.select().from(heartbeatRuns).where(eq(heartbeatRuns.id, queued!.id));
    expect(run).toMatchObject({ status: "succeeded", exitCode: 0, signal: null, sessionIdAfter: "heartbeat-fixture-session",
      resultJson: { summary: "supervised heartbeat completed" }, usageJson: { inputTokens: 3, outputTokens: 5 } });
    expect(directExecute).not.toHaveBeenCalled();
    expect(await fs.readFile(path.join(root, "effects"), "utf8")).toBe("started\n");
    const [custody] = await db.select().from(legacyAdapterTransports).where(eq(legacyAdapterTransports.runId, run.id));
    expect(custody).toMatchObject({ companyId, agentId, adapterType: "codex_local" });
    expect(custody.eventCursor).toBeGreaterThan(0);
    expect(custody.preparedFinalizationJson).toMatchObject({ version: 1, issueId,
      configuredModel: "fixture", taskSessionReused: false });
    expect(custody.preparedFinalizationJson).not.toHaveProperty("config");
    expect(custody.preparedFinalizationJson).not.toHaveProperty("authToken");
    expect(custody.preparedFinalizationJson?.sessionConfigMetadata).not.toHaveProperty("fingerprints");
    const events = await db.select().from(heartbeatRunEvents).where(eq(heartbeatRunEvents.runId, run.id));
    expect(events.some(event => event.eventType === "legacy_adapter_worker")).toBe(true);
    expect(run.stdoutExcerpt).toContain("turn.completed");
    console.info("supervised-heartbeat-source-proof", JSON.stringify({ controllerPid: process.pid, runId: run.id,
      processPid: run.processPid, processGroupId: run.processGroupId, custodyId: custody.custodyId,
      cursor: custody.eventCursor, status: run.status, exitCode: run.exitCode, sessionId: run.sessionIdAfter,
      usage: run.usageJson, startEffects: 1 }));
  }, 90000);
  it.each(["running", "succeeded"])("preserves replacement-owned %s row and resources when a pending failure settles late", async replacementStatus => {
    const replacementBoot = randomUUID(), gatewayId = randomUUID(), tokenId = randomUUID(), profileId = randomUUID();
    const replacementDeadline = new Date(Date.now() + 60000);
    await db.insert(toolProfiles).values({ id: profileId, companyId, profileKey: gatewayId, name: gatewayId });
    await db.insert(toolMcpGateways).values({ id: gatewayId, companyId, profileId, slug: gatewayId, name: gatewayId });
    runtimeRelease.mockClear();
    let calls = 0;
    logHooks.begin = async runId => {
      await db.insert(toolMcpGatewayTokens).values({ id: tokenId, companyId, gatewayId,
        name: "Retained worker token", tokenHash: randomUUID(), subjectType: "heartbeat_run", subjectId: runId });
    };
    logHooks.finalize = async runId => {
      calls += 1;
      if (calls === 1) throw new Error("owned log finalization failed");
      // The generic failure handler already checked authority. Its pending I/O
      // settles after a replacement has acquired the row: the write must fence.
      await db.update(heartbeatRuns).set({ controllerBootId: replacementBoot,
        controllerLeaseExpiresAt: new Date(Date.now() + 60000), status: replacementStatus,
        executionControlDeadlineAt: replacementDeadline,
        ...(replacementStatus === "succeeded" ? { finishedAt: new Date(), resultJson: { summary: "replacement result" } } : {})
      }).where(eq(heartbeatRuns.id, runId));
      throw new Error("late original controller failure");
    };
    const heartbeat = heartbeatService(db, { runtimeEnv: { PAPERCLIP_ENABLE_LEGACY_SUPERVISION: "1" }, legacyAdapterWorkerInstanceRoot: root });
    const queued = await heartbeat.wakeup(agentId, { source: "automation", triggerDetail: "system", reason: "issue_commented",
      payload: { issueId }, contextSnapshot: { issueId, taskId: issueId, skipIssueComment: true } });
    expect(queued).not.toBeNull();
    await heartbeat.drainActiveRunExecutions();
    logHooks.begin = null; logHooks.finalize = null;
    const [run] = await db.select().from(heartbeatRuns).where(eq(heartbeatRuns.id, queued!.id));
    expect(calls).toBe(2);
    expect(run).toMatchObject({ status: replacementStatus, controllerBootId: replacementBoot, error: null });
    expect(run.executionControlDeadlineAt?.getTime()).toBe(replacementDeadline.getTime());
    const [token] = await db.select().from(toolMcpGatewayTokens).where(eq(toolMcpGatewayTokens.id, tokenId));
    expect(token.revokedAt).toBeNull();
    expect(runtimeRelease).not.toHaveBeenCalledWith(run.id);
    const [custody] = await db.select().from(legacyAdapterTransports).where(eq(legacyAdapterTransports.runId, run.id));
    const prepared = custody.preparedFinalizationJson!;
    expect(custody.eventCursor).toBeGreaterThan(0);
    expect(prepared.scratch).not.toBeNull();
    expect(await fs.stat(prepared.scratch!.dir)).toBeTruthy();
    // Explicit fixture disposition after assertions; this is not adoption proof.
    await db.update(heartbeatRuns).set({ status: "succeeded", finishedAt: new Date() }).where(eq(heartbeatRuns.id, run.id));
    await fs.rm(prepared.scratch!.dir, { recursive: true, force: true });
    console.info("late-failure-source-proof", JSON.stringify({ runId: run.id, replacementBoot, replacementStatus,
      tokenRetained: token.revokedAt === null, scratchRetained: true, terminalFailureSuppressed: true }));
  }, 90000);

  it("keeps an owned post-join failure terminal and releases its resources", async () => {
    runtimeRelease.mockClear();
    logHooks.finalize = async () => { throw new Error("owned finalization failure"); };
    const heartbeat = heartbeatService(db, { runtimeEnv: { PAPERCLIP_ENABLE_LEGACY_SUPERVISION: "1" }, legacyAdapterWorkerInstanceRoot: root });
    const queued = await heartbeat.wakeup(agentId, { source: "automation", triggerDetail: "system", reason: "issue_commented",
      payload: { issueId }, contextSnapshot: { issueId, taskId: issueId, skipIssueComment: true } });
    await heartbeat.drainActiveRunExecutions();
    logHooks.finalize = null;
    const [run] = await db.select().from(heartbeatRuns).where(eq(heartbeatRuns.id, queued!.id));
    expect(run.status).toBe("failed");
    expect(run.error).toContain("owned finalization failure");
    expect(runtimeRelease).toHaveBeenCalledWith(run.id);
    const [custody] = await db.select().from(legacyAdapterTransports).where(eq(legacyAdapterTransports.runId, run.id));
    await expect(fs.stat(custody.preparedFinalizationJson!.scratch!.dir)).rejects.toMatchObject({ code: "ENOENT" });
  }, 90000);

});
