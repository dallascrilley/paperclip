import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { randomUUID } from "node:crypto";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { afterAll, beforeAll, describe, expect, it, vi } from "vitest";
import { eq } from "drizzle-orm";
import { agents, companies, createDb, heartbeatRuns, heartbeatRunEvents, legacyAdapterTransports, projects, projectWorkspaces, issues } from "@paperclipai/db";
import { startEmbeddedPostgresTestDatabase } from "./helpers/embedded-postgres.js";
import { drainHeartbeatRunsToQuiescence } from "./helpers/drain-heartbeat-runs.js";

const directExecute = vi.hoisted(() => vi.fn(async () => { throw new Error("Direct adapter must not execute"); }));
vi.mock("../adapters/index.js", () => ({
  getServerAdapter: () => ({ type: "codex_local", execute: directExecute, supportsLocalAgentJwt: false }),
  findActiveServerAdapter: () => ({ type: "codex_local", execute: directExecute, supportsLocalAgentJwt: false }),
  runningProcesses: new Map(),
}));
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
    const events = await db.select().from(heartbeatRunEvents).where(eq(heartbeatRunEvents.runId, run.id));
    expect(events.some(event => event.eventType === "legacy_adapter_worker")).toBe(true);
    expect(run.stdoutExcerpt).toContain("turn.completed");
    console.info("supervised-heartbeat-source-proof", JSON.stringify({ controllerPid: process.pid, runId: run.id,
      processPid: run.processPid, processGroupId: run.processGroupId, custodyId: custody.custodyId,
      cursor: custody.eventCursor, status: run.status, exitCode: run.exitCode, sessionId: run.sessionIdAfter,
      usage: run.usageJson, startEffects: 1 }));
  }, 90000);
});
