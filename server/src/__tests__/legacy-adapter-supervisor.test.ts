import { spawn } from "node:child_process";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { afterEach, describe, expect, it } from "vitest";
import type { AdapterExecutionContext } from "@paperclipai/adapter-utils";
import { readDurableChildOutput, readDurableChildResult } from "@paperclipai/adapter-utils/durable-child-process";
import { cancelLegacyAdapterWorker, readLegacyAdapterWorkerResult, startLegacyAdapterWorker } from "../services/legacy-adapter-supervisor.js";

const fixtures: Array<{ root: string; directory?: string }> = [];
async function wait<T>(read: () => Promise<T>, ready: (value: T) => boolean, timeoutMs = 10000) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const value = await read();
    if (ready(value)) return value;
    await new Promise(resolve => setTimeout(resolve, 25));
  }
  throw new Error("Adapter worker receipt did not become ready");
}
async function fixture(mode: "sleeping" | "logging" = "logging") {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "paperclip-adapter-worker-"));
  const owned = { root, directory: undefined as string | undefined };
  fixtures.push(owned);
  const home = path.join(root, "home");
  const codexHome = path.join(root, "codex");
  await fs.mkdir(home);
  await fs.mkdir(codexHome);
  const effects = path.join(root, "effects");
  const command = path.join(root, "codex-fixture");
  await fs.writeFile(command, `#!${process.execPath}
    const fs = require('node:fs');
    fs.appendFileSync(${JSON.stringify(effects)}, 'started\\n');
    process.stdin.resume();
    process.stdout.write(JSON.stringify({type:'thread.started',thread_id:'fixture-session'})+'\\n');
    ${mode === "logging" ? "setInterval(() => process.stdout.write(JSON.stringify({type:'item.started',item:{type:'command_execution',command:'fixture'}})+'\\n'), 75);" : ""}
    setTimeout(() => {
      process.stdout.write(JSON.stringify({type:'item.completed',item:{type:'agent_message',text:'fixture completed'}})+'\\n');
      process.stdout.write(JSON.stringify({type:'turn.completed',usage:{input_tokens:3,output_tokens:5}})+'\\n');
      process.exit(0);
    }, 1000);
  `, { mode: 0o700 });
  const context: AdapterExecutionContext = {
    runId: "fixture-run", agent: { id: "fixture-agent", companyId: "fixture-company", name: "Fixture", adapterType: "codex_local", adapterConfig: {} },
    runtime: { sessionId: null, sessionParams: null, sessionDisplayId: null, taskKey: null },
    config: { engine: "cli", command, cwd: root, model: "fixture", timeoutSec: 10, graceSec: 1,
      managedAiConnection: true, env: { CODEX_HOME: codexHome, OPENAI_API_KEY: "" } },
    context: {}, onLog: async () => {},
  };
  const workerEnv = { HOME: home, PATH: process.env.PATH, PAPERCLIP_HOME: path.join(root, "paperclip"),
    PAPERCLIP_CODEX_AUTH_CACHE: "0", CODEX_HOME: codexHome, OPENAI_API_KEY: "" };
  return { root, owned, context, workerEnv, effects };
}
afterEach(async () => {
  for (const owned of fixtures) {
    if (owned.directory) {
      await cancelLegacyAdapterWorker(owned.directory);
      await wait(() => readDurableChildResult(owned.directory!), value => value !== null);
    }
    await fs.rm(owned.root, { recursive: true, force: true });
  }
  fixtures.length = 0;
});

describe("legacy adapter worker supervision", () => {
  it.each(["sleeping", "logging"] as const)("retains codex_local parser and %s executor after controller death", async mode => {
    const f = await fixture(mode);
    const modulePath = fileURLToPath(new URL("../services/legacy-adapter-supervisor.ts", import.meta.url));
    const serializedContext = { ...f.context, onLog: undefined };
    const bootstrap = `
      const { startLegacyAdapterWorker } = await import(${JSON.stringify(modulePath)});
      const context = ${JSON.stringify(serializedContext)};
      context.onLog = async () => {};
      const handle = await startLegacyAdapterWorker({ root: ${JSON.stringify(f.root)}, context, secretValues: [], workerEnv: ${JSON.stringify(f.workerEnv)} });
      console.log(JSON.stringify(handle));
      setInterval(() => {}, 1000);
    `;
    const loader = import.meta.resolve("tsx/esm");
    const controller = spawn(process.execPath, ["--import", loader, "--input-type=module", "-e", bootstrap], { stdio: ["ignore", "pipe", "pipe"] });
    let controlOutput = "";
    let controlError = "";
    controller.stdout.on("data", chunk => { controlOutput += chunk; });
    controller.stderr.on("data", chunk => { controlError += chunk; });
    const exited = new Promise<void>(resolve => controller.once("exit", () => resolve()));
    try {
      await wait(async () => controlOutput, value => value.includes("\n"));
      const handle = JSON.parse(controlOutput.trim());
      f.owned.directory = handle.directory;
      const before = await wait(() => readDurableChildOutput(handle.directory, "stdout"), value => value.bytes.toString().includes('"kind":"spawn"'));
      const event = before.bytes.toString().split("\n").map(line => { try { return JSON.parse(line); } catch { return null; } }).find(value => value?.kind === "spawn");
      expect(event.meta.pid).toBeGreaterThan(0);
      controller.kill("SIGKILL");
      await exited;
      process.kill(event.meta.pid, 0);
      const binding = { runId: f.context.runId, companyId: f.context.agent.companyId, adapterType: "codex_local" };
      const result = await wait(() => readLegacyAdapterWorkerResult(handle.directory, binding), value => value !== null);
      expect(result!.adapterResult).toMatchObject({ exitCode: 0, signal: null, timedOut: false, summary: "fixture completed", usage: { inputTokens: 3, cachedInputTokens: 0, outputTokens: 5 }, sessionId: "fixture-session" });
      const later = await readDurableChildOutput(handle.directory, "stdout", before.offset);
      expect(later.bytes.toString()).toContain("turn.completed");
      expect(await fs.readFile(f.effects, "utf8")).toBe("started\n");
      await expect(readLegacyAdapterWorkerResult(handle.directory, { ...binding, companyId: "other-company" })).rejects.toThrow("binding mismatch");
      console.info("legacy-adapter-worker-proof", JSON.stringify({ adapterType: "codex_local", mode,
        previousControllerPid: controller.pid, supervisorPid: handle.supervisorPid,
        executor: event.meta, beforeOffset: before.offset, afterOffset: later.offset,
        adapterResult: result!.adapterResult, startEffects: 1 }));
    } finally { controller.kill("SIGKILL"); await exited; }
    expect(controlError).not.toContain("Error");
  });

  it("redacts a synthetic secret split across stdout chunks and the parsed result", async () => {
    const f = await fixture();
    const synthetic = "fixture-secret-value";
    await fs.writeFile(String(f.context.config.command), `#!${process.execPath}
      process.stdin.resume();
      process.stdout.write(JSON.stringify({type:'thread.started',thread_id:'fixture-session'})+'\\n');
      const message = JSON.stringify({type:'item.completed',item:{type:'agent_message',text:process.env.FIXTURE_SECRET}})+'\\n';
      const cut = message.indexOf(process.env.FIXTURE_SECRET) + 7;
      process.stdout.write(message.slice(0, cut));
      setTimeout(() => {
        process.stdout.write(message.slice(cut));
        process.stdout.write(JSON.stringify({type:'turn.completed',usage:{input_tokens:0,output_tokens:0}})+'\\n');
        process.exit(0);
      }, 150);
    `, { mode: 0o700 });
    f.context.config.env = { ...(f.context.config.env as Record<string, string>), FIXTURE_SECRET: synthetic };
    const handle = await startLegacyAdapterWorker({ root: f.root, context: f.context, secretValues: [synthetic], workerEnv: f.workerEnv });
    f.owned.directory = handle.directory;
    const result = await wait(() => readLegacyAdapterWorkerResult(handle.directory, {
      runId: f.context.runId, companyId: f.context.agent.companyId, adapterType: "codex_local",
    }), value => value !== null);
    expect(JSON.stringify(result)).not.toContain(synthetic);
    const output = await readDurableChildOutput(handle.directory, "stdout");
    expect(output.bytes.toString()).not.toContain(synthetic);
    const logs = output.bytes.toString().split("\n").filter(Boolean).map(line => JSON.parse(line)).filter(value => value.kind === "log").map(value => value.chunk).join("");
    expect(logs).not.toContain(synthetic);
    expect(result!.adapterResult.summary).not.toBe(synthetic);
    expect(result!.adapterResult.exitCode).toBe(0);
  });

  it("joins the real detached executor on durable cancellation", async () => {
    const f = await fixture();
    const handle = await startLegacyAdapterWorker({ root: f.root, context: f.context, secretValues: [], workerEnv: f.workerEnv });
    f.owned.directory = handle.directory;
    await wait(() => readDurableChildOutput(handle.directory, "stdout"), value => value.bytes.toString().includes('"kind":"spawn"'));
    await wait(async () => { try { return await fs.readFile(f.effects, "utf8"); } catch { return null; } }, value => value === "started\n");
    await cancelLegacyAdapterWorker(handle.directory);
    await cancelLegacyAdapterWorker(handle.directory);
    const result = await wait(() => readLegacyAdapterWorkerResult(handle.directory, {
      runId: f.context.runId, companyId: f.context.agent.companyId, adapterType: "codex_local",
    }), value => value !== null);
    expect(result!.adapterResult.signal).toBe("SIGTERM");
    await wait(() => readDurableChildResult(handle.directory), value => value !== null);
    expect(await fs.readFile(f.effects, "utf8")).toBe("started\n");
  });

  it("rejects unsupported transports and an already-cancelled invocation before spawn", async () => {
    const f = await fixture();
    await expect(startLegacyAdapterWorker({ root: f.root, context: { ...f.context, config: { ...f.context.config, engine: "acp" } }, secretValues: [] })).rejects.toThrow("cannot be supervised");
    await expect(startLegacyAdapterWorker({ root: f.root, context: { ...f.context, executionTransport: { remoteExecution: { transport: "ssh" } } }, secretValues: [] })).rejects.toThrow("cannot be supervised");
    const abort = new AbortController();
    abort.abort(new Error("Stopped before dispatch"));
    await expect(startLegacyAdapterWorker({ root: f.root, context: { ...f.context, signal: abort.signal }, secretValues: [] })).rejects.toThrow("Stopped before dispatch");
    expect(await fs.readdir(f.root)).not.toContain("fixture-run");
  });
});
