import fs from "node:fs/promises";
import { constants } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import type { AdapterExecutionContext, AdapterExecutionResult, AdapterRuntimeMcpServer } from "@paperclipai/adapter-utils";
import { startDurableChild } from "@paperclipai/adapter-utils/durable-child-process";
import { sanitizeInheritedPaperclipEnv } from "@paperclipai/adapter-utils/server-utils";

/** Initial supported capability; other legacy transports retain the drain gate. */
export const SUPERVISED_LEGACY_ADAPTERS = new Set(["codex_local"]);
export type LegacyAdapterWorkerInput = Pick<AdapterExecutionContext,
  "runId" | "agent" | "runtime" | "config" | "context" | "executionContinuation" |
  "runtimeCommandSpec" | "runtimeTools" | "authToken"
> & { mcpServers: AdapterRuntimeMcpServer[]; secretValues: string[] };

export interface LegacyAdapterWorkerReceipt {
  version: 1;
  runId: string;
  companyId: string;
  adapterType: string;
  adapterResult: AdapterExecutionResult;
  completedAt: string;
}

/** Not wired into heartbeat dispatch yet; callers must fence run ownership. */
export async function startLegacyAdapterWorker(input: {
  root: string;
  context: AdapterExecutionContext;
  secretValues: string[];
  workerEnv?: NodeJS.ProcessEnv;
}) {
  const context = input.context;
  context.signal?.throwIfAborted();
  if (!SUPERVISED_LEGACY_ADAPTERS.has(context.agent.adapterType ?? "") ||
      context.config.engine !== "cli" || context.executionTarget?.kind === "remote" ||
      context.executionTransport?.remoteExecution || context.stopRemoteStartup) {
    throw new Error("Legacy adapter transport cannot be supervised");
  }
  const mcpServers = context.runtimeMcp?.getServers() ?? [];
  const workerInput: LegacyAdapterWorkerInput = {
    runId: context.runId, agent: context.agent, runtime: context.runtime,
    config: context.config, context: context.context,
    executionContinuation: context.executionContinuation,
    runtimeCommandSpec: context.runtimeCommandSpec,
    runtimeTools: context.runtimeTools,
    authToken: context.authToken,
    mcpServers,
    secretValues: [...new Set([
      ...input.secretValues, context.authToken ?? "", context.runtimeTools?.bearerToken ?? "",
      ...mcpServers.map(server => server.token),
    ])].filter(value => typeof value === "string" && value.length > 0),
  };
  // Refuse silently dropped functions or cyclic input before creating custody.
  const serialized = JSON.stringify(workerInput, (_key, value) => {
    if (typeof value === "function" || typeof value === "bigint") throw new Error("Non-serializable adapter worker input");
    return value;
  });
  const worker = new URL(import.meta.url.endsWith(".ts") ? "./legacy-adapter-worker.ts" : "./legacy-adapter-worker.js", import.meta.url);
  const directory = path.join(input.root, context.runId);
  const sourceLoader = worker.pathname.endsWith(".ts") ? ["--import", import.meta.resolve("tsx/esm")] : [];
  return startDurableChild({
    root: input.root, runId: context.runId,
    command: process.execPath, args: [...sourceLoader, fileURLToPath(worker), directory],
    cwd: input.root,
    env: input.workerEnv ?? sanitizeInheritedPaperclipEnv(process.env),
    stdin: serialized,
    // Adapter/process monitors own execution deadlines. This supervisor must
    // not SIGKILL an adapter worker before it joins its detached executors.
    timeoutMs: 0,
    maxOutputBytes: 16 * 1024 * 1024,
  });
}

export async function readLegacyAdapterWorkerResult(directory: string, binding: {
  runId: string; companyId: string; adapterType: string;
}): Promise<LegacyAdapterWorkerReceipt | null> {
  let file;
  try { file = await fs.open(path.join(directory, "adapter-result.json"), constants.O_RDONLY | constants.O_NOFOLLOW); }
  catch (error) { if ((error as NodeJS.ErrnoException).code === "ENOENT") return null; throw error; }
  try {
    const stat = await file.stat();
    if (!stat.isFile() || stat.size > 64 * 1024 * 1024) throw new Error("Invalid adapter worker receipt");
    const value = JSON.parse(await file.readFile("utf8")) as LegacyAdapterWorkerReceipt;
    if (value.version !== 1 || value.runId !== binding.runId || value.companyId !== binding.companyId ||
        value.adapterType !== binding.adapterType || !Number.isFinite(Date.parse(value.completedAt)) ||
        !value.adapterResult || (value.adapterResult.exitCode !== null && !Number.isInteger(value.adapterResult.exitCode)) ||
        (value.adapterResult.signal !== null && typeof value.adapterResult.signal !== "string") ||
        typeof value.adapterResult.timedOut !== "boolean" || value.adapterResult.nativeFinalization) {
      throw new Error("Adapter worker receipt binding mismatch");
    }
    return value;
  } finally { await file.close(); }
}

/** The worker joins its actual detached child; do not kill the worker group. */
export async function cancelLegacyAdapterWorker(directory: string) {
  try { await fs.writeFile(path.join(directory, "adapter-cancel"), "", { flag: "wx", mode: 0o600 }); }
  catch (error) { if ((error as NodeJS.ErrnoException).code !== "EEXIST") throw error; }
}
