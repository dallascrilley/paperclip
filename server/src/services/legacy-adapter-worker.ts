import fs from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import type { AdapterExecutionResult } from "@paperclipai/adapter-utils";
import { runningProcesses, signalRunningProcess } from "@paperclipai/adapter-utils/server-utils";
import { redactSensitiveText, redactEventPayload } from "../redaction.js";
import { redactRegisteredSecretValues } from "./run-secret-redaction.js";
import type { LegacyAdapterWorkerInput, LegacyAdapterWorkerReceipt } from "./legacy-adapter-supervisor.js";

async function writeAtomicReceipt(directory: string, receipt: LegacyAdapterWorkerReceipt) {
  const serialized = JSON.stringify(receipt);
  if (Buffer.byteLength(serialized) > 64 * 1024 * 1024) throw new Error("Adapter worker result exceeds retention limit");
  const file = await fs.open(path.join(directory, "adapter-result.pending"), "wx", 0o600);
  try { await file.writeFile(serialized); await file.sync(); }
  finally { await file.close(); }
  await fs.rename(path.join(directory, "adapter-result.pending"), path.join(directory, "adapter-result.json"));
}

/** Own the complete parser, monitor and cleanup lifecycle independently. */
export async function runLegacyAdapterWorker(directory: string, input: LegacyAdapterWorkerInput) {
  if (input.agent.adapterType !== "codex_local" || input.config.engine !== "cli" ||
      !/^[a-zA-Z0-9][a-zA-Z0-9_-]{0,127}$/.test(input.runId) ||
      path.basename(directory) !== input.runId) throw new Error("Unsupported adapter worker binding");
  const values = input.secretValues.filter(value => value.length > 0).sort((a, b) => b.length - a.length);
  const sanitize = <T>(value: T): T => redactRegisteredSecretValues(value, values);
  let sendQueue = Promise.resolve();
  const send = (value: Record<string, unknown>) => {
    const line = JSON.stringify(sanitize(redactEventPayload(value))) + "\n";
    sendQueue = sendQueue.then(() => new Promise<void>((resolve, reject) =>
      process.stdout.write(line, error => error ? reject(error) : resolve())));
    return sendQueue;
  };
  const controller = new AbortController();
  let cancelled = false;
  let active = true;
  let killTimer: NodeJS.Timeout | undefined;
  const stop = () => {
    if (cancelled || !active) return;
    cancelled = true;
    controller.abort(new Error("Adapter worker cancelled"));
    const tracked = runningProcesses.get(input.runId);
    if (tracked) signalRunningProcess(tracked, "SIGTERM");
    killTimer = setTimeout(() => {
      const current = runningProcesses.get(input.runId);
      if (current) signalRunningProcess(current, "SIGKILL");
    }, 5000);
  };
  process.on("SIGTERM", stop);
  process.on("SIGINT", stop);
  const cancellationTimer = setInterval(() => {
    void fs.access(path.join(directory, "adapter-cancel")).then(stop, () => {});
  }, 50);
  // Retain a suffix that can contain a split registered secret. Raw parser
  // input remains in the adapter's memory, while durable logs are redacted.
  const suffixLength = Math.max(0, ...values.map(value => value.length - 1));
  const pending = { stdout: "", stderr: "" };
  const flush = async (stream: "stdout" | "stderr", final = false) => {
    let text = pending[stream];
    let cut = final ? text.length : Math.max(0, text.length - suffixLength);
    // A full secret can straddle the proposed emit boundary. Keep it whole
    // until it can be replaced rather than exposing an unredacted prefix.
    for (const secret of values) {
      let position = text.indexOf(secret);
      while (position >= 0) {
        if (position < cut && position + secret.length > cut) cut = position;
        position = text.indexOf(secret, position + 1);
      }
    }
    text = text.slice(0, cut);
    pending[stream] = pending[stream].slice(cut);
    if (text) await send({ kind: "log", stream, chunk: redactSensitiveText(sanitize(text)) });
  };
  let result: AdapterExecutionResult;
  try {
    const { execute } = await import("@paperclipai/adapter-codex-local/server");
    result = await execute({
      ...input,
      runtimeMcp: { getServers: () => input.mcpServers },
      signal: controller.signal,
      onCancellationReady: async () => {},
      onLog: async (stream, chunk) => { pending[stream] += chunk; await flush(stream); },
      onMeta: async meta => send({ kind: "meta", meta }),
      onEvent: async event => send({ kind: "event", event }),
      onRuntimeProgress: async progress => send({ kind: "progress", progress }),
      onDispatch: () => { void send({ kind: "dispatch" }).catch(() => {}); },
      onSpawn: async meta => {
        await send({ kind: "spawn", meta });
        if (cancelled) {
          const tracked = runningProcesses.get(input.runId);
          if (tracked) signalRunningProcess(tracked, "SIGTERM");
        }
      },
    });
  } catch (error) {
    result = { exitCode: 1, signal: null, timedOut: false,
      errorCode: "legacy_adapter_worker_failed",
      errorMessage: redactSensitiveText(sanitize(error instanceof Error ? error.message : "Adapter worker failed")) };
  } finally {
    active = false;
    clearInterval(cancellationTimer);
    clearTimeout(killTimer);
    process.off("SIGTERM", stop);
    process.off("SIGINT", stop);
    await flush("stdout", true);
    await flush("stderr", true);
  }
  await sendQueue;
  await writeAtomicReceipt(directory, {
    version: 1, runId: input.runId, companyId: input.agent.companyId,
    adapterType: input.agent.adapterType!,
    adapterResult: sanitize({
      ...result,
      // Typed usage/session fields are adapter contracts, not arbitrary event
      // payloads. Generic key redaction must not replace numeric token counts.
      errorMessage: result.errorMessage ? redactSensitiveText(result.errorMessage) : result.errorMessage,
      summary: result.summary ? redactSensitiveText(result.summary) : result.summary,
      resultJson: redactEventPayload(result.resultJson ?? null),
    }), completedAt: new Date().toISOString(),
  });
}

if (process.argv[1] && fileURLToPath(import.meta.url) === path.resolve(process.argv[1])) {
  const directory = process.argv[2];
  let bootstrap = "";
  process.stdin.setEncoding("utf8");
  for await (const chunk of process.stdin) bootstrap += chunk;
  const input = JSON.parse(bootstrap) as LegacyAdapterWorkerInput;
  bootstrap = "";
  await runLegacyAdapterWorker(directory, input);
}
