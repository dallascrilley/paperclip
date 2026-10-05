import { spawn } from "node:child_process";
import { constants } from "node:fs";
import fs from "node:fs/promises";
import path from "node:path";

/**
 * Experimental child transport primitive; heartbeat dispatch does not use it yet.
 * A process exit receipt is not an adapter result or a finalized Paperclip run.
 * Integration must bind the directory to the company/run, redact retained
 * output, recover adapter parsing, and fence the database result consumer.
 */

/** A private spool owned by one independent supervisor, never a PID-only claim. */
export interface DurableChildIdentity {
  version: 1;
  supervisorPid: number;
  executorPid: number;
  processGroupId: number | null;
  startedAt: string;
}

export interface DurableChildResult {
  version: 1;
  exitCode: number | null;
  signal: string | null;
  timedOut: boolean;
  cancelled: boolean;
  stdoutTruncated: boolean;
  stderrTruncated: boolean;
  errorMessage: string | null;
  completedAt: string;
}

// Keep bootstrap code in the compiled module so source and installed packages
// use the same supervisor, without an untracked external script or shell.
const SUPERVISOR = String.raw`
const fs = require('node:fs');
const path = require('node:path');
const { spawn } = require('node:child_process');
let input = '';
process.stdin.setEncoding('utf8');
process.stdin.on('data', chunk => { input += chunk; });
process.stdin.on('end', () => {
  const spec = JSON.parse(input);
  input = '';
  const root = spec.directory;
  const writeReceipt = (name, value) => {
    const temporary = path.join(root, name + '.pending');
    const fd = fs.openSync(temporary, 'wx', 0o600);
    try { fs.writeFileSync(fd, JSON.stringify(value)); fs.fsyncSync(fd); }
    finally { fs.closeSync(fd); }
    fs.renameSync(temporary, path.join(root, name));
  };
  const stdout = fs.openSync(path.join(root, 'stdout'), 'wx', 0o600);
  const stderr = fs.openSync(path.join(root, 'stderr'), 'wx', 0o600);
  const sizes = { stdout: 0, stderr: 0 };
  const truncated = { stdout: false, stderr: false };
  const child = spawn(spec.command, spec.args, {
    cwd: spec.cwd, env: spec.env, detached: process.platform !== 'win32',
    shell: false, stdio: [spec.stdin == null ? 'ignore' : 'pipe', 'pipe', 'pipe'],
  });
  let timedOut = false;
  let cancelled = false;
  let errorMessage = null;
  let killTimer;
  let stopping = false;
  let closed = false;
  const signal = value => {
    if (!child.pid || closed) return;
    if (process.platform === 'win32' && (child.exitCode !== null || child.signalCode !== null)) return;
    try { process.kill(process.platform === 'win32' ? child.pid : -child.pid, value); }
    catch (error) { if (error.code !== 'ESRCH') throw error; }
  };
  const stop = () => {
    if (stopping) return;
    stopping = true;
    signal('SIGTERM');
    killTimer = setTimeout(() => signal('SIGKILL'), spec.graceMs);
  };
  const cancellation = setInterval(() => {
    if (fs.existsSync(path.join(root, 'cancel'))) { cancelled = true; stop(); }
  }, 50);
  const timeout = spec.timeoutMs > 0 ? setTimeout(() => { timedOut = true; stop(); }, spec.timeoutMs) : null;
  child.once('spawn', () => writeReceipt('identity.json', {
    version: 1, supervisorPid: process.pid, executorPid: child.pid,
    processGroupId: process.platform === 'win32' ? null : child.pid,
    startedAt: new Date().toISOString(),
  }));
  const append = (stream, fd, chunk) => {
    const retained = chunk.subarray(0, Math.max(0, spec.maxOutputBytes - sizes[stream]));
    for (let written = 0; written < retained.length;) {
      const count = fs.writeSync(fd, retained, written, retained.length - written);
      if (count <= 0) throw new Error('Durable child spool write made no progress');
      written += count;
      sizes[stream] += count;
    }
    if (retained.length < chunk.length) truncated[stream] = true;
  };
  child.stdout.on('data', chunk => append('stdout', stdout, chunk));
  child.stderr.on('data', chunk => append('stderr', stderr, chunk));
  child.once('error', error => { errorMessage = error.message; });
  if (child.stdin) {
    child.stdin.on('error', error => { if (error.code !== 'EPIPE') errorMessage = error.message; });
    child.stdin.end(spec.stdin);
  }
  child.once('close', (exitCode, exitSignal) => {
    closed = true;
    clearInterval(cancellation); clearTimeout(timeout); clearTimeout(killTimer);
    fs.fsyncSync(stdout); fs.fsyncSync(stderr);
    fs.closeSync(stdout); fs.closeSync(stderr);
    writeReceipt('result.json', {
      version: 1, exitCode, signal: exitSignal, timedOut, cancelled,
      stdoutTruncated: truncated.stdout, stderrTruncated: truncated.stderr,
      errorMessage, completedAt: new Date().toISOString(),
    });
  });
});
`;

export async function startDurableChild(input: {
  /** Caller-owned instance/run directory. Never accepts a reused spool. */
  root: string;
  /** Stable run-scoped key. A duplicate start fails closed, never launches twice. */
  runId: string;
  command: string;
  args: string[];
  cwd: string;
  env: NodeJS.ProcessEnv;
  stdin?: string;
  timeoutMs?: number;
  graceMs?: number;
  maxOutputBytes?: number;
}): Promise<{ directory: string; supervisorPid: number }> {
  const maxOutputBytes = input.maxOutputBytes ?? 16 * 1024 * 1024;
  const graceMs = input.graceMs ?? 5_000;
  const timeoutMs = input.timeoutMs ?? 0;
  for (const value of [maxOutputBytes, graceMs, timeoutMs]) {
    if (!Number.isSafeInteger(value) || value < 0) throw new Error("Invalid durable child limit");
  }
  if (!path.isAbsolute(input.root) || !path.isAbsolute(input.cwd)) {
    throw new Error("Durable child paths must be absolute");
  }
  if (!/^[a-zA-Z0-9][a-zA-Z0-9_-]{0,127}$/.test(input.runId)) {
    throw new Error("Invalid durable child run ID");
  }
  const directory = path.join(input.root, input.runId);
  await fs.mkdir(directory, { mode: 0o700 });
  const supervisor = spawn(process.execPath, ["-e", SUPERVISOR], {
    detached: process.platform !== "win32",
    stdio: ["pipe", "ignore", "ignore"],
    // Do not pass run/provider secrets in argv or inherited supervisor env.
    env: { PATH: process.env.PATH, SystemRoot: process.env.SystemRoot },
  });
  await new Promise<void>((resolve, reject) => {
    supervisor.once("error", reject);
    supervisor.once("spawn", resolve);
  });
  const spec = JSON.stringify({ ...input, root: undefined, directory, maxOutputBytes, graceMs, timeoutMs });
  await new Promise<void>((resolve, reject) => {
    supervisor.stdin.once("error", reject);
    supervisor.stdin.end(spec, () => resolve());
  });
  // After the complete bootstrap, the supervisor has no controller-owned pipe
  // for executor output. It independently drains, times out and joins the child.
  supervisor.unref();
  return { directory, supervisorPid: supervisor.pid! };
}

async function readPrivateFile(directory: string, name: string, maxBytes: number) {
  const file = await fs.open(path.join(directory, name), constants.O_RDONLY | constants.O_NOFOLLOW);
  try {
    const stat = await file.stat();
    if (!stat.isFile() || stat.size > maxBytes) throw new Error("Invalid durable child receipt");
    return await file.readFile();
  } finally {
    await file.close();
  }
}

export async function readDurableChildIdentity(directory: string): Promise<DurableChildIdentity | null> {
  try {
    const value = JSON.parse((await readPrivateFile(directory, "identity.json", 4096)).toString());
    if (value.version !== 1 || !Number.isSafeInteger(value.supervisorPid) || value.supervisorPid <= 0 ||
        !Number.isSafeInteger(value.executorPid) || value.executorPid <= 0 ||
        (value.processGroupId !== null && value.processGroupId !== value.executorPid) ||
        !Number.isFinite(Date.parse(value.startedAt))) throw new Error("Invalid durable child identity");
    return value;
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") return null;
    throw error;
  }
}

export async function readDurableChildResult(directory: string): Promise<DurableChildResult | null> {
  try {
    const value = JSON.parse((await readPrivateFile(directory, "result.json", 65536)).toString());
    if (value.version !== 1 || (value.exitCode !== null && !Number.isInteger(value.exitCode)) ||
        (value.signal !== null && typeof value.signal !== "string") ||
        [value.timedOut, value.cancelled, value.stdoutTruncated, value.stderrTruncated].some(v => typeof v !== "boolean") ||
        (value.errorMessage !== null && typeof value.errorMessage !== "string") ||
        !Number.isFinite(Date.parse(value.completedAt))) throw new Error("Invalid durable child result");
    return value;
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") return null;
    throw error;
  }
}

/** Read from a byte cursor; callers retain their decoder and commit cursor with run events. */
export async function readDurableChildOutput(directory: string, stream: "stdout" | "stderr", offset = 0, limit = 65536) {
  if (!Number.isSafeInteger(offset) || offset < 0 || !Number.isSafeInteger(limit) || limit < 1 || limit > 1024 * 1024) {
    throw new Error("Invalid durable child output cursor");
  }
  let file;
  try { file = await fs.open(path.join(directory, stream), constants.O_RDONLY | constants.O_NOFOLLOW); }
  catch (error) { if ((error as NodeJS.ErrnoException).code === "ENOENT") return { bytes: Buffer.alloc(0), offset }; throw error; }
  try {
    const stat = await file.stat();
    if (!stat.isFile()) throw new Error("Invalid durable child output file");
    const buffer = Buffer.alloc(limit);
    const { bytesRead } = await file.read(buffer, 0, limit, offset);
    return { bytes: buffer.subarray(0, bytesRead), offset: offset + bytesRead };
  } finally { await file.close(); }
}

export async function cancelDurableChild(directory: string) {
  try { await fs.writeFile(path.join(directory, "cancel"), "", { flag: "wx", mode: 0o600 }); }
  catch (error) { if ((error as NodeJS.ErrnoException).code !== "EEXIST") throw error; }
}
