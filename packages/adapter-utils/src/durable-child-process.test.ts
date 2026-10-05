import { spawn } from "node:child_process";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { afterEach, describe, expect, it } from "vitest";
import {
  cancelDurableChild,
  readDurableChildIdentity,
  readDurableChildOutput,
  readDurableChildResult,
  startDurableChild,
} from "./durable-child-process.js";

const roots = new Set<string>();
const directories = new Set<string>();
async function until<T>(read: () => Promise<T>, ready: (value: T) => boolean, timeoutMs = 5000) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const value = await read();
    if (ready(value)) return value;
    await new Promise(resolve => setTimeout(resolve, 25));
  }
  throw new Error("Durable child receipt did not become ready");
}
async function root() {
  const directory = await fs.mkdtemp(path.join(os.tmpdir(), "paperclip-durable-child-"));
  roots.add(directory);
  return directory;
}
async function start(command: string, args: string[], extra: Partial<Parameters<typeof startDurableChild>[0]> = {}) {
  const cwd = await root();
  const handle = await startDurableChild({ root: cwd, runId: "test-run", cwd, command, args, env: {}, timeoutMs: 3000, graceMs: 50, ...extra });
  directories.add(handle.directory);
  return handle;
}
afterEach(async () => {
  for (const directory of directories) {
    await cancelDurableChild(directory);
    await until(() => readDurableChildResult(directory), value => value !== null);
  }
  directories.clear();
  for (const directory of roots) await fs.rm(directory, { recursive: true, force: true });
  roots.clear();
});

describe("durable child supervision", () => {
  it.each(["sleeping", "logging"])("retains the %s executor and terminal receipt after controller SIGKILL", async mode => {
    const cwd = await root();
    const effect = path.join(cwd, "effects");
    const modulePath = fileURLToPath(new URL("./durable-child-process.ts", import.meta.url));
    const executor = `
      const fs = require('node:fs');
      fs.appendFileSync(${JSON.stringify(effect)}, 'started\\n');
      process.stdout.write('before\\n');
      ${mode === "logging" ? "setInterval(() => process.stdout.write('tick\\n'), 30);" : ""}
      setTimeout(() => { process.stdout.write('after\\n'); process.exit(7); }, 600);
    `;
    const bootstrap = `
      const { startDurableChild } = await import(${JSON.stringify(modulePath)});
      const handle = await startDurableChild(${JSON.stringify({ root: cwd, runId: "test-run", cwd, command: process.execPath, args: ["-e", executor], env: {}, timeoutMs: 3000, graceMs: 50 })});
      console.log(JSON.stringify(handle));
      setInterval(() => {}, 1000);
    `;
    const controller = spawn(process.execPath, ["--experimental-strip-types", "--input-type=module", "-e", bootstrap], { stdio: ["ignore", "pipe", "pipe"] });
    let stdout = "";
    let stderr = "";
    controller.stdout.on("data", chunk => { stdout += chunk; });
    controller.stderr.on("data", chunk => { stderr += chunk; });
    const controllerExit = new Promise<void>(resolve => controller.once("exit", () => resolve()));
    try {
      await until(async () => stdout, value => value.includes("\n"));
      const handle = JSON.parse(stdout.trim()) as { directory: string; supervisorPid: number };
      directories.add(handle.directory);
      const identity = await until(() => readDurableChildIdentity(handle.directory), value => value !== null);
      expect(identity).toMatchObject({ supervisorPid: handle.supervisorPid });
      expect(identity!.executorPid).not.toBe(controller.pid);
      if (process.platform !== "win32") expect(identity!.processGroupId).toBe(identity!.executorPid);
      const before = await until(() => readDurableChildOutput(handle.directory, "stdout"), value => value.bytes.toString().includes("before\n"));
      controller.kill("SIGKILL");
      await controllerExit;
      process.kill(identity!.executorPid, 0);
      expect(await readDurableChildIdentity(handle.directory)).toEqual(identity);
      const replacementScript = `
        const { readDurableChildIdentity, readDurableChildOutput, readDurableChildResult } = await import(${JSON.stringify(modulePath)});
        const directory = ${JSON.stringify(handle.directory)};
        let result;
        const deadline = Date.now() + 5000;
        while (!(result = await readDurableChildResult(directory))) {
          if (Date.now() > deadline) throw new Error('replacement reader timed out');
          await new Promise(resolve => setTimeout(resolve, 25));
        }
        const output = await readDurableChildOutput(directory, 'stdout', ${before.offset});
        console.log(JSON.stringify({ replacementPid: process.pid, identity: await readDurableChildIdentity(directory), result, output: output.bytes.toString(), offset: output.offset }));
      `;
      const replacement = spawn(process.execPath, ["--experimental-strip-types", "--input-type=module", "-e", replacementScript], { stdio: ["ignore", "pipe", "pipe"] });
      let replacementOutput = "";
      let replacementError = "";
      replacement.stdout.on("data", chunk => { replacementOutput += chunk; });
      replacement.stderr.on("data", chunk => { replacementError += chunk; });
      const replacementExit = await new Promise<number | null>((resolve, reject) => {
        replacement.once("error", reject);
        replacement.once("exit", code => resolve(code));
      });
      expect(replacementExit, replacementError).toBe(0);
      const receipt = JSON.parse(replacementOutput);
      expect(receipt.replacementPid).not.toBe(controller.pid);
      expect(receipt.identity).toEqual(identity);
      expect(receipt.output).toContain("after\n");
      expect(receipt.offset).toBeGreaterThan(before.offset);
      expect(receipt.result).toMatchObject({ exitCode: 7, signal: null, timedOut: false, cancelled: false });
      console.info("durable-child-proof", JSON.stringify({ mode, previousControllerPid: controller.pid, beforeCursor: before.offset, ...receipt, startEffects: 1 }));
      expect(await fs.readFile(effect, "utf8")).toBe("started\n");
      expect((await fs.stat(handle.directory)).mode & 0o777).toBe(0o700);
      expect((await fs.stat(path.join(handle.directory, "stdout"))).mode & 0o777).toBe(0o600);
    } finally {
      controller.kill("SIGKILL");
      await controllerExit;
    }
    expect(stderr).not.toContain("Error");
  });

  it("keeps draining beyond the output retention cap and persists the real exit", async () => {
    const handle = await start(process.execPath, ["-e", "process.stdout.write('x'.repeat(100000)); process.stderr.write('error');"], { maxOutputBytes: 32 });
    expect(await until(() => readDurableChildResult(handle.directory), value => value !== null)).toMatchObject({ exitCode: 0, stdoutTruncated: true, stderrTruncated: false });
    expect((await readDurableChildOutput(handle.directory, "stdout")).bytes.toString()).toBe("x".repeat(32));
    expect((await readDurableChildOutput(handle.directory, "stderr")).bytes.toString()).toBe("error");
  });

  it("owns timeout independently of the controller", async () => {
    const handle = await start(process.execPath, ["-e", "setInterval(() => {}, 1000)"], { timeoutMs: 100 });
    expect(await until(() => readDurableChildResult(handle.directory), value => value !== null)).toMatchObject({ timedOut: true, cancelled: false, signal: "SIGTERM" });
  });

  it("accepts an idempotent cancellation request in the same private spool", async () => {
    const handle = await start(process.execPath, ["-e", "setInterval(() => {}, 1000)"]);
    await until(() => readDurableChildIdentity(handle.directory), value => value !== null);
    await cancelDurableChild(handle.directory);
    await cancelDurableChild(handle.directory);
    expect(await until(() => readDurableChildResult(handle.directory), value => value !== null)).toMatchObject({ timedOut: false, cancelled: true, signal: "SIGTERM" });
  });

  it("refuses a second executor for the same run ID", async () => {
    const handle = await start(process.execPath, ["-e", "setInterval(() => {}, 1000)"]);
    await until(() => readDurableChildIdentity(handle.directory), value => value !== null);
    await expect(startDurableChild({
      root: path.dirname(handle.directory), runId: "test-run", cwd: path.dirname(handle.directory),
      command: process.execPath, args: ["-e", "throw new Error('duplicate executor')"], env: {},
    })).rejects.toMatchObject({ code: "EEXIST" });
  });

  it.skipIf(process.platform === "win32")("rejects symlink receipts and invalid byte cursors", async () => {
    const handle = await start(process.execPath, ["-e", "process.stdout.write('done')"]);
    await until(() => readDurableChildResult(handle.directory), value => value !== null);
    await fs.rename(path.join(handle.directory, "stdout"), path.join(handle.directory, "other"));
    await fs.symlink(path.join(handle.directory, "other"), path.join(handle.directory, "stdout"));
    await expect(readDurableChildOutput(handle.directory, "stdout")).rejects.toMatchObject({ code: "ELOOP" });
    await expect(readDurableChildOutput(handle.directory, "stderr", -1)).rejects.toThrow("cursor");
  });

  it.skipIf(process.platform === "win32")("joins pipe-holding descendants after the executor leader exits", async () => {
    const handle = await start(process.execPath, ["-e", `
      require('node:child_process').spawn(process.execPath, ['-e', 'setInterval(() => {}, 1000)'], { stdio: 'inherit' });
      setTimeout(() => process.exit(0), 25);
    `], { timeoutMs: 150 });
    expect(await until(() => readDurableChildResult(handle.directory), value => value !== null)).toMatchObject({ exitCode: 0, timedOut: true });
  });

  it("records spawn failure instead of treating missing PID as a successful run", async () => {
    const handle = await start("/does-not-exist-paperclip-durable-child", []);
    expect(await until(() => readDurableChildResult(handle.directory), value => value !== null)).toMatchObject({ errorMessage: expect.stringContaining("ENOENT") });
    expect(await readDurableChildIdentity(handle.directory)).toBeNull();
  });
});
