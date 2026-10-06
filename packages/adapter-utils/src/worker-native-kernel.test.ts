import { createHash } from "node:crypto";
import { existsSync } from "node:fs";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import { createDarwinKernelPrimitives, observeDarwinKernelProcess, type DarwinKernelSource } from "./worker-native-kernel.js";

// This exact source seam is an explicit bounded local test configuration. The
// producer has no default deployment path and never activates this worktree.
const source: DarwinKernelSource = {
  pythonPath: "/usr/bin/python3",
  libraryDirectory: "/Users/dallascrilley/Workspace/_worktrees/system-diagnostics/paperclip-DAL-71-enforce-shared-reservations-for-cua-guest-start-and-resume/lib",
  kernelSha256: "b022da978f73df744b262eac25b1020627a6f8fa0fa377df7a7a8db2e7d9bd85",
  authoritySha256: "d27acb61f8b796b918bcd3674a3b790110eb50997347dbafd97658a4ff9870f6",
};
const darwin = process.platform === "darwin";
describe.runIf(darwin && existsSync(join(source.libraryDirectory, "host_capacity_kernel.py")))("actual DarwinKernel.process seam (read-only own PID)", () => {
  it("observes own boot, kernel birth and PGID, and verifies their exact match", async () => {
    const result = await observeDarwinKernelProcess(source, process.pid, 1000);
    expect(result.status).toBe("available");
    if (result.status !== "available") throw new Error(result.reason);
    expect(result.value).toMatchObject({ pid: process.pid, birth: expect.stringMatching(/^[1-9][0-9]*:[0-9]{6}$/), pgid: expect.any(Number) });
    expect(await createDarwinKernelPrimitives(source).kernel(result.value, 1000)).toBe(true);
  });

  it("refuses PID birth reuse, different boot, and different process group", async () => {
    const result = await observeDarwinKernelProcess(source, process.pid, 1000);
    if (result.status !== "available") throw new Error(result.reason);
    const kernel = createDarwinKernelPrimitives(source).kernel;
    expect(await kernel({ ...result.value, birth: "1:000000" }, 1000)).toBe(false);
    expect(await kernel({ ...result.value, pgid: result.value.pgid === 1 ? 2 : 1 }, 1000)).toBe(false);
    expect(await kernel({ ...result.value, boot: "11111111-1111-1111-1111-111111111111" }, 1000)).toBe(false);
  });

  it("retains claims when pinned source does not match", async () => {
    expect(await observeDarwinKernelProcess({ ...source, kernelSha256: "0".repeat(64) }, process.pid, 1000))
      .toMatchObject({ status: "unavailable", retain_claim: true });
  });
});

describe("kernel-only support boundaries", () => {
  it("keeps all non-kernel primitives unsupported", async () => {
    const primitives = createDarwinKernelPrimitives(source);
    expect(await primitives.registration(null!, null!, 1000)).toBe(false);
    expect(await primitives.succession(null!, null!, "lease", 1000)).toBe(false);
    expect(await primitives.lifetime(null!, 1000)).toBeNull();
    expect(await primitives.completion(null!, 1000)).toBeNull();
    expect(await primitives.fenceLaunch(null!, 1000)).toBeNull();
  });

  it("refuses invalid PID/deadline and retains claims", async () => {
    for (const [pid, timeout] of [[0, 1000], [process.pid, 0], [process.pid, 0.5], [process.pid, Number.NaN]]) {
      expect(await observeDarwinKernelProcess(source, pid, timeout)).toMatchObject({ status: "unavailable", retain_claim: true });
    }
  });

  it.runIf(darwin)("kills a hung read-only probe at its deadline and retains claims", async () => {
    const directory = await mkdtemp(join(process.env.PAPERCLIP_RUN_SCRATCH_DIR ?? process.env.PAPERCLIP_SCRATCH_DIR ?? tmpdir(), "dal-80-kernel-test-"));
    const authority = "class Refused(RuntimeError): pass\n";
    // Isolated fault injection only, no native worker/guest, no ledger.
    const kernel = "import time\nclass DarwinKernel:\n def process(self, pid, deadline=None):\n  time.sleep(60)\n";
    const hash = (value: string) => createHash("sha256").update(value).digest("hex");
    try {
      await writeFile(join(directory, "host_capacity.py"), authority);
      await writeFile(join(directory, "host_capacity_kernel.py"), kernel);
      const started = performance.now();
      expect(await observeDarwinKernelProcess({ ...source, libraryDirectory: directory,
        kernelSha256: hash(kernel), authoritySha256: hash(authority) }, process.pid, 250))
        .toMatchObject({ status: "unavailable", retain_claim: true });
      expect(performance.now() - started).toBeLessThan(1500);
    } finally { await rm(directory, { recursive: true, force: true }); }
  });
});
