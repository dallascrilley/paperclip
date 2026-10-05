import fs from "node:fs/promises";
import path from "node:path";
import os from "node:os";
import { afterEach, describe, expect, it, vi } from "vitest";
import { consumeLegacyAdapterEventBatch, readLegacyAdapterEventBatch } from "../services/legacy-adapter-events.js";

const owned: string[] = [];
const binding = { runId: "event-fixture", companyId: "event-company", adapterType: "codex_local" };
async function fixture() {
  const root = process.env.PAPERCLIP_RUN_SCRATCH_DIR ?? process.env.PAPERCLIP_SCRATCH_DIR ?? os.tmpdir();
  const directory = await fs.mkdtemp(path.join(root, "adapter-event-test-"));
  owned.push(directory);
  await fs.writeFile(path.join(directory, "stdout"), "", { mode: 0o600 });
  return { directory, binding, cursor: 0 };
}
async function terminal(directory: string, changes = {}) {
  await fs.writeFile(path.join(directory, "adapter-result.json"), JSON.stringify({
    version: 1, ...binding, completedAt: new Date().toISOString(),
    adapterResult: { exitCode: 0, signal: null, timedOut: false, summary: "completed" },
  }));
  await fs.writeFile(path.join(directory, "result.json"), JSON.stringify({
    version: 1, exitCode: 0, signal: null, timedOut: false, cancelled: false,
    stdoutTruncated: false, stderrTruncated: false, errorMessage: null,
    completedAt: new Date().toISOString(), ...changes,
  }));
}
afterEach(async () => { await Promise.all(owned.splice(0).map(directory => fs.rm(directory, { recursive: true, force: true }))); });

describe("durable adapter event framing and commit", () => {
  it("leaves split UTF-8 and incomplete JSON at the durable cursor for replacement readers", async () => {
    const f = await fixture();
    const first = Buffer.from(JSON.stringify({ kind: "dispatch" }) + "\n");
    const second = Buffer.from(JSON.stringify({ kind: "log", stream: "stdout", chunk: "later 🧭 output" }) + "\n");
    const cut = second.indexOf(Buffer.from("🧭")) + 2;
    await fs.writeFile(path.join(f.directory, "stdout"), Buffer.concat([first, second.subarray(0, cut)]));
    const before = await readLegacyAdapterEventBatch(f);
    expect(before).toEqual({ events: [{ kind: "dispatch" }], nextCursor: first.length, terminal: null });
    await fs.appendFile(path.join(f.directory, "stdout"), second.subarray(cut));
    await terminal(f.directory);
    const replacement = await readLegacyAdapterEventBatch({ ...f, cursor: before.nextCursor });
    expect(replacement.events).toEqual([{ kind: "log", stream: "stdout", chunk: "later 🧭 output" }]);
    expect(replacement.nextCursor).toBe(first.length + second.length);
    expect(replacement.terminal?.adapterResult.summary).toBe("completed");
  });

  it("commits events and cursor once through host compare-and-swap; stale consumers fail", async () => {
    const f = await fixture();
    await fs.writeFile(path.join(f.directory, "stdout"), '{"kind":"dispatch"}\n');
    let cursor = 0;
    const applied: unknown[] = [];
    const commit = vi.fn(async (batch) => {
      if (batch.expectedCursor !== cursor) return false;
      cursor = batch.nextCursor;
      applied.push(...batch.events);
      return true;
    });
    await consumeLegacyAdapterEventBatch({ ...f, commit });
    await expect(consumeLegacyAdapterEventBatch({ ...f, commit })).rejects.toThrow("lost ownership or cursor");
    expect(applied).toEqual([{ kind: "dispatch" }]);
    expect(cursor).toBe(20);
    const lostLease = vi.fn(async () => false);
    await expect(consumeLegacyAdapterEventBatch({ ...f, cursor, commit: lostLease })).rejects.toThrow("lost ownership");
  });

  it("does not expose a result before the supervisor joins; rejects lost output and unbound results", async () => {
    const f = await fixture();
    await terminal(f.directory);
    await fs.rename(path.join(f.directory, "result.json"), path.join(f.directory, "held-result"));
    expect((await readLegacyAdapterEventBatch(f)).terminal).toBeNull();
    await fs.rename(path.join(f.directory, "held-result"), path.join(f.directory, "result.json"));
    await expect(readLegacyAdapterEventBatch({ ...f, binding: { ...binding, companyId: "other-company" } })).rejects.toThrow("binding mismatch");
    await terminal(f.directory, { stdoutTruncated: true });
    await expect(readLegacyAdapterEventBatch(f)).rejects.toThrow("retention lost events");
  });

  it("rejects malformed frames, invalid boundaries, oversized events and incomplete terminal output", async () => {
    const f = await fixture();
    await fs.writeFile(path.join(f.directory, "stdout"), '{"kind":"spawn","meta":{"pid":4,"processGroupId":5,"startedAt":"2026-10-05"}}\n');
    await expect(readLegacyAdapterEventBatch(f)).rejects.toThrow("spawn identity");
    await expect(readLegacyAdapterEventBatch({ ...f, cursor: 3 })).rejects.toThrow("frame boundary");
    await fs.writeFile(path.join(f.directory, "stdout"), Buffer.alloc(1024 * 1024, 120));
    await expect(readLegacyAdapterEventBatch(f)).rejects.toThrow("frame limit");
    await fs.writeFile(path.join(f.directory, "stdout"), '{"kind":"dispatch"}');
    await terminal(f.directory);
    await expect(readLegacyAdapterEventBatch(f)).rejects.toThrow("incomplete event");
    await fs.writeFile(path.join(f.directory, "stdout"), "");
    await fs.unlink(path.join(f.directory, "adapter-result.json"));
    await expect(readLegacyAdapterEventBatch(f)).rejects.toThrow("without a bound terminal result");
  });
});
