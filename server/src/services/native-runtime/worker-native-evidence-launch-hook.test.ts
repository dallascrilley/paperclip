import { expect, it, vi } from "vitest";
vi.mock("node:child_process", async importOriginal => ({
  ...await importOriginal<typeof import("node:child_process")>(),
  spawn: vi.fn(() => { throw new Error("fixture_spawn_forbidden"); }),
}));
import { spawn } from "node:child_process";
import { executePaperclipNativeSession } from "./native-session-executor.js";

it("native owning route refuses unavailable evidence before backend or runner spawn", async () => {
  const beforeDispatch = vi.fn(async () => { throw new Error("native_registration_unavailable"); });
  const input = {
    execution: { binding: { runId: "original-run" } },
    nativeEvidence: { route: "native_local", beforeDispatch },
  } as unknown as Parameters<typeof executePaperclipNativeSession>[0];
  await expect(executePaperclipNativeSession(input)).rejects.toThrow("native_registration_unavailable");
  expect(beforeDispatch).toHaveBeenCalledWith("native_local", "original-run");
  expect(spawn).not.toHaveBeenCalled();
});
it("native owning route cannot launch recovered intent even with a successful callback", async () => {
  const beforeDispatch = vi.fn(async () => {});
  const input = {
    execution: { binding: { runId: "original-run" } }, restartRecovery: {},
    nativeEvidence: { route: "native_local", beforeDispatch },
  } as unknown as Parameters<typeof executePaperclipNativeSession>[0];
  await expect(executePaperclipNativeSession(input)).rejects.toThrow("worker_native_evidence_recovered_intent_cannot_launch");
  expect(beforeDispatch).not.toHaveBeenCalled();
  expect(spawn).not.toHaveBeenCalled();
});
