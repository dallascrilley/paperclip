import { expect, it, vi } from "vitest";
vi.mock("node:child_process", async importOriginal => ({
  ...await importOriginal<typeof import("node:child_process")>(),
  spawn: vi.fn(() => { throw new Error("fixture_spawn_forbidden"); }),
}));
import { spawn } from "node:child_process";
import { runChildProcess } from "./server-utils.js";

it("legacy owning launch refuses unavailable evidence before any native spawn", async () => {
  const beforeDispatch = vi.fn(async () => { throw new Error("native_registration_unavailable"); });
  await expect(runChildProcess("original-run", "/fixture/no-worker", [], {
    cwd: process.cwd(), env: {}, timeoutSec: 1, graceSec: 1, onLog: async () => {},
    nativeEvidence: { route: "legacy_local", beforeDispatch },
  })).rejects.toThrow("native_registration_unavailable");
  expect(beforeDispatch).toHaveBeenCalledWith("legacy_local", "original-run");
  expect(spawn).not.toHaveBeenCalled();
});
it("legacy owning launch refuses a remote target before native evidence or spawn", async () => {
  const beforeDispatch = vi.fn(async () => {});
  await expect(runChildProcess("original-run", "/fixture/no-worker", [], {
    cwd: process.cwd(), env: {}, timeoutSec: 1, graceSec: 1, onLog: async () => {},
    nativeEvidence: { route: "legacy_local", beforeDispatch },
    remoteExecution: {} as never,
  })).rejects.toThrow("worker_native_evidence_remote_route_refused");
  expect(beforeDispatch).not.toHaveBeenCalled();
  expect(spawn).not.toHaveBeenCalled();
});

it("legacy owning route refuses structurally recovered intent before observer or spawn", async () => {
  const beforeDispatch = vi.fn(async () => {});
  await expect(runChildProcess("original-run", "/fixture/no-worker", [], {
    cwd: process.cwd(), env: {}, timeoutSec: 1, graceSec: 1, onLog: async () => {},
    nativeEvidence: { route: "legacy_local", beforeDispatch }, restartRecovery: true,
  })).rejects.toThrow("worker_native_evidence_recovered_intent_cannot_launch");
  expect(beforeDispatch).not.toHaveBeenCalled();
  expect(spawn).not.toHaveBeenCalled();
});
