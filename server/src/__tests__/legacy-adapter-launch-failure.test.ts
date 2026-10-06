import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { AdapterExecutionContext } from "@paperclipai/adapter-utils";
import type { Db } from "@paperclipai/db";
import { preparedFinalizationFixture } from "./legacy-adapter-finalization-inputs.fixture.js";
const mocks = vi.hoisted(() => ({ reserve: vi.fn(), start: vi.fn() }));
vi.mock("../services/legacy-adapter-custody.js", () => ({ reserveLegacyAdapterCustody: mocks.reserve, commitLegacyAdapterEvents: vi.fn() }));
vi.mock("../services/legacy-adapter-supervisor.js", () => ({ startLegacyAdapterWorker: mocks.start, cancelLegacyAdapterWorker: vi.fn() }));
import { executeSupervisedLegacyAdapter, LegacyAdapterControllerDetachedError, LegacyAdapterPrelaunchError } from "../services/legacy-adapter-execution.js";

describe("supervised launch uncertainty", () => {
  let root: string;
  beforeEach(async () => {
    vi.resetAllMocks();
    root = await fs.mkdtemp(path.join(process.env.PAPERCLIP_RUN_SCRATCH_DIR ?? os.tmpdir(), "launch-uncertainty-"));
    mocks.reserve.mockResolvedValue({ custodyId: "custody", eventCursor: 0 });
  });
  afterEach(async () => { await fs.rm(root, { recursive: true, force: true }); });
  function input() {
    return { db: {} as Db, instanceRoot: root,
      context: { runId: "run", agent: { id: "agent", companyId: "company", adapterType: "codex_local" }, onLog: vi.fn() } as unknown as AdapterExecutionContext,
      secretValues: [], preparedFinalization: preparedFinalizationFixture(), assertOwned: vi.fn(async () => {}),
      cancellationRequested: async () => false, project: () => ({ eventType: "fixture" }) };
  }
  it("certifies a failure before reservation without launching", async () => {
    const args = input();
    args.assertOwned.mockRejectedValue(new Error("lost before reservation"));
    await expect(executeSupervisedLegacyAdapter(args)).rejects.toBeInstanceOf(LegacyAdapterPrelaunchError);
    expect(mocks.reserve).not.toHaveBeenCalled();
    expect(mocks.start).not.toHaveBeenCalled();
  });
  it("retains uncertain reservation settlement", async () => {
    mocks.reserve.mockImplementation(async () => {
      await fs.writeFile(path.join(root, "committed-reservation"), "retained");
      throw new Error("reservation acknowledgement lost");
    });
    await expect(executeSupervisedLegacyAdapter(input())).rejects.toBeInstanceOf(LegacyAdapterControllerDetachedError);
    expect(await fs.readFile(path.join(root, "committed-reservation"), "utf8")).toBe("retained");
    expect(mocks.reserve).toHaveBeenCalledTimes(1);
    expect(mocks.start).not.toHaveBeenCalled();
  });
  it("detaches on ownership loss after reservation without launching", async () => {
    const args = input();
    args.assertOwned.mockResolvedValueOnce(undefined).mockRejectedValueOnce(new Error("replacement owns row"));
    await expect(executeSupervisedLegacyAdapter(args)).rejects.toBeInstanceOf(LegacyAdapterControllerDetachedError);
    expect(mocks.reserve).toHaveBeenCalledTimes(1);
    expect(mocks.start).not.toHaveBeenCalled();
    expect(await fs.stat(path.join(root, "data", "legacy-adapter-workers", "company", "custody"))).toBeTruthy();
  });
  it("preserves launch effects when worker acknowledgement fails, without retry", async () => {
    mocks.start.mockImplementation(async ({ root: spool }) => {
      await fs.writeFile(path.join(spool, "worker-started"), "one executor");
      throw new Error("worker acknowledgement lost");
    });
    await expect(executeSupervisedLegacyAdapter(input())).rejects.toBeInstanceOf(LegacyAdapterControllerDetachedError);
    expect(mocks.start).toHaveBeenCalledTimes(1);
    expect(await fs.readFile(path.join(root, "data", "legacy-adapter-workers", "company", "custody", "worker-started"), "utf8")).toBe("one executor");
  });
});
