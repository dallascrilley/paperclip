import { describe, expect, it } from "vitest";
import { WorkerNativeEvidenceProducer, createWorkerNativeProducerHook, workerReservationPolicy, type WorkerRecord, type WorkerPeer, type WorkerNativePrimitives, type WorkerProof, type WorkerRoute } from "./worker-native-evidence.js";

// Synthetic owning component. No process, native worker, socket or ledger.
function fixture(route: WorkerRoute = "legacy_local") {
  let time = 10;
  const kernel = { boot: "kernel-boot", pid: 100, birth: "123:456", pgid: 100 };
  const record: WorkerRecord = {
    version: 1, route,
    original: { companyId: "company", agentId: "agent", taskId: "task", runId: "run", controllerLeaseId: "lease" },
    source: { path: "/supported/owner", sha256: "a".repeat(64) },
    reservation: { owner: "owner", kind: "worker", request_id: "request", target: "target", budget_gib: 4, growth_gib: 1, reserve_gib: 2 },
    token: "exact-common-token", controller: kernel, launcher: kernel,
    workload: { boot: kernel.boot, identity: "original-group", processes: [{ pid: 100, birth: "123:456" }, { pid: 101, birth: "124:456" }] },
    preparedAt: 0, deadline: 10_000,
  };
  const peer: WorkerPeer = { uid: 501, gid: 20, process: kernel };
  const proof: WorkerProof = { status: "running", authoritative: true, boot: kernel.boot, target: "target", launcher: kernel, workload: record.workload! };
  const primitives: WorkerNativePrimitives = {
    registration: async r => JSON.stringify(r) === JSON.stringify(record),
    kernel: async k => k.boot === kernel.boot && [100, 101].includes(k.pid) && ["123:456", "124:456"].includes(k.birth),
    succession: async (r, s, lease) => r.original.taskId === "task" && s.birth === kernel.birth && lease === "successor-lease",
    lifetime: async () => structuredClone(proof),
    completion: async () => ({ ...structuredClone(proof), status: "completed", no_survivors: true, launch_settled: true, receipt_id: "durable-original-completion" }),
    fenceLaunch: async () => ({ durable: true, receipt_id: "durable-native-queue-fence", native_launch_settled: true }),
  };
  const build = (p: WorkerNativePrimitives | null = primitives) => new WorkerNativeEvidenceProducer(record, peer, p, () => time);
  return { record, peer, proof, primitives, build, setTime: (v: number) => { time = v; } };
}

describe.each(["legacy_local", "native_local"] as const)("%s native producer", route => {
  it("returns unavailable by default and retains claims", async () => {
    const f = fixture(route);
    expect(await f.build(null).authorize(f.peer, f.record.reservation, f.record.token)).toMatchObject({ status: "unavailable", retain_claim: true });
  });
  it.each(["uid", "gid", "birth", "boot", "pid", "pgid"])("refuses caller %s spoof", async field => {
    const f = fixture(route), peer = structuredClone(f.peer);
    if (field === "uid" || field === "gid") peer[field]++;
    else Object.assign(peer.process, { [field]: typeof peer.process[field as keyof typeof peer.process] === "number" ? 999 : "spoof" });
    expect(await f.build().authorize(peer, f.record.reservation, f.record.token)).toMatchObject({ status: "unavailable", reason: "caller_mismatch" });
  });
  it.each(["owner", "kind", "request_id", "target", "budget_gib", "growth_gib", "reserve_gib"])("refuses changed %s", async field => {
    const f = fixture(route), payload = { ...f.record.reservation, [field]: "spoof" };
    expect(await f.build().authorize(f.peer, payload as typeof f.record.reservation, f.record.token)).toMatchObject({ status: "unavailable", reason: "payload_mismatch" });
  });
  it("refuses token spoof, added fields and supplied source provenance", async () => {
    const f = fixture(route), p = f.build();
    expect((await p.authorize(f.peer, f.record.reservation, "wrong")).status).toBe("unavailable");
    expect((await p.authorize(f.peer, { ...f.record.reservation, extra: true } as typeof f.record.reservation, f.record.token)).status).toBe("unavailable");
    f.primitives.registration = async () => false;
    expect((await p.authorize(f.peer, f.record.reservation, f.record.token)).status).toBe("unavailable");
  });
  it("refuses kernel birth reuse independently of matching registration", async () => {
    const f = fixture(route); f.primitives.kernel = async () => false;
    expect(await f.build().authorize(f.peer, f.record.reservation, f.record.token)).toMatchObject({ reason: "kernel_birth_unverified" });
  });
  it("preserves original task/group through verified succession and exec descendants", async () => {
    const f = fixture(route), p = f.build();
    expect(await p.adopt(f.peer, f.record.reservation, f.record.token!, f.record.controller, "successor-lease")).toMatchObject({ status: "available", value: { workload: f.record.workload } });
    expect((await p.adopt(f.peer, f.record.reservation, f.record.token!, f.record.controller, "other-task-lease")).status).toBe("unavailable");
    f.proof.workload = { ...f.proof.workload, processes: [{ pid: 100, birth: "123:456" }] };
    expect((await p.adopt(f.peer, f.record.reservation, f.record.token!, f.record.controller, "successor-lease")).status).toBe("unavailable");
  });
  it("retains claims for unknown or surviving workload and unsettled launch", async () => {
    const f = fixture(route), p = f.build();
    f.primitives.completion = async () => null;
    expect((await p.complete(f.peer, f.record.reservation, f.record.token!)).status).toBe("unavailable");
    f.primitives.completion = async () => ({ ...f.proof, status: "completed", receipt_id: "terminal-row" });
    expect((await p.complete(f.peer, f.record.reservation, f.record.token!)).status).toBe("unavailable");
    f.primitives.completion = async () => ({ ...f.proof, status: "completed", no_survivors: true, receipt_id: "pid-absence" });
    expect((await p.complete(f.peer, f.record.reservation, f.record.token!)).status).toBe("unavailable");
  });
  it("returns independently verified durable completion", async () => {
    const f = fixture(route);
    expect((await f.build().complete(f.peer, f.record.reservation, f.record.token!)).status).toBe("available");
  });
  it("requires a separate native queue fence after authority fence_absent", async () => {
    const f = fixture(route), p = f.build();
    f.primitives.fenceLaunch = async () => null;
    expect(await p.settleAbsent(f.peer, f.record.reservation, f.record.token)).toMatchObject({ reason: "original_native_queue_unsettled", retain_claim: true });
    f.primitives.fenceLaunch = async () => ({ durable: true, receipt_id: "queue-fence", native_launch_settled: true });
    expect((await p.settleAbsent(f.peer, f.record.reservation, f.record.token)).status).toBe("available");
  });
  it("never extends the original preparation deadline or launches recovered intent", async () => {
    const f = fixture(route), p = f.build();
    const h = createWorkerNativeProducerHook(route, p, f.peer, f.record.reservation, f.record.token);
    await expect(h.beforeDispatch(route, "run")).resolves.toBeUndefined();
    await expect(h.beforeDispatch(route, "other-run")).rejects.toThrow("route_mismatch");
    f.setTime(10_001);
    await expect(h.beforeDispatch(route, "run")).rejects.toThrow("registration_unverified");
    const recovered = createWorkerNativeProducerHook(route, p, f.peer, f.record.reservation, f.record.token, true);
    await expect(recovered.beforeDispatch(route, "run")).rejects.toThrow("recovered_intent_cannot_launch");
  });
  it("refuses late callbacks and 180-second preparation", async () => {
    const f = fixture(route);
    f.primitives.registration = async () => { f.setTime(1010); return true; };
    expect((await f.build().authorize(f.peer, f.record.reservation, f.record.token, true)).status).toBe("unavailable");
    f.record.deadline = 180_000;
    expect(await f.build().authorize(f.peer, f.record.reservation, f.record.token)).toMatchObject({ reason: "invalid_protected_registration" });
  });
});
it("merges stronger policy floors once, default debit seven and zero credit", () => {
  expect(workerReservationPolicy(4)).toEqual({ budget_gib: 4, growth_gib: 1, reserve_gib: 2, reflected_credit_gib: 0 });
  expect(workerReservationPolicy(8, { budget: 6, growth: 2, reserve: 3 })).toEqual({ budget_gib: 8, growth_gib: 2, reserve_gib: 3, reflected_credit_gib: 0 });
});
