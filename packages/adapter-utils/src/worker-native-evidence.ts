/** DAL-80 source-only producer contract. No authority ledger or launch executor. */
export type WorkerRoute = "legacy_local" | "native_local";
export interface KernelIdentity { boot: string; pid: number; birth: string; pgid: number }
export interface WorkerOriginal {
  companyId: string; agentId: string; taskId: string; runId: string;
  controllerLeaseId: string;
}
export interface WorkerReservation {
  owner: string; kind: "worker"; request_id: string; target: string;
  budget_gib: number; growth_gib: number; reserve_gib: number;
}
export interface WorkerRecord {
  version: 1; route: WorkerRoute; original: WorkerOriginal;
  source: { path: string; sha256: string };
  reservation: WorkerReservation; token: string | null;
  controller: KernelIdentity; launcher: KernelIdentity | null;
  workload: { boot: string; identity: string; processes: Array<{ pid: number; birth: string; pgid: number }> } | null;
  /** Original monotonic deadline. Persisted values require same-kernel verification. */
  preparedAt: number; deadline: number;
}
export interface WorkerPeer { uid: number; gid: number; process: KernelIdentity }
export type EvidenceResult<T> = { status: "available"; value: T } | {
  status: "unavailable"; reason: string; retain_claim: true;
};
export interface WorkerProof {
  status: "running" | "completed"; authoritative: true;
  boot: string; target: string; launcher: KernelIdentity;
  workload: NonNullable<WorkerRecord["workload"]>;
  /** Owning lifetime verifier binds each added process to an original ancestor. */
  descendants?: Array<{ pid: number; birth: string; ancestorPid: number; ancestorBirth: string }>;
  no_survivors?: true; launch_settled?: true; receipt_id?: string;
}
/** Implementations must verify through owning native records, never caller JSON.
 * Each callback must bound its own I/O by timeoutMs; late replies refuse. */
export interface WorkerNativePrimitives {
  registration(record: WorkerRecord, peer: WorkerPeer, timeoutMs: number): Promise<boolean>;
  kernel(identity: KernelIdentity, timeoutMs: number): Promise<boolean>;
  succession(record: WorkerRecord, successor: KernelIdentity, leaseId: string, timeoutMs: number): Promise<boolean>;
  lifetime(record: WorkerRecord, timeoutMs: number): Promise<WorkerProof | null>;
  completion(record: WorkerRecord, timeoutMs: number): Promise<WorkerProof | null>;
  fenceLaunch(record: WorkerRecord, timeoutMs: number): Promise<{ durable: true; receipt_id: string; native_launch_settled: true } | null>;
}
const keys = ["owner", "kind", "request_id", "target", "budget_gib", "growth_gib", "reserve_gib"] as const;
const unavailable = (reason: string) => ({ status: "unavailable" as const, reason, retain_claim: true as const });
const sameProcess = (a: { pid: number; birth: string; pgid: number }, b: { pid: number; birth: string; pgid: number }) =>
  a.pid === b.pid && a.birth === b.birth && a.pgid === b.pgid;
const sameKernel = (a: KernelIdentity, b: KernelIdentity) =>
  a.boot === b.boot && a.pid === b.pid && a.birth === b.birth && a.pgid === b.pgid;
const validKernel = (k: KernelIdentity) => Boolean(k.boot && k.birth && Number.isSafeInteger(k.pid) && k.pid > 0 && Number.isSafeInteger(k.pgid) && k.pgid > 0);

/** Server/broker supplies these protected values out of band. They are never
 * read from process args, environment, peer-supplied source hashes, or RPC JSON. */
export class WorkerNativeEvidenceProducer {
  private readonly record: WorkerRecord;
  private readonly allowedPeer: WorkerPeer;
  constructor(
    record: WorkerRecord,
    allowedPeer: WorkerPeer,
    private readonly primitives: WorkerNativePrimitives | null = null,
    private readonly now: () => number = () => performance.now(),
  ) {
    this.record = structuredClone(record);
    this.allowedPeer = structuredClone(allowedPeer);
  }
  private async bounded<T>(fn: (timeout: number) => Promise<T>, preparation = false): Promise<T | null> {
    const timeout = preparation ? Math.min(1000, this.record.deadline - this.now()) : 1000;
    if (timeout <= 0) return null;
    const until = this.now() + timeout;
    let timer: ReturnType<typeof setTimeout> | undefined;
    try {
      const result = await Promise.race([
        Promise.resolve().then(() => fn(timeout)),
        new Promise<null>(resolve => { timer = setTimeout(() => resolve(null), timeout); }),
      ]);
      return this.now() < until ? result : null;
    } catch { return null; }
    finally { if (timer !== undefined) clearTimeout(timer); }
  }
  private validRecord(preparation: boolean): boolean {
    const r = this.record;
    return r.version === 1 && ["legacy_local", "native_local"].includes(r.route)
      && Object.values(r.original).every(v => typeof v === "string" && v.length > 0)
      && Object.keys(r.original).length === 5
      && ["companyId", "agentId", "taskId", "runId", "controllerLeaseId"].every(k => k in r.original)
      && r.source.path.startsWith("/") && /^[a-f0-9]{64}$/.test(r.source.sha256)
      && validKernel(r.controller) && (!r.launcher || validKernel(r.launcher))
      && r.reservation.kind === "worker" && keys.every(k => r.reservation[k] !== undefined)
      && Object.keys(r.reservation).length === keys.length
      && [r.reservation.owner, r.reservation.request_id, r.reservation.target].every(v => typeof v === "string" && v.length > 0)
      && [r.reservation.budget_gib, r.reservation.growth_gib, r.reservation.reserve_gib].every(Number.isFinite)
      && r.reservation.budget_gib >= 4 && r.reservation.growth_gib >= 1 && r.reservation.reserve_gib >= 2
      && Number.isFinite(r.preparedAt) && Number.isFinite(r.deadline)
      && r.deadline > r.preparedAt && r.deadline - r.preparedAt <= 10_000
      && r.preparedAt <= this.now() && (!preparation || this.now() < r.deadline);
  }
  async observeDispatch<T>(observer: () => Promise<T>): Promise<T | null> {
    if (!this.validRecord(true)) return null;
    return this.bounded(observer, true);
  }
  matchesDispatch(route: WorkerRoute, runId: string): boolean {
    return this.record.route === route && this.record.original.runId === runId;
  }
  async authorize(peer: WorkerPeer, request: WorkerReservation, token: string | null, preparation = false): Promise<EvidenceResult<WorkerReservation>> {
    if (!this.validRecord(preparation)) return unavailable("invalid_protected_registration");
    peer = structuredClone(peer);
    request = structuredClone(request);
    if (!sameKernel(peer.process, this.allowedPeer.process) || peer.uid !== this.allowedPeer.uid || peer.gid !== this.allowedPeer.gid)
      return unavailable("caller_mismatch");
    if (Object.keys(request).length !== keys.length || !keys.every(k => request[k] === this.record.reservation[k]))
      return unavailable("payload_mismatch");
    if (typeof token !== "string" || !token.length || token !== this.record.token) return unavailable("token_mismatch");
    if (!this.primitives) return unavailable("native_registration_unavailable");
    if (!await this.bounded(t => this.primitives!.registration(structuredClone(this.record), structuredClone(peer), t), preparation))
      return unavailable("registration_unverified");
    if (!await this.bounded(t => this.primitives!.kernel(structuredClone(peer.process), t), preparation))
      return unavailable("kernel_birth_unverified");
    return { status: "available", value: structuredClone(this.record.reservation) };
  }
  /** Takeover cannot change original task, launcher, workload, payload or token. */
  async adopt(peer: WorkerPeer, request: WorkerReservation, token: string, successor: KernelIdentity, leaseId: string): Promise<EvidenceResult<WorkerProof>> {
    successor = structuredClone(successor);
    const auth = await this.authorize(peer, request, token);
    if (auth.status !== "available") return auth;
    if (!validKernel(successor) || sameKernel(successor, this.record.controller) || !leaseId || !await this.bounded(t => this.primitives!.kernel(structuredClone(successor), t)) || !await this.bounded(t => this.primitives!.succession(structuredClone(this.record), structuredClone(successor), leaseId, t)))
      return unavailable("controller_succession_unverified");
    const proof = await this.bounded(t => this.primitives!.lifetime(structuredClone(this.record), t));
    if (!proof || !this.matches(proof) || proof.status !== "running") return unavailable("workload_lifetime_unverified");
    // Every process birth is independently checked; missing descendants must be
    // refused by the exhaustive native lifetime primitive, not inferred here.
    for (const p of proof.workload.processes) {
      if (!await this.bounded(t => this.primitives!.kernel({ boot: proof.boot, ...p }, t)))
        return unavailable("workload_birth_unverified");
    }
    return { status: "available", value: structuredClone(proof) };
  }
  private matches(p: WorkerProof): boolean {
    const r = this.record;
    return p.authoritative === true && r.launcher !== null && r.workload !== null
      && p.boot === r.launcher.boot && p.target === r.reservation.target
      && sameKernel(p.launcher, r.launcher)
      && p.workload.boot === p.boot && p.workload.identity === r.workload.identity
      && r.workload.processes.length > 0 && p.workload.processes.every(process => validKernel({ boot: p.boot, ...process }))
      && new Set(p.workload.processes.map(process => `${process.pid}:${process.birth}`)).size === p.workload.processes.length
      && r.workload.processes.every(original => p.workload.processes.some(process => sameProcess(process, original)))
      && p.workload.processes.every(process => r.workload!.processes.some(original => sameProcess(process, original))
        || p.descendants?.some(link => link.pid === process.pid && link.birth === process.birth
          && r.workload!.processes.some(original => original.pid === link.ancestorPid && original.birth === link.ancestorBirth)));
  }
  async complete(peer: WorkerPeer, request: WorkerReservation, token: string): Promise<EvidenceResult<WorkerProof>> {
    const auth = await this.authorize(peer, request, token);
    if (auth.status !== "available") return auth;
    const proof = await this.bounded(t => this.primitives!.completion(structuredClone(this.record), t));
    if (!proof || !this.matches(proof) || proof.status !== "completed" || proof.no_survivors !== true
      || proof.launch_settled !== true || !proof.receipt_id) return unavailable("completion_or_launch_settlement_unverified");
    return { status: "available", value: structuredClone(proof) };
  }
  /** Authority fence_absent alone is insufficient. This never launches/retries. */
  async settleAbsent(peer: WorkerPeer, request: WorkerReservation, token: string | null): Promise<EvidenceResult<{ durable: true; receipt_id: string; native_launch_settled: true }>> {
    const auth = await this.authorize(peer, request, token);
    if (auth.status !== "available") return auth;
    const receipt = await this.bounded(t => this.primitives!.fenceLaunch(structuredClone(this.record), t));
    if (!receipt || receipt.durable !== true || receipt.native_launch_settled !== true || !receipt.receipt_id)
      return unavailable("original_native_queue_unsettled");
    return { status: "available", value: structuredClone(receipt) };
  }
}

/** Original protected preparation context, never reconstructed from recovery. */
export function workerReservationPolicy(requiredGiB: number, floors = { budget: 4, growth: 1, reserve: 2 }) {
  if (![requiredGiB, ...Object.values(floors)].every(v => Number.isFinite(v) && v >= 0)) throw new Error("invalid_worker_policy");
  return { budget_gib: Math.max(4, requiredGiB, floors.budget), growth_gib: Math.max(1, floors.growth), reserve_gib: Math.max(2, floors.reserve), reflected_credit_gib: 0 };
}

/** Spawn metadata is only a hint to the owning protected writer. It cannot
 * establish provenance, kernel birth, exhaustive lifetime or completion. */
export interface WorkerNativeProducerHook {
  route: WorkerRoute;
  beforeDispatch(actualRoute: WorkerRoute, runId: string): Promise<void>;
}

export class WorkerNativeEvidenceUnavailable extends Error {
  readonly retain_claim = true;
  constructor(readonly reason: string) { super(`worker_native_evidence_unavailable:${reason}`); }
}
/** Only fresh, original preparation can pass this hook. Recovery receives an
 * unavailable hook and can inspect/adopt/complete through the producer APIs. */
export function createWorkerNativeProducerHook(
  route: WorkerRoute, producer: WorkerNativeEvidenceProducer,
  /** Protected owning transport observer, called fresh at each dispatch boundary.
   * Never implement from args, environment or caller RPC JSON. */
  observeDispatch: () => Promise<{ peer: WorkerPeer; request: WorkerReservation; token: string | null; recovered: boolean }>,
): WorkerNativeProducerHook {
  return {
    route,
    async beforeDispatch(actualRoute, runId) {
      if (actualRoute !== route || !producer.matchesDispatch(actualRoute, runId)) throw new WorkerNativeEvidenceUnavailable("route_mismatch");
      // Bound observation too: a stalled protected observer cannot extend preparation.
      const observed = await producer.observeDispatch(observeDispatch);
      if (!observed) throw new WorkerNativeEvidenceUnavailable("dispatch_observation_unavailable");
      if (observed.recovered) throw new WorkerNativeEvidenceUnavailable("recovered_intent_cannot_launch");
      const result = await producer.authorize(observed.peer, observed.request, observed.token, true);
      if (result.status !== "available") throw new WorkerNativeEvidenceUnavailable(result.reason);
    },
  };
}
