# Worker native evidence producer v1

Date: 2026-10-06. Status: source-only partial candidate; native review required.
Owner: Delivery Engineer, DAL-80 / DAL-61. Authority owner: DAL-76.
Contract: System Diagnostics PR163 commit `077fbeee5c4187996ecc8e1a765f3fedc4943899`, `docs/specs/host-capacity-native-interface-v1.md`.

## Source APIs and route bindings

`packages/adapter-utils/src/worker-native-evidence.ts` exports the version-1
`WorkerRecord`, `WorkerNativePrimitives`, `WorkerNativeEvidenceProducer`,
`createWorkerNativeProducerHook`, and `workerReservationPolicy` contracts.
The component record contains the original company, agent, task, run and
controller lease, pinned source, seven reservation fields, exact common token,
kernel controller/launcher identity, original workload and preparation deadline.
The constructor copies protected records and the allowed server/broker peer.
No environment variable, command argument, run-ID socket or private JSON file
can configure production primitives through this API.

| Owning route | Source hook | Behavior |
| --- | --- | --- |
| Legacy local adapter | `runChildProcess` in `packages/adapter-utils/src/server-utils.ts`, `opts.nativeEvidence` | Verify the original route/run before target resolution or spawn. Refuse remote execution with this local hook. |
| Native local session/runner | `executePaperclipNativeSession` in `server/src/services/native-runtime/native-session-executor.ts`, `input.nativeEvidence` | Verify before backend preparation, and again at `onSessionAdmission`, before new session or retained-session attachment. Refuse recovered launch and remote routes. |

These optional hooks have no production caller or activation switch in this
candidate. Their absence preserves existing unreserved pre-activation work.
DAL-61 retains the integration and installed acceptance stages. No changes to
its inherited dirty admission implementation belong to this candidate.

## Independent verification boundary

`authorize(peer, request, token, preparation)` returns the exact seven-field
payload only after independent registration and kernel verification. The peer
must match allowed uid/gid plus kernel boot, PID birth and PGID. All payload
fields and the non-empty original common token must match. Null tokens refuse.
The hook reads peer, payload, token and recovery state from a protected observer
at each dispatch boundary. Both owning routes also refuse structural recovery. Component registration must verify
company/agent/task/run/lease and source independently.

`adopt(peer, request, token, successor, leaseId)` requires independent kernel
and native succession proof. It retains the original launcher/workload and
rechecks every workload birth and each process PGID. Native lifetime verification must enumerate all
exec, reparented and warm descendants. The original process set must remain a subset. Each added process needs an
ancestry link from the owning lifetime verifier to an original process. Kernel
verification checks each added birth and PGID. Missing original processes,
unbound additions and predecessor identities refuse. The API never launches work from recovery.

`complete(peer, request, token)` requires the original boot/target/launcher and
workload, independent completion, no survivors, original launch settlement,
and a durable receipt. `settleAbsent(peer, request, token)` requires a separate
durable original native queue fence. Authority `fence_absent` with
`native_launch_settled=false` cannot satisfy this contract. No API releases a
capacity claim, creates a retry, opens a ledger or executes a native command.

The producer races each primitive and observer against a timer, at most one
second. Each primitive must also bound its own I/O. A timeout, late reply or
exception returns unavailable and `retain_claim=true`. The timer does not prove
native launch settlement or completion.
Preparation checks use the remaining original monotonic deadline. The record
rejects any interval above 10 seconds, including 180-second intervals.
Preparation requires `preparedAt <= now < deadline`. Later inspection and
completion use bounded calls without reopening the preparation window.
Policy maps B=max(4,requiredGiB,stronger B), G=max(1,stronger G), and
R=max(2,stronger R). Default B+G+R is 7 GiB. Reflected credit is zero.
The authority merges reserve once; the producer does not debit it again.

## Actual supported and missing primitives

The source implements typed producer APIs, exact comparison/refusal, route
hooks, immutable original snapshots, deadline/policy checks and synthetic
verifier fixtures. It does not establish successful production native evidence.
Default primitives are null and return unavailable.

`worker-native-kernel.ts` supplies an explicit kernel-only factory. It reuses
SystemDiagnostics `DarwinKernel.process` from commit
`feaf477b74d942ee28aa32fa6aad6313409b0ffe`. Protected configuration supplies
an absolute Python executable and library directory plus pinned SHA256 values.
The probe verifies and executes the exact module bytes with isolated Python.
The process deadline and subprocess kill timeout are at most one second.
The seam reads the kernel boot UUID and exact process birth and PGID. A second
kernel read checks identity stability. No default path or production caller exists.

Kernel module SHA256:
`b022da978f73df744b262eac25b1020627a6f8fa0fa377df7a7a8db2e7d9bd85`.
Authority module SHA256:
`d27acb61f8b796b918bcd3674a3b790110eb50997347dbafd97658a4ff9870f6`.

| Primitive | Actual support in this candidate |
| --- | --- |
| `registration` | Missing. Kernel-only factory returns false. |
| `kernel` | Supported through the pinned Darwin seam with explicit protected configuration. Own-PID tests cover boot, birth, PGID and timeout. |
| `succession` | Missing. Factory returns false. |
| `lifetime` | Missing. Factory returns null. Synthetic ancestry tests do not prove exhaustive native lifetime. |
| `completion` | Missing. Factory returns null. Kernel absence cannot prove no survivors. |
| `fenceLaunch` | Missing. Factory returns null. No durable native queue settlement exists. |

Protected durable registration and authenticated provider transport remain
missing for both routes. The kernel probe proves no logical ownership,
controller succession, exhaustive lifetime, completion or launch settlement.
The producer default remains null. Reserve-bearing dispatch requires the
non-null durable common token, consistent with PR163 intent/adopt/release.

DAL-76 must not register a concrete provider from these synthetic callbacks.
It retains authority adapter/service ownership. DAL-61 retains producer-native
implementation and integration ownership. Exact combined-source approval and
fresh admission remain prerequisites for production or heavy proof.
