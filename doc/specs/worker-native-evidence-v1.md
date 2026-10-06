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
fields and the original token must match. Component registration must verify
company/agent/task/run/lease and source independently.

`adopt(peer, request, token, successor, leaseId)` requires independent kernel
and native succession proof. It retains the original launcher/workload and
rechecks every workload birth. Native lifetime verification must enumerate all
exec, reparented and warm descendants. A shorter or different original group
refuses. The API never launches work from recovery.

`complete(peer, request, token)` requires the original boot/target/launcher and
workload, independent completion, no survivors, original launch settlement,
and a durable receipt. `settleAbsent(peer, request, token)` requires a separate
durable original native queue fence. Authority `fence_absent` with
`native_launch_settled=false` cannot satisfy this contract. No API releases a
capacity claim, creates a retry, opens a ledger or executes a native command.

Each primitive must bound its own I/O to the supplied timeout, at most one
second. Late replies and exceptions return unavailable and `retain_claim=true`.
Preparation checks use the remaining original monotonic deadline. The record
rejects any interval above 10 seconds, including 180-second intervals.
Policy maps B=max(4,requiredGiB,stronger B), G=max(1,stronger G), and
R=max(2,stronger R). Default B+G+R is 7 GiB. Reflected credit is zero.
The authority merges reserve once; the producer does not debit it again.

## Actual supported and missing primitives

The source implements typed producer APIs, exact comparison/refusal, route
hooks, immutable original snapshots, deadline/policy checks and synthetic
verifier fixtures. It does not establish successful production native evidence.
Default primitives are null and return unavailable.

Existing records support PID/PGID/wall time, controller generation and lease,
authenticated PRP commands and suspended-session maintenance. These do not
prove the required native facts. Existing random controller boot UUIDs and
`ps lstart` or `/proc` directory ctime cannot supply exact kernel boot/birth.
Rust `process_supervisor.rs` documents the separate inherited lifetime fence
needed for escaped/reparented descendants.

The following production primitives remain missing for both routes:

- Protected original registration and independently verified caller/source.
- Exact kernel boot/PID birth/PGID and supported controller succession.
- Exhaustive inherited workload lifetime across exec/warm descendants.
- Independently verified no-survivors completion.
- Durable original native launch-queue fence and settlement.
- Protected durable owning record writer and authenticated provider transport.

DAL-76 must not register a concrete provider from these synthetic callbacks.
It retains authority adapter/service ownership. DAL-61 retains producer-native
implementation and integration ownership. Exact combined-source approval and
fresh admission remain prerequisites for production or heavy proof.
