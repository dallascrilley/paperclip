import { execFile } from "node:child_process";
import { isAbsolute } from "node:path";
import type { EvidenceResult, KernelIdentity, WorkerNativePrimitives } from "./worker-native-evidence.js";

/** Protected deployment configuration, never worker/RPC-supplied paths or hashes.
 * There is deliberately no default installation or production activation. */
export interface DarwinKernelSource {
  pythonPath: string;
  libraryDirectory: string;
  kernelSha256: string;
  authoritySha256: string;
}

// Reuse the owning SystemDiagnostics implementation rather than copying its
// ctypes layout. Execute the exact verified bytes, avoiding an import/read race.
const probe = String.raw`
import hashlib, json, pathlib, sys, time, types
started = time.monotonic()
root, kernel_hash, authority_hash, pid, seconds = sys.argv[1:]
deadline = started + float(seconds)
for name, expected in [('host_capacity', authority_hash), ('host_capacity_kernel', kernel_hash)]:
    path = pathlib.Path(root) / (name + '.py')
    source = path.read_bytes()
    if hashlib.sha256(source).hexdigest() != expected:
        raise RuntimeError('native kernel source mismatch')
    module = types.ModuleType(name)
    module.__file__ = str(path)
    sys.modules[name] = module
    exec(compile(source, str(path), 'exec'), module.__dict__)
result = sys.modules['host_capacity_kernel'].DarwinKernel().process(int(pid), deadline=deadline)
print(json.dumps({key: result[key] for key in ('boot', 'pid', 'birth', 'pgid')}))
`;

const unavailable = (reason: string): EvidenceResult<KernelIdentity> => ({
  status: "unavailable", reason, retain_claim: true,
});

function validIdentity(value: unknown): value is KernelIdentity {
  if (!value || typeof value !== "object") return false;
  const k = value as KernelIdentity;
  return typeof k.boot === "string"
    && /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/.test(k.boot)
    && k.boot !== "00000000-0000-0000-0000-000000000000"
    && typeof k.birth === "string" && /^[1-9][0-9]*:[0-9]{6}$/.test(k.birth)
    && Number.isSafeInteger(k.pid) && k.pid > 0 && k.pid <= 2147483647
    && Number.isSafeInteger(k.pgid) && k.pgid > 0 && k.pgid <= 2147483647;
}

/** Bounded read-only kernel observation. Disappearance, permission errors,
 * malformed replies and timeout retain claims; none prove no survivors. */
export async function observeDarwinKernelProcess(
  source: DarwinKernelSource, pid: number, timeoutMs: number,
): Promise<EvidenceResult<KernelIdentity>> {
  if (process.platform !== "darwin") return unavailable("darwin_kernel_unavailable");
  if (!Number.isSafeInteger(pid) || pid <= 0 || pid > 2147483647
    || !Number.isFinite(timeoutMs) || timeoutMs <= 0
    || !isAbsolute(source.pythonPath) || !isAbsolute(source.libraryDirectory)
    || !/^[a-f0-9]{64}$/.test(source.kernelSha256)
    || !/^[a-f0-9]{64}$/.test(source.authoritySha256)) return unavailable("invalid_kernel_probe_configuration");
  // Node's process timeout accepts integer milliseconds. Round down so a
  // fractional remaining preparation window is never extended.
  const bound = Math.floor(Math.min(timeoutMs, 1000));
  if (bound <= 0) return unavailable("kernel_observation_deadline_expired");
  const started = performance.now();
  return new Promise(resolve => {
    execFile(source.pythonPath, ["-I", "-c", probe, source.libraryDirectory,
      source.kernelSha256, source.authoritySha256, String(pid), String(bound / 1000)],
    { timeout: bound, killSignal: "SIGKILL", maxBuffer: 4096, encoding: "utf8", env: {} }, (error, stdout) => {
      if (error || performance.now() - started >= bound) return resolve(unavailable("kernel_observation_unavailable"));
      try {
        const value: unknown = JSON.parse(stdout);
        if (!validIdentity(value) || value.pid !== pid) return resolve(unavailable("kernel_observation_malformed"));
        resolve({ status: "available", value });
      } catch { resolve(unavailable("kernel_observation_malformed")); }
    });
  });
}

/** Only kernel is supported. This object never creates a ledger, launches a
 * worker, settles a queue, or asserts ownership/lifetime/completion. */
export function createDarwinKernelPrimitives(source: DarwinKernelSource): WorkerNativePrimitives {
  const protectedSource = structuredClone(source);
  return {
    registration: async () => false,
    async kernel(identity, timeoutMs) {
      if (!validIdentity(identity)) return false;
      const result = await observeDarwinKernelProcess(protectedSource, identity.pid, timeoutMs);
      return result.status === "available" && result.value.boot === identity.boot
        && result.value.birth === identity.birth && result.value.pgid === identity.pgid;
    },
    succession: async () => false,
    lifetime: async () => null,
    completion: async () => null,
    fenceLaunch: async () => null,
  };
}
