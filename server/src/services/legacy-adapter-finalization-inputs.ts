import { z } from "zod";
import { managedAiCleanupDescriptorSchema } from "./managed-ai-cleanup-descriptor.js";

const nullableString = z.string().nullable();
const strings = z.array(z.string());
const sessionParams = z.object({
  sessionId: z.string(), cwd: z.string().optional(), workspaceId: z.string().optional(),
  repoUrl: z.string().optional(), repoRef: z.string().optional(),
}).strict().nullable();

/** Host-only, credential-free facts required by common session/usage finalization.
 * Fingerprint canonicalJson, adapter config, invocation context and runtime
 * credentials deliberately have no field here. Never accept this from a worker.
 */
export const legacyAdapterFinalizationInputsSchema = z.object({
  version: z.literal(1),
  taskKey: nullableString,
  issueId: nullableString,
  configuredModel: nullableString,
  scratch: z.object({ dir: z.string(), metadata: z.object({ version: z.literal(1),
    companyId: z.string().uuid(), agentId: z.string().uuid(), runId: z.string().uuid(),
    issueId: nullableString, issueIdentifier: nullableString, createdAt: z.string().datetime(),
  }).strict() }).strict().nullable(),
  managedAiCleanup: managedAiCleanupDescriptorSchema.nullable(),
  workspace: z.object({ executionWorkspaceId: z.string().uuid().nullable(),
    cwd: z.string(), worktreePath: nullableString, branchName: nullableString }).strict(),
  previousSessionParams: sessionParams,
  previousSessionId: nullableString,
  previousSessionDisplayId: nullableString,
  taskSessionReused: z.boolean(),
  sessionRotation: z.object({ rotate: z.boolean(), reason: nullableString }).strict(),
  sessionConfigMetadata: z.object({
    aiCredentialIdentity: z.string().optional(), version: z.number().int(),
    fingerprint: z.string(), categories: strings, categoryFingerprints: z.record(z.string(), z.string()),
  }).strict(),
  configFreshness: z.object({
    version: z.number().int(),
    session: z.object({
      fingerprintVersion: z.number().int(), categories: strings, reset: z.boolean(), resetReasons: strings,
      changedCategories: strings, taskSessionAvailable: z.boolean(), taskSessionReused: z.boolean(),
      storedFingerprintPresent: z.boolean(), nextFingerprint: nullableString,
    }).strict(),
    workspace: z.object({
      fingerprintVersion: z.number().int(), categories: strings, action: z.string(), changedCategories: strings,
      reasons: strings, reuseRequested: z.boolean(), workspaceReused: z.boolean(), configSnapshotRefreshed: z.boolean(),
      storedFingerprintPresent: z.boolean(), storedFingerprint: nullableString, inferredFingerprint: nullableString,
      nextFingerprint: nullableString, previousWorkspaceId: nullableString, activeWorkspaceId: nullableString,
    }).strict(),
  }).strict(),
}).strict();
export type LegacyAdapterFinalizationInputs = z.infer<typeof legacyAdapterFinalizationInputsSchema>;

export function prepareLegacyAdapterFinalizationInputs(input: LegacyAdapterFinalizationInputs) {
  const parsed = legacyAdapterFinalizationInputsSchema.parse(input);
  if (Buffer.byteLength(JSON.stringify(parsed)) > 256 * 1024) throw new Error("Legacy finalization preparation exceeds limit");
  return parsed;
}
