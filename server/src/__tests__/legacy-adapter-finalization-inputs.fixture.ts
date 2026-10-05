import type { LegacyAdapterFinalizationInputs } from "../services/legacy-adapter-finalization-inputs.js";
export function preparedFinalizationFixture(): LegacyAdapterFinalizationInputs {
  return {
    version: 1, taskKey: "fixture-task", issueId: null, configuredModel: "fixture-model",
    managedAiCleanup: null,
    workspace: { executionWorkspaceId: null, cwd: "/fixture", worktreePath: null, branchName: null },
    previousSessionParams: { sessionId: "fixture-session", cwd: "/fixture" },
    previousSessionId: "fixture-session", previousSessionDisplayId: "fixture-session", taskSessionReused: true,
    sessionRotation: { rotate: false, reason: null },
    sessionConfigMetadata: { version: 1, fingerprint: "session-hash", categories: ["model"], categoryFingerprints: { model: "model-hash" } },
    configFreshness: { version: 1,
      session: { fingerprintVersion: 1, categories: ["model"], reset: false, resetReasons: [], changedCategories: [],
        taskSessionAvailable: true, taskSessionReused: true, storedFingerprintPresent: true, nextFingerprint: "session-hash" },
      workspace: { fingerprintVersion: 1, categories: [], action: "reuse", changedCategories: [], reasons: [], reuseRequested: true,
        workspaceReused: true, configSnapshotRefreshed: false, storedFingerprintPresent: true, storedFingerprint: "workspace-hash",
        inferredFingerprint: null, nextFingerprint: "workspace-hash", previousWorkspaceId: null, activeWorkspaceId: null },
    },
  };
}
