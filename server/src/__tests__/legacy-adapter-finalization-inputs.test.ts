import { describe, expect, it } from "vitest";
import { prepareLegacyAdapterFinalizationInputs } from "../services/legacy-adapter-finalization-inputs.js";
import { preparedFinalizationFixture } from "./legacy-adapter-finalization-inputs.fixture.js";

describe("host preparation for legacy finalization", () => {
  it("makes an independent snapshot of admitted session/usage facts", () => {
    const input = preparedFinalizationFixture();
    const snapshot = prepareLegacyAdapterFinalizationInputs(input);
    input.previousSessionParams!.sessionId = "later-session";
    input.sessionConfigMetadata.categoryFingerprints.model = "later-hash";
    expect(snapshot.previousSessionParams?.sessionId).toBe("fixture-session");
    expect(snapshot.sessionConfigMetadata.categoryFingerprints.model).toBe("model-hash");
  });

  it("rejects invocation secrets, arbitrary session fields, canonical config text and unsupported versions", () => {
    const input = preparedFinalizationFixture();
    for (const invalid of [
      { ...input, authToken: "credential" }, { ...input, config: { command: "executor" } },
      { ...input, previousSessionParams: { ...input.previousSessionParams, accessToken: "credential" } },
      { ...input, sessionConfigMetadata: { ...input.sessionConfigMetadata, fingerprints: { canonicalJson: "private-config" } } },
      { ...input, version: 2 },
    ]) expect(() => prepareLegacyAdapterFinalizationInputs(invalid as typeof input)).toThrow();
  });

  it("rejects oversized preparation before reservation or executor launch", () => {
    const input = preparedFinalizationFixture();
    input.taskKey = "x".repeat(256 * 1024);
    expect(() => prepareLegacyAdapterFinalizationInputs(input)).toThrow("exceeds limit");
  });
});
