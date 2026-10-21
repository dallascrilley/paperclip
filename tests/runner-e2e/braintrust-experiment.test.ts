import { expect, it, vi } from "vitest";
import { buildBraintrustExperiment, importBraintrustExperiment, type BraintrustExperimentReceipt } from "./braintrust-experiment.js";
import { buildRunnerCampaign } from "./history.js";
import { runnerMatrix } from "./catalog.js";
import { emptyFeedbackSummary } from "./feedback.js";
import type { RunnerE2EResult } from "./types.js";

const projectId = "11111111-1111-4111-8111-111111111111";
const experimentId = "22222222-2222-4222-8222-222222222222";
function fixture() {
  const cell = runnerMatrix.find(cell => cell.id === "core-compatibility.runner-codex.local.message-marker")!;
  const first: RunnerE2EResult = { schema: "paperclip.runner-e2e.result/v2", executionId: cell.id,
    suiteId: cell.suite.id, suiteDefinitionHash: cell.suiteDefinitionHash,
    accountingIdentity: { campaignId: "retained-pilot", attemptId: "first" }, source: { sha: "a".repeat(40), ref: null, workflowRunUrl: null },
    attempt: 1, status: "failed", cleanup: "passed", profileId: cell.profile.id, caseId: cell.task.id,
    environmentId: cell.environment.id, provider: cell.profile.provider, model: cell.profile.model, runtimeMode: cell.profile.expectedRuntimeMode,
    startedAt: "2026-10-20T00:00:00.000Z", finishedAt: "2026-10-20T00:00:01.000Z", durationMs: 1000,
    error: "private-canary-error", usage: { privateCanary: "private-canary-usage" }, feedback: emptyFeedbackSummary() };
  const second: RunnerE2EResult = { ...structuredClone(first), source: { ...first.source!, sha: "b".repeat(40) },
    accountingIdentity: { campaignId: "retained-pilot", attemptId: "second" }, attempt: 2, status: "passed", error: undefined,
    startedAt: "2026-10-20T00:01:00.000Z", finishedAt: "2026-10-20T00:01:01.000Z",
    tracing: { projectId, traceId: "c".repeat(32), spanId: "d".repeat(16), status: "accepted", verification: "unverified", nativeConfiguration: "configured" } };
  const originals = [first, second];
  const campaign = buildRunnerCampaign({ campaignId: "retained-pilot", generatedAt: second.finishedAt,
    expected: [cell.id], results: [second], attempts: originals });
  return { campaign, originals };
}

it("imports all attempts with their own source and original grades, excluding private evidence and duplicate cost metrics", () => {
  const { campaign, originals } = fixture();
  const plan = buildBraintrustExperiment(projectId, campaign, originals);
  expect(plan.events).toHaveLength(2);
  expect(plan.events.map(row => row.scores.product_pass).sort()).toEqual([0, 1]);
  expect(plan.events.map(row => row.input.source_revision).sort()).toEqual(["a".repeat(40), "b".repeat(40)]);
  expect(JSON.stringify(plan)).not.toMatch(/private-canary|estimated_cost|prompt_tokens/);
  expect(plan.events.every(row => row.metadata.request_ledger === null)).toBe(true);
  expect(plan.events.find(row => row.metadata.attempt_id === "second")!.metadata.trace).toMatchObject({ root_span_id: "c".repeat(32) });
  expect(buildBraintrustExperiment(projectId, { ...campaign, generatedAt: "2026-10-21T00:00:00Z" }, [...originals].reverse())).toEqual(plan);
  expect(buildBraintrustExperiment(projectId, campaign, [...originals, originals[0]!])).toEqual(plan);
});

it.each(["missing", "foreign", "modified-grade", "ambiguous-source", "missing-source", "foreign-trace", "missing-hash"])("rejects %s evidence before network requests", async (kind) => {
  const { campaign, originals } = fixture();
  if (kind === "missing") originals.pop();
  if (kind === "foreign") originals[0]!.accountingIdentity!.campaignId = "other";
  if (kind === "modified-grade") originals[0]!.status = "passed";
  if (kind === "ambiguous-source") originals.push({ ...originals[0]!, source: { ...originals[0]!.source!, sha: "c".repeat(40) } });
  if (kind === "missing-source") originals[0]!.source = undefined;
  if (kind === "foreign-trace") originals[1]!.tracing!.projectId = experimentId;
  if (kind === "missing-hash") originals[0]!.suiteDefinitionHash = undefined;
  const transport = vi.fn<typeof fetch>();
  await expect(importBraintrustExperiment({ projectId, apiKey: "key" }, campaign, originals, async () => {}, transport)).rejects.toThrow();
  expect(transport).not.toHaveBeenCalled();
});

it("changes experiment identity for changed evidence while keeping unknown accounting unknown", () => {
  const { campaign, originals } = fixture();
  const before = buildBraintrustExperiment(projectId, campaign, originals);
  campaign.accounting!.missingAttempts = 1;
  const after = buildBraintrustExperiment(projectId, campaign, originals);
  expect(after.payloadHash).not.toBe(before.payloadHash);
  expect(after.metadata.request_ledger).toBeNull();
  expect(after.metadata.missing_attempts).toBe(1);
});

it("keeps precise ledger amounts and reservations separate from trace metrics and unobserved attempts", () => {
  const { campaign, originals } = fixture();
  campaign.operations = { version: 1, scope: "shared_gateway_ledger", ledgerCampaignId: campaign.campaignId,
    journalHash: "c".repeat(64), records: 5, deadlineAt: "2026-10-21T01:00:00Z", limitUsd: "100.000000000",
    knownUsd: "0.000000001", heldUsd: "4.000000000", committedUsd: "4.000000001", requests: 1,
    unresolvedRequests: 1, conflictingRequests: 0, inflight: 0,
    breakdown: { estimated: "0.000000001", reported: "0.000000000", reconciled: "0.000000000", allocated: "0.000000000" },
    attempts: [{ executionId: originals[0]!.executionId, attemptId: "first", requests: 1, unresolvedRequests: 1,
      knownUsd: "0.000000001", heldUsd: "4.000000000" }] };
  const plan = buildBraintrustExperiment(projectId, campaign, originals);
  expect(plan.metadata.request_ledger).toMatchObject({ known_usd: "0.000000001", additional_held_usd: "4.000000000" });
  expect(plan.events.find(row => row.metadata.attempt_id === "first")!.metadata.request_ledger).toMatchObject({ unresolved_requests: 1 });
  expect(plan.events.find(row => row.metadata.attempt_id === "second")!.metadata.request_ledger).toBeNull();
  expect(plan.events.every(row => Object.keys(row.metrics).sort().join() === "end,start")).toBe(true);
});

function receiver(campaign: ReturnType<typeof fixture>["campaign"], originals: RunnerE2EResult[]) {
  const plan = buildBraintrustExperiment(projectId, campaign, originals);
  return vi.fn<typeof fetch>(async (url, init) => {
    expect(String(url).startsWith("https://api.braintrust.dev/v1/")).toBe(true);
    expect(init?.redirect).toBe("error");
    const sent = JSON.parse(String(init?.body));
    if (String(url).endsWith("/experiment")) {
      expect(sent).toMatchObject({ public: false, ensure_new: false, project_id: projectId, name: plan.name });
      return Response.json({ ...sent, id: experimentId });
    }
    return Response.json({ row_ids: sent.events.map((row: { id: string }) => row.id) });
  });
}

it("retries identical evidence using the same private experiment and stable row IDs without invoking agents", async () => {
  const { campaign, originals } = fixture();
  const transport = receiver(campaign, originals), receipts: BraintrustExperimentReceipt[] = [];
  for (let i = 0; i < 2; i++) await importBraintrustExperiment({ projectId, apiKey: "private-key" }, campaign, originals,
    async receipt => { receipts.push(receipt); }, transport);
  expect(receipts.map(receipt => receipt.status)).toEqual(["created", "accepted", "created", "accepted"]);
  expect(receipts[1]).toEqual(receipts[3]);
  expect(receipts[1]).toMatchObject({ acceptedRows: 2, expectedRows: 2, verification: "unverified" });
  expect(transport.mock.calls[0]![1]!.body).toBe(transport.mock.calls[2]![1]!.body);
  expect(transport.mock.calls[1]![1]!.body).toBe(transport.mock.calls[3]![1]!.body);
});

it("checkpoints accepted batches before a later insert fails", async () => {
  const base = fixture().originals[0]!;
  const originals = Array.from({ length: 101 }, (_, i) => ({ ...base, attempt: i + 1,
    accountingIdentity: { campaignId: "retained-pilot", attemptId: `attempt-${i}` } }));
  const campaign = buildRunnerCampaign({ campaignId: "retained-pilot", generatedAt: base.finishedAt,
    expected: [base.executionId], results: [originals[100]!], attempts: originals });
  const normal = receiver(campaign, originals), receipts: BraintrustExperimentReceipt[] = [];
  let calls = 0;
  await expect(importBraintrustExperiment({ projectId, apiKey: "key" }, campaign, originals,
    async receipt => { receipts.push(receipt); }, async (url, init) => {
      if (++calls === 3) throw new Error("private-network-error");
      return normal(url, init);
    })).rejects.toThrow("Remote details withheld");
  expect(receipts).toHaveLength(2);
  expect(receipts[1]).toMatchObject({ status: "created", acceptedRows: 100, expectedRows: 101 });
});

it.each(["public", "wrong-project", "wrong-hash", "partial-insert", "receiver-error", "oversized"])("retains safe incomplete status for %s", async kind => {
  const { campaign, originals } = fixture(), receipts: BraintrustExperimentReceipt[] = [];
  const normal = receiver(campaign, originals);
  const transport: typeof fetch = async (url, init) => {
    const response = await normal(url, init), value = await response.json() as Record<string, unknown>;
    if (String(url).endsWith("/experiment")) {
      if (kind === "public") value.public = true;
      if (kind === "wrong-project") value.project_id = experimentId;
      if (kind === "wrong-hash") value.metadata = { payload_hash: "a".repeat(64) };
      if (kind === "oversized") return new Response("x".repeat(1_048_577));
    } else {
      if (kind === "partial-insert") value.row_ids = [];
      if (kind === "receiver-error") return new Response("private-upstream-failure", { status: 503 });
    }
    return Response.json(value);
  };
  await expect(importBraintrustExperiment({ projectId, apiKey: "private-key" }, campaign, originals,
    async receipt => { receipts.push(receipt); }, transport)).rejects.toThrow("Remote details withheld");
  expect(receipts.some(receipt => receipt.status === "accepted")).toBe(false);
  expect(JSON.stringify(receipts)).not.toMatch(/private-key|private-upstream/);
  if (["partial-insert", "receiver-error"].includes(kind)) expect(receipts[0]).toMatchObject({ status: "created", acceptedRows: 0 });
});
