import express from "express";
import request from "supertest";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { normalizeIssueExecutionPolicy } from "../services/issue-execution-policy.ts";

const mockIssueService = vi.hoisted(() => ({
  getById: vi.fn(),
  getByIdForUpdate: vi.fn(),
  findOpenAncestorCreatedByAgent: vi.fn(async () => null),
  assertCheckoutOwner: vi.fn(),
  update: vi.fn(),
  createChild: vi.fn(),
  addComment: vi.fn(),
  findMentionedAgents: vi.fn(),
  getRelationSummaries: vi.fn(),
  listWakeableBlockedDependents: vi.fn(),
  getWakeableParentAfterChildCompletion: vi.fn(),
}));

const mockHeartbeatService = vi.hoisted(() => ({
  wakeup: vi.fn(async () => undefined),
  triggerIssueMonitor: vi.fn(async () => ({ outcome: "triggered" as const })),
  reportRunActivity: vi.fn(async () => undefined),
  getRun: vi.fn(async () => null),
  getActiveRunForAgent: vi.fn(async () => null),
  cancelRun: vi.fn(async () => null),
}));

const mockAccessService = vi.hoisted(() => ({
  canUser: vi.fn(async () => false),
  decide: vi.fn(),
  hasPermission: vi.fn(async () => false),
}));
const mockDbSelectWhere = vi.hoisted(() => vi.fn(() => ({
  for: () => ({
    then: (onFulfilled: (rows: unknown[]) => unknown, onRejected?: (reason: unknown) => unknown) =>
      Promise.resolve([{
        id: "55555555-5555-4555-8555-555555555555",
        companyId: "company-1",
        agentId: "33333333-3333-4333-8333-333333333333",
        contextSnapshot: { issueId: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa" },
        permissions: null,
      }]).then(onFulfilled, onRejected),
  }),
  then: (onFulfilled: (rows: unknown[]) => unknown, onRejected?: (reason: unknown) => unknown) =>
    Promise.resolve([{
      id: "55555555-5555-4555-8555-555555555555",
      companyId: "company-1",
      agentId: "33333333-3333-4333-8333-333333333333",
      contextSnapshot: { issueId: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa" },
      permissions: null,
    }]).then(onFulfilled, onRejected),
})));
const mockDbSelectFrom = vi.hoisted(() => vi.fn(() => ({ where: mockDbSelectWhere })));
const mockDbSelect = vi.hoisted(() => vi.fn(() => ({ from: mockDbSelectFrom })));
const mockDb = vi.hoisted(() => ({
  select: mockDbSelect,
  transaction: vi.fn(async (callback: (tx: { select: typeof mockDbSelect }) => Promise<unknown>) =>
    callback({ select: mockDbSelect })),
}));

const mockLogActivity = vi.hoisted(() => vi.fn(async () => undefined));
const mockIssueThreadInteractionService = vi.hoisted(() => ({
  expirePendingInteractionsForTerminalIssue: vi.fn(async () => []),
  listForIssue: vi.fn(async () => []),
  expireRequestConfirmationsSupersededByComment: vi.fn(async () => []),
}));
const mockIssueApprovalService = vi.hoisted(() => ({
  listApprovalsForIssue: vi.fn(async () => []),
}));
const mockRunnerGoalService = vi.hoisted(() => ({
  projection: vi.fn(async () => null),
  act: vi.fn(),
}));

function registerModuleMocks() {
  vi.doMock("../services/queued-interaction-response.js", () => ({
    hasQueuedInteractionResponse: vi.fn(async () => false),
  }));
  vi.doMock("../services/runner-goals.js", () => ({
    runnerGoalService: () => mockRunnerGoalService,
    RunnerGoalActionError: class RunnerGoalActionError extends Error {},
    RunnerGoalConflictError: class RunnerGoalConflictError extends Error {},
  }));

  vi.doMock("../services/index.js", () => ({
    companyService: () => ({
      getById: vi.fn(async () => ({ id: "company-1" })),
    }),
    accessService: () => mockAccessService,
    agentService: () => ({
      getById: vi.fn(async (agentId: string) => ({
        id: agentId,
        companyId: "company-1",
        permissions: null,
      })),
      resolveByReference: vi.fn(async (_companyId: string, reference: string) => ({
        ambiguous: false,
        agent: {
          id: reference,
          companyId: "company-1",
          status: "idle",
          orgChainHealth: { status: "healthy" },
        },
      })),
    }),
    companySkillService: () => ({
      completeTestRunForIssue: vi.fn(async () => null),
    }),
    documentAnnotationService: () => ({ remapOpenThreadsForDocument: async () => [] }),
    documentService: () => ({}),
    executionWorkspaceService: () => ({}),
    feedbackService: () => ({
      listIssueVotesForUser: vi.fn(async () => []),
      saveIssueVote: vi.fn(async () => ({ vote: null, consentEnabledNow: false, sharingEnabled: false })),
    }),
    goalService: () => ({}),
    heartbeatService: () => mockHeartbeatService,
    environmentService: () => ({
      getById: vi.fn(async () => null),
    }),
    instanceSettingsService: () => ({
      get: vi.fn(async () => ({
        id: "instance-settings-1",
        general: {
          censorUsernameInLogs: false,
          feedbackDataSharingPreference: "prompt",
        },
      })),
      listCompanyIds: vi.fn(async () => ["company-1"]),
    }),
    issueApprovalService: () => mockIssueApprovalService,
    issueReferenceService: () => ({
      deleteDocumentSource: async () => undefined,
      diffIssueReferenceSummary: () => ({
        addedReferencedIssues: [],
        removedReferencedIssues: [],
        currentReferencedIssues: [],
      }),
      emptySummary: () => ({ outbound: [], inbound: [] }),
      listIssueReferenceSummary: async () => ({ outbound: [], inbound: [] }),
      syncComment: async () => undefined,
      syncDocument: async () => undefined,
      syncIssue: async () => undefined,
    }),
    issueRecoveryActionService: () => ({
      getActiveForIssue: vi.fn(async () => null),
      listActiveForIssues: vi.fn(async () => new Map()),
    }),
    issueService: () => mockIssueService,
    issueThreadInteractionService: () => mockIssueThreadInteractionService,
    logActivity: mockLogActivity,
    projectService: () => ({}),
    routineService: () => ({
      syncRunStatusForIssue: vi.fn(async () => undefined),
    }),
    workProductService: () => ({}),
  }));
}

type TestActor =
  | {
      type: "board";
      userId: string;
      companyIds: string[];
      source: "local_implicit";
      isInstanceAdmin: boolean;
    }
  | {
      type: "agent";
      agentId: string;
      companyId: string;
      runId: string | null;
    };

function loadRouteModules() {
  return Promise.all([
    import("../middleware/index.js"),
    import("../routes/issues.js"),
  ]);
}

let routeModules: Awaited<ReturnType<typeof loadRouteModules>>;

async function createApp(actor?: TestActor) {
  const [{ errorHandler }, { issueRoutes }] = routeModules;
  const app = express();
  app.use(express.json());
  app.use((req, _res, next) => {
    (req as any).actor = actor ?? {
      type: "board",
      userId: "local-board",
      companyIds: ["company-1"],
      source: "local_implicit",
      isInstanceAdmin: false,
    };
    next();
  });
  app.use("/api", issueRoutes(mockDb as any, {} as any));
  app.use(errorHandler);
  return app;
}

describe("issue execution policy routes", () => {
  beforeEach(async () => {
    vi.resetModules();
    vi.doUnmock("../services/index.js");
    vi.doUnmock("../routes/issues.js");
    vi.doUnmock("../middleware/index.js");
    registerModuleMocks();
    vi.clearAllMocks();
    mockIssueService.assertCheckoutOwner.mockResolvedValue({ adoptedFromRunId: null });
    mockIssueService.getByIdForUpdate.mockImplementation(async () => mockIssueService.getById());
    mockIssueService.findMentionedAgents.mockResolvedValue([]);
    mockIssueService.getRelationSummaries.mockResolvedValue({ blockedBy: [], blocks: [] });
    mockIssueService.listWakeableBlockedDependents.mockResolvedValue([]);
    mockIssueService.getWakeableParentAfterChildCompletion.mockResolvedValue(null);
    mockIssueThreadInteractionService.listForIssue.mockResolvedValue([]);
    mockIssueThreadInteractionService.expireRequestConfirmationsSupersededByComment.mockResolvedValue([]);
    mockIssueApprovalService.listApprovalsForIssue.mockResolvedValue([]);
    mockDbSelect.mockImplementation(() => ({ from: mockDbSelectFrom }));
    mockDbSelectFrom.mockImplementation(() => ({ where: mockDbSelectWhere }));
    mockDbSelectWhere.mockImplementation(() => ({
      for: () => ({
        then: (onFulfilled: (rows: unknown[]) => unknown, onRejected?: (reason: unknown) => unknown) =>
          Promise.resolve([{
            id: "55555555-5555-4555-8555-555555555555",
            companyId: "company-1",
            agentId: "33333333-3333-4333-8333-333333333333",
            contextSnapshot: { issueId: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa" },
            permissions: null,
          }]).then(onFulfilled, onRejected),
      }),
      then: (onFulfilled: (rows: unknown[]) => unknown, onRejected?: (reason: unknown) => unknown) =>
        Promise.resolve([{
          id: "55555555-5555-4555-8555-555555555555",
          companyId: "company-1",
          agentId: "33333333-3333-4333-8333-333333333333",
          contextSnapshot: { issueId: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa" },
          permissions: null,
        }]).then(onFulfilled, onRejected),
    }));
    mockIssueService.createChild.mockResolvedValue({
      issue: {
        id: "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
        companyId: "company-1",
        identifier: "PAP-1002",
        title: "Child issue",
      },
      parentBlockerAdded: false,
    });
    mockAccessService.canUser.mockResolvedValue(false);
    mockAccessService.decide.mockImplementation(async (input: { actor?: { type?: string; source?: string }; action?: string }) => {
      const allowed = input.actor?.type === "board" && input.actor.source === "local_implicit"
        ? true
        : input.actor?.type === "agent" && [
            "company_scope:read",
            "issue:read",
            "issue:mutate",
            "runtime:manage",
          ].includes(input.action ?? "")
          ? true
          : Boolean(await mockAccessService.canUser() || await mockAccessService.hasPermission());
      return {
        allowed,
        action: input.action,
        reason: allowed ? "allow_explicit_grant" : "deny_missing_grant",
        explanation: allowed ? "Allowed by test grant." : `Missing permission: ${input.action ?? "action"}`,
      };
    });
    mockAccessService.hasPermission.mockResolvedValue(false);
    // Finish the module graph in setup before a test can send a request. An
    // import that outlives a test timeout can otherwise use the next test's mocks.
    routeModules = await loadRouteModules();
  });

  it("reauthorizes a terminal verdict against the review policy held under the update lock", async () => {
    const issue = {
      id: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
      companyId: "company-1",
      status: "in_review",
      reviewPolicy: "anyone",
      assigneeAgentId: "33333333-3333-4333-8333-333333333333",
      assigneeUserId: null,
      createdByUserId: "local-board",
      identifier: "PAP-1002",
      title: "Concurrent policy update",
      executionPolicy: null,
      executionState: null,
    };
    mockIssueService.getById.mockResolvedValue(issue);
    mockIssueService.getByIdForUpdate.mockResolvedValue({
      ...issue,
      reviewPolicy: "human_only",
    });

    const res = await request(await createApp({
      type: "agent",
      agentId: "33333333-3333-4333-8333-333333333333",
      companyId: "company-1",
      runId: "55555555-5555-4555-8555-555555555555",
    }))
      .patch("/api/issues/aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")
      .send({ status: "done" });

    expect(res.status).toBe(403);
    expect(res.body).toMatchObject({
      details: {
        code: "review_policy_denied",
        policy: "human_only",
      },
    });
    expect(mockDb.transaction).toHaveBeenCalled();
    expect(mockIssueService.getByIdForUpdate).toHaveBeenCalled();
    expect(mockIssueService.update).not.toHaveBeenCalled();
  });

  it("rejects an agent-authored in_review transition without a review path", async () => {
    const issue = {
      id: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
      companyId: "company-1",
      status: "todo",
      assigneeAgentId: "33333333-3333-4333-8333-333333333333",
      assigneeUserId: null,
      createdByUserId: "local-board",
      identifier: "PAP-1003",
      title: "Missing review path",
      executionPolicy: null,
      executionState: null,
    };
    mockIssueService.getById.mockResolvedValue(issue);

    const res = await request(await createApp({
      type: "agent",
      agentId: "33333333-3333-4333-8333-333333333333",
      companyId: "company-1",
      runId: "55555555-5555-4555-8555-555555555555",
    }))
      .patch("/api/issues/aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")
      .send({ status: "in_review" });

    expect(res.status).toBe(422);
    expect(res.body.error).toContain("invalid_issue_disposition");
    expect(res.body.error).toContain("request_confirmation");
    expect(res.body.details).toMatchObject({
      code: "invalid_issue_disposition",
      missing: "review_path",
    });
    expect(mockIssueService.update).not.toHaveBeenCalled();
  });

  it("allows an agent-authored in_review transition with a pending confirmation interaction", async () => {
    const issue = {
      id: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
      companyId: "company-1",
      status: "todo",
      assigneeAgentId: "33333333-3333-4333-8333-333333333333",
      assigneeUserId: null,
      createdByUserId: "local-board",
      identifier: "PAP-1004",
      title: "Pending confirmation",
      executionPolicy: null,
      executionState: null,
    };
    mockIssueService.getById.mockResolvedValue(issue);
    mockIssueThreadInteractionService.listForIssue.mockResolvedValue([
      {
        id: "11111111-1111-4111-8111-111111111111",
        kind: "request_confirmation",
        status: "pending",
        createdByAgentId: "33333333-3333-4333-8333-333333333333",
        sourceRunId: "55555555-5555-4555-8555-555555555555",
      },
    ]);
    mockIssueService.update.mockImplementation(async (_id: string, patch: Record<string, unknown>) => ({
      ...issue,
      ...patch,
      updatedAt: new Date(),
    }));

    const res = await request(await createApp({
      type: "agent",
      agentId: "33333333-3333-4333-8333-333333333333",
      companyId: "company-1",
      runId: "55555555-5555-4555-8555-555555555555",
    }))
      .patch("/api/issues/aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")
      .send({ status: "in_review" });

    expect(res.status).toBe(200);
    expect(mockIssueService.update).toHaveBeenCalledWith(
      "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
      expect.objectContaining({ status: "in_review" }),
      expect.anything(),
    );
    expect(mockLogActivity).toHaveBeenCalledWith(
      expect.anything(),
      expect.objectContaining({
        action: "issue.updated",
        details: expect.not.objectContaining({ reviewInteractionId: expect.anything() }),
      }),
      expect.any(Array),
    );
    expect(mockLogActivity.mock.calls[0]?.[0]).toBe(mockIssueService.update.mock.calls[0]?.[2]);
  });

  it("binds an explicitly designated same-run confirmation to the review transition", async () => {
    const issue = {
      id: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
      companyId: "company-1",
      status: "todo",
      assigneeAgentId: "33333333-3333-4333-8333-333333333333",
      assigneeUserId: null,
      createdByUserId: "local-board",
      identifier: "PAP-1004",
      title: "Pending confirmation",
      executionPolicy: null,
      executionState: null,
    };
    mockIssueService.getById.mockResolvedValue(issue);
    mockIssueThreadInteractionService.listForIssue.mockResolvedValue([{
      id: "11111111-1111-4111-8111-111111111111",
      kind: "request_confirmation",
      status: "pending",
      createdByAgentId: "33333333-3333-4333-8333-333333333333",
      sourceRunId: "55555555-5555-4555-8555-555555555555",
      payload: { version: 1, prompt: "Approve this review?" },
    }]);
    mockIssueService.update.mockImplementation(async (_id: string, patch: Record<string, unknown>) => ({
      ...issue,
      ...patch,
      updatedAt: new Date(),
    }));

    const res = await request(await createApp({
      type: "agent",
      agentId: "33333333-3333-4333-8333-333333333333",
      companyId: "company-1",
      runId: "55555555-5555-4555-8555-555555555555",
    }))
      .patch("/api/issues/aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")
      .send({
        status: "in_review",
        reviewInteractionId: "11111111-1111-4111-8111-111111111111",
      });

    expect(res.status).toBe(200);
    expect(mockIssueService.update).toHaveBeenCalledWith(
      "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
      expect.not.objectContaining({ reviewInteractionId: expect.anything() }),
      expect.anything(),
    );
    expect(mockLogActivity).toHaveBeenCalledWith(
      expect.anything(),
      expect.objectContaining({
        action: "issue.updated",
        details: expect.objectContaining({
          reviewInteractionId: "11111111-1111-4111-8111-111111111111",
        }),
      }),
      expect.any(Array),
    );
  });

  it("binds a user-designated confirmation to the review transition activity", async () => {
    const issue = {
      id: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
      companyId: "company-1",
      status: "todo",
      assigneeAgentId: null,
      assigneeUserId: "local-board",
      createdByUserId: "local-board",
      identifier: "PAP-1004",
      title: "Pending confirmation",
      executionPolicy: null,
      executionState: null,
    };
    mockIssueService.getById.mockResolvedValue(issue);
    mockIssueThreadInteractionService.listForIssue.mockResolvedValue([{
      id: "11111111-1111-4111-8111-111111111111",
      kind: "request_confirmation",
      status: "pending",
      createdByAgentId: null,
      createdByUserId: "local-board",
      sourceRunId: null,
      payload: { version: 1, prompt: "Approve this review?" },
    }]);
    mockIssueService.update.mockImplementation(async (_id: string, patch: Record<string, unknown>) => ({
      ...issue,
      ...patch,
      changes: { status: { from: "todo", to: "in_review" } },
      updatedAt: new Date(),
    }));

    const res = await request(await createApp())
      .patch("/api/issues/aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")
      .send({
        status: "in_review",
        reviewInteractionId: "11111111-1111-4111-8111-111111111111",
      });

    expect(res.status).toBe(200);
    expect(mockIssueService.update).toHaveBeenCalledWith(
      "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
      expect.not.objectContaining({ reviewInteractionId: expect.anything() }),
      expect.anything(),
    );
    expect(mockLogActivity).toHaveBeenCalledWith(
      expect.anything(),
      expect.objectContaining({
        action: "issue.updated",
        actorType: "user",
        actorId: "local-board",
        details: expect.objectContaining({
          reviewInteractionId: "11111111-1111-4111-8111-111111111111",
        }),
      }),
      expect.any(Array),
    );
  });

  it("keeps a review transition and its confirmation binding in one rollback boundary", async () => {
    const issue = {
      id: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
      companyId: "company-1",
      status: "todo",
      assigneeAgentId: "33333333-3333-4333-8333-333333333333",
      assigneeUserId: null,
      createdByUserId: "local-board",
      identifier: "PAP-1004",
      title: "Pending confirmation",
      executionPolicy: null,
      executionState: null,
    };
    mockIssueService.getById.mockResolvedValue(issue);
    mockIssueThreadInteractionService.listForIssue.mockResolvedValue([{
      id: "11111111-1111-4111-8111-111111111111",
      kind: "request_confirmation",
      status: "pending",
      createdByAgentId: "33333333-3333-4333-8333-333333333333",
      sourceRunId: "55555555-5555-4555-8555-555555555555",
      payload: { version: 1, prompt: "Approve this review?" },
    }]);
    mockIssueService.update.mockImplementation(async (_id: string, patch: Record<string, unknown>) => ({
      ...issue,
      ...patch,
      changes: { status: { from: "todo", to: "in_review" } },
      updatedAt: new Date(),
    }));
    mockLogActivity.mockRejectedValueOnce(new Error("activity insert failed"));

    const res = await request(await createApp({
      type: "agent",
      agentId: "33333333-3333-4333-8333-333333333333",
      companyId: "company-1",
      runId: "55555555-5555-4555-8555-555555555555",
    }))
      .patch("/api/issues/aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")
      .send({
        status: "in_review",
        reviewInteractionId: "11111111-1111-4111-8111-111111111111",
      });

    expect(res.status).toBe(500);
    expect(mockDb.transaction).toHaveBeenCalled();
    const updateTx = mockIssueService.update.mock.calls[0]?.[2];
    const activityTx = mockLogActivity.mock.calls[0]?.[0];
    expect(activityTx).toBe(updateTx);
  });

  it("rejects a review binding to a confirmation from another run", async () => {
    const issue = {
      id: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
      companyId: "company-1",
      status: "todo",
      assigneeAgentId: "33333333-3333-4333-8333-333333333333",
      assigneeUserId: null,
      createdByUserId: "local-board",
      identifier: "PAP-1004",
      title: "Pending confirmation",
      executionPolicy: null,
      executionState: null,
    };
    mockIssueService.getById.mockResolvedValue(issue);
    mockIssueThreadInteractionService.listForIssue.mockResolvedValue([{
      id: "11111111-1111-4111-8111-111111111111",
      kind: "request_confirmation",
      status: "pending",
      createdByAgentId: "33333333-3333-4333-8333-333333333333",
      sourceRunId: "44444444-4444-4444-8444-444444444444",
      payload: { version: 1, prompt: "Approve another run's request?" },
    }]);

    const res = await request(await createApp({
      type: "agent",
      agentId: "33333333-3333-4333-8333-333333333333",
      companyId: "company-1",
      runId: "55555555-5555-4555-8555-555555555555",
    }))
      .patch("/api/issues/aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")
      .send({
        status: "in_review",
        reviewInteractionId: "11111111-1111-4111-8111-111111111111",
      });

    expect(res.status).toBe(422);
    expect(res.body).toMatchObject({
      error: expect.stringContaining("created by this agent run"),
      details: { code: "invalid_review_interaction" },
    });
    expect(mockIssueService.update).not.toHaveBeenCalled();
  });

  it("allows an agent-authored in_review transition with a typed execution participant", async () => {
    const issue = {
      id: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
      companyId: "company-1",
      status: "todo",
      assigneeAgentId: "33333333-3333-4333-8333-333333333333",
      assigneeUserId: null,
      createdByUserId: "local-board",
      identifier: "PAP-1005",
      title: "Execution participant",
      executionPolicy: null,
      executionState: null,
    };
    const policy = normalizeIssueExecutionPolicy({
      stages: [
        {
          id: "11111111-1111-4111-8111-111111111111",
          type: "review",
          participants: [{ type: "agent", agentId: "44444444-4444-4444-8444-444444444444" }],
        },
      ],
    })!;
    mockIssueService.getById.mockResolvedValue(issue);
    mockIssueService.update.mockImplementation(async (_id: string, patch: Record<string, unknown>) => ({
      ...issue,
      ...patch,
      updatedAt: new Date(),
    }));

    const res = await request(await createApp({
      type: "agent",
      agentId: "33333333-3333-4333-8333-333333333333",
      companyId: "company-1",
      runId: "55555555-5555-4555-8555-555555555555",
    }))
      .patch("/api/issues/aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")
      .send({ status: "in_review", executionPolicy: policy });

    expect(res.status).toBe(200);
    expect(mockIssueService.update).toHaveBeenCalledWith(
      "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
      expect.objectContaining({
        status: "in_review",
        executionState: expect.objectContaining({
          status: "pending",
          currentParticipant: expect.objectContaining({
            type: "agent",
            agentId: "44444444-4444-4444-8444-444444444444",
          }),
        }),
      }),
      expect.anything(),
    );
  });

  it("allows an agent-authored in_review transition with a scheduled monitor", async () => {
    const issue = {
      id: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
      companyId: "company-1",
      status: "todo",
      assigneeAgentId: "33333333-3333-4333-8333-333333333333",
      assigneeUserId: null,
      createdByUserId: "local-board",
      identifier: "PAP-1006",
      title: "External review monitor",
      executionPolicy: null,
      executionState: null,
      monitorAttemptCount: 0,
      monitorNextCheckAt: null,
      monitorLastTriggeredAt: null,
      monitorNotes: null,
      monitorScheduledBy: null,
    };
    mockIssueService.getById.mockResolvedValue(issue);
    mockIssueService.update.mockImplementation(async (_id: string, patch: Record<string, unknown>) => ({
      ...issue,
      ...patch,
      updatedAt: new Date(),
    }));

    const res = await request(await createApp({
      type: "agent",
      agentId: "33333333-3333-4333-8333-333333333333",
      companyId: "company-1",
      runId: "55555555-5555-4555-8555-555555555555",
    }))
      .patch("/api/issues/aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")
      .send({
        status: "in_review",
        executionPolicy: {
          monitor: {
            nextCheckAt: "2026-12-01T12:00:00.000Z",
            scheduledBy: "assignee",
            notes: "Wait for external QA report.",
          },
        },
      });

    expect(res.status).toBe(200);
    expect(mockIssueService.update).toHaveBeenCalledWith(
      "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
      expect.objectContaining({
        status: "in_review",
        monitorNextCheckAt: new Date("2026-12-01T12:00:00.000Z"),
      }),
      expect.anything(),
    );
  });

  it("allows board-authored in_review repair updates without a review path", async () => {
    const issue = {
      id: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
      companyId: "company-1",
      status: "todo",
      assigneeAgentId: "33333333-3333-4333-8333-333333333333",
      assigneeUserId: null,
      createdByUserId: "local-board",
      identifier: "PAP-1007",
      title: "Board repair",
      executionPolicy: null,
      executionState: null,
    };
    mockIssueService.getById.mockResolvedValue(issue);
    mockIssueService.update.mockImplementation(async (_id: string, patch: Record<string, unknown>) => ({
      ...issue,
      ...patch,
      updatedAt: new Date(),
    }));

    const res = await request(await createApp())
      .patch("/api/issues/aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")
      .send({ status: "in_review" });

    expect(res.status).toBe(200);
    expect(mockDb.transaction).toHaveBeenCalled();
    expect(mockLogActivity).toHaveBeenCalledWith(
      expect.anything(),
      expect.objectContaining({
        action: "issue.updated",
        actorType: "user",
        actorId: "local-board",
        details: expect.objectContaining({ status: "in_review" }),
      }),
      expect.any(Array),
    );
    expect(mockLogActivity.mock.calls[0]?.[0]).toBe(mockIssueService.update.mock.calls[0]?.[2]);
    expect(mockIssueThreadInteractionService.listForIssue).not.toHaveBeenCalled();
    expect(mockIssueApprovalService.listApprovalsForIssue).not.toHaveBeenCalled();
  });

  it("allows a board user to cancel an active agent review task", async () => {
    const policy = normalizeIssueExecutionPolicy({
      stages: [
        {
          id: "11111111-1111-4111-8111-111111111111",
          type: "review",
          participants: [{ type: "agent", agentId: "33333333-3333-4333-8333-333333333333" }],
        },
      ],
    })!;
    const issue = {
      id: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
      companyId: "company-1",
      status: "in_review",
      assigneeAgentId: "33333333-3333-4333-8333-333333333333",
      assigneeUserId: null,
      createdByUserId: "local-board",
      identifier: "PAP-1008",
      title: "Active review",
      executionPolicy: policy,
      executionState: {
        status: "pending",
        currentStageId: "11111111-1111-4111-8111-111111111111",
        currentStageIndex: 0,
        currentStageType: "review",
        currentParticipant: { type: "agent", agentId: "33333333-3333-4333-8333-333333333333" },
        returnAssignee: { type: "agent", agentId: "44444444-4444-4444-8444-444444444444" },
        completedStageIds: [],
        lastDecisionId: null,
        lastDecisionOutcome: null,
      },
    };
    mockIssueService.getById.mockResolvedValue(issue);
    mockIssueService.update.mockImplementation(async (_id: string, patch: Record<string, unknown>) => ({
      ...issue,
      ...patch,
      updatedAt: new Date(),
    }));

    const res = await request(await createApp())
      .patch("/api/issues/aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")
      .send({ status: "cancelled" });

    expect(res.status).toBe(200);
    expect(mockIssueService.update).toHaveBeenCalledWith(
      "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
      expect.objectContaining({
        status: "cancelled",
        executionState: null,
        actorAgentId: null,
        actorUserId: "local-board",
      }),
      expect.anything(),
      undefined,
      expect.any(Array),
    );
    expect(mockHeartbeatService.cancelRun).not.toHaveBeenCalled();
  });

  it("allows a board user to cancel a drifted pending agent review task", async () => {
    const policy = normalizeIssueExecutionPolicy({
      stages: [
        {
          id: "11111111-1111-4111-8111-111111111111",
          type: "review",
          participants: [{ type: "agent", agentId: "33333333-3333-4333-8333-333333333333" }],
        },
      ],
    })!;
    const issue = {
      id: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
      companyId: "company-1",
      status: "blocked",
      assigneeAgentId: "44444444-4444-4444-8444-444444444444",
      assigneeUserId: null,
      createdByUserId: "local-board",
      identifier: "PAP-1009",
      title: "Drifted active review",
      executionPolicy: policy,
      executionState: {
        status: "pending",
        currentStageId: "11111111-1111-4111-8111-111111111111",
        currentStageIndex: 0,
        currentStageType: "review",
        currentParticipant: { type: "agent", agentId: "33333333-3333-4333-8333-333333333333" },
        returnAssignee: { type: "agent", agentId: "44444444-4444-4444-8444-444444444444" },
        completedStageIds: [],
        lastDecisionId: null,
        lastDecisionOutcome: null,
      },
    };
    mockIssueService.getById.mockResolvedValue(issue);
    mockIssueService.update.mockImplementation(async (_id: string, patch: Record<string, unknown>) => ({
      ...issue,
      ...patch,
      updatedAt: new Date(),
    }));

    const res = await request(await createApp())
      .patch("/api/issues/aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")
      .send({ status: "cancelled" });

    expect(res.status).toBe(200);
    expect(mockIssueService.update).toHaveBeenCalledWith(
      "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
      expect.objectContaining({
        status: "cancelled",
        executionState: null,
        actorAgentId: null,
        actorUserId: "local-board",
      }),
      expect.anything(),
      undefined,
      expect.any(Array),
    );
    const updatePatch = mockIssueService.update.mock.calls[0]?.[1] as Record<string, unknown>;
    expect(updatePatch.status).toBe("cancelled");
    expect(updatePatch.assigneeAgentId).toBeUndefined();
    expect(updatePatch.assigneeUserId).toBeUndefined();
    expect(mockHeartbeatService.cancelRun).not.toHaveBeenCalled();
  });

  it("keeps the review stage pending when a board user reassigns to an eligible participant", async () => {
    const policy = normalizeIssueExecutionPolicy({
      stages: [
        {
          id: "11111111-1111-4111-8111-111111111111",
          type: "review",
          participants: [
            { type: "agent", agentId: "33333333-3333-4333-8333-333333333333" },
            { type: "agent", agentId: "55555555-5555-4555-8555-555555555555" },
          ],
        },
      ],
    })!;
    const issue = {
      id: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
      companyId: "company-1",
      status: "in_review",
      assigneeAgentId: "33333333-3333-4333-8333-333333333333",
      assigneeUserId: null,
      createdByUserId: "local-board",
      identifier: "PAP-1010",
      title: "Reassigned review",
      executionPolicy: policy,
      executionState: {
        status: "pending",
        currentStageId: "11111111-1111-4111-8111-111111111111",
        currentStageIndex: 0,
        currentStageType: "review",
        currentParticipant: { type: "agent", agentId: "33333333-3333-4333-8333-333333333333" },
        returnAssignee: { type: "agent", agentId: "44444444-4444-4444-8444-444444444444" },
        completedStageIds: [],
        lastDecisionId: null,
        lastDecisionOutcome: null,
      },
    };
    mockIssueService.getById.mockResolvedValue(issue);
    mockIssueService.update.mockImplementation(async (_id: string, patch: Record<string, unknown>) => ({
      ...issue,
      ...patch,
      updatedAt: new Date(),
    }));

    const res = await request(await createApp())
      .patch("/api/issues/aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")
      .send({ assigneeAgentId: "55555555-5555-4555-8555-555555555555" });

    expect(res.status).toBe(200);
    const updatePatch = mockIssueService.update.mock.calls[0]?.[1] as Record<string, unknown>;
    expect(updatePatch.status).toBe("in_review");
    expect(updatePatch.assigneeAgentId).toBe("55555555-5555-4555-8555-555555555555");
    expect(updatePatch.assigneeUserId).toBeNull();
    expect(updatePatch.executionState).toMatchObject({
      status: "pending",
      currentStageId: "11111111-1111-4111-8111-111111111111",
      currentStageType: "review",
      currentParticipant: { type: "agent", agentId: "55555555-5555-4555-8555-555555555555" },
      returnAssignee: { type: "agent", agentId: "44444444-4444-4444-8444-444444444444" },
    });
    expect(mockHeartbeatService.cancelRun).not.toHaveBeenCalled();
  });

  it("dissolves the review when a board user reassigns an in_review task to a non-participant", async () => {
    const policy = normalizeIssueExecutionPolicy({
      stages: [
        {
          id: "11111111-1111-4111-8111-111111111111",
          type: "review",
          participants: [{ type: "agent", agentId: "33333333-3333-4333-8333-333333333333" }],
        },
      ],
    })!;
    const issue = {
      id: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
      companyId: "company-1",
      status: "in_review",
      assigneeAgentId: "33333333-3333-4333-8333-333333333333",
      assigneeUserId: null,
      createdByUserId: "local-board",
      identifier: "PAP-1011",
      title: "Reassigned away from review",
      executionPolicy: policy,
      executionState: {
        status: "pending",
        currentStageId: "11111111-1111-4111-8111-111111111111",
        currentStageIndex: 0,
        currentStageType: "review",
        currentParticipant: { type: "agent", agentId: "33333333-3333-4333-8333-333333333333" },
        returnAssignee: { type: "agent", agentId: "44444444-4444-4444-8444-444444444444" },
        completedStageIds: [],
        lastDecisionId: null,
        lastDecisionOutcome: null,
      },
    };
    mockIssueService.getById.mockResolvedValue(issue);
    mockIssueService.update.mockImplementation(async (_id: string, patch: Record<string, unknown>) => ({
      ...issue,
      ...patch,
      updatedAt: new Date(),
    }));

    const res = await request(await createApp())
      .patch("/api/issues/aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")
      .send({ assigneeAgentId: "55555555-5555-4555-8555-555555555555" });

    expect(res.status).toBe(200);
    const updatePatch = mockIssueService.update.mock.calls[0]?.[1] as Record<string, unknown>;
    expect(updatePatch.status).toBe("in_progress");
    expect(updatePatch.executionState).toBeNull();
    expect(updatePatch.assigneeAgentId).toBe("55555555-5555-4555-8555-555555555555");
    expect(mockHeartbeatService.cancelRun).not.toHaveBeenCalled();
  });

  it("does not auto-start execution review when reviewers are added to an already in_review issue", async () => {
    const policy = normalizeIssueExecutionPolicy({
      stages: [
        {
          id: "11111111-1111-4111-8111-111111111111",
          type: "review",
          participants: [{ type: "agent", agentId: "33333333-3333-4333-8333-333333333333" }],
        },
      ],
    })!;
    const issue = {
      id: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
      companyId: "company-1",
      status: "in_review",
      assigneeAgentId: null,
      assigneeUserId: "local-board",
      createdByUserId: "local-board",
      identifier: "PAP-999",
      title: "Execution policy edit",
      executionPolicy: null,
      executionState: null,
    };
    mockIssueService.getById.mockResolvedValue(issue);
    mockIssueService.update.mockImplementation(async (_id: string, patch: Record<string, unknown>) => ({
      ...issue,
      ...patch,
      updatedAt: new Date(),
    }));

    const res = await request(await createApp())
      .patch("/api/issues/aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")
      .send({ executionPolicy: policy });

    expect(res.status).toBe(200);
    expect(mockIssueService.update).toHaveBeenCalledWith(
      "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
      expect.objectContaining({
        executionPolicy: policy,
        actorAgentId: null,
        actorUserId: "local-board",
      }),
    );
    const updatePatch = mockIssueService.update.mock.calls[0]?.[1] as Record<string, unknown>;
    expect(updatePatch.status).toBeUndefined();
    expect(updatePatch.assigneeAgentId).toBeUndefined();
    expect(updatePatch.assigneeUserId).toBeUndefined();
    expect(updatePatch.executionState).toBeUndefined();
    expect(mockHeartbeatService.wakeup).not.toHaveBeenCalled();
  });

  it("triggers a scheduled monitor immediately from the dedicated route", async () => {
    const issue = {
      id: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
      companyId: "company-1",
      status: "in_progress",
      assigneeAgentId: "33333333-3333-4333-8333-333333333333",
      assigneeUserId: null,
      createdByUserId: "local-board",
      identifier: "PAP-1001",
      title: "Manual monitor trigger",
      executionPolicy: normalizeIssueExecutionPolicy({
        monitor: {
          nextCheckAt: "2026-04-11T12:30:00.000Z",
          notes: "Check deployment",
          scheduledBy: "board",
        },
      }),
      executionState: null,
    };
    mockIssueService.getById.mockResolvedValue(issue);

    const res = await request(await createApp())
      .post("/api/issues/aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa/monitor/check-now")
      .send({});

    expect(res.status).toBe(200);
    expect(res.body).toEqual({ ok: true });
    expect(mockHeartbeatService.triggerIssueMonitor).toHaveBeenCalledWith(
      "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
      expect.objectContaining({
        actorType: "user",
        actorId: "local-board",
        agentId: null,
      }),
    );
  });

  it("lets a board user create a child issue with a scheduled monitor", async () => {
    mockIssueService.getById.mockResolvedValue({
      id: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
      companyId: "company-1",
      status: "in_progress",
      assigneeAgentId: "11111111-1111-4111-8111-111111111111",
      assigneeUserId: null,
      createdByUserId: "local-board",
      identifier: "PAP-1001",
      title: "Parent issue",
      executionPolicy: null,
      executionState: null,
    });

    const res = await request(await createApp())
      .post("/api/issues/aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa/children")
      .send({
        title: "Child monitor",
        status: "in_review",
        assigneeAgentId: "33333333-3333-4333-8333-333333333333",
        executionPolicy: {
          monitor: {
            nextCheckAt: "2026-04-11T12:30:00.000Z",
            scheduledBy: "assignee",
          },
        },
      });

    expect(res.status).toBe(201);
    const createPayload = mockIssueService.createChild.mock.calls[0]?.[1] as {
      executionPolicy: { monitor: { scheduledBy: string } };
    };
    expect(createPayload.executionPolicy.monitor.scheduledBy).toBe("board");
    expect(mockLogActivity).toHaveBeenCalledWith(
      expect.anything(),
      expect.objectContaining({
        action: "issue.monitor_scheduled",
        details: expect.objectContaining({
          scheduledBy: "board",
        }),
      }),
    );
  });

  it("rejects child monitor scheduling by a non-assignee agent even with task assignment permission", async () => {
    mockAccessService.hasPermission.mockResolvedValue(true);
    mockIssueService.getById.mockResolvedValue({
      id: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
      companyId: "company-1",
      status: "in_progress",
      assigneeAgentId: "11111111-1111-4111-8111-111111111111",
      assigneeUserId: null,
      createdByUserId: "local-board",
      identifier: "PAP-1001",
      title: "Parent issue",
      executionPolicy: null,
      executionState: null,
    });

    const res = await request(await createApp({
      type: "agent",
      agentId: "22222222-2222-4222-8222-222222222222",
      companyId: "company-1",
      runId: "run-1",
    }))
      .post("/api/issues/aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa/children")
      .send({
        title: "Child monitor",
        status: "in_review",
        assigneeAgentId: "33333333-3333-4333-8333-333333333333",
        executionPolicy: {
          monitor: {
            nextCheckAt: "2026-04-11T12:30:00.000Z",
            scheduledBy: "board",
          },
        },
      });

    expect(res.status).toBe(403);
    expect(res.body.error).toBe("Only the assignee agent or a board user can manage issue monitors");
    expect(mockIssueService.createChild).not.toHaveBeenCalled();
  });

  it("normalizes spoofed child monitor scheduledBy to the assignee actor", async () => {
    mockAccessService.hasPermission.mockResolvedValue(true);
    mockIssueService.getById.mockResolvedValue({
      id: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
      companyId: "company-1",
      status: "in_progress",
      assigneeAgentId: "33333333-3333-4333-8333-333333333333",
      assigneeUserId: null,
      createdByUserId: "local-board",
      identifier: "PAP-1001",
      title: "Parent issue",
      executionPolicy: null,
      executionState: null,
    });

    const res = await request(await createApp({
      type: "agent",
      agentId: "33333333-3333-4333-8333-333333333333",
      companyId: "company-1",
      runId: "run-1",
    }))
      .post("/api/issues/aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa/children")
      .send({
        title: "Child monitor",
        status: "in_review",
        assigneeAgentId: "33333333-3333-4333-8333-333333333333",
        executionPolicy: {
          monitor: {
            nextCheckAt: "2026-04-11T12:30:00.000Z",
            scheduledBy: "board",
            externalRef: "https://example.test/deploy?token=secret",
          },
        },
      });

    expect(res.status).toBe(201);
    const createPayload = mockIssueService.createChild.mock.calls[0]?.[1] as {
      executionPolicy: { monitor: { scheduledBy: string; externalRef: string | null } };
    };
    expect(createPayload.executionPolicy.monitor.scheduledBy).toBe("assignee");
    expect(createPayload.executionPolicy.monitor.externalRef).toBe("[redacted]");
    expect(mockLogActivity).toHaveBeenCalledWith(
      expect.anything(),
      expect.objectContaining({
        action: "issue.monitor_scheduled",
        entityId: "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
        details: expect.not.objectContaining({ externalRef: expect.anything() }),
      }),
    );
  });

  describe("review-to-return-owner approval persistence", () => {
    const executorId = "33333333-3333-4333-8333-333333333333";
    const reviewerId = "44444444-4444-4444-8444-444444444444";
    const strangerId = "66666666-6666-4666-8666-666666666666";
    const issueId = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa";
    let activeActorAgentId = executorId;
    const agentActor = (agentId: string): TestActor => {
      activeActorAgentId = agentId;
      return { type: "agent", agentId, companyId: "company-1", runId: "55555555-5555-4555-8555-555555555555" };
    };

    function fixture(ownerType: "agent" | "user") {
      const owner = ownerType === "agent"
        ? { type: "agent" as const, agentId: executorId, userId: null }
        : { type: "user" as const, agentId: null, userId: "delivery-user" };
      const reviewer = { type: "agent" as const, agentId: reviewerId, userId: null };
      const policy = normalizeIssueExecutionPolicy({ stages: [
        { type: "review", participants: [owner, reviewer] },
        { type: "approval", participants: [owner] },
      ] })!;
      let persisted: any = {
        id: issueId, companyId: "company-1", status: "in_review", identifier: "PAP-1008",
        title: "Separate delivery approval", assigneeAgentId: reviewerId, assigneeUserId: null,
        createdByUserId: "local-board", executionPolicy: policy,
        executionState: { status: "pending", currentStageId: policy.stages[0].id,
          currentStageIndex: 0, currentStageType: "review", currentParticipant: reviewer,
          returnAssignee: owner, completedStageIds: [], lastDecisionId: null, lastDecisionOutcome: null },
      };
      const decisions: any[] = [];
      let failInsert = false;
      mockDbSelectWhere.mockImplementation(() => {
        const rows = () => [{ id: "55555555-5555-4555-8555-555555555555", companyId: "company-1",
          agentId: activeActorAgentId, contextSnapshot: { issueId }, permissions: null }];
        const query = { then: (resolve: (rows: unknown[]) => unknown) => Promise.resolve(rows()).then(resolve) };
        return { ...query, for: () => query };
      });
      mockIssueService.getById.mockImplementation(async () => persisted);
      mockIssueService.update.mockImplementation(async (_id: string, patch: Record<string, unknown>, tx: any) => {
        const updated = { ...persisted, ...patch, updatedAt: new Date() };
        if (tx) tx.stagedIssue = updated;
        else persisted = updated;
        return updated;
      });
      mockIssueService.addComment.mockResolvedValue({ id: "comment-id", body: "Stage decision" });
      // Emulate commit/rollback to verify the native route puts both writes in
      // one transaction. This is route proof, not a live database acceptance.
      mockDb.transaction.mockImplementation(async (callback: any) => {
        const pendingDecisions: any[] = [];
        const tx: any = { select: mockDbSelect, stagedIssue: null,
          insert: vi.fn(() => ({ values: vi.fn(async (decision: any) => {
            if (failInsert) throw new Error("decision insert failed");
            pendingDecisions.push(decision);
          }) })),
        };
        const result = await callback(tx);
        if (tx.stagedIssue) persisted = tx.stagedIssue;
        decisions.push(...pendingDecisions);
        return result;
      });
      return { owner, reviewer, policy, decisions, current: () => persisted,
        failInsert: () => { failInsert = true; },
        drift: (currentParticipant: unknown) => {
          persisted.executionState = { ...persisted.executionState, currentStageId: policy.stages[1].id,
            currentStageIndex: 1, currentStageType: "approval", currentParticipant,
            completedStageIds: [policy.stages[0].id], lastDecisionId: "77777777-7777-4777-8777-777777777777" };
        },
      };
    }

    it.each(["agent", "user"] as const)("persists only review before a separate pending %s approval", async (ownerType) => {
      const f = fixture(ownerType);
      const patch = async (actor: TestActor, body: Record<string, unknown>) =>
        request(await createApp(actor)).patch(`/api/issues/${issueId}`).send(body);
      const wrongReview = await patch(agentActor(executorId), { status: "done", comment: "Not the reviewer" });
      expect(wrongReview.status).toBe(422);
      expect(f.decisions).toEqual([]);
      expect(mockIssueService.update).not.toHaveBeenCalled();
      const noReviewComment = await patch(agentActor(reviewerId), { status: "done" });
      expect(noReviewComment.status).toBe(422);
      const reviewed = await patch(agentActor(reviewerId), { status: "done", comment: "Independent review accepted" });
      expect(reviewed.status).toBe(200);
      expect(f.current()).toMatchObject({ status: "in_review", assigneeAgentId: f.owner.agentId,
        assigneeUserId: f.owner.userId, executionState: { status: "pending", currentStageId: f.policy.stages[1].id,
          currentStageIndex: 1, currentStageType: "approval", currentParticipant: f.owner,
          returnAssignee: f.owner, completedStageIds: [f.policy.stages[0].id], lastDecisionId: f.decisions[0]?.id } });
      expect(f.decisions).toEqual([expect.objectContaining({ companyId: "company-1", issueId,
        stageId: f.policy.stages[0].id, stageType: "review", actorAgentId: reviewerId,
        actorUserId: null, outcome: "approved", body: "Independent review accepted" })]);
      const updateTx = mockIssueService.update.mock.calls[0]?.[2];
      expect(updateTx.insert).toHaveBeenCalledOnce();
      if (ownerType === "agent") expect(mockHeartbeatService.wakeup).toHaveBeenCalledWith(executorId, expect.anything());
      const wrongApproval = await patch(agentActor(reviewerId), { status: "done", comment: "Not the approver" });
      expect(wrongApproval.status).toBe(422);
      expect(f.decisions).toHaveLength(1);
      const ownerActor: TestActor = ownerType === "agent" ? agentActor(executorId) : {
        type: "board", userId: "delivery-user", companyIds: ["company-1"], source: "local_implicit", isInstanceAdmin: false,
      };
      const noApprovalComment = await patch(ownerActor, { status: "done" });
      expect(noApprovalComment.status).toBe(422);
      const approved = await patch(ownerActor, { status: "done", comment: "Separate delivery approval" });
      expect(approved.status).toBe(200);
      expect(f.decisions).toHaveLength(2);
      expect(f.decisions[1]).toMatchObject({ stageId: f.policy.stages[1].id, stageType: "approval", outcome: "approved",
        actorAgentId: f.owner.agentId, actorUserId: f.owner.userId, body: "Separate delivery approval" });
      expect(f.current().executionState.completedStageIds).toEqual(f.policy.stages.map((stage) => stage.id));
    });

    it("keeps an approval-only executor gate pending until a separate decision", async () => {
      const f = fixture("agent");
      const approvalPolicy = normalizeIssueExecutionPolicy({ stages: [f.policy.stages[1]] })!;
      Object.assign(f.current(), { status: "in_progress", assigneeAgentId: executorId,
        executionPolicy: approvalPolicy, executionState: null });
      const app = await createApp(agentActor(executorId));
      const started = await request(app).patch(`/api/issues/${issueId}`)
        .send({ status: "done", comment: "Ready for configured approval" });
      expect(started.status).toBe(200);
      expect(f.current()).toMatchObject({ status: "in_review", executionState: {
        status: "pending", currentStageId: approvalPolicy.stages[0].id,
        currentParticipant: f.owner, completedStageIds: [], lastDecisionId: null,
      } });
      expect(f.decisions).toEqual([]);
      const approved = await request(app).patch(`/api/issues/${issueId}`)
        .send({ status: "done", comment: "Explicit approval decision" });
      expect(approved.status).toBe(200);
      expect(f.decisions).toEqual([expect.objectContaining({ stageId: approvalPolicy.stages[0].id,
        stageType: "approval", actorAgentId: executorId, outcome: "approved" })]);
    });

    it("rolls back the review and handoff when decision persistence fails", async () => {
      const f = fixture("agent");
      const original = f.current();
      f.failInsert();
      const res = await request(await createApp(agentActor(reviewerId))).patch(`/api/issues/${issueId}`)
        .send({ status: "done", comment: "Independent review accepted" });
      expect(res.status).toBe(500);
      expect(f.current()).toBe(original);
      expect(f.decisions).toEqual([]);
      expect(mockHeartbeatService.wakeup).not.toHaveBeenCalled();
      expect(mockIssueService.update.mock.calls[0]?.[2].insert).toHaveBeenCalledOnce();
    });

    it.each(["agent", "user"] as const)("reselects the configured %s approval owner without a decision", async (ownerType) => {
      for (const participant of [null, { type: "agent", agentId: strangerId }]) {
        const f = fixture(ownerType);
        f.drift(participant);
        const res = await request(await createApp(agentActor(reviewerId))).patch(`/api/issues/${issueId}`)
          .send({ title: "Reconcile pending approval" });
        expect(res.status).toBe(200);
        expect(f.current()).toMatchObject({ assigneeAgentId: f.owner.agentId, assigneeUserId: f.owner.userId,
          executionState: { status: "pending", currentParticipant: f.owner, currentStageId: f.policy.stages[1].id,
            completedStageIds: [f.policy.stages[0].id], lastDecisionId: "77777777-7777-4777-8777-777777777777" } });
        expect(f.decisions).toEqual([]);
      }
    });

    it("rejects a stage decision from an agent in another company", async () => {
      const f = fixture("agent");
      const res = await request(await createApp({ ...agentActor(reviewerId), companyId: "company-2" } as TestActor))
        .patch(`/api/issues/${issueId}`).send({ status: "done", comment: "Cross-company verdict" });
      expect(res.status).toBe(404);
      expect(mockIssueService.update).not.toHaveBeenCalled();
      expect(f.decisions).toEqual([]);
    });
  });
});
