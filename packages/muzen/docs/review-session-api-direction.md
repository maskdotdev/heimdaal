# Muzen Review Session API Direction

Status: working direction, not final.

This note captures the current product/API direction for making Muzen a durable
review workflow system rather than a thin runner SDK. It is intentionally not an
ADR: names, package boundaries, and exact types still need design pressure.

## Goal

Muzen should make this workflow feel natural:

1. An application receives an on-demand review request, usually from a webhook.
2. Muzen creates a durable review session.
3. Worker processes claim sessions and run the review swarm.
4. Clients stream and replay session events.
5. The application fetches the final structured review result.

The caller should not need to know about runner paths, manual run IDs, changed
file lists, worker leases, Postgres notifications, or manual `Promise.all`
orchestration.

## Preferred API Shape

The current preferred direction is a flat command API on a configured Muzen
instance. Avoid fluent chains as the primary DX.

```ts
import {
  createMuzen,
  github,
  gitlab,
  openai,
  anthropic,
  postgres,
} from "@muzen/server";

export const muzen = createMuzen({
  store: postgres({
    url: process.env.DATABASE_URL,
  }),

  providers: {
    github: github({
      token: process.env.GITHUB_TOKEN,
      baseUrl: process.env.GITHUB_BASE_URL ?? "https://api.github.com",
    }),

    gitlab: gitlab({
      token: process.env.GITLAB_TOKEN,
      baseUrl: process.env.GITLAB_BASE_URL,
    }),
  },

  models: {
    default: openai({
      apiKey: process.env.OPENAI_API_KEY,
      baseUrl: process.env.OPENAI_BASE_URL,
      model: process.env.OPENAI_REVIEW_MODEL,
    }),

    deep: anthropic({
      apiKey: process.env.ANTHROPIC_API_KEY,
      baseUrl: process.env.ANTHROPIC_BASE_URL,
      model: process.env.ANTHROPIC_REVIEW_MODEL,
    }),
  },

  review: {
    model: "default",
    concurrency: {
      sessions: 50,
      agentsPerSession: 8,
    },
  },
});
```

Configuration is first-class because real hosts need to bring their own provider
tokens, model keys, base URLs, routing profiles, and concurrency policies.

## Submitting Reviews

Programmatic review request:

```ts
const session = await muzen.submitReview({
  source: github.pullRequest({
    owner: "maskdotdev",
    repo: "heimdaal",
    number: 123,
  }),

  dedupe: "source-head",
  cancelSuperseded: true,
});
```

Webhook review request:

```ts
const session = await muzen.receiveReviewWebhook({
  provider: "github",
  payload,
  dedupe: "source-head",
  cancelSuperseded: true,
});

return Response.json({
  sessionId: session.id,
  status: session.status,
});
```

Per-review overrides should be available, but optional:

```ts
await muzen.submitReview({
  source: github.pullRequest({ owner, repo, number }),
  model: "deep",
  scope: {
    include: ["packages/muzen/**"],
    exclude: ["**/*.snap"],
  },
});
```

The product contract is "review this source." File changes are inferred by the
provider adapter from the PR/MR/diff. `scope` is an override, not the default
way to use Muzen.

## Events

Streaming and replay are core product behavior.

```ts
const events = muzen.streamReviewEvents(session.id, {
  after: request.headers.get("Last-Event-ID"),
});

return muzen.reviewEventsSSE(events);
```

Or, if the transport helper should be fully flat:

```ts
return muzen.reviewEventsResponse(session.id, {
  after: request.headers.get("Last-Event-ID"),
});
```

The open design question is which transport shape is primary. The underlying
primitive should remain transport-neutral:

```ts
for await (const event of muzen.streamReviewEvents(sessionId, { after })) {
  render(event);
}
```

Durable events should be enough to rebuild the review UI and resume from a
cursor:

- `session.created`
- `session.queued`
- `session.claimed`
- `session.started`
- `source.resolved`
- `scope.inferred`
- `scope.overridden`
- `repo.materialized`
- `plan.created`
- `agent.started`
- `agent.completed`
- `tool.started`
- `tool.completed`
- `finding.created`
- `finding.updated`
- `review.result_created`
- `session.completed`
- `session.failed`
- `session.cancelled`

High-volume deltas, stdout/stderr tails, token streams, and heartbeats can be
ephemeral or persisted by explicit policy.

## Results

The happy path should not expose artifacts. A review session has a final result:

```ts
const result = await muzen.waitForReview(session.id, {
  timeout: "10m",
});
```

```ts
const result = await muzen.getReviewResult(session.id);
```

Expected result shape:

```ts
type ReviewResult = {
  sessionId: string;
  status: "completed" | "failed" | "cancelled";
  conclusion: "approved" | "commented" | "changes_requested";
  summary: string;
  findings: ReviewFinding[];
  coverage: {
    filesConsidered: number;
    filesReviewed: number;
    filesSkipped: number;
  };
};
```

Muzen may store large outputs, raw transcripts, or rendered reports in an
internal blob/artifact store, but that should not be the ordinary review API.

## Cancellation

Cancellation should be direct:

```ts
await muzen.cancelReview(session.id, {
  reason: "superseded_by_new_push",
});
```

Bulk/domain cancellation should be possible without exposing worker internals:

```ts
await muzen.cancelReviews({
  source: github.pullRequest({ owner, repo, number }),
  reason: "new_commit_pushed",
});
```

## Workers

Applications start worker processes separately from request handlers:

```ts
await muzen.startWorkers();
```

Workers claim durable sessions, maintain leases, retry failures, honor
cancellation, run the review swarm, persist events, and write the final review
result.

The API may grow more deployment-specific helpers later:

```ts
await muzen.startWorkers({
  queues: ["reviews"],
  concurrency: 50,
});
```

## Internal Architecture Direction

Borrow the durable shape proven by Argus:

- The database row is the durable queue item.
- `LISTEN` / `NOTIFY` is only a wake-up signal.
- Workers claim due sessions with `FOR UPDATE SKIP LOCKED`.
- Claimed sessions use leases, attempts, retry policy, and backoff.
- Events are persisted before being streamed.
- Streams replay from `Last-Event-ID` and then follow live notifications.
- Polling remains as a fallback when notifications are missed.

These mechanics are infrastructure details. The public API should talk in terms
of sessions, events, results, policies, and providers.

## Vocabulary

- Session: durable user-visible review unit.
- Attempt: internal worker execution try.
- Source: provider-backed thing being reviewed, such as a pull request.
- Scope: inferred or overridden set of files/paths to consider.
- Provider: integration that resolves source, diff, metadata, and comments.
- Model profile: named model routing configuration with API key/base URL/model.
- Agent: participant inside a review swarm.
- Event: replayable state transition or optional ephemeral progress signal.
- Result: final structured review outcome.

Prefer not to use `run` in the public product vocabulary except for low-level
runner compatibility.

## What Moves Out Of The Happy Path

- `runnerPath`: advanced runtime configuration only.
- `changedFiles`: replaced by inferred provider scope and optional `scope`.
- `runId`: replaced by generated `session.id` and optional idempotency/dedupe.
- `Promise.all`: replaced by durable queue and worker-managed concurrency.
- Fluent chains: available only if they prove useful; not the main API style.
- Artifacts: internal storage detail unless an advanced/debug API needs them.

## Migration Direction

1. Introduce the session API beside the current SDK.
2. Implement current `startReview` behavior as a compatibility layer over
   `submitReview` plus `waitForReview`.
3. Treat `changedFiles` as a deprecated alias for `scope.files`.
4. Treat `runId` as a deprecated alias for an explicit idempotency key or
   session ID override, if an override remains supported.
5. Move `runnerPath` under advanced runtime configuration.
6. Rewrite examples around webhook/request, durable session creation, event
   streaming, and final result retrieval.

## Open Questions

- Are the flat names right: `submitReview`, `receiveReviewWebhook`,
  `streamReviewEvents`, `waitForReview`, `getReviewResult`, `cancelReview`?
- Should provider source builders be imported functions like
  `github.pullRequest(...)`, or methods on the configured Muzen instance?
- Should the primary streaming helper return an `AsyncIterable`, an SSE
  `Response`, or both with separate names?
- How should multi-tenant hosts provide per-customer provider/model
  credentials without making the single-tenant setup noisy?
- Does `review` remain the only first-class domain, or should the same session
  machinery be generic enough for other Muzen swarm workloads later?
