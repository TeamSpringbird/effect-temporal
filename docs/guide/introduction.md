# What is effect-temporal?

effect-temporal runs [Effect](https://effect.website) durable-workflow programs on [Temporal](https://temporal.io). You author workflows with `effect/unstable/workflow` — `Workflow`, `Activity`, `DurableClock`, `DurableDeferred` — and this package supplies the engine that executes them on a Temporal cluster, plus the durable primitives entity workflows need that the upstream API does not define.

```ts
const OrderFlow = Workflow.make("orderFlow", {
  payload: { orderId: Schema.String },
  idempotencyKey: ({ orderId }) => orderId,
  success: Schema.String,
  error: Schema.TaggedStruct("PaymentDeclined", { reason: Schema.String }),
});
```

The package is two layers:

1. **An engine.** `effect/unstable/workflow` defines durable-workflow programs against an abstract `WorkflowEngine`; this package implements that engine over Temporal. Effect ships its own engine (`effect/unstable/cluster`, persisting to its own SQL tables) — this one is for codebases that already run Temporal and do not want a second durable-execution system.
2. **An extension layer.** Durable capabilities [declared once](/guide/declaring-capabilities) with the `definition` module and called directly in handlers: `defineActivity` (typed activities), `defineDeferred` (one-shot approvals), `defineMailbox` (repeated inbound signals), `defineUpdate` (request/response with typed channels), `defineState` (queryable published state), plus `continueAsNew` (unbounded workflows), `version`/`evolved` (logic and schema evolution), schedules, and Nexus operations backed by these workflows.

The library is one npm package, `@springbird/effect-temporal`, with tree-shakeable subpath modules:

| Module | Runs in | What it is |
| --- | --- | --- |
| `@springbird/effect-temporal/definition` | everywhere | `define*` capability declarations, `version`, `evolved`, the `WorkflowOps` seam — engine-free |
| `@springbird/effect-temporal/engine-sandbox` | the workflow bundle | `workflowBundle` (hosts registrations, provides `WorkflowOps`), raw activity calls, `continueAsNew` |
| `@springbird/effect-temporal/engine-client` | ordinary Node | the client-side engine + standalone read/signal operations |
| `@springbird/effect-temporal/client` | ordinary Node | `WorkflowClient` — the one client service |
| `@springbird/effect-temporal/activities` | worker registration | activity implementation tables (`handle`, `implementActivities`) + the attach bridge |
| `@springbird/effect-temporal/typed-activity`, `/mailbox`, `/update`, `/state-cell` | everywhere | the low-level primitive definitions `define*` builds on |
| `@springbird/effect-temporal/versioning` | the workflow bundle | low-level patch-marker version chains (`Versioning.match`) |
| `@springbird/effect-temporal/nexus` | worker registration | workflow-backed Nexus operations |
| `@springbird/effect-temporal/testing` | tests | the in-memory `WorkflowOps` runtime, a typed fake Temporal client, a live test harness |
| `@springbird/effect-temporal/lint` | your lint config | oxlint/ESLint rules for the authoring footguns |

## Why Effect-native matters

Temporal's TypeScript SDK gives you durable execution with untyped seams: workflow arguments, results, signals, and failures all travel as loosely-typed payloads, and failure means catching `ApplicationFailure` and inspecting strings. effect-temporal keeps every one of those seams schema-typed:

- **Engine-agnostic authoring.** Workflows register with `Workflow.toLayer`, capabilities are [declared once](/guide/declaring-capabilities) and called directly, and every in-handler operation requires one service — `WorkflowOps`. `workflowBundle` provides Temporal's implementation; the testing module provides an in-memory one; the identical handler runs on both. Choosing a backend is choosing a Layer.
- **Payloads, results, and errors are schemas.** The engine validates and encodes what crosses each boundary; your workflow body and your client both see decoded, typed values — including typed *failures*, which land in the Effect error channel on the reading side instead of an exception to string-match.
- **Composition is Effect.** `Effect.raceFirst` a mailbox against a durable timer, wrap a step in `Workflow.withCompensation`, pipe a timeout onto an activity call — interruption, finalizers, and compensation compose the way the rest of your Effect code does, and cancellation reaches the server (an interrupted activity call is cancelled server-side, not abandoned).
- **One definition, both sides.** A workflow, activity, mailbox, update, or state cell is declared once and imported by the bundle, the worker, and every client. A misspelled name or drifted payload shape is a compile error, not a production incident.

## What Temporal keeps giving you

The Effect program runs *inside* the Temporal workflow sandbox — this is not a shim that hides Temporal. Every run is a real Temporal execution: visible in the Temporal UI, red on typed failure, Cancelled on interrupt, with real activity retries, real durable timers, signals and queries and updates in history, schedules, and Nexus. Your existing Temporal operational tooling — metrics, replayers, workers, deployment story — applies unchanged.

## When not to use it

- You do not run Temporal and do not want to: use Effect's own `effect/unstable/cluster` engine, or a job queue like [effect-mq](https://github.com/TeamWarp/effect-mq) if what you need is background jobs rather than long-running orchestration.
- You need queue semantics (rate limiting, priorities, fan-out over homogeneous work): that is a job queue's shape, not a workflow's.

## Where to next

- [Getting started](/guide/getting-started): install to running workflow in five minutes.
- [Defining workflows](/guide/defining-workflows): schemas, idempotency, execution ids.
- [How the engine works](/reference/how-it-works): the sandbox, the scheduler, and why replay stays deterministic.
