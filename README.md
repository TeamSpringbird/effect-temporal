# effect-temporal

Durable [Effect](https://effect.website) workflows on [Temporal](https://temporal.io):
author workflows with `effect/unstable/workflow` — schemas, typed errors,
composition — and run them on Temporal's retries, timers, signals, history,
and the operational tooling around them. One schema'd definition per
workflow, activity, or message channel, shared by the workflow bundle, the
worker, and every client — the two sides cannot drift.

**→ Documentation: [effect-temporal.com](https://www.effect-temporal.com)** —
start with
[What is effect-temporal?](https://www.effect-temporal.com/guide/introduction) and
[Getting started](https://www.effect-temporal.com/guide/getting-started).
The same pages live in [docs/](docs/) (`pnpm docs:dev` to browse locally).

```ts
import { Effect, Schema } from "effect";
import * as Workflow from "effect/unstable/workflow/Workflow";
import * as DurableClock from "effect/unstable/workflow/DurableClock";
import * as DurableDeferred from "effect/unstable/workflow/DurableDeferred";
import * as TypedActivity from "@springbird/effect-temporal/typed-activity";
import { callActivity, workflowBundle } from "@springbird/effect-temporal/engine-sandbox";
import { WorkflowClient } from "@springbird/effect-temporal/client";

// Define once — shared by the workflow bundle, the worker, and every client.
const OrderFlow = Workflow.make("orderFlow", {
  payload: { orderId: Schema.String },
  idempotencyKey: ({ orderId }) => orderId,
  success: Schema.String,
});
const Charge = TypedActivity.make("charge", {
  payload: { orderId: Schema.String },
  success: Schema.String,
});
const ManagerApproval = DurableDeferred.make("manager-approval", {
  success: Schema.String,
});

// The body is an Effect, running durably inside
// the Temporal sandbox — the same authoring Effect's other engines use:
const OrderFlowLive = OrderFlow.toLayer((payload) =>
  Effect.gen(function* () {
    const paid = yield* callActivity(Charge, { orderId: payload.orderId });
    yield* DurableClock.sleep({ name: "cooling-off", duration: "3 days" });
    const approver = yield* DurableDeferred.await(ManagerApproval);
    return `${paid}:approved-by:${approver}`;
  }),
);
export default workflowBundle(OrderFlowLive); // the bundle's dynamic default

// Drive it from ordinary Node — typed success/error, idempotent by digest id.
const program = Effect.gen(function* () {
  const wf = yield* WorkflowClient;
  return yield* wf.execute(OrderFlow, { orderId: "ord_123" });
});
```

```sh
pnpm add @springbird/effect-temporal   # or npm / yarn / bun
```

## Highlights

- **A real `WorkflowEngine`** — implements Effect's durable-workflow engine
  contract over Temporal, for codebases that already run Temporal and do not
  want a second durable-execution system (Effect's own `effect/unstable/cluster`
  engine persists to its own SQL tables).
- **One package, tree-shakeable modules** — `@springbird/effect-temporal/engine-sandbox`
  (workflow bundle), `/engine-client` + `/client` (ordinary Node),
  `/typed-activity` + `/activities` (worker), `/testing`, `/nexus`, `/lint`.
  Nexus, worker, and testing peers are optional.
- **Typed end to end** — payloads, results, and *failures* are schemas at
  every crossing: workflow results, activity calls, signals, queries, update
  responses. Typed failures land in the Effect error channel on the reading
  side; runs show red in the Temporal UI.
- **Entity workflows, complete** — `DurableMailbox` (repeated inbound
  signals), `DurableUpdate` (request/response with typed success *and*
  failure), `StateCell` (queryable published state, readable after the run
  closes), `continueAsNew`, and patch-marker versioning
  (`Versioning.match` chains) make the long-lived, observable, mutable
  entity expressible end to end.
- **Cancellation that composes** — workflow cancel interrupts the handler
  fiber (finalizers and `Workflow.withCompensation` run, their activity
  calls still work), and workflow-internal interruption — `Effect.timeout`,
  a lost race — cancels the in-flight server-side call rather than
  abandoning it.
- **Global idempotency** — the execution id (a digest of the payload's
  idempotency key) is the Temporal workflow id under `REJECT_DUPLICATE`; a
  repeated `execute`, a racing parent, or a Nexus caller attaches and gets
  the original result. Explicit caller-chosen workflow ids are first-class
  for brownfield fleets.
- **Deterministic by construction** — the whole Effect program runs inside
  the workflow sandbox on a microtask scheduler; clocks, randomness, and
  timers resolve to Temporal's replay-stable primitives. The mechanism is
  documented in [How the engine works](https://www.effect-temporal.com/reference/how-it-works).
- **Testing story** — `makeFakeTemporalClient` (typed start/signal/
  termination records, loud on everything unstubbed) for service tests, and
  `startWorkflowTestHarness` over Temporal's time-skipping test server for
  real workflow semantics.
- **Lint the footguns** — an oxlint/ESLint plugin ships in the package
  (`@springbird/effect-temporal/lint` + presets) for the authoring rules a linter can
  see.
- **Sample-backed** — every capability mirrors a
  [temporalio/samples-typescript](https://github.com/temporalio/samples-typescript)
  sample, each validated by a test: [EXAMPLES.md](https://github.com/TeamSpringbird/effect-temporal/blob/main/EXAMPLES.md) is the
  coverage matrix.

## Repository layout

```
src/                the published modules (one file per subpath export)
src/__tests__/      the test suite — every file boots a real Temporal
                    test server (time-skipping or local dev)
src/lint.js         the oxlint/ESLint plugin
oxlint-presets/     shipped lint presets
docs/               the VitePress documentation site
examples/           runnable end-to-end demos — no Docker, no setup:
                    order-saga (the one-shot saga: typed activities,
                    compensation, approval, cancellation) and subscription
                    (the long-lived entity: updates, mailboxes,
                    continue-as-new). pnpm run build, then
                    pnpm --dir examples/<name> start
EXAMPLES.md         temporalio/samples-typescript coverage matrix
```

## Development

```sh
pnpm install
pnpm run typecheck   # tsc, strict + exactOptionalPropertyTypes
pnpm run lint        # oxlint, dogfooding the shipped plugin
pnpm run build       # emit dist (ESM + declarations)
pnpm run test        # vitest — real Temporal test servers, ~15s warm
pnpm run docs:dev    # the docs site, locally
```

Run `build` before `test` on a fresh clone: the lint test consumes the
shipped preset, which resolves the plugin through the package's own
`exports` to `dist/lint.js`. Release history lives in
[CHANGELOG.md](https://github.com/TeamSpringbird/effect-temporal/blob/main/CHANGELOG.md).

## Versioning policy

Pre-1.0: **minor bumps may break APIs**. The `effect` peer dependency is
pinned **exactly** (currently `4.0.0-beta.101`) and on purpose — the engine
implements interfaces from `effect/unstable/*`, whose API can move between
releases. Each release states the one `effect` version it is built and
tested against; tracking a new `effect` release is a new release of this
package.

## Releasing

CI (`.github/workflows/ci.yml`) runs typecheck/lint/build/test/docs on every
push and PR. Publishing to npm happens on version tags:

```sh
# bump "version" in package.json, then
git tag v0.2.0 && git push --tags
```

The publish job runs `npm publish` with provenance; it needs an `NPM_TOKEN`
repository secret.

## Acknowledgements

Thanks to [Warp](https://warp.co) — open-sourcing
[effect-mq](https://github.com/TeamWarp/effect-mq) was the push for us to
open-source our own Effect wrapper around Temporal, and its codebase set the
bar for what clean Effect v4 packages look like. If effect-temporal's shape
feels familiar, that's why: where effect-mq is Effect-native background jobs
without extra infrastructure, effect-temporal is Effect-native durable
workflows for teams already running Temporal.

## License

MIT
