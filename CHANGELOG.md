# Changelog

## Versioning policy

effect-temporal is pre-1.0: **minor bumps may break APIs**; patch bumps are fixes only.

The `effect` peer dependency is pinned **exactly** on purpose — the engine implements
interfaces from `effect/unstable/*`, whose API can move between releases. Each release
of this package states the one `effect` version it is built and tested against, and
tracking a new `effect` release is a new release of this package.

## Unreleased / planned — 0.5.0

The removal PR is this checklist. Everything below was deprecated in 0.4.0
with its replacement named in the JSDoc and reported by the
`prefer-definition` lint rule; nothing else changes.

- REMOVE the `typed-activity` module and its package export
  (`TypedActivity.make` → `defineActivity`; `PayloadOf`/`SuccessOf`/`ErrorOf`/
  `AnyTypedActivity`/`TypedActivity`/`TypedActivityOptions`/
  `DEFAULT_ACTIVITY_OPTIONS` → `definition`; `codecsFor`/`ACTIVITY_EXIT_TYPE`/
  `TypedActivityCodecs` → `wire`).
- REMOVE the `versioning` module and its package export (`match` → `versioned`,
  `version` → `version` from `definition`). Move `deprecateVersion` /
  `deprecatePatch` / `patched` (the Temporal-only retirement step) to `bundle`.
- REMOVE from `engine-sandbox`: `callActivity`, `takeMailbox`, `pollMailbox`,
  `takeUpdate`, `setStateCell`, `sleepUntil`, `continueAsNew`, and the
  `UpdateRequest<S, E, P>` alias. The Temporal `WorkflowOps` runtime keeps
  their bodies as private functions. `callRawActivity`, `offerMailbox`
  (workflow → workflow), `callNexusWorkflowOperation`, `SandboxRun`, and
  `workflowBundle` stay.
- REMOVE `make` from `mailbox`, `update`, `state-cell` (→ `defineMailbox`,
  `defineUpdate`, `defineState`) and drop those three package exports — the
  modules become internal wire homes (`MAILBOX_SIGNAL`, `WORKFLOW_UPDATE`,
  `STATE_CELL_QUERY`, codecs) consumed by the engine halves and `testing`.
- DELETE the deprecation entries from `prefer-definition` once the symbols are
  gone (the rule stays, empty tables are fine, so a future deprecation has a
  home).
- KEEP `histories/definition-order-0.3.0` in the replay drill; record a
  0.4.0 history alongside it.

## 0.4.0 (2026-09-10)

Closes the gaps between the `definition` module and the legacy authoring
surface: after this release a consumer imports `definition`, `bundle`,
`activities`, `client`/`engine-client`, `testing`, and (engine-level) `wire`
— never `engine-sandbox` except for the engine-level escape hatches,
never `typed-activity`, `versioning`, or `mailbox`.

- NEW: timers on the `WorkflowOps` seam — `sleep({ name, duration })` and
  `sleepUntil({ name, timestamp })` from `definition`, requiring only
  `WorkflowOps`. On Temporal they dispatch to `DurableClock.sleep` / the
  existing `sleepUntil` (same timestamp rule, now the shared
  `sleepUntilTarget`: zone-less and unparseable timestamps die). In
  `makeTestWorkflowOps` they follow Effect's `Clock`, so a `TestClock` can
  `adjust` past them — deliberately not instant, so a mailbox take racing a
  grace-period timer is testable in both orders (pinned on both engines).
- NEW: `continueAsNew(workflow, payload, options?)` on the seam and in
  `definition`. In memory it interrupts the handler fiber and records the
  continuation (`world.continuedAsNew`, `world.continuedAsNewOf(W)`), payload
  round-tripped through the workflow's payload schema (a schema-invalid
  payload dies, as on the wire).
- NEW: child workflows on the seam — `executeChild(workflow, payload,
  { discard? })` from `definition`. On Temporal it is upstream `execute`
  under the sandbox engine (identical commands: `startChild` with the digest
  id, `REQUEST_CANCEL`/`ABANDON`, attach-on-taken). `makeTestWorkflowOps`
  accepts `workflows: [handleWorkflow(Child, handler)]` and runs children
  in-process with schema round-tripping, discard-forks, and attach on a
  taken id.
- NEW: `versioned(site, { v1: run1, v2: run2 })` — the run-table form of
  `version` (key order is the chain order, oldest first), built on the same
  `WorkflowOps.version`; the `versioning-on-main-fiber` lint rule recognises
  it (alias-aware) alongside `version`.
- NEW: the activity type helpers live on `definition` — `PayloadOf`,
  `SuccessOf`, `ErrorOf`, `AnyTypedActivity`, `TypedActivity`,
  `TypedActivityOptions`, `DEFAULT_ACTIVITY_OPTIONS`. `codecsFor`,
  `TypedActivityCodecs`, and `ACTIVITY_EXIT_TYPE` moved to `wire` (the
  `typed-activity` module re-exports everything, deprecated).
- NEW: every client-side operation takes the declaration directly —
  `offerMailbox(Priority, …)`, `executeUpdate(SetAmount, …)`,
  `readStateCell(Status, …)`, `deferredState(Approval, …)` in `engine-client`
  and on `WorkflowClient`; the underlying primitive (`Priority.mailbox`, …) is
  still accepted (`MailboxLike` / `UpdateLike` / `StateCellLike` /
  `DeferredLike`). NEW `completeDeferred(Approval, { client, workflowId,
  exit })` / `wf.completeDeferred(Approval, workflowId, exit)`: the client
  half of `Approval.await` — same done-signal as `DurableDeferred.done`, no
  token, no `WorkflowEngine`. The workflow → workflow `offerMailbox`
  (engine-sandbox) accepts the declaration too.
- NEW: `makeFakeTemporalClient` gains `offer(Priority, workflowId, payload)`
  and `offersTo(Priority)` (decoded through the declaration) — no
  `MAILBOX_SIGNAL` import in consumer tests. The live harness client gains
  `offer` / `request` / `stateOf` / `resolve`, mirroring the in-memory world.
- NEW: the `bundle` module — `workflowBundle`'s home, "the one file the
  Temporal worker points at". `engine-sandbox` still exports it; its other
  exports are documented as engine-level (`callRawActivity`, workflow →
  workflow `offerMailbox`, `callNexusWorkflowOperation`, `SandboxRun`) or
  deprecated (below).
- NEW: `prefer-definition` lint rule, on in the `recommended` preset as an
  error: reports any import of a deprecated symbol from this package's
  modules (published specifier or relative path) with the replacement in the
  message, so `oxlint` fails on regressions.
- NEW: `replay-compat.test.ts` — a history recorded on 0.3.0
  (`fixtures/histories/definition-order-0.3.0`) replays through the current
  bundle. Wire identity is unchanged: activity types, signal/query/update
  names, and patch-marker ids are byte-identical.
- DEPRECATED (removed in 0.5.0; replacement in each JSDoc):
  `engine-sandbox`'s `callActivity`, `takeMailbox`, `pollMailbox`,
  `takeUpdate`, `setStateCell`, `sleepUntil`, `continueAsNew`, and the
  `UpdateRequest<S, E, P>` alias (use `definition`'s
  `UpdateRequest<P, S, E>` — one parameter order); `TypedActivity.make`;
  `DurableMailbox.make`, `DurableUpdate.make`, `StateCell.make`;
  `Versioning.match` / `Versioning.version`; the whole `typed-activity` and
  `versioning` modules.
- `defineDeferred` / `defineMailbox` / `defineUpdate` / `defineState` return
  named interfaces (`DefinedDeferred`, `DefinedMailbox`, `DefinedUpdate`,
  `DefinedState`) — structurally what they returned before.
- The repository authors with the new surface throughout: fixtures and
  examples import `workflowBundle` from `bundle`, the examples use `sleep`
  and `continueAsNew` from `definition` and drive declarations directly;
  the versioning-chain and loop fixtures deliberately stay on the deprecated
  calls so the replay drills keep covering them.
- `effect` peer/dev pin: `4.0.0-rc.112` (was `4.0.0-beta.101`); full suite
  and both examples green against it. `@temporalio/*` stays `1.19.0`.

Built and tested against `effect@4.0.0-rc.112` and `@temporalio/*@1.19.0`.

## 0.3.0 (2026-09-10)

- NEW: the `definition` module — declare each capability once and use it
  directly inside handlers: `defineActivity` (callable: `yield* Charge({ orderId })`),
  `defineDeferred` (`.await`), `defineMailbox` (`.take`/`.poll`),
  `defineUpdate` (`.take`), `defineState` (`.set`), plus `version` (patch-marker
  logic branches) and `evolved` (newest-first schema evolution with pure
  migrations). Every primitive requires only the `WorkflowOps` service — the
  one seam an engine implements — so handlers import nothing from
  `engine-sandbox` and are engine-agnostic. Client-side driving uses the
  declaration's underlying primitive (`U.update`, `M.mailbox`, `C.cell`,
  `D.deferred`) with the existing `engine-client` ops.
- NEW: `makeTestWorkflowOps` in `testing` — an in-memory `WorkflowOps`
  runtime (activities run their `handle` bindings with schema-validated
  payloads; deferreds/mailboxes/updates/state driven via
  `resolve`/`offer`/`request`/`stateOf`), so the same handler that runs on
  Temporal runs in a plain unit test with no engine and no test server.
- `workflowBundle` provides the Temporal `WorkflowOps` runtime to hosted
  layers automatically; bundle authoring is otherwise unchanged.
- The repository's fixtures, examples, and docs author with the `definition`
  module throughout. The low-level per-primitive calls (`callActivity`,
  `takeMailbox`, `pollMailbox`, `takeUpdate`, `setStateCell`) remain
  exported from `engine-sandbox` as the machinery underneath.

## 0.2.0 (2026-08-27)

- BREAKING: workflow bundles are authored with `Workflow.toLayer`, hosted behind `workflowBundle(layer)` — one
  dynamic default export per bundle, the same registration-driven authoring
  the cluster and in-memory engines use. Handlers can require services
  provided by ordinary Layers in the registration environment.
  `makeTemporalWorkflow` is REMOVED: one way to author. (If per-type
  `workflowDefinitionOptions` — e.g. Worker Versioning behavior — becomes a
  need, the worker-level `defaultVersioningBehavior` covers the dynamic
  workflow, and a per-type escape hatch can return later.)

## 0.1.1 (2026-08-26)

Initial public release. (0.1.0 was published without provenance during
release setup and unpublished; npm version numbers are never reusable.)

Initial standalone release, extracted from the Springbird monorepo.

- Temporal engine for `effect/unstable/workflow` (`Workflow`, `Activity`,
  `DurableClock`, `DurableDeferred`): sandbox half (`engine-sandbox`) and client
  half (`engine-client`), plus the `WorkflowClient` service.
- Durable extension primitives: `DurableMailbox`, `DurableUpdate`, `StateCell`,
  `continueAsNew`, patch-marker versioning (`Versioning.match`), schedules, and
  workflow-backed Nexus operations.
- Typed activities (`TypedActivity.make`, `callActivity`, `implementActivities`).
- Testing: `makeFakeTemporalClient` (typed, loud fake) and
  `startWorkflowTestHarness` (real test server).
- Lint plugin + oxlint presets for the authoring footguns.
- Fiber-level interruption (`Effect.timeout`, lost races) now cancels the
  in-flight server-side activity/Nexus call instead of abandoning it.
- Hardening from the pre-release audit: guarded workflow→workflow
  `DurableDeferred.done` against the closed-receiver race; Nexus operations
  decode their input before deriving the idempotent execution id (transforming
  schemas now digest correctly); the attach-poll loop stops on interruption
  instead of accruing history; malformed mailbox/update payloads are dropped or
  answered with a defect instead of poisoning the run; `sleepUntil` rejects
  zone-less date-time strings and unparseable timestamps; in-sandbox UTF-8
  digests match `TextEncoder` on lone surrogates.

Built and tested against `effect@4.0.0-beta.101` and `@temporalio/*@1.19.0`.
