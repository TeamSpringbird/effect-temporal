# Changelog

## Versioning policy

effect-temporal is pre-1.0: **minor bumps may break APIs**; patch bumps are fixes only.

The `effect` peer dependency is pinned **exactly** on purpose — the engine implements
interfaces from `effect/unstable/*`, whose API can move between releases. Each release
of this package states the one `effect` version it is built and tested against, and
tracking a new `effect` release is a new release of this package.

## 0.3.0 (unreleased)

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
