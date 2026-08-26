# Changelog

## Versioning policy

effect-temporal is pre-1.0: **minor bumps may break APIs**; patch bumps are fixes only.

The `effect` peer dependency is pinned **exactly** on purpose — the engine implements
interfaces from `effect/unstable/*`, whose API can move between releases. Each release
of this package states the one `effect` version it is built and tested against, and
tracking a new `effect` release is a new release of this package.

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
