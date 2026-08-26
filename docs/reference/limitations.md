# Limitations

The honest list. Most of these are deliberate trade-offs; the rest are edges of the current implementation.

## Pinned `effect` version

The engine implements interfaces from `effect/unstable/*`, whose API can move between releases — the `effect` peer dependency is pinned **exactly**, and tracking a new `effect` release is a new release of this package. Plan for lockstep upgrades until Effect v4's workflow API stabilizes.

## Attach polling costs history

Attaching to a foreign execution (a [child id](/guide/child-workflows#idempotency-stays-global) already taken) polls through the bridge activity: one activity plus one timer of history per poll, backing off from 5s to a 60s cap. Fine for quick attaches; hour-scale foreign runs still accrue history. Don't use attach as a long-wait primitive — share results through a workflow both parties `execute`, or a [mailbox](/guide/mailboxes).

## Effect timers are history events

`Effect.sleep`, `Effect.timeout*`, and `Schedule` delays inside the sandbox become durable Temporal timers — [deterministic and correct](/reference/how-it-works#clocks-three-of-them-all-agreeing), but each is a history event. A retry policy with many short delays writes many timers. Prefer activity-level retries (the activity definition's `retry` options) for retrying I/O, and named `DurableClock.sleep` for waits that matter.

## Schedule fires sit outside idempotency

Fired runs get schedule-generated workflow ids and are addressed by those ids; a digest-addressed `execute` can run concurrently with a fired run. See [Schedules](/guide/schedules#fired-runs-sit-outside-the-idempotency-contract).

## Nexus attach refuses closed executions

A repeated Nexus operation call attaches to a running execution but fails loudly against a closed one — deliberately stricter than direct `execute`. See [Nexus](/guide/nexus#serving-a-workflow-as-an-operation).

## Mailbox offers are best-effort

Offers to closed executions are dropped by design, and on the workflow side any delivery failure is logged and swallowed — a mailbox offer is not a delivery guarantee. Use an [update](/guide/updates) when the sender needs an answer.

## The sandbox is still the sandbox

Temporal's determinism constraints apply to the code you write inside `makeTemporalWorkflow`: no direct I/O, no non-deterministic module state, versioned changes to in-flight code. The [authoring rules](/guide/lint-rules) and the engine remove the accidental ways to trip — they cannot remove the model.

## Polyfilled sandbox globals

The sandbox exposes no `crypto`, `TextEncoder`, or `performance`; this package installs deterministic polyfills for exactly the surface Effect needs (SHA-256 digests, UTF-8 encoding, `performance.now`). Code relying on other Node globals inside a workflow body will still fail — reach for an activity instead.
