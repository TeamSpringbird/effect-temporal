# How the engine works

Effect and Temporal each manage their own execution and their own clocks. This page is the account of how the two runtimes are made to agree — what makes it safe to run a full Effect fiber runtime inside Temporal's deterministic replay sandbox.

## The two halves

`effect/unstable/workflow` defines workflow programs against an abstract `WorkflowEngine`. This package implements that engine twice:

- **`engine-sandbox`** runs *inside* the Temporal workflow sandbox. `workflowBundle` builds the bundle's one dynamic workflow function from `Workflow.toLayer` registrations: per run it decodes the payload, provides the engine, runs your handler as an Effect program, and encodes the exit. Engine operations map to sandbox primitives — child starts to `startChild`, deferreds to signals + `condition()`, clocks to durable timers.
- **`engine-client`** runs in ordinary Node. Engine operations map to Temporal client calls — `execute` starts (or attaches) and awaits, `poll` describes, `interrupt` cancels.

One consequence worth knowing: **nothing ever suspends**. Effect's engine contract has a suspend/resume path for engines that park workflows; this engine blocks durably instead (a `condition()` or timer in the sandbox), so `resume` is a no-op and a `Suspended` result is a bug.

## Execution: Effect fibers on the sandbox's microtasks

Effect's default scheduler falls back to `setTimeout` where `setImmediate` is missing — and in the sandbox, `setTimeout` is a **durable Temporal timer**. Left alone, every fiber yield would write a timer into history. The engine therefore runs the program on a scheduler whose flushes ride `Promise.resolve().then` — plain microtasks, invisible to history.

Determinism holds because everything the fibers can observe is deterministic: microtask ordering is a pure function of the code, and the only environmental inputs — time, randomness, activity results, signal deliveries — are Temporal's replay-stable versions of themselves.

## Clocks: three of them, all agreeing

- **Effect's millisecond clock** (`Clock.currentTimeMillis`) reads `Date.now()`, which the sandbox patches to deterministic workflow time. Free.
- **Effect's nanosecond clock** reads `performance.now()` where `process.hrtime` is absent — and caches a wall-clock origin on first read, once per V8 context. Under `reuseV8Context` that cache is *shared across workflow instances*, which would bind every later instance to whichever instance read first. The sandbox polyfill pins `performance.now = Date.now`, collapsing the cached origin to zero — every instance sharing the cache stays on the one deterministic clock.
- **Effect's sleep** (`Effect.sleep`, and everything built on it — `timeout`, `Schedule` delays) lands on `setTimeout`: a durable timer. Deterministic, cancellable, real — just remember each one is a history event.

`Effect.Random` needs no treatment: it delegates to `Math.random()` per call, which the sandbox patches to a per-run deterministic PRNG.

## Instance isolation under reuseV8Context

Temporal's default worker mode evaluates each module once per V8 context and swaps `globalThis` per workflow instance — so module-level mutable state is shared across instances. The engine keeps **all run state in an Effect service** (`SandboxRun`) created per run and threaded through context; the module level holds only immutable values (codecs, shared activity proxies, the scheduler — which owns no task state). The [lint rule](/guide/lint-rules) `no-module-level-mutable` holds your workflow code to the same standard.

## Cancellation: one non-cancellable scope, per-call child scopes

Temporal scope association follows promise chains, but the Effect scheduler multiplexes fibers through shared microtasks — so scope nesting cannot express per-fiber cancellation. The engine inverts the model:

- the run executes in one **non-cancellable** Temporal scope;
- every activity/Nexus call gets its **own** `CancellationScope`, registered with the run;
- workflow cancellation is observed on the Effect side (a racer watching `cancelRequested`) and translated: cancel the in-flight call scopes, interrupt the handler fiber (finalizers and compensation run), wait bounded for cancelled calls to settle, rethrow `CancelledFailure` so Temporal records Cancelled;
- fiber-level interruption (timeouts, races) cancels its call's scope directly.

Compensation calls made during the unwind get fresh scopes nothing cancels — undo work is not undone.

## The wire

Every crossing — arguments, results, signals, queries, update payloads, failure details — carries **schema-encoded JSON**. Exits get an explicit JSON codec (`Schema.Exit`'s encoded form is still an `Exit` instance, which would not survive Temporal's payload converter). Typed failures and defects fail the run with the encoded exit riding an `ApplicationFailure`'s details; every reading side — client `execute`, `poll`, a parent awaiting a child, a Nexus caller — classifies the thrown error chain and decodes the exit back into typed channels.

## The polyfills

The sandbox exposes no `crypto`, `TextEncoder`, or `performance`, and Effect needs all three (child-id digests want SHA-256; the clock wants `performance.now`). Deterministic polyfills — `@noble/hashes` plus a UTF-8 encoder composed from ECMAScript built-ins — are installed per run *and* at module evaluation in-sandbox, and are pinned byte-for-byte against Node's implementations by the test suite, so in-sandbox child ids always equal `MyChild.executionId(payload)` computed outside.
