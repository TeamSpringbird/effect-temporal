# Cancellation & compensation

Cancellation in effect-temporal is Effect interruption, wired to Temporal's cancellation machinery so that both sides always agree.

## Cancelling a run

```ts
const wf = yield* WorkflowClient;
yield* wf.interrupt(workflowId);                // app code: the graceful stop
// yield* OrderFlow.interrupt(executionId);     // engine-level (Effect's own API)
// await handle.cancel()                        // any Temporal client/UI
```

Interrupting a closed or unknown execution is a no-op. For a hard stop that skips the unwind entirely — no finalizers, no compensation — `wf.terminate` maps to Temporal termination.

## What happens on cancel

1. In-flight activity and Nexus call scopes are **cancelled server-side** — the work is told to stop, not abandoned.
2. The handler fiber is interrupted: Effect **finalizers and `Workflow.withCompensation` steps run during the unwind** — and their own activity calls still work.
3. The run waits (bounded, 30s) for its cancelled calls to settle — a Nexus operation's cancellation handshake only proceeds while the caller is open.
4. The run records as **Cancelled** in Temporal; readers see an interrupted exit.

## Compensation

`Workflow.withCompensation` registers an undo step that runs if the workflow later fails or is cancelled — the saga pattern, in ordinary Effect:

```ts
const reservation = yield* Reserve({ sku }).pipe(
  Workflow.withCompensation((value) =>
    Release({ reservation: value }).pipe(Effect.asVoid),
  ),
);
```

On a typed failure or an interrupt downstream, `Release` runs during the unwind. On success, it never does. Compensation activities started during an unwind are fresh calls — nothing cancels them; they run to completion.

## Workflow-internal interruption

Interruption inside the body composes the same way. When `Effect.timeout` fires or an `Effect.race` is lost, the interrupted fiber's in-flight call is **cancelled server-side** while the run itself continues:

```ts
const fast = yield* Slow(payload).pipe(Effect.timeoutOption("30 seconds"));
// None on timeout — and the server-side activity received a cancel request,
// visible as ActivityTaskCancelRequested in history.
```

This matters for activities with side effects mid-flight and for anything holding resources: a timed-out call is stopped, not left running blind.

The one escape from this: a plain `Effect.promise(() => acts.foo())` has no cancellation scope — on interrupt its activity runs to completion server-side, abandoned. The [lint rules](/guide/lint-rules) flag that form.

## Interaction with the message primitives

During the unwind:

- pending [updates](/guide/updates) that never get a `respond` fail to their callers when the run completes as Cancelled;
- [mailbox](/guide/mailboxes) messages still buffered are lost with the run;
- [state cells](/guide/queryable-state) keep serving their last published snapshot after the run closes — publish a terminal status in a finalizer if observers need to see "cancelled".

## Children

Awaited [child workflows](/guide/child-workflows) are cancelled with the parent (engine-side best-effort plus `REQUEST_CANCEL` parent-close policy), and their own compensation runs. Discarded children are unaffected by design.

The reverse also holds: a child or attached execution cancelled **from outside** surfaces to the awaiting parent as fiber interruption — the parent unwinds (compensation runs) and its own run records as **Failed with a `CancelledFailure` cause** rather than Cancelled, since the parent itself was never cancel-requested. Callers of the parent still decode an interrupted exit.
