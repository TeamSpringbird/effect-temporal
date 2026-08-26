# Child workflows

Calling one workflow's `execute` inside another's body starts a Temporal **child workflow**. Both must be shim workflows exported from the **same bundle** — a child's Temporal type resolves within the bundle that runs the parent.

```ts
const ParentDemoLive = ParentDemo.toLayer((payload) =>
  Effect.gen(function* () {
    const reservation = yield* callActivity(Reserve, { sku: payload.sku, quantity: 1 });

    // Starts a Temporal child workflow; typed results and failures compose
    // into the parent like any Effect.
    const child = yield* ChildDemo.execute({
      requestId: `${payload.requestId}-child`,
    });

    return `${reservation}|${child}`;
  }),
);
```

A child's typed failure lands in the parent's error channel; a child's defect dies the parent step. Catch, retry, or compensate with ordinary Effect combinators.

## Lifetime: awaited vs discarded

- **Awaited children** (the default) carry `REQUEST_CANCEL` parent-close policy: cancelling the parent cancels them, so their own compensation runs — even when the parent is terminated outright.
- **Discarded children** — `ChildDemo.execute(payload, { discard: true })` — outlive the parent (`ABANDON`): fire-and-forget spawns whose lifecycle is their own.

## Idempotency stays global

A child's workflow id is its **digest execution id**, exactly as if a client had started it — the [idempotency contract](/guide/defining-workflows#idempotency-and-execution-ids) does not stop at process boundaries. When the id is already taken — another parent started it, or a completed earlier run holds it under `REJECT_DUPLICATE` — the parent **attaches** and returns that execution's result instead of failing.

Attach works through the bridge activity (`makeEffectWorkflowActivities` — a workflow cannot await an execution it does not own), polling the foreign execution's status:

- each poll costs one activity plus one timer of history;
- the interval backs off exponentially from 5s to a 60s cap;
- fine for quick attaches — hour-scale foreign runs still accrue history, so don't lean on attach as a long-wait primitive.

```ts
// Two parents race to start the same child: one starts it, the other
// attaches; both see the same typed result.
const result = yield* SharedStep.execute({ dedupeKey });
```

## Cancellation composes

Interrupting the parent interrupts awaited children through both channels: the engine cancels them best-effort as the parent unwinds, and `REQUEST_CANCEL` is the server-side backstop. A cancelled child surfaces in the parent as an interrupted exit; the parent's finalizers and compensation still run. See [Cancellation & compensation](/guide/cancellation).

The other direction composes too: a child (or attached execution) **cancelled from outside** surfaces to the awaiting parent as fiber interruption. The parent unwinds — compensation runs — and its own run records as **Failed with a `CancelledFailure` cause**, not Cancelled (the parent itself was never cancel-requested); callers of the parent still decode an interrupted exit.
