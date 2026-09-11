# Continue-as-new

Temporal caps a run's history; a workflow that loops forever — an entity, a poller, a batch cursor — must periodically **continue as new**: end the current run and atomically start a fresh one with the same workflow id and a reset history.

```ts
import { continueAsNew } from "@springbird/effect-temporal/definition";

const LoopDemoLive = LoopDemo.toLayer((payload) =>
  Effect.gen(function* () {
    yield* Record({ iteration: payload.iteration });
    if (payload.iteration >= 2) return `done:${payload.iteration}`;
    // Ends this run; the next starts with the carried payload.
    return yield* continueAsNew(LoopDemo, {
      requestId: payload.requestId,
      iteration: payload.iteration + 1,
    });
  }),
);
```

`continueAsNew(workflow, payload)` has type `Effect<never, never, WorkflowOps>` — nothing runs after it. The next run receives the payload you pass, so all carried state must be encodable through the payload schema; a payload that fails it dies **before** the run ends, on every engine.

## It unwinds as a throw

Like Temporal's native API, continue-as-new ends the run by throwing: Effect **finalizers and `Workflow.withCompensation` steps fire on the way out**. Call it:

- at iteration boundaries, in tail position;
- **outside compensation regions** — a compensated step between you and the `continueAsNew` will compensate as the run unwinds, which is almost never what an iteration boundary means;
- after draining mailboxes (below).

## What does not survive the run change

The new run starts with fresh state:

- **Mailbox buffers.** Messages offered but not yet taken are gone. Drain with the mailbox's `.poll` into carried state first:

  ```ts
  let pending: Report[] = [];
  while (true) {
    const next = yield* Reports.poll;
    if (Option.isNone(next)) break;
    pending.push(next.value);
  }
  return yield* continueAsNew(BatchDemo, { ...payload, carried: pending });
  ```

- **State cells.** Cells are per-run: readers see `None` in the new run until it republishes. Republish early in the body if outside observers poll the cell.
- **Pending updates.** An [update](/guide/updates) not yet answered fails to its caller when the run ends — answer before continuing.

## Memo

Pass a Temporal memo for the next run when you use memos for ops tooling:

```ts
yield* continueAsNew(LoopDemo, nextPayload, { memo: { cursor: "2026-08-01" } });
```

## In tests

In the [in-memory runtime](/guide/testing#the-in-memory-runtime) `continueAsNew` interrupts the handler fiber and records the continuation; the test reads it back typed:

```ts
const exit = yield* Effect.exit(loopHandler({ requestId: "r", iteration: 0 }).pipe(Effect.provide(world.layer)));
// exit is an interruption
const next = yield* world.continuedAsNewOf(LoopDemo);
// Option.some({ requestId: "r", iteration: 1 })
```

On the live harness a `wf.execute` follows the continue-as-new chain to the final run's result, as a real client does.
