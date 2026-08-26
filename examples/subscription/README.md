# Subscription — the long-lived entity example

Temporal's canonical subscription workflow, in Effect: a **long-lived
entity** whose loop races the next billing timer against inbound messages.
Where [order-saga](../order-saga/README.md) is a one-shot saga, this is the
other archetype — and it exercises the other half of the package:

- **billing cycles** on durable timers inside an entity loop
- **`DurableUpdate`** — a plan change is request/response with typed
  channels: the caller receives the previous plan, or a typed rejection
- **`DurableMailbox`** — cancellation is a fire-and-forget message
- **`continueAsNew`** every couple of cycles: history stays flat while the
  workflow id and carried state survive the run change — including the
  documented drain-the-mailbox-first rule, so a cancellation racing the
  final cycle is honored instead of lost
- **`StateCell`** — status is readable mid-flight, republished after every
  continue-as-new, and still readable after the entity closes

```
src/definitions.ts   the shared contract (workflow, activity, update,
                     mailbox, state cell)
src/workflows.ts     the entity loop (runs in the Temporal sandbox)
src/main.ts          worker + client + the narrated lifecycle
```

## Run it

From the repository root:

```sh
pnpm install
pnpm run build                        # the example consumes the built package
pnpm --dir examples/subscription start
```

No Docker and no Temporal setup: the demo boots a local Temporal dev server
via `@temporalio/testing` (downloaded automatically on first run).

## What to watch for

- The status cell is read **mid-flight** without perturbing the run, again
  right after continue-as-new (republished carried state), and again
  **after the run closes**.
- The update's two outcomes are both **typed**: the previous plan in the
  success channel, `"plan-below-minimum"` in the error channel.
- Step 3 polls `describe()` until the **runId changes** while the workflow
  id stays put — that's continue-as-new keeping the entity addressable
  forever with bounded history.
- The final `wf.execute` **attaches to the run chain** and returns the
  entity's closing summary.
