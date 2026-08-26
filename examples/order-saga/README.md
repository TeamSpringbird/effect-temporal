# Order saga — runnable end-to-end example

The common use cases working together in one small app: **typed activities**
with a **typed domain failure**, a **compensated step**, a **durable timer**,
an **external approval**, **queryable state**, the **idempotency contract**,
and **graceful cancellation** — authored with Effect, executed by Temporal.

```
src/definitions.ts   the shared contract (workflow, activities, approval,
                     state cell) — imported by all three worlds
src/workflows.ts     the workflow bundle (runs in the Temporal sandbox)
src/main.ts          worker + client + the three demo scenarios
```

## Run it

From the repository root:

```sh
pnpm install
pnpm run build                      # the example consumes the built package
pnpm --dir examples/order-saga start
```

No Docker and no Temporal setup: the demo boots a local Temporal dev server
via `@temporalio/testing` (downloaded automatically on first run). To run
against a real cluster instead, swap the `TestWorkflowEnvironment` block in
`main.ts` for a `Connection.connect(...)` + `new Client(...)` and point the
`Worker` at that connection.

## What to watch for

- **A. Happy path** — the state cell is read mid-flight without touching the
  run; the approval arrives as a signal from outside; a second `execute`
  with the same payload *attaches* and returns the same result with
  `reserve` still called exactly once.
- **B. Declined card** — `CardDeclined` is a typed error, not an exception:
  it lands in the Effect error channel, is never retried, and the
  compensation has already released the reservation. The run shows FAILED in
  the Temporal UI with the typed exit riding the failure details.
- **C. Cancellation** — `wf.interrupt` cancels the run mid-timer: the body
  unwinds, the compensation releases, and the run records CANCELLED.
