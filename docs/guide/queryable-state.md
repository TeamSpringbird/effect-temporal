# Queryable state

A `StateCell` makes workflow state observable from outside: the workflow publishes typed snapshots to a named cell, and clients read the latest one through a Temporal query — mid-flight or **after the run has closed**.

```ts
// definitions
import * as StateCell from "@springbird/effect-temporal/state-cell";

export const CurrentLanguage = StateCell.make("current-language", {
  value: Schema.String,
});
```

## Publishing (workflow side)

```ts
import { setStateCell } from "@springbird/effect-temporal/engine-sandbox";

yield* setStateCell(CurrentLanguage, "english");
```

Each publish replaces the previous snapshot. Publish after every state change you want observers to see.

## Reading (client side)

```ts
const wf = yield* WorkflowClient;
const snapshot = yield* wf.readStateCell(CurrentLanguage, workflowId);
// Option.none() while the execution is unknown or the cell unpublished;
// Option.some(typed value) otherwise — including after the run closed.
```

Without the service, the standalone form is `readStateCell(CurrentLanguage, { client, workflowId })` from `@springbird/effect-temporal/engine-client`.

## Why snapshots, not query functions

Temporal query handlers are synchronous and read-only — they cannot run your Effect. So the abstraction is a **published snapshot** rather than an on-demand computation: the workflow decides when the observable state changes, the query just serves the latest value. Two consequences:

- readers never perturb the workflow (queries don't enter history);
- cells are **per-run** — a run started by [`continueAsNew`](/guide/continue-as-new) begins empty until it republishes.

## The entity pattern in full

Updates, a state cell, and an approval compose into the long-lived observable entity — this is the shape the package exists for:

```ts
export const effectMessageDemo = makeTemporalWorkflow(MessageDemo, () =>
  Effect.gen(function* () {
    let language: string = SUPPORTED_LANGUAGES[0];
    yield* setStateCell(CurrentLanguage, language);
    while (true) {
      const winner = yield* Effect.raceFirst(
        takeUpdate(SetLanguage).pipe(Effect.map((request) => ({ kind: "update" as const, request }))),
        DurableDeferred.await(Approved).pipe(Effect.map((approver) => ({ kind: "approved" as const, approver }))),
      );
      if (winner.kind === "approved") return `approved:${language} by ${winner.approver}`;

      const requested = winner.request.payload.language;
      if ((SUPPORTED_LANGUAGES as readonly string[]).includes(requested)) {
        yield* winner.request.respond(Exit.succeed(language));
        language = requested;
        yield* setStateCell(CurrentLanguage, language);
      } else {
        yield* winner.request.respond(Exit.fail(`unsupported:${requested}`));
      }
    }
  }),
);
```

Requests mutate state and get typed answers; observers poll the cell without touching the loop; a one-shot approval ends it. Every seam is a schema.
