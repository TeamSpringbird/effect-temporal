# Updates

An update is request/response into a running workflow — Temporal updates with typed channels. Unlike a fire-and-forget [mailbox](/guide/mailboxes) message, the caller blocks for the workflow's answer and receives the handler's typed success **or typed failure** back in its own Effect channels.

```ts
// definitions
import { defineUpdate } from "@springbird/effect-temporal/definition";

export const SetLanguage = defineUpdate("set-language", {
  payload: Schema.Struct({ language: Schema.String }),
  success: Schema.String, // the previous language
  error: Schema.String,   // "unsupported:<lang>"
});
```

## Serving requests (workflow side)

`.take` durably awaits the next request, in delivery order; each request carries a one-shot `respond`:

```ts
let language = "english";
while (true) {
  const request = yield* SetLanguage.take;
  const requested = request.payload.language;
  if (SUPPORTED.includes(requested)) {
    yield* request.respond(Exit.succeed(language)); // answer: the previous value
    language = requested;
  } else {
    yield* request.respond(Exit.fail(`unsupported:${requested}`));
  }
}
```

Serve updates from the main loop, or race a take against your other message sources — the [message-passing pattern](/guide/queryable-state#the-entity-pattern-in-full) combines updates, a state cell, and an approval in one loop.

Rules of the road:

- **Respond exactly once.** Responding twice is a defect (it fails the run — a bug in the body, surfaced loudly).
- **Respond before the run ends.** An unanswered update fails when the workflow completes; the caller sees a defect. Answer pending requests before returning or [continuing as new](/guide/continue-as-new).

## Calling (client side)

Callers address the declaration's underlying primitive, `SetLanguage.update`:

```ts
const wf = yield* WorkflowClient;
const previous = yield* wf.executeUpdate(SetLanguage.update, workflowId, { language: "french" });
// success channel: string (previous language)
// error channel: string (typed failure from respond)
```

Without the service — the standalone form `WorkflowClient` delegates to:

```ts
import { executeUpdate } from "@springbird/effect-temporal/engine-client";

const previous = yield* executeUpdate(SetLanguage.update, {
  client,
  workflowId,
  payload: { language: "french" },
});
```

An update expects an answer, so — unlike mailbox offers — a missing one is never a silent no-op: calling an **unknown or closed execution is a defect**, and so is a run that completes without answering. The one exception: a run **cancelled** while the update is pending surfaces to the caller as *interruption*, exactly as if it had been awaiting the cancelled run itself.

A request that fails the payload schema is the caller's bug: it is **answered with a defect** (so the caller hears about it) and **never reaches the workflow body** — one malformed request from a drifted producer or raw client cannot kill a long-lived run.

## Choosing between the message primitives

| | delivery | response | closed execution |
| --- | --- | --- | --- |
| deferred (`defineDeferred`) | one-shot signal | none (it *is* the workflow's input) | no-op |
| mailbox (`defineMailbox`) | repeated signals, ordered | none | no-op |
| update (`defineUpdate`) | repeated updates, ordered | typed success/failure per request | defect (interruption if cancelled while pending) |
