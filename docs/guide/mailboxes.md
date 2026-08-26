# Mailboxes

A `DurableMailbox` is a durable inbound message queue for a workflow — the repeated-signal counterpart to the one-shot [`DurableDeferred`](/guide/timers-and-approvals#approvals-durabledeferred). Deliveries ride a Temporal signal (recorded in history, so consumption is deterministic on replay) and buffer until the workflow takes them.

```ts
// definitions — shared by the workflow body and every offering side
import * as DurableMailbox from "@springbird/effect-temporal/mailbox";

export const StateUpdates = DurableMailbox.make("state-updates", {
  payload: Schema.Union([
    Schema.Struct({ op: Schema.Literal("set"), key: Schema.String, value: Schema.Finite }),
    Schema.Struct({ op: Schema.Literal("del"), key: Schema.String }),
    Schema.Struct({ op: Schema.Literal("finish") }),
  ]),
});
```

## Taking messages (workflow side)

`takeMailbox` durably awaits the next message, in delivery order. The long-lived entity loop is the canonical shape:

```ts
import { setStateCell, takeMailbox } from "@springbird/effect-temporal/engine-sandbox";

const StateDemoLive = StateDemo.toLayer(() =>
  Effect.gen(function* () {
    const state = new Map<string, number>();
    while (true) {
      const update = yield* takeMailbox(StateUpdates);
      if (update.op === "finish") break;
      if (update.op === "set") state.set(update.key, update.value);
      else state.delete(update.key);
      yield* setStateCell(StateSnapshot, Object.fromEntries(state));
    }
    return "done";
  }),
);
```

The claim happens synchronously on the taking fiber after the wait, so an interrupted take never steals a message from a later one. Race a take against a timer for deadline patterns — see [Timers & approvals](/guide/timers-and-approvals#composing-time-with-messages).

`pollMailbox` takes without waiting — `None` when the buffer is empty. Its canonical use is draining buffers before [`continueAsNew`](/guide/continue-as-new#what-does-not-survive-the-run-change), since buffered messages do not survive the run change.

## Offering messages

From a **client** — via the `WorkflowClient` service:

```ts
const wf = yield* WorkflowClient;
yield* wf.offerMailbox(StateUpdates, workflowId, { op: "set", key: "a", value: 1 });
```

From **another workflow** (workflow → workflow):

```ts
import { offerMailbox } from "@springbird/effect-temporal/engine-sandbox";

yield* offerMailbox(Reports, { workflowId: orchestratorId, payload: report });
```

Offers are **fire-and-forget**: offering to a closed or unknown execution is a no-op — the receiver finishing first is a normal race, matching `DurableDeferred.done`. On the workflow side, any other delivery failure is also swallowed (logged as a worker warning, never fatal to the offering run); a mailbox offer is not a delivery guarantee. When the sender must *know* the message was handled, use an [update](/guide/updates) instead.

## Ordering and durability

- Messages are delivered in signal order, per mailbox.
- Buffered messages survive worker crashes and replays — they are history, not memory.
- Buffered messages do **not** survive `continueAsNew` — drain first.
- A message that fails the payload schema — raw signal access, a drifted producer, a schema tightened while old messages sat buffered in history — is **dropped at take time with a worker-log warning**, matching the fire-and-forget delivery contract rather than poisoning the run. (`pollMailbox` drops the same way.)
