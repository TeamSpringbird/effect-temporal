# Activities

All I/O in a workflow body goes through a Temporal activity. Two forms exist, and the choice is a rule, not a taste:

| Form | Use when | Wire | Failure semantics |
| --- | --- | --- | --- |
| **Typed** — `TypedActivity.make` + `callActivity` + `implementActivities` | you implement the activity — **the default** | schema-validated both ways | typed failures land in the Effect error channel, non-retryable; infra errors retry then die |
| **Raw** — `proxyActivities` + `callRawActivity` | the activity is not yours (existing Temporal activities, another team's worker), or you need proxy options per call site | whatever the proxy's functions take, unvalidated | everything thrown is a defect once retries exhaust; no typed channel |

Both run under the same per-call cancellation scope. If you find yourself
building a typed error channel on top of a raw call, that's the sign you
wanted a `TypedActivity`.

::: info Portability note
Unlike workflow definitions — which are pure upstream API and run on any
engine — `TypedActivity` and `callActivity` are this package's own, and a
workflow using them is Temporal-shaped. That is a deliberate consequence of
Temporal's execution model: upstream `Activity.make` carries its
implementation as a **closure** over workflow state, which other engines can
run in-process, but Temporal executes activities on a separate worker that a
closure cannot reach. `TypedActivity` is the serializable projection that
boundary forces: a name plus schemas, implemented on the worker. Upstream
`Activity.make` still works here as an in-sandbox typed seam (see below) —
it just isn't where I/O can live under Temporal.
:::

## Typed activities

Declare once; the definition is temporal-free and loads in the sandbox bundle, the worker, and clients alike.

```ts
import * as TypedActivity from "@springbird/effect-temporal/typed-activity";

export const Reserve = TypedActivity.make("reserve", {
  payload: { sku: Schema.String, quantity: Schema.Finite },
  success: Schema.String,
  error: Schema.TaggedStruct("OutOfStock", { sku: Schema.String }),
  options: { startToCloseTimeout: "1 minute", retry: { maximumAttempts: 3 } },
});
```

Call it from a workflow body with `callActivity`:

```ts
import { callActivity } from "@springbird/effect-temporal/engine-sandbox";

const reservation = yield* callActivity(Reserve, { sku, quantity: 1 });
// reservation: string; error channel: { _tag: "OutOfStock"; sku: string }
```

The payload is schema-encoded onto the wire and validated before the implementation runs; the result decodes back; a typed failure lands in the Effect error channel.

### Failure and retry semantics

The line between the two failure kinds is the line between domain outcomes and infrastructure:

- **Typed failure** (matches the `error` schema): a domain outcome. The worker throws it as a **non-retryable** `ApplicationFailure` carrying the encoded value; the calling workflow decodes it into its error channel. No retries — retrying "out of stock" doesn't restock the shelf.
- **Everything else** (defects, thrown infra errors): a retryable activity failure. Temporal retries per the definition's `retry` options; once retries exhaust, the workflow sees a defect.

### Implementing on the worker

`implementActivities` binds definitions to Effect handlers over an `ActivityRunner` — the seam where your runtime, spans, and error reporting live:

```ts
import { handle, implementActivities, type ActivityRunner } from "@springbird/effect-temporal/activities";

const runner: ActivityRunner<never> = {
  run: (_name, _payload, effect) => Effect.runPromiseExit(effect),
};

const activities = implementActivities(runner, [
  handle(Reserve, ({ sku, quantity }) =>
    quantity <= onHand(sku)
      ? Effect.succeed(`${sku}x${quantity}`)
      : Effect.fail({ _tag: "OutOfStock", sku } as const),
  ),
]);
```

Payloads are decoded (validated) before a handler runs — a payload failing its schema throws non-retryable. The returned record keeps literal keys, so spreading tables together preserves per-name lookups and a misspelled name is a compile error.

An `ActivityRunner` with a service requirement `R` lets handlers use your application services; supply the runner from your `ManagedRuntime`.

## Raw calls

`callRawActivity` invokes any Temporal activity proxy as an Effect:

```ts
import { proxyActivities } from "@temporalio/workflow";
import { callRawActivity } from "@springbird/effect-temporal/engine-sandbox";

const acts = proxyActivities<{ charge(orderId: string): Promise<string> }>({
  startToCloseTimeout: "10 minutes",
});

const paid = yield* callRawActivity(() => acts.charge(orderId));
```

Use it for activities you don't control the definition of, or when you need proxy options per call site.

### Optionally naming a raw call: `Activity.make`

You'll see raw calls wrapped in the upstream `Activity.make` in this package's own test fixtures:

```ts
const paid = yield* Activity.make({
  name: "charge",
  success: Schema.String,
  execute: callRawActivity(() => acts.charge(orderId)),
});
```

Be clear about what that wrapper is: under this engine `Activity.make` is a **typed seam in the Effect program, not a Temporal Activity** — it gives the step a name, Effect-level success/error schemas, and `Activity.CurrentAttempt` in context, while durability still comes entirely from the `callRawActivity` inside it. It does **not** validate the wire or create a typed failure channel from the worker — that's what `TypedActivity` is for. Wrap a raw call when the step deserves a name and a schema'd shape in your program (the fixtures do it because they mirror the upstream API's style); call `callRawActivity` bare when it doesn't.

## Cancellation reaches the server

Both `callActivity` and `callRawActivity` run under a per-call cancellation scope. When the calling fiber is interrupted — the workflow is cancelled, an `Effect.timeout` fires, an `Effect.race` is lost — the in-flight server-side activity is **cancelled**, not abandoned:

```ts
const result = yield* callActivity(Slow, payload).pipe(
  Effect.timeoutOption("30 seconds"),
); // None on timeout; the server-side activity receives a cancellation request
```

A plain `Effect.promise(() => acts.foo())` also works inside a workflow body, but its activity is *not* cancelled on interrupt — it runs to completion server-side, abandoned by the closing run. The [lint rules](/guide/lint-rules) steer you toward the cancellable forms.

## The attach bridge

Every worker that runs these workflows must also register `makeEffectWorkflowActivities(client)`. It provides the one internal activity the engine needs: polling a foreign execution's result during [child-workflow attach](/guide/child-workflows), which neither `startChild` nor external handles can do.

```ts
activities: {
  ...implementActivities(runner, bindings),
  ...makeEffectWorkflowActivities(client),
}
```
