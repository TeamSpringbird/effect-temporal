# Worker activity runner

The optional `@springbird/effect-temporal/worker` entrypoint supplies application
services, tracing, and Temporal cancellation to activity Effects:

```ts
import { makeActivityRunner } from '@springbird/effect-temporal/worker'

const runner = makeActivityRunner({
  context: await Effect.runPromise(runtime.contextEffect),
  heartbeatInterval: '5 seconds',
})

const activities = implementActivities(runner, [
  handle(Reserve, reserve),
])
```

Use this runner wherever the getting-started guide passes a custom `ActivityRunner`.
The existing runner interface remains supported. This is a worker-only module;
never import it into a workflow bundle.

Heartbeats are **opt-in**. Omit `heartbeatInterval` to disable them. When enabled,
configure `heartbeatTimeout` on the activity declaration with enough margin for
the heartbeat interval. Heartbeats report worker liveness, not business progress,
and stop when the activity exits. Temporal normally delivers cancellation to
long-running activities through heartbeats. The runner passes Temporal's abort
signal into Effect and waits for finalizers; cancellation is reported to Temporal
as cancellation, not as a retryable failure.

Declarations accept the SDK's full `ActivityOptions` (with `startToCloseTimeout`
required), including `heartbeatTimeout`, retry intervals, and cancellation type.
There is no need for an identity helper or type assertion around inline options.

Typed activity errors remain non-retryable domain outcomes. Defects remain
retryable infrastructure failures. This helper does not change that policy.
