/**
 * Sandbox half: `workflowBundle(layer)` hosts plain
 * `Workflow.toLayer` registrations behind one dynamic Temporal workflow —
 * export it as the workflow bundle's DEFAULT export and every registered
 * tag becomes a startable Temporal workflow type.
 *
 * The whole Effect program runs inside the workflow sandbox, on a
 * microtask-driven scheduler (the sandbox has no `setImmediate`, and Effect's
 * default scheduler would fall back to `setTimeout` — a durable timer per
 * fiber yield). `Activity` is a typed seam, not a Temporal Activity:
 * durability comes from the Temporal activity proxies, timers, and signals
 * the effects call, each memoized in history. All I/O must go through an
 * activity proxy via `callRawActivity`; anything else is
 * nondeterministic on replay.
 *
 * Cancellation: the run executes in one non-cancellable Temporal scope —
 * scope association follows promise chains and the Effect scheduler
 * multiplexes fibers through shared microtasks, so association is per-run
 * and finer scoping cannot be expressed by nesting. On cancel, the run
 * cancels each in-flight `callRawActivity` scope, interrupts the
 * handler fiber (running finalizers and `Workflow.withCompensation` steps,
 * whose fresh activity scopes nothing cancels), and rethrows
 * `CancelledFailure` so Temporal records the run as Cancelled.
 *
 * `deferredResult` blocks durably instead of suspending, so the Encoded
 * contract's suspend/resume path is unreachable here.
 *
 * @since 0.1.0
 */

// This module IMPLEMENTS callRawActivity: its internals are the raw
// sandbox promises the lint rule steers workflow authors away from.
// oxlint-disable effect-temporal/prefer-call-temporal-activity

import * as Cause from "effect/Cause";
import * as Context from "effect/Context";
import * as Duration from "effect/Duration";
import type { Scope } from "effect/Scope";
import * as ScopeImpl from "effect/Scope";
import * as Layer from "effect/Layer";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import { MixedScheduler } from "effect/Scheduler";
import * as Activity from "effect/unstable/workflow/Activity";
import * as Workflow from "effect/unstable/workflow/Workflow";
import * as WorkflowEngine from "effect/unstable/workflow/WorkflowEngine";
import {
  ApplicationFailure,
  CancellationScope,
  type ChildWorkflowHandle,
  CancelledFailure,
  condition,
  log,
  ContinueAsNew,
  continueAsNew as temporalContinueAsNew,
  makeContinueAsNewFunc,
  defineUpdate,
  defineQuery,
  defineSignal,
  getExternalWorkflowHandle,
  proxyActivities,
  setHandler,
  sleep,
  startChild,
  workflowInfo,
} from "@temporalio/workflow";
import {
  asJsonCodec,
  DEFERRED_DONE_SIGNAL,
  DEFERRED_STATE_QUERY,
  decodeDeferredExit,
  encodeDeferredExit,
  EXIT_FAILURE_TYPE,
  makeClassifyThrown,
  wireCodecsFor,
  type DeferredDoneSignalPayload,
  type EffectWorkflowBridgeResult,
} from "./wire.js";
import * as Clock from "effect/Clock";
import * as DurableClock from "effect/unstable/workflow/DurableClock";
import { ensureSandboxPolyfills } from "./sandbox-polyfills.js";
import {
  ACTIVITY_EXIT_TYPE,
  codecsFor,
  type AnyTypedActivity,
  type ErrorOf,
  type PayloadOf,
  type SuccessOf,
} from "./typed-activity.js";
import {
  MAILBOX_SIGNAL,
  mailboxCodec,
  type DurableMailbox,
  type MailboxSignalPayload,
} from "./mailbox.js";
import { STATE_CELL_QUERY, stateCellCodec, type StateCell } from "./state-cell.js";
import * as DurableDeferred from "effect/unstable/workflow/DurableDeferred";
import { WorkflowOps, type WorkflowOpsRuntime } from "./definition.js";
import * as Versioning from "./versioning.js";
import {
  updateCodec,
  WORKFLOW_UPDATE,
  type DurableUpdate,
  type WorkflowUpdatePayload,
} from "./update.js";

// Scheduler flushes ride `Promise.resolve().then` (`queueMicrotask` does not
// exist in the sandbox). The no-op canceller is safe: the only caller that
// cancels then drains synchronously, so the stray microtask re-enters
// `runTasks()` on empty buckets.
const sandboxScheduler = new MixedScheduler("async", (f: () => void) => {
  void Promise.resolve().then(f);
  return () => {};
});

const deferredDoneSignal = defineSignal<[DeferredDoneSignalPayload]>(DEFERRED_DONE_SIGNAL);
const deferredStateQuery = defineQuery<unknown, [string]>(DEFERRED_STATE_QUERY);
const mailboxSignal = defineSignal<[MailboxSignalPayload]>(MAILBOX_SIGNAL);
const stateCellQuery = defineQuery<unknown, [string]>(STATE_CELL_QUERY);
const workflowUpdate = defineUpdate<unknown, [WorkflowUpdatePayload]>(WORKFLOW_UPDATE);

interface RunState {
  /** deferred name → wire-encoded exit, set by the done-signal handler. */
  readonly deferredExits: Map<string, unknown>;
  /** clock-deferred name → pending durable-timer parameters. `doneExit` is
   * the encoded-leaf void exit the clock's own schema round-trips —
   * precomputed at schedule time so the two sides cannot disagree. */
  readonly clocks: Map<
    string,
    { readonly millis: number; readonly doneExit: Exit.Exit<unknown, unknown> }
  >;
  /** Per-call scopes of calls currently in flight (activities, nexus
   * operations), with their promises; cancelled as a set when the run is
   * interrupted. Post-interrupt calls (compensation) are not registered —
   * nothing would cancel them. */
  readonly inFlight: Map<CancellationScope, Promise<unknown>>;
  /** Promises of calls cancelled by the interrupt, awaited (bounded) before
   * the run completes: a nexus operation's cancellation handshake needs the
   * caller still open. */
  readonly cancelWaits: Promise<unknown>[];
  /** mailbox name → wire-encoded payloads awaiting a take, in signal order. */
  readonly mailboxes: Map<string, unknown[]>;
  /** cell name → latest wire-encoded published snapshot. */
  readonly stateCells: Map<string, unknown>;
  /** update name → requests awaiting a take, in delivery order. */
  readonly updates: Map<string, PendingUpdate[]>;
  interrupted: boolean;
}

interface PendingUpdate {
  /** Wire-encoded request payload. */
  readonly payload: unknown;
  /** Wire-encoded response exit, set by `respond`. */
  wireExit: unknown;
  done: boolean;
}

/**
 * The current run's state, threaded through Effect context rather than
 * module state: under the worker's default `reuseV8Context`, module-level
 * variables are shared across every workflow instance on a thread. Per-run
 * closures and Effect context are the only isolation the sandbox guarantees.
 *
 * @since 0.1.0
 * @category models
 */
export interface SandboxRun {
  readonly _: unique symbol;
}
const SandboxRunTag = Context.Service<SandboxRun, RunState>(
  "effect-temporal/SandboxRun",
);

/**
 * Invoke a Temporal activity proxy as an Effect whose in-flight server-side
 * work is cancelled when the CALLING FIBER is interrupted — whether by the
 * workflow being cancelled, or by workflow-internal interruption such as
 * `Effect.timeout` or a lost `Effect.race`:
 *
 * ```ts
 * Activity.make({
 *   name: "charge",
 *   success: Schema.String,
 *   execute: callRawActivity(() => acts.charge(orderId)),
 * })
 * ```
 *
 * A plain `Effect.promise(() => acts.charge(...))` also works, but its
 * activity is not cancelled on interrupt — it runs to completion
 * server-side, abandoned by the closing run.
 *
 * @since 0.1.0
 * @category workflow
 */
export const callRawActivity = <A>(create: () => Promise<A>): Effect.Effect<A, never, SandboxRun> =>
  Effect.gen(function* () {
    const run = yield* SandboxRunTag;
    const scope = new CancellationScope();
    let inFlight: Promise<unknown> | undefined;
    return yield* Effect.promise(() => {
      const promise = scope.run(create);
      inFlight = promise;
      if (!run.interrupted) {
        run.inFlight.set(scope, promise);
        void promise
          .finally(() => run.inFlight.delete(scope))
          .catch(() => {
            // Rejection is delivered through `promise` itself; this chain
            // only exists for the bookkeeping.
          });
      }
      return promise;
    }).pipe(
      // The zero-arity promise carries no abort channel, so interruption of
      // this fiber unwinds immediately — the scope cancel is what tells the
      // server to stop the in-flight work rather than abandon it. Idempotent
      // with the run-level cancel sweep (cancelling an already-cancelled
      // scope is a no-op), and the cancelled call still lands in
      // `cancelWaits` so a run that later ends Cancelled waits for it to
      // settle like any run-cancelled call.
      Effect.onInterrupt(() =>
        Effect.sync(() => {
          scope.cancel();
          if (inFlight !== undefined && run.inFlight.delete(scope)) {
            run.cancelWaits.push(inFlight.catch(() => undefined));
          }
        }),
      ),
    );
  });

const classifyActivityThrown = makeClassifyThrown(
  { ApplicationFailure, CancelledFailure },
  ACTIVITY_EXIT_TYPE,
);

/** One proxy per distinct name + option set (two definitions sharing a name
 * with different options each get their own, honoring their own timeouts);
 * immutable, so sharing across workflow instances under `reuseV8Context` is
 * safe. */
const typedActivityProxies = new Map<string, (wire: unknown) => Promise<unknown>>();

const typedActivityProxyKey = (activity: AnyTypedActivity): string =>
  `${activity.name}|${JSON.stringify(activity.options)}`;

/**
 * Call a `TypedActivity` definition: the payload is schema-encoded onto the
 * wire, the result decoded, and a typed failure the worker raised (a
 * non-retryable `ACTIVITY_EXIT_TYPE` failure) lands in the Effect error
 * channel. Infra failures and exhausted retries stay defects. Runs under the
 * same per-call cancellable scope as `callRawActivity`.
 *
 * @since 0.1.0
 * @category workflow
 */
export const callActivity = <A extends AnyTypedActivity>(
  activity: A,
  payload: PayloadOf<A>,
): Effect.Effect<SuccessOf<A>, ErrorOf<A>, SandboxRun> => {
  const codecs = codecsFor(activity);
  const proxyKey = typedActivityProxyKey(activity);
  let proxy = typedActivityProxies.get(proxyKey);
  if (proxy === undefined) {
    proxy = proxyActivities<Record<string, (wire: unknown) => Promise<unknown>>>(activity.options)[
      activity.name
    ]!;
    typedActivityProxies.set(proxyKey, proxy);
  }
  const wire = codecs.payload.encode(payload);
  return callRawActivity(() => proxy(wire)).pipe(
    // JSON has no `undefined`: a plain-function activity (a test stub, or an
    // implementation not built with implementActivities) that resolves nothing
    // reaches us as undefined where the void codec expects null.
    Effect.map((result) => codecs.success.decode(result === undefined ? null : result)),
    Effect.catchDefect((defect) => {
      const thrown = classifyActivityThrown(defect);
      return thrown.kind === "wire"
        ? Effect.fail(codecs.error.decode(thrown.exit))
        : Effect.die(defect);
    }),
  );
};

/**
 * Sleep durably until an absolute time, no-op when it is already past. The
 * target is read against the sandbox's deterministic clock, so the delay is
 * stable on replay.
 *
 * @since 0.1.0
 * @category workflow
 */
export const sleepUntil = (options: {
  readonly name: string;
  /** Epoch milliseconds, or a date-time string CARRYING ITS ZONE (`Z` or an
   * explicit offset; date-only forms are UTC per ECMAScript). Zone-less
   * date-times are rejected: `Date.parse` reads them in the worker's local
   * timezone, which is nondeterministic across workers and replays. */
  readonly timestamp: number | string;
}) =>
  Effect.gen(function* () {
    const now = yield* Clock.currentTimeMillis;
    if (
      typeof options.timestamp === "string" &&
      options.timestamp.includes("T") &&
      !/(?:Z|[+-]\d{2}:?\d{2})$/.test(options.timestamp)
    ) {
      return yield* Effect.die(
        `sleepUntil "${options.name}": date-time string "${options.timestamp}" has no timezone — zone-less strings parse in the worker's LOCAL timezone, which differs across workers and replays. Add "Z" or an explicit offset, or pass epoch millis.`,
      );
    }
    const target =
      typeof options.timestamp === "number" ? options.timestamp : Date.parse(options.timestamp);
    if (Number.isNaN(target)) {
      return yield* Effect.die(
        `sleepUntil "${options.name}": unparseable timestamp "${String(options.timestamp)}"`,
      );
    }
    const delay = target - now;
    if (delay > 0) {
      yield* DurableClock.sleep({ name: options.name, duration: Duration.millis(delay) });
    }
  });

const updateBuffer = (run: RunState, name: string): PendingUpdate[] => {
  let buffer = run.updates.get(name);
  if (buffer === undefined) {
    buffer = [];
    run.updates.set(name, buffer);
  }
  return buffer;
};

/**
 * A taken update request: the decoded payload and the one-shot typed
 * response channel the caller is blocked on. Respond before the run ends —
 * an unanswered update fails when the workflow completes.
 *
 * @since 0.1.0
 * @category models
 */
export interface UpdateRequest<S extends Schema.Top, E extends Schema.Top, P> {
  readonly payload: P;
  readonly respond: (exit: Exit.Exit<S["Type"], E["Type"]>) => Effect.Effect<void>;
}

/**
 * Durably await the next `executeUpdate` request for `update`, in delivery
 * order. The claim is synchronous on the taking fiber, like `takeMailbox`.
 *
 * @since 0.1.0
 * @category workflow
 */
export const takeUpdate = <P extends Schema.Top, S extends Schema.Top, E extends Schema.Top>(
  update: DurableUpdate<P, S, E>,
): Effect.Effect<UpdateRequest<S, E, P["Type"]>, never, SandboxRun> =>
  Effect.gen(function* () {
    const run = yield* SandboxRunTag;
    const buffer = updateBuffer(run, update.name);
    const codecs = updateCodec(update);
    while (true) {
      const pending = buffer.shift();
      if (pending !== undefined) {
        // A request that fails the payload schema is the CALLER'S bug: it is
        // answered with a defect (so the caller hears about it) and never
        // reaches the workflow body — one malformed request from a drifted
        // producer or raw client must not kill a long-lived run.
        let payload: P["Type"];
        try {
          payload = codecs.decodePayload(pending.payload);
        } catch (error) {
          pending.wireExit = codecs.encodeExit(
            Exit.die(
              `effect-workflow: update "${update.name}" payload failed schema decode: ${String(error)}`,
            ),
          );
          pending.done = true;
          continue;
        }
        return {
          payload,
          respond: (exit) =>
            Effect.sync(() => {
              if (pending.done) {
                throw new Error(`respond called twice for update "${update.name}"`);
              }
              pending.wireExit = codecs.encodeExit(exit);
              pending.done = true;
            }),
        };
      }
      yield* Effect.promise(() => condition(() => buffer.length > 0));
    }
  });

const mailboxBuffer = (run: RunState, name: string): unknown[] => {
  let buffer = run.mailboxes.get(name);
  if (buffer === undefined) {
    buffer = [];
    run.mailboxes.set(name, buffer);
  }
  return buffer;
};

/**
 * Durably await the next message offered to `mailbox`, in delivery order.
 * The claim happens synchronously on the taking fiber after the wait, so an
 * interrupted take never steals a message from a later one.
 *
 * @since 0.1.0
 * @category workflow
 */
export const takeMailbox = <S extends Schema.Top>(
  mailbox: DurableMailbox<S>,
): Effect.Effect<S["Type"], never, SandboxRun> =>
  Effect.gen(function* () {
    const run = yield* SandboxRunTag;
    const buffer = mailboxBuffer(run, mailbox.name);
    const decode = mailboxCodec(mailbox).decode;
    while (true) {
      const wire = buffer.shift();
      if (wire !== undefined) {
        // Offers are fire-and-forget, so a message that fails the payload
        // schema (raw signal access, a drifted producer, a schema tightened
        // while old messages sat buffered in history) is DROPPED with a
        // warning — matching the delivery contract — rather than poisoning
        // the run at take-time.
        try {
          return decode(wire);
        } catch (error) {
          log.warn("takeMailbox: dropped message failing schema decode", {
            mailbox: mailbox.name,
            error: String(error),
          });
          continue;
        }
      }
      yield* Effect.promise(() => condition(() => buffer.length > 0));
    }
  });

/**
 * Take the next message if one is buffered, without waiting — `None` when
 * the mailbox is empty. The non-blocking counterpart to `takeMailbox`; its
 * canonical use is draining reports into carried state before
 * `continueAsNew`, since buffered messages do not survive the run change.
 *
 * @since 0.1.0
 * @category workflow
 */
export const pollMailbox = <S extends Schema.Top>(
  mailbox: DurableMailbox<S>,
): Effect.Effect<Option.Option<S["Type"]>, never, SandboxRun> =>
  Effect.gen(function* () {
    const run = yield* SandboxRunTag;
    const buffer = mailboxBuffer(run, mailbox.name);
    // Same drop-with-warning contract as `takeMailbox` for malformed wire.
    while (true) {
      const wire = buffer.shift();
      if (wire === undefined) return Option.none();
      try {
        return Option.some(mailboxCodec(mailbox).decode(wire));
      } catch (error) {
        log.warn("pollMailbox: dropped message failing schema decode", {
          mailbox: mailbox.name,
          error: String(error),
        });
      }
    }
  });

/**
 * Offer a message to ANOTHER workflow's mailbox (workflow → workflow).
 * Offering to a closed or unknown execution is a no-op — the receiver
 * finishing first is a normal race, matching `DurableDeferred.done`.
 *
 * @since 0.1.0
 * @category workflow
 */
export const offerMailbox = <S extends Schema.Top>(
  mailbox: DurableMailbox<S>,
  options: { readonly workflowId: string; readonly payload: S["Type"] },
): Effect.Effect<void> => {
  const wire = mailboxCodec(mailbox).encode(options.payload);
  return Effect.promise(async () => {
    try {
      await getExternalWorkflowHandle(options.workflowId).signal(MAILBOX_SIGNAL, {
        mailboxName: mailbox.name,
        payload: wire,
      });
    } catch (error) {
      // The receiver having closed is the normal race; anything else is a
      // real delivery failure — visible in the worker log, but never fatal
      // to a fire-and-forget offer.
      if (error instanceof Error && /not found/i.test(error.message)) return;
      log.warn("offerMailbox: delivery failed", { mailbox: mailbox.name, error: String(error) });
    }
  });
};

/**
 * Publish a snapshot to `cell`, replacing the previous one. Readable from
 * outside via `readStateCell` (engine-client), including after the run
 * closes.
 *
 * @since 0.1.0
 * @category workflow
 */
export const setStateCell = <S extends Schema.Top>(
  cell: StateCell<S>,
  value: S["Type"],
): Effect.Effect<void, never, SandboxRun> =>
  Effect.gen(function* () {
    const run = yield* SandboxRunTag;
    run.stateCells.set(cell.name, stateCellCodec(cell).encode(value));
  });

/**
 * End this run and atomically start a fresh one of the SAME workflow with
 * `payload`, keeping the workflow id (and so the execution id) while
 * resetting history — Temporal's continue-as-new, for unbounded workflows.
 *
 * Like the native API, this unwinds the current run as a throw: Effect
 * finalizers and `Workflow.withCompensation` steps run on the way out, so
 * call it at iteration boundaries, outside compensation regions. Mailbox
 * messages buffered but not yet taken do not carry into the new run — drain
 * before continuing.
 *
 * @since 0.1.0
 * @category workflow
 */
export const continueAsNew = <
  Tag extends string,
  Payload extends Workflow.AnyStructSchema,
  Success extends Schema.Top,
  Error extends Schema.Top,
>(
  workflow: Workflow.Workflow<Tag, Payload, Success, Error>,
  payload: Payload["Type"],
  options?: { readonly memo?: Record<string, unknown> },
): Effect.Effect<never> => {
  const wire = wireCodecsFor(workflow).encodePayload(payload);
  if (options?.memo !== undefined) {
    const next = makeContinueAsNewFunc<(wire: unknown) => Promise<unknown>>({
      memo: options.memo,
    });
    return Effect.promise(() => next(wire));
  }
  return Effect.promise(() => temporalContinueAsNew<(wire: unknown) => Promise<unknown>>(wire));
};

/**
 * The subset of a Nexus service client `callNexusWorkflowOperation`
 * needs — pass the result of `createNexusServiceClient` here.
 *
 * @since 0.1.0
 * @category models
 */
export interface NexusOperationClient {
  executeOperation(
    operation: string,
    input: unknown,
    options?: { readonly scheduleToCloseTimeout?: string | number },
  ): Promise<unknown>;
}

/**
 * Call a Nexus operation backed by a SHIM workflow (exposed with
 * `effectWorkflowRunOperation` from the `nexus` module): the payload is
 * wire-encoded on the way out and the operation's result — the target
 * workflow's encoded exit — decodes back into this effect's typed
 * success/error channels. Runs under the same per-call cancellable scope as
 * `callRawActivity`, so interruption cancels the in-flight operation.
 *
 * Operations NOT backed by a shim workflow are plain workflow-API promises:
 * call them with `callRawActivity` directly.
 *
 * @since 0.1.0
 * @category workflow
 */
export const callNexusWorkflowOperation = <
  Tag extends string,
  Payload extends Workflow.AnyStructSchema,
  Success extends Schema.Top,
  Error extends Schema.Top,
>(options: {
  readonly client: NexusOperationClient;
  readonly operation: string;
  readonly workflow: Workflow.Workflow<Tag, Payload, Success, Error>;
  readonly payload: Payload["Type"];
  readonly scheduleToCloseTimeout?: string | number;
}): Effect.Effect<Success["Type"], Error["Type"], SandboxRun> => {
  const codecs = wireCodecsFor(options.workflow);
  const wire = codecs.encodePayload(options.payload);
  return callRawActivity(async () => {
    try {
      return {
        outcome: "wire" as const,
        wire: await options.client.executeOperation(options.operation, wire, {
          scheduleToCloseTimeout: options.scheduleToCloseTimeout ?? "60 seconds",
        }),
      };
    } catch (error) {
      return { outcome: "thrown" as const, error };
    }
  }).pipe(
    Effect.flatMap((result) =>
      result.outcome === "wire"
        ? codecs.decodeExit(result.wire)
        : exitFromThrown(codecs.decodeExit, result.error),
    ),
    // SAFETY: the exit was decoded through the target workflow's own wire
    // codecs (its success/error schemas), so the channels are exactly
    // Success["Type"] / Error["Type"] — the codec seam types them as unknown.
  ) as Effect.Effect<Success["Type"], Error["Type"], SandboxRun>;
};

/** The attach bridge (see `activities.ts`): awaiting a foreign execution's
 * result, which neither `startChild` nor external handles can do. Workers
 * running shim workflows must register `makeEffectWorkflowActivities`. */
const bridge = proxyActivities<{
  effectWorkflowPollResult(input: { workflowId: string }): Promise<EffectWorkflowBridgeResult>;
}>({
  startToCloseTimeout: "30 seconds",
});

/** How long an interrupted run waits for its cancelled calls to settle
 * before completing anyway. */
const CANCEL_SETTLE_TIMEOUT_MILLIS = 30_000;

/** Attach re-poll bounds: each poll is one activity + one timer in
 * history, so the interval backs off exponentially to cap growth on
 * long-running foreign executions. */
const ATTACH_POLL_MILLIS = 5_000;
const ATTACH_POLL_MAX_MILLIS = 60_000;

const classifySandboxThrown = makeClassifyThrown({ ApplicationFailure, CancelledFailure });

/** Map a thrown run/child failure to the exit it carries: our
 * `ApplicationFailure` marker holds the encoded typed exit, cancellation is
 * an interrupt, anything else is a defect. */
const exitFromThrown = (
  decodeExit: (wire: unknown) => Exit.Exit<unknown, unknown>,
  error: unknown,
): Exit.Exit<unknown, unknown> => {
  const thrown = classifySandboxThrown(error);
  switch (thrown.kind) {
    case "interrupted":
      return Exit.interrupt();
    case "wire":
      return decodeExit(thrown.exit);
    case "other":
      return Exit.die(thrown.error);
  }
};

const unreachable = (method: string) =>
  Effect.die(
    `TemporalSandboxEngine.${method}: reachable only through the client half — inside the sandbox this indicates a shim bug`,
  );

const makeSandboxEngine = (state: RunState): WorkflowEngine.WorkflowEngine["Service"] =>
  WorkflowEngine.makeUnsafe({
    // The bundle export is the registration.
    register: () => Effect.void,

    // Child workflow: the child's Temporal type must live in the same
    // bundle, and its workflow id is its digest execution id — so the
    // idempotency contract stays global. An id already taken (another
    // parent, or a completed earlier run under REJECT_DUPLICATE) attaches
    // via the bridge activity and returns that execution's result.
    execute: (workflow, { executionId, payload, discard }) => {
      const codecs = wireCodecsFor(workflow);
      const wirePayload = codecs.encodePayload(payload);
      // Set when the awaiting fiber is interrupted (a timeout or lost race
      // around a child execute, or the run unwinding): the attach-poll loop
      // below must stop emitting bridge activities and timers into history —
      // the abandoned promise's result is discarded anyway.
      let interrupted = false;
      return Effect.promise(async () => {
        let owned = true;
        let handle: ChildWorkflowHandle<(wire: unknown) => Promise<unknown>> | undefined;
        try {
          handle = await startChild<(wire: unknown) => Promise<unknown>>(workflow._tag, {
            workflowId: executionId,
            args: [wirePayload],
            workflowIdReusePolicy: "REJECT_DUPLICATE",
            // Awaited children get cancel-on-parent-close so their own
            // compensation runs even when the parent is terminated outright;
            // discarded children outlive the parent by design.
            parentClosePolicy: discard ? "ABANDON" : "REQUEST_CANCEL",
          });
        } catch (error) {
          // WorkflowExecutionAlreadyStartedError is not re-exported by
          // @temporalio/workflow, so match by name.
          if (!(error instanceof Error && error.name === "WorkflowExecutionAlreadyStartedError")) {
            throw error;
          }
          owned = false;
        }
        if (discard) return undefined;
        if (owned && handle !== undefined) {
          try {
            const wire = await handle.result();
            return new Workflow.Complete({ exit: codecs.decodeExit(wire) });
          } catch (error) {
            return new Workflow.Complete({ exit: exitFromThrown(codecs.decodeExit, error) });
          }
        }
        let pollWaitMillis = ATTACH_POLL_MILLIS;
        while (!interrupted) {
          const polled = await bridge.effectWorkflowPollResult({ workflowId: executionId });
          switch (polled.kind) {
            case "running":
              break;
            case "wire":
              return new Workflow.Complete({ exit: codecs.decodeExit(polled.exit) });
            case "interrupted":
              return new Workflow.Complete({ exit: Exit.interrupt() });
            case "defect":
              return new Workflow.Complete({ exit: Exit.die(new Error(polled.message)) });
          }
          if (interrupted) break;
          await sleep(pollWaitMillis);
          pollWaitMillis = Math.min(pollWaitMillis * 2, ATTACH_POLL_MAX_MILLIS);
        }
        return new Workflow.Complete({ exit: Exit.interrupt() });
      }).pipe(
        Effect.onInterrupt(() =>
          Effect.sync(() => {
            interrupted = true;
          }),
        ),
        // SAFETY: `makeUnsafe`'s unknown-seam — the engine contract types
        // `execute` against per-workflow generics it cannot thread through a
        // handler record; the promise produces exactly the Result-or-undefined
        // (undefined iff `discard`) shape the contract reads back.
      ) as never;
    },

    poll: () => unreachable("poll"),

    // Reached by makeUnsafe's finalizer when an interrupted parent tears
    // down an awaited child. Best-effort: the child may already be closed,
    // and REQUEST_CANCEL parent-close is the backstop.
    interrupt: (_workflow, executionId) =>
      Effect.promise(async () => {
        try {
          await getExternalWorkflowHandle(executionId).cancel();
        } catch {
          return;
        }
      }),
    interruptUnsafe: () => unreachable("interruptUnsafe"),
    resume: () => unreachable("resume"),

    // Runs the activity body in-process; `executeEncoded` already encodes
    // the success/typed-failure channels, and defects escape to be captured
    // by `Workflow.intoResult` at the run root.
    activityExecute: (activity, attempt) =>
      activity.executeEncoded.pipe(
        Effect.map((value) => new Workflow.Complete({ exit: Exit.succeed(value) })),
        Effect.catch((error) => Effect.succeed(new Workflow.Complete({ exit: Exit.fail(error) }))),
        Effect.scoped,
        Effect.provideService(Activity.CurrentAttempt, attempt),
      ),

    // Blocks durably instead of suspending: a clock deferred awaits its
    // Temporal timer, every other deferred awaits the done-signal.
    deferredResult: (deferred) =>
      Effect.promise(async () => {
        const clock = state.clocks.get(deferred.name);
        if (clock !== undefined) {
          await sleep(clock.millis);
          return Option.some(clock.doneExit);
        }
        await condition(() => state.deferredExits.has(deferred.name));
        const wire = state.deferredExits.get(deferred.name);
        // Reconstruct a real Exit instance — makeUnsafe's decode path
        // starts with `Exit.isExit`, which the JSON payload would fail.
        return Option.some(decodeDeferredExit(wire));
      }),

    // Completing our own deferred lands in run state; another workflow's
    // routes as the external form of the same done-signal.
    deferredDone: ({ executionId, deferredName, exit }) =>
      Effect.promise(async () => {
        const wireExit = encodeDeferredExit(exit);
        if (executionId === workflowInfo().workflowId) {
          state.deferredExits.set(deferredName, wireExit);
          return;
        }
        try {
          await getExternalWorkflowHandle(executionId).signal(DEFERRED_DONE_SIGNAL, {
            deferredName,
            exit: wireExit,
          });
        } catch (error) {
          // The receiver having closed is the normal race (`deferredDone` on
          // closed or unknown executions is a no-op, same as the client
          // half); anything else is a real delivery failure — visible in the
          // worker log, but a completion signal must never fail the SENDER'S
          // run.
          if (error instanceof Error && /not found/i.test(error.message)) return;
          log.warn("deferredDone: delivery failed", {
            deferred: deferredName,
            executionId,
            error: String(error),
          });
        }
      }),

    // Records the timer; the sleep itself runs when the workflow awaits the
    // clock's deferred.
    scheduleClock: (_workflow, { clock }) => {
      // SAFETY: `Schema.Exit`'s encoded form is still an Exit instance (see
      // wire.ts) — encoding only touches the leaves — so the encoded value
      // is a genuine `Exit` with encoded leaves, which is what
      // `deferredResult` hands back to `makeUnsafe`'s decode path.
      const doneExit = Schema.encodeSync(asJsonCodec(clock.deferred.exitSchema))(
        Exit.succeed(undefined),
      ) as Exit.Exit<unknown, unknown>;
      return Effect.sync(() => {
        state.clocks.set(clock.deferred.name, {
          millis: Duration.toMillis(clock.duration),
          doneExit,
        });
      });
    },
  });

/** An exit produced by cancellation: interrupt reasons from the race, or a
 * `CancelledFailure` defect from a Temporal await that saw the cancel
 * first. Either way the run must end as Cancelled. */
const isCancellationExit = (exit: Exit.Exit<unknown, unknown>): boolean => {
  if (!Exit.isFailure(exit)) return false;
  const cause = exit.cause;
  if (Cause.hasInterrupts(cause)) return true;
  return Cause.hasDies(cause) && Cause.squash(cause) instanceof CancelledFailure;
};

/** The erased registered-handler shape: `never` payload for contravariant
 * assignability, R = what the run wrapper provides. */
type SandboxHandler = (
  payload: never,
  executionId: string,
) => Effect.Effect<
  unknown,
  unknown,
  WorkflowEngine.WorkflowEngine | WorkflowEngine.WorkflowInstance | Scope | SandboxRun
>;

/** One workflow run: decode the payload, wire the message handlers, race
 * the body against cancellation, map the exit onto Temporal's outcomes. */
const runInSandbox = async (
  workflow: Workflow.Any,
  handler: SandboxHandler,
  wirePayload: unknown,
): Promise<unknown> => {
  const codecs = wireCodecsFor(workflow);
  {
    ensureSandboxPolyfills();
    const state: RunState = {
      deferredExits: new Map(),
      clocks: new Map(),
      inFlight: new Map(),
      cancelWaits: [],
      mailboxes: new Map(),
      stateCells: new Map(),
      updates: new Map(),
      interrupted: false,
    };
    setHandler(deferredDoneSignal, ({ deferredName, exit }) => {
      state.deferredExits.set(deferredName, exit);
    });
    setHandler(deferredStateQuery, (deferredName) => state.deferredExits.get(deferredName) ?? null);
    setHandler(mailboxSignal, ({ mailboxName, payload }) => {
      mailboxBuffer(state, mailboxName).push(payload);
    });
    setHandler(stateCellQuery, (cellName) => state.stateCells.get(cellName) ?? null);
    // Update handlers may be async and await workflow APIs: buffer the
    // request, wait for the body's `respond`, return the wire exit as the
    // update result.
    setHandler(workflowUpdate, async ({ updateName, payload }) => {
      const pending: PendingUpdate = { payload, wireExit: undefined, done: false };
      updateBuffer(state, updateName).push(pending);
      await condition(() => pending.done);
      return pending.wireExit;
    });

    // The run itself is non-cancellable; cancellation is handled at the
    // Effect level via the scope's still-firing `cancelRequested` and
    // reaches the server through the per-call scopes `callRawActivity`
    // registers (see the module doc).
    return await CancellationScope.nonCancellable(async () => {
      const scope = CancellationScope.current();
      // Resolves (never rejects) once cancellation is requested.
      const cancelRequested = scope.cancelRequested.then(
        () => undefined,
        () => undefined,
      );

      const engine = makeSandboxEngine(state);
      const executionId = workflowInfo().workflowId;
      const instance = WorkflowEngine.WorkflowInstance.initial(workflow, executionId);
      // A malformed payload is a caller bug: fail the RUN, not the workflow
      // task — a thrown decode error here would make Temporal retry the task
      // forever, hanging the execution instead of surfacing the defect.
      let payload: unknown;
      try {
        payload = codecs.decodePayload(wirePayload);
      } catch (error) {
        throw ApplicationFailure.create({
          type: EXIT_FAILURE_TYPE,
          nonRetryable: true,
          message: "effect-workflow: payload failed schema decode (exit in details)",
          details: [codecs.encodeExit(Exit.die(error))],
        });
      }

      // Wins the race only when cancel is requested: marks the instance
      // interrupted (so `intoResult` yields a Complete instead of
      // propagating the interrupt), cancels in-flight call scopes, and
      // fails with an interrupt CAUSE — a race treats a self-interrupted
      // racer as withdrawn, not as the winner. The losing handler fiber is
      // interrupted, running its finalizers and compensation.
      const cancelled = Effect.promise(() => cancelRequested).pipe(
        Effect.andThen(
          Effect.sync(() => {
            instance.interrupted = true;
            state.interrupted = true;
            for (const [scope, promise] of state.inFlight) {
              scope.cancel();
              state.cancelWaits.push(promise.catch(() => undefined));
            }
            state.inFlight.clear();
          }),
        ),
        Effect.andThen(Effect.failCause(Cause.interrupt())),
      );

      // SAFETY: payload was decoded through this workflow's own schema, and
      // the handler was registered for this workflow.
      const body = Effect.raceFirst(handler(payload as never, executionId), cancelled);

      const program = Workflow.intoResult(body).pipe(
        Effect.provideService(WorkflowEngine.WorkflowEngine, engine),
        Effect.provideService(WorkflowEngine.WorkflowInstance, instance),
        Effect.provideService(SandboxRunTag, state),
      );

      // SAFETY: `intoResult` captures every failure into the Result, the
      // three provides discharge the engine/instance/run services, and the
      // remaining R is the schemas' encoding services — `never` for the
      // context-free schemas this model uses.
      const result = await Effect.runPromise(
        program as Effect.Effect<Workflow.Result<unknown, unknown>>,
        { scheduler: sandboxScheduler },
      );

      if (result._tag === "Suspended") {
        throw new Error(
          "effect-workflow shim: got Workflow.Suspended — this engine blocks instead of suspending, so a Suspended result is a shim bug",
        );
      }
      if (Exit.isFailure(result.exit) && Cause.hasDies(result.exit.cause)) {
        // A `continueAsNew` surfaces as a die defect; the ORIGINAL error
        // must escape the workflow function raw for the SDK to start the
        // next run.
        const defect = Cause.squash(result.exit.cause);
        if (defect instanceof ContinueAsNew) throw defect;
      }
      if (isCancellationExit(result.exit)) {
        // Compensation already ran during the unwind. Cancelled calls are
        // awaited (bounded) before completing: a nexus operation's
        // cancellation handshake only proceeds while this caller is open.
        if (state.cancelWaits.length > 0) {
          await Promise.race([
            Promise.allSettled(state.cancelWaits),
            sleep(CANCEL_SETTLE_TIMEOUT_MILLIS),
          ]);
        }
        // CancelledFailure is what makes Temporal record the run as
        // Cancelled.
        throw new CancelledFailure("effect-workflow: interrupted");
      }
      if (Exit.isFailure(result.exit)) {
        // Fail the run (red in the Temporal UI); the encoded exit rides the
        // failure's details and decodes back to the typed channel wherever
        // it is read.
        throw ApplicationFailure.create({
          type: EXIT_FAILURE_TYPE,
          nonRetryable: true,
          message: "effect-workflow: typed failure or defect (exit in details)",
          details: [codecs.encodeExit(result.exit)],
        });
      }
      return codecs.encodeExit(result.exit);
    });
  }
};

interface RegisteredWorkflow {
  readonly workflow: Workflow.Any;
  readonly execute: SandboxHandler;
}

const registrationOnly = (method: string) =>
  Effect.die(
    `TemporalRegistrationEngine.${method}: reachable only inside a workflow run — during registration only \`register\` exists`,
  );

/** Type-level placeholder: the real per-run state is provided at run time
 * (run-site context wins over registration context). */
const registrationSandboxRun = new Proxy({} as RunState, {
  get() {
    throw new Error(
      "SandboxRun accessed during workflow registration — sandbox operations only run inside a workflow body",
    );
  },
});

/** layer → registry, memoized per V8 context (reuseV8Context-safe). */
const workflowRegistries = new Map<
  Layer.Layer<never, never, WorkflowEngine.WorkflowEngine | SandboxRun | WorkflowOps>,
  Promise<Map<string, RegisteredWorkflow>>
>();

const buildRegistry = (
  workflows: Layer.Layer<never, never, WorkflowEngine.WorkflowEngine | SandboxRun | WorkflowOps>,
): Effect.Effect<Map<string, RegisteredWorkflow>> =>
  Effect.gen(function* () {
    const registry = new Map<string, RegisteredWorkflow>();
    const registrationEngine = WorkflowEngine.makeUnsafe({
      register: (workflow, execute) =>
        Effect.sync(() => {
          if (registry.has(workflow._tag)) {
            throw new Error(
              `effect-workflow: workflow tag "${workflow._tag}" registered twice — each tag may appear in ONE toLayer within the layer passed to workflowBundle`,
            );
          }
          registry.set(workflow._tag, { workflow, execute });
        }),
      execute: () => registrationOnly("execute") as never,
      poll: () => registrationOnly("poll") as never,
      interrupt: () => registrationOnly("interrupt"),
      interruptUnsafe: () => registrationOnly("interruptUnsafe"),
      resume: () => registrationOnly("resume"),
      activityExecute: () => registrationOnly("activityExecute") as never,
      deferredResult: () => registrationOnly("deferredResult") as never,
      deferredDone: () => registrationOnly("deferredDone"),
      scheduleClock: () => registrationOnly("scheduleClock"),
    });
    // Never closed: registrations live for the V8 context, so registration
    // layers must not own resources.
    const scope = yield* ScopeImpl.make();
    yield* Layer.buildWithScope(
      Layer.provide(
        workflows,
        Layer.mergeAll(
          Layer.succeed(WorkflowEngine.WorkflowEngine, registrationEngine),
          Layer.succeed(SandboxRunTag, registrationSandboxRun),
          Layer.succeed(WorkflowOps, temporalWorkflowOps),
        ),
      ),
      scope,
    );
    return registry;
  });

/**
 * The Temporal implementation of the declared-workflow ops seam: every
 * operation dispatches into this module's machinery. Provided automatically
 * to the layers `workflowBundle` hosts.
 *
 * @since 0.3.0
 * @category workflow
 */
export const temporalWorkflowOps: WorkflowOpsRuntime = {
  // SAFETY: each op requires SandboxRun (and the engine) at the type level;
  // the per-run wrapper provides them — same discipline as SandboxHandler.
  activity: (activity, payload) => callActivity(activity, payload as never) as never,
  deferredAwait: (deferred) => DurableDeferred.await(deferred) as never,
  mailboxTake: (mailbox) => takeMailbox(mailbox) as never,
  mailboxPoll: (mailbox) => pollMailbox(mailbox) as never,
  updateTake: (update) => takeUpdate(update) as never,
  stateSet: (cell, value) => setStateCell(cell, value) as never,
  version: (site, names) =>
    Versioning.version(site, names as unknown as readonly [string, ...string[]]),
};

/**
 * Host `Workflow.toLayer` registrations behind one dynamic Temporal
 * workflow, exported as the bundle's DEFAULT export — Temporal routes every
 * workflow type to it, and the registry dispatches by tag:
 *
 * ```ts
 * // workflows.ts — the bundle entry
 * export default workflowBundle(
 *   Layer.mergeAll(
 *     OrderFlow.toLayer(orderHandler),
 *     BillingFlow.toLayer(billingHandler),
 *   ),
 * );
 * ```
 *
 * The same authoring runs on any `WorkflowEngine` (cluster, in-memory);
 * choosing Temporal is choosing this default export plus the client half's engine layer.
 *
 * @since 0.2.0
 * @category constructors
 */
export const workflowBundle = (
  workflows: Layer.Layer<never, never, WorkflowEngine.WorkflowEngine | SandboxRun | WorkflowOps>,
): ((wirePayload: unknown) => Promise<unknown>) => {
  return async function runDynamic(wirePayload: unknown): Promise<unknown> {
    ensureSandboxPolyfills();
    let registryPromise = workflowRegistries.get(workflows);
    if (registryPromise === undefined) {
      registryPromise = Effect.runPromise(buildRegistry(workflows), {
        scheduler: sandboxScheduler,
      });
      workflowRegistries.set(workflows, registryPromise);
    }
    const registry = await registryPromise;
    const workflowType = workflowInfo().workflowType;
    const entry = registry.get(workflowType);
    if (entry === undefined) {
      throw ApplicationFailure.create({
        nonRetryable: true,
        message: `effect-workflow: no workflow registered for Temporal type "${workflowType}" — include its \`toLayer\` in the layer passed to workflowBundle`,
      });
    }
    return runInSandbox(entry.workflow, entry.execute, wirePayload);
  };
};
