/**
 * Engine-agnostic workflow capabilities: declare each primitive ONCE —
 * activities, deferreds, mailboxes, updates, state cells — and use it
 * directly inside workflow handlers:
 *
 * ```ts
 * const Charge = defineActivity("charge", {
 *   payload: { orderId: Schema.String },
 *   success: Schema.String,
 * });
 * const Approval = defineDeferred("order/approval", { success: Schema.String });
 *
 * const OrderLive = OrderFlow.toLayer((payload) =>
 *   Effect.gen(function* () {
 *     const receipt = yield* Charge({ orderId: payload.orderId });
 *     yield* sleep({ name: "cooling-off", duration: "3 days" });
 *     const approver = yield* Approval.await;
 *     return `${receipt}:by:${approver}`;
 *   }),
 * );
 * ```
 *
 * Every operation — activity calls, message channels, state, timers
 * (`sleep`, `sleepUntil`), `continueAsNew`, child workflows
 * (`executeChild`), and versioning (`version`, `versioned`) — requires only
 * the `WorkflowOps` service — the one seam an engine implements. The
 * `bundle` module's `workflowBundle` provides the Temporal runtime; the
 * `testing` module provides an in-memory one, so the same handler runs on
 * real Temporal or in a plain unit test. Declarations are temporal-free and
 * carry the wire identity explicitly (their `name`), so refactoring code
 * never changes the wire.
 *
 * This module must never import `@temporalio/*` or the sandbox half: it is
 * the portable surface.
 *
 * @since 0.3.0
 */

import * as Context from "effect/Context";
import type * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import type * as Exit from "effect/Exit";
import type * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as SchemaGetter from "effect/SchemaGetter";
import * as DurableDeferred from "effect/unstable/workflow/DurableDeferred";
import type * as Workflow from "effect/unstable/workflow/Workflow";
import * as DurableMailbox from "./mailbox.js";
import * as DurableUpdate from "./update.js";
import * as StateCell from "./state-cell.js";

// ─── Activity declarations (the types) ───────────────────────────────────────

/**
 * Applied when a declaration declares no options of its own.
 *
 * @since 0.4.0
 * @category models
 */
export const DEFAULT_ACTIVITY_OPTIONS = {
  startToCloseTimeout: "10 minutes",
  retry: { maximumAttempts: 3 },
} as const;

/**
 * The Temporal activity options a declaration carries — the subset of
 * `proxyActivities` options a declared activity pins at declaration time.
 *
 * @since 0.4.0
 * @category models
 */
export interface TypedActivityOptions {
  readonly startToCloseTimeout: string | number;
  readonly retry?: {
    readonly maximumAttempts?: number;
    readonly nonRetryableErrorTypes?: string[];
  };
}

/**
 * The serializable projection of a declared activity: name, the three
 * channel schemas, and the Temporal options every call site honors. This is
 * what crosses to the worker (`handle` / `implementActivities`) and what an
 * engine's `WorkflowOps.activity` receives.
 *
 * @since 0.4.0
 * @category models
 */
export interface TypedActivity<
  Name extends string,
  Payload extends Schema.Top,
  Success extends Schema.Top,
  Error extends Schema.Top,
> {
  readonly name: Name;
  readonly payloadSchema: Payload;
  readonly successSchema: Success;
  readonly errorSchema: Error;
  readonly options: TypedActivityOptions;
}

/**
 * Type-erased activity declaration, for APIs that operate on any activity.
 *
 * @since 0.4.0
 * @category models
 */
export type AnyTypedActivity = TypedActivity<string, Schema.Top, Schema.Top, Schema.Top>;

/**
 * Extracts a declared activity's decoded payload type:
 * `PayloadOf<typeof Charge>`.
 *
 * @since 0.4.0
 * @category models
 */
export type PayloadOf<A> =
  A extends TypedActivity<string, infer P, Schema.Top, Schema.Top> ? P["Type"] : never;
/**
 * Extracts a declared activity's decoded success type.
 *
 * @since 0.4.0
 * @category models
 */
export type SuccessOf<A> =
  A extends TypedActivity<string, Schema.Top, infer S, Schema.Top> ? S["Type"] : never;
/**
 * Extracts a declared activity's decoded error type.
 *
 * @since 0.4.0
 * @category models
 */
export type ErrorOf<A> =
  A extends TypedActivity<string, Schema.Top, Schema.Top, infer E> ? E["Type"] : never;

/**
 * Build the serializable projection of an activity declaration. Shared by
 * `defineActivity` and the deprecated `TypedActivity.make`.
 *
 * @internal
 */
export const makeTypedActivity = <
  const Name extends string,
  Payload extends Schema.Struct.Fields | Schema.Top,
  Success extends Schema.Top = Schema.Void,
  Error extends Schema.Top = Schema.Never,
>(
  name: Name,
  definition: {
    readonly payload: Payload;
    readonly success?: Success;
    readonly error?: Error;
    readonly options?: TypedActivityOptions;
  },
): TypedActivity<
  Name,
  Payload extends Schema.Struct.Fields ? Schema.Struct<Payload> : Payload,
  Success,
  Error
> => ({
  name,
  // SAFETY: the branch mirrors the conditional return type — a schema stays
  // itself, struct fields become `Schema.Struct(fields)` — but TypeScript
  // cannot resolve the conditional over the unbound `Payload`.
  payloadSchema: (Schema.isSchema(definition.payload)
    ? definition.payload
    : Schema.Struct(definition.payload as Schema.Struct.Fields)) as never,
  // SAFETY: when the option is omitted the type parameter takes its default
  // (`Schema.Void` / `Schema.Never`), which is exactly the fallback value.
  successSchema: (definition.success ?? Schema.Void) as Success,
  errorSchema: (definition.error ?? Schema.Never) as Error,
  options: definition.options ?? DEFAULT_ACTIVITY_OPTIONS,
});

// ─── The ops seam ────────────────────────────────────────────────────────────

/**
 * A taken update request: the decoded payload and its one-shot typed
 * response channel. Type parameters are `<Payload, Success, Error>` — the
 * same order as `defineUpdate`'s schemas.
 *
 * @since 0.3.0
 * @category models
 */
export interface UpdateRequest<P, S, E> {
  readonly payload: P;
  readonly respond: (exit: Exit.Exit<S, E>) => Effect.Effect<void>;
}

/**
 * Options for `sleep`: a name unique per sleep within a run, and a duration.
 *
 * @since 0.4.0
 * @category models
 */
export interface SleepOptions {
  readonly name: string;
  readonly duration: Duration.Input;
}

/**
 * Options for `sleepUntil`: a name and an absolute target.
 *
 * @since 0.4.0
 * @category models
 */
export interface SleepUntilOptions {
  readonly name: string;
  /** Epoch milliseconds, or a date-time string CARRYING ITS ZONE (`Z` or an
   * explicit offset; date-only forms are UTC per ECMAScript). Zone-less
   * date-times are rejected: `Date.parse` reads them in the worker's local
   * timezone, which is nondeterministic across workers and replays. */
  readonly timestamp: number | string;
}

/**
 * Options for `continueAsNew`.
 *
 * @since 0.4.0
 * @category models
 */
export interface ContinueAsNewOptions {
  readonly memo?: Record<string, unknown>;
}

/**
 * What an engine implements to host declared capabilities: one operation
 * per primitive kind, dispatching on the declaration instances. The typed
 * surfaces below narrow this seam exactly once each.
 *
 * Declaration schemas must be context-free: a schema requiring decoding or
 * encoding services would have that requirement erased by this seam and
 * defect at runtime.
 *
 * @since 0.3.0
 * @category models
 */
export interface WorkflowOpsRuntime {
  readonly activity: (activity: AnyTypedActivity, payload: unknown) => Effect.Effect<unknown, unknown>;
  readonly deferredAwait: (
    deferred: DurableDeferred.DurableDeferred<Schema.Constraint>,
  ) => Effect.Effect<unknown>;
  readonly mailboxTake: (mailbox: DurableMailbox.DurableMailbox<Schema.Top>) => Effect.Effect<unknown>;
  readonly mailboxPoll: (
    mailbox: DurableMailbox.DurableMailbox<Schema.Top>,
  ) => Effect.Effect<Option.Option<unknown>>;
  readonly updateTake: (
    update: DurableUpdate.DurableUpdate<Schema.Top, Schema.Top, Schema.Top>,
  ) => Effect.Effect<UpdateRequest<unknown, unknown, unknown>>;
  readonly stateSet: (cell: StateCell.StateCell<Schema.Top>, value: unknown) => Effect.Effect<void>;
  readonly version: <const Names extends readonly [string, ...string[]]>(
    site: string,
    names: Names,
  ) => Effect.Effect<Names[number]>;
  /** A durable named timer. @since 0.4.0 */
  readonly sleep: (options: SleepOptions) => Effect.Effect<void>;
  /** A durable timer to an absolute instant (no-op when already past);
   * engines share the timestamp rule of `sleepUntilTarget`. @since 0.4.0 */
  readonly sleepUntil: (options: SleepUntilOptions) => Effect.Effect<void>;
  /** End this run and start a fresh one of `workflow` with `payload` (the
   * DECODED payload — engines encode it through the workflow's own
   * schema). @since 0.4.0 */
  readonly continueAsNew: (
    workflow: Workflow.Any,
    payload: unknown,
    options?: ContinueAsNewOptions,
  ) => Effect.Effect<never>;
  /** Start a child workflow with the DECODED payload: awaited (`discard:
   * false`) the child's typed exit lands in the channels; discarded the
   * child's execution id is returned and it outlives the parent.
   * @since 0.4.0 */
  readonly executeChild: (
    workflow: Workflow.Any,
    payload: unknown,
    options: { readonly discard: boolean },
  ) => Effect.Effect<unknown, unknown>;
}

/**
 * The service an engine provides to run declared capabilities —
 * `workflowBundle` provides the Temporal runtime automatically; the
 * `testing` module provides an in-memory one.
 *
 * @since 0.3.0
 * @category services
 */
export class WorkflowOps extends Context.Service<WorkflowOps, WorkflowOpsRuntime>()(
  "effect-temporal/WorkflowOps",
) {}

const withOps = <A, E>(f: (runtime: WorkflowOpsRuntime) => Effect.Effect<A, E>) =>
  Effect.flatMap(WorkflowOps, f);

// ─── Activities ──────────────────────────────────────────────────────────────

/**
 * A declared activity: callable with its typed payload inside any workflow
 * handler, and carrying the underlying `TypedActivity` projection for
 * worker binding (`implementActivities` + `handle`) and wire identity.
 *
 * @since 0.3.0
 * @category models
 */
export interface DefinedActivity<
  Name extends string,
  Payload extends Schema.Top,
  Success extends Schema.Top,
  Error extends Schema.Top,
> extends TypedActivity<Name, Payload, Success, Error> {
  (payload: Payload["Type"]): Effect.Effect<Success["Type"], Error["Type"], WorkflowOps>;
}

/**
 * Declare an activity where its implementation lives; call it with its
 * payload from any workflow handler: `yield* Charge({ orderId })`.
 *
 * @since 0.3.0
 * @category constructors
 */
export const defineActivity = <
  const Name extends string,
  Payload extends Schema.Struct.Fields | Schema.Top,
  Success extends Schema.Top = Schema.Void,
  Error extends Schema.Top = Schema.Never,
>(
  name: Name,
  decl: {
    readonly payload: Payload;
    readonly success?: Success;
    readonly error?: Error;
    readonly options?: TypedActivityOptions;
  },
): DefinedActivity<
  Name,
  Payload extends Schema.Struct.Fields ? Schema.Struct<Payload> : Payload,
  Success,
  Error
> => {
  const activity = makeTypedActivity(name, decl);
  const call = (payload: unknown) => withOps((runtime) => runtime.activity(activity, payload));
  // defineProperties, not Object.assign: a function's own `name` is
  // non-writable (assignment throws in strict mode) but configurable.
  // SAFETY: the callable narrows the runtime's unknown seam to the schemas
  // this very declaration carries.
  return Object.defineProperties(call, Object.getOwnPropertyDescriptors(activity)) as never;
};

// ─── Message channels and state ──────────────────────────────────────────────

/**
 * A declared deferred: `await` inside a handler; the underlying `deferred`
 * is what the client half addresses (`wf.completeDeferred`,
 * `wf.deferredState` accept the declaration directly).
 *
 * @since 0.4.0
 * @category models
 */
export interface DefinedDeferred<Success extends Schema.Constraint> {
  readonly name: string;
  readonly deferred: DurableDeferred.DurableDeferred<Success, Schema.Never>;
  readonly await: Effect.Effect<Success["Type"], never, WorkflowOps>;
}

/**
 * A one-shot typed completion an outside party resolves. `await` blocks
 * durably inside a handler.
 *
 * @since 0.3.0
 * @category constructors
 */
export const defineDeferred = <Success extends Schema.Constraint>(
  name: string,
  decl: { readonly success: Success },
): DefinedDeferred<Success> => {
  const deferred = DurableDeferred.make<Success, Schema.Never>(name, { success: decl.success });
  return {
    name,
    deferred,
    // SAFETY: the seam hands back the value the engine decoded through this
    // very deferred's success schema.
    await: withOps((runtime) => runtime.deferredAwait(deferred)) as Effect.Effect<
      Success["Type"],
      never,
      WorkflowOps
    >,
  };
};

/**
 * A declared mailbox: `take`/`poll` inside a handler; client-side offers
 * (`wf.offerMailbox`, `offerMailbox`) accept the declaration directly.
 *
 * @since 0.4.0
 * @category models
 */
export interface DefinedMailbox<Payload extends Schema.Top> {
  readonly name: string;
  readonly mailbox: DurableMailbox.DurableMailbox<Payload>;
  readonly take: Effect.Effect<Payload["Type"], never, WorkflowOps>;
  readonly poll: Effect.Effect<Option.Option<Payload["Type"]>, never, WorkflowOps>;
}

/**
 * A durable inbound message queue. `take`/`poll` consume inside a handler.
 *
 * @since 0.3.0
 * @category constructors
 */
export const defineMailbox = <Payload extends Schema.Top>(
  name: string,
  decl: { readonly payload: Payload },
): DefinedMailbox<Payload> => {
  const mailbox = DurableMailbox.make(name, { payload: decl.payload });
  return {
    name,
    mailbox,
    // SAFETY: the seam decodes through this mailbox's own payload schema.
    take: withOps((runtime) => runtime.mailboxTake(mailbox)) as Effect.Effect<
      Payload["Type"],
      never,
      WorkflowOps
    >,
    poll: withOps((runtime) => runtime.mailboxPoll(mailbox)) as Effect.Effect<
      Option.Option<Payload["Type"]>,
      never,
      WorkflowOps
    >,
  };
};

/**
 * A declared update: `take` inside a handler (respond exactly once);
 * client-side requests (`wf.executeUpdate`, `executeUpdate`) accept the
 * declaration directly.
 *
 * @since 0.4.0
 * @category models
 */
export interface DefinedUpdate<
  Payload extends Schema.Top,
  Success extends Schema.Top,
  Error extends Schema.Top,
> {
  readonly name: string;
  readonly update: DurableUpdate.DurableUpdate<Payload, Success, Error>;
  readonly take: Effect.Effect<
    UpdateRequest<Payload["Type"], Success["Type"], Error["Type"]>,
    never,
    WorkflowOps
  >;
}

/**
 * Request/response into a running workflow with typed channels. `take`
 * consumes requests inside a handler (respond exactly once).
 *
 * @since 0.3.0
 * @category constructors
 */
export const defineUpdate = <
  Payload extends Schema.Top,
  Success extends Schema.Top,
  Error extends Schema.Top,
>(
  name: string,
  decl: { readonly payload: Payload; readonly success: Success; readonly error: Error },
): DefinedUpdate<Payload, Success, Error> => {
  const update = DurableUpdate.make(name, decl);
  return {
    name,
    update,
    // SAFETY: the seam decodes the payload and encodes the response through
    // this update's own schemas.
    take: withOps((runtime) => runtime.updateTake(update)) as Effect.Effect<
      UpdateRequest<Payload["Type"], Success["Type"], Error["Type"]>,
      never,
      WorkflowOps
    >,
  };
};

/**
 * A declared state cell: `set` inside a handler; client-side reads
 * (`wf.readStateCell`, `readStateCell`) accept the declaration directly.
 *
 * @since 0.4.0
 * @category models
 */
export interface DefinedState<Value extends Schema.Top> {
  readonly name: string;
  readonly cell: StateCell.StateCell<Value>;
  readonly set: (value: Value["Type"]) => Effect.Effect<void, never, WorkflowOps>;
}

/**
 * Observable workflow state. `set` publishes inside a handler.
 *
 * @since 0.3.0
 * @category constructors
 */
export const defineState = <Value extends Schema.Top>(
  name: string,
  decl: { readonly value: Value },
): DefinedState<Value> => {
  const cell = StateCell.make(name, { value: decl.value });
  return {
    name,
    cell,
    set: (value) => withOps((runtime) => runtime.stateSet(cell, value)),
  };
};

// ─── Addressing declarations from the client half ────────────────────────────

/**
 * A mailbox as either its declaration or its underlying primitive — what
 * every client-side offer accepts.
 *
 * @since 0.4.0
 * @category models
 */
export type MailboxLike<S extends Schema.Top> =
  | DurableMailbox.DurableMailbox<S>
  | { readonly mailbox: DurableMailbox.DurableMailbox<S> };

/**
 * An update as either its declaration or its underlying primitive.
 *
 * @since 0.4.0
 * @category models
 */
export type UpdateLike<P extends Schema.Top, S extends Schema.Top, E extends Schema.Top> =
  | DurableUpdate.DurableUpdate<P, S, E>
  | { readonly update: DurableUpdate.DurableUpdate<P, S, E> };

/**
 * A state cell as either its declaration or its underlying primitive.
 *
 * @since 0.4.0
 * @category models
 */
export type StateCellLike<S extends Schema.Top> =
  | StateCell.StateCell<S>
  | { readonly cell: StateCell.StateCell<S> };

/**
 * A deferred as either its declaration or its underlying primitive.
 *
 * @since 0.4.0
 * @category models
 */
export type DeferredLike<Success extends Schema.Constraint, Error extends Schema.Constraint> =
  | DurableDeferred.DurableDeferred<Success, Error>
  | { readonly deferred: DurableDeferred.DurableDeferred<Success, Error> };

/**
 * The primitive behind a mailbox-like value.
 *
 * @since 0.4.0
 * @category utils
 */
export const toMailbox = <S extends Schema.Top>(
  mailbox: MailboxLike<S>,
): DurableMailbox.DurableMailbox<S> => ("mailbox" in mailbox ? mailbox.mailbox : mailbox);

/**
 * The primitive behind an update-like value.
 *
 * @since 0.4.0
 * @category utils
 */
export const toUpdate = <P extends Schema.Top, S extends Schema.Top, E extends Schema.Top>(
  update: UpdateLike<P, S, E>,
): DurableUpdate.DurableUpdate<P, S, E> => ("update" in update ? update.update : update);

/**
 * The primitive behind a state-cell-like value.
 *
 * @since 0.4.0
 * @category utils
 */
export const toStateCell = <S extends Schema.Top>(cell: StateCellLike<S>): StateCell.StateCell<S> =>
  "cell" in cell ? cell.cell : cell;

/**
 * The primitive behind a deferred-like value.
 *
 * @since 0.4.0
 * @category utils
 */
export const toDeferred = <Success extends Schema.Constraint, Error extends Schema.Constraint>(
  deferred: DeferredLike<Success, Error>,
): DurableDeferred.DurableDeferred<Success, Error> =>
  "deferred" in deferred ? deferred.deferred : deferred;

// ─── Timers ──────────────────────────────────────────────────────────────────

/**
 * Sleep durably for `duration`. Names must be unique per sleep within a run
 * (suffix loop iterations). On Temporal this is a real timer — no worker
 * resources while waiting, survives restarts; in the in-memory runtime it
 * follows Effect's `Clock`, so a `TestClock` can `adjust` past it.
 *
 * @since 0.4.0
 * @category timers
 */
export const sleep = (options: SleepOptions): Effect.Effect<void, never, WorkflowOps> =>
  withOps((runtime) => runtime.sleep(options));

/**
 * Sleep durably until an absolute time, no-op when it is already past. The
 * target is read against the engine's deterministic clock, so the delay is
 * stable on replay. Zone-less date-time strings and unparseable timestamps
 * die loudly (see `sleepUntilTarget`).
 *
 * @since 0.4.0
 * @category timers
 */
export const sleepUntil = (options: SleepUntilOptions): Effect.Effect<void, never, WorkflowOps> =>
  withOps((runtime) => runtime.sleepUntil(options));

/**
 * The one timestamp rule every engine's `sleepUntil` applies: epoch millis
 * pass through; a date-time string must carry its zone (`Z` or an explicit
 * offset — date-only forms are UTC per ECMAScript); anything unparseable is
 * a defect. Zone-less strings would parse in the worker's LOCAL timezone,
 * which differs across workers and replays.
 *
 * @since 0.4.0
 * @category timers
 */
export const sleepUntilTarget = (options: SleepUntilOptions): Effect.Effect<number> =>
  Effect.suspend(() => {
    if (
      typeof options.timestamp === "string" &&
      options.timestamp.includes("T") &&
      !/(?:Z|[+-]\d{2}:?\d{2})$/.test(options.timestamp)
    ) {
      return Effect.die(
        `sleepUntil "${options.name}": date-time string "${options.timestamp}" has no timezone — zone-less strings parse in the worker's LOCAL timezone, which differs across workers and replays. Add "Z" or an explicit offset, or pass epoch millis.`,
      );
    }
    const target =
      typeof options.timestamp === "number" ? options.timestamp : Date.parse(options.timestamp);
    if (Number.isNaN(target)) {
      return Effect.die(
        `sleepUntil "${options.name}": unparseable timestamp "${String(options.timestamp)}"`,
      );
    }
    return Effect.succeed(target);
  });

// ─── Run composition ─────────────────────────────────────────────────────────

/**
 * End this run and atomically start a fresh one of the SAME workflow with
 * `payload`, keeping the workflow id while resetting history — Temporal's
 * continue-as-new, for unbounded workflows. Type `Effect<never>`: nothing
 * runs after it. Like the native API it unwinds the run — finalizers and
 * `Workflow.withCompensation` steps fire on the way out — so call it at
 * iteration boundaries, outside compensation regions, after draining
 * mailboxes (buffered messages do not carry into the new run).
 *
 * In the in-memory runtime the handler fiber is interrupted and the world
 * records the continuation (`world.continuedAsNew`), payload round-tripped
 * through the workflow's schema.
 *
 * @since 0.4.0
 * @category run
 */
export const continueAsNew = <
  Tag extends string,
  Payload extends Workflow.AnyStructSchema,
  Success extends Schema.Top,
  Error extends Schema.Top,
>(
  workflow: Workflow.Workflow<Tag, Payload, Success, Error>,
  payload: Payload["Type"],
  options?: ContinueAsNewOptions,
): Effect.Effect<never, never, WorkflowOps> =>
  withOps((runtime) => runtime.continueAsNew(workflow, payload, options));

/**
 * Options for `executeChild`.
 *
 * @since 0.4.0
 * @category models
 */
export interface ExecuteChildOptions<Discard extends boolean> {
  /** `true`: start the child and return its execution id without awaiting;
   * the child outlives the parent (ABANDON). Default: await the child's
   * typed result (REQUEST_CANCEL on parent close). */
  readonly discard?: Discard;
}

/**
 * Start a child workflow from a handler — the engine-agnostic form of
 * `Child.execute(payload)` inside a workflow body. Both must be hosted by
 * the same bundle. The child's workflow id is its digest execution id, so
 * the idempotency contract stays global: an id already taken attaches and
 * returns that execution's result. Typed results and failures compose into
 * the parent's channels; `{ discard: true }` returns the execution id.
 *
 * @since 0.4.0
 * @category run
 */
export const executeChild = <
  Tag extends string,
  Payload extends Workflow.AnyStructSchema,
  Success extends Schema.Top,
  Error extends Schema.Top,
  const Discard extends boolean = false,
>(
  workflow: Workflow.Workflow<Tag, Payload, Success, Error>,
  payload: Payload["Type"],
  options?: ExecuteChildOptions<Discard>,
): Effect.Effect<
  Discard extends true ? string : Success["Type"],
  Discard extends true ? never : Error["Type"],
  WorkflowOps
> =>
  // SAFETY: the seam returns the child's exit decoded through the child's
  // own success/error schemas (or its execution id when discarded).
  withOps((runtime) =>
    runtime.executeChild(workflow, payload, { discard: options?.discard === true }),
  ) as never;

// ─── Versioning ──────────────────────────────────────────────────────────────

/**
 * Patch-marker version selection at a code site (see the versioning
 * guide): the newest name on fresh executions, the recorded name on
 * replays. Engines without replay always answer the newest.
 *
 * @since 0.3.0
 * @category versioning
 */
export const version = <const Names extends readonly [string, ...string[]]>(
  site: string,
  names: Names,
): Effect.Effect<Names[number], never, WorkflowOps> =>
  withOps((runtime) => runtime.version(site, names));

/**
 * The run-table form of `version`: one effect per version name, keyed
 * OLDEST FIRST — the key order IS the chain order (the first key is the
 * original, unguarded behavior; each later key is guarded by its own
 * marker `${site}-${name}`). The selected case runs; result, error, and
 * service channels are unioned across cases.
 *
 * ```ts
 * const greeting = yield* versioned("greeting", {
 *   v1: greetV1,
 *   v2: greetV2,
 * });
 * ```
 *
 * Evaluate at a deterministic point on the main workflow fiber, never
 * inside forks or races (the `versioning-on-main-fiber` lint rule checks
 * this call too).
 *
 * @since 0.4.0
 * @category versioning
 */
export const versioned = <
  const Cases extends { readonly [name: string]: Effect.Effect<unknown, unknown, unknown> },
>(
  site: string,
  cases: Cases,
): Effect.Effect<
  Effect.Success<Cases[keyof Cases]>,
  Effect.Error<Cases[keyof Cases]>,
  Effect.Services<Cases[keyof Cases]> | WorkflowOps
> => {
  const names = Object.keys(cases);
  const first = names[0];
  if (first === undefined) {
    return Effect.die(`versioned "${site}": at least one case is required`);
  }
  // SAFETY: `version` answers one of exactly these keys, so the lookup
  // always hits, and the matched case's channels are covered by the union
  // over `Cases[keyof Cases]`.
  return Effect.flatMap(
    version(site, [first, ...names.slice(1)]),
    (name) => cases[name]!,
  ) as Effect.Effect<
    Effect.Success<Cases[keyof Cases]>,
    Effect.Error<Cases[keyof Cases]>,
    Effect.Services<Cases[keyof Cases]> | WorkflowOps
  >;
};

// ─── Schema evolution ────────────────────────────────────────────────────────

/**
 * Newest-first schema evolution for any declared boundary: decode tries
 * `current`, then `legacy` migrated forward by a PURE function (purity is
 * what keeps replay deterministic). Encoding always writes the newest
 * shape, and handlers only ever see the newest Type. Chain `evolved` calls
 * for further generations.
 *
 * @since 0.3.0
 * @category schemas
 */
export const evolved = <Current extends Schema.Top, Legacy extends Schema.Top>(
  current: Current,
  legacy: Legacy,
  migrate: (value: Legacy["Type"]) => Current["Type"],
) =>
  Schema.Union([
    current,
    legacy.pipe(
      Schema.decodeTo(current, {
        decode: SchemaGetter.transform(migrate),
        encode: SchemaGetter.forbidden(() => "legacy shapes are never written"),
      }),
    ),
  ]);
