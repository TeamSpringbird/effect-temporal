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
 *     const approver = yield* Approval.await;
 *     return `${receipt}:by:${approver}`;
 *   }),
 * );
 * ```
 *
 * Every operation requires only the `WorkflowOps` service — the one seam an
 * engine implements. `workflowBundle` provides the Temporal runtime; the
 * `testing` module provides an in-memory one, so the same handler runs on
 * real Temporal or in a plain unit test. Declarations are temporal-free and
 * carry the wire identity explicitly (their `name`), so refactoring code
 * never changes the wire.
 *
 * @since 0.3.0
 */

import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as SchemaGetter from "effect/SchemaGetter";
import * as DurableDeferred from "effect/unstable/workflow/DurableDeferred";
import * as DurableMailbox from "./mailbox.js";
import * as DurableUpdate from "./update.js";
import * as StateCell from "./state-cell.js";
import * as TypedActivity from "./typed-activity.js";

// ─── The ops seam ────────────────────────────────────────────────────────────

/**
 * A taken update request: the decoded payload and its one-shot typed
 * response channel.
 *
 * @since 0.3.0
 * @category models
 */
export interface UpdateRequest<P, S, E> {
  readonly payload: P;
  readonly respond: (exit: Exit.Exit<S, E>) => Effect.Effect<void>;
}

/**
 * What an engine implements to host declared capabilities: one operation
 * per primitive kind, dispatching on the declaration instances. The typed
 * surfaces below narrow this seam exactly once each.
 *
 * @since 0.3.0
 * @category models
 */
export interface WorkflowOpsRuntime {
  readonly activity: (
    activity: TypedActivity.AnyTypedActivity,
    payload: unknown,
  ) => Effect.Effect<unknown, unknown>;
  readonly deferredAwait: (
    deferred: DurableDeferred.DurableDeferred<Schema.Constraint>,
  ) => Effect.Effect<unknown>;
  readonly mailboxTake: (
    mailbox: DurableMailbox.DurableMailbox<Schema.Top>,
  ) => Effect.Effect<unknown>;
  readonly mailboxPoll: (
    mailbox: DurableMailbox.DurableMailbox<Schema.Top>,
  ) => Effect.Effect<Option.Option<unknown>>;
  readonly updateTake: (
    update: DurableUpdate.DurableUpdate<Schema.Top, Schema.Top, Schema.Top>,
  ) => Effect.Effect<UpdateRequest<unknown, unknown, unknown>>;
  readonly stateSet: (cell: StateCell.StateCell<Schema.Top>, value: unknown) => Effect.Effect<void>;
  readonly version: (site: string, names: ReadonlyArray<string>) => Effect.Effect<string>;
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

// ─── Activities ──────────────────────────────────────────────────────────────

/**
 * A declared activity: callable with its typed payload inside any workflow
 * handler, and carrying the underlying `TypedActivity` for worker binding
 * (`implementActivities` + `handle`) and wire identity.
 *
 * @since 0.3.0
 * @category models
 */
export interface DefinedActivity<
  Name extends string,
  Payload extends Schema.Top,
  Success extends Schema.Top,
  Error extends Schema.Top,
> extends TypedActivity.TypedActivity<Name, Payload, Success, Error> {
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
    readonly options?: TypedActivity.TypedActivityOptions;
  },
): DefinedActivity<
  Name,
  Payload extends Schema.Struct.Fields ? Schema.Struct<Payload> : Payload,
  Success,
  Error
> => {
  const activity = TypedActivity.make(name, decl);
  const call = (payload: unknown) =>
    Effect.flatMap(WorkflowOps, (runtime) => runtime.activity(activity, payload));
  // defineProperties, not Object.assign: a function's own `name` is
  // non-writable (assignment throws in strict mode) but configurable.
  // SAFETY: the callable narrows the runtime's unknown seam to the schemas
  // this very declaration carries.
  return Object.defineProperties(call, Object.getOwnPropertyDescriptors(activity)) as never;
};

// ─── Message channels and state ──────────────────────────────────────────────

/**
 * A one-shot typed completion an outside party resolves. `await` blocks
 * durably inside a handler; the underlying `deferred` drives the client
 * side (`DurableDeferred.done`, `deferredState`).
 *
 * @since 0.3.0
 * @category constructors
 */
export const defineDeferred = <Success extends Schema.Constraint>(
  name: string,
  decl: { readonly success: Success },
) => {
  const deferred = DurableDeferred.make(name, { success: decl.success });
  return {
    name,
    deferred,
    await: Effect.flatMap(WorkflowOps, (runtime) =>
      runtime.deferredAwait(deferred as DurableDeferred.DurableDeferred<Schema.Constraint>),
    ) as Effect.Effect<Success["Type"], never, WorkflowOps>,
  } as const;
};

/**
 * A durable inbound message queue. `take`/`poll` consume inside a handler;
 * the underlying `mailbox` drives the client side (`offerMailbox`).
 *
 * @since 0.3.0
 * @category constructors
 */
export const defineMailbox = <Payload extends Schema.Top>(
  name: string,
  decl: { readonly payload: Payload },
) => {
  const mailbox = DurableMailbox.make(name, { payload: decl.payload });
  const withRuntime = <T>(f: (runtime: WorkflowOpsRuntime) => Effect.Effect<T>) =>
    Effect.flatMap(WorkflowOps, f);
  return {
    name,
    mailbox,
    take: withRuntime((runtime) => runtime.mailboxTake(mailbox)) as Effect.Effect<
      Payload["Type"],
      never,
      WorkflowOps
    >,
    poll: withRuntime((runtime) => runtime.mailboxPoll(mailbox)) as Effect.Effect<
      Option.Option<Payload["Type"]>,
      never,
      WorkflowOps
    >,
  } as const;
};

/**
 * Request/response into a running workflow with typed channels. `take`
 * consumes requests inside a handler (respond exactly once); the underlying
 * `update` drives the client side (`executeUpdate`).
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
) => {
  const update = DurableUpdate.make(name, decl);
  return {
    name,
    update,
    take: Effect.flatMap(WorkflowOps, (runtime) =>
      runtime.updateTake(update),
    ) as Effect.Effect<UpdateRequest<Payload["Type"], Success["Type"], Error["Type"]>, never, WorkflowOps>,
  } as const;
};

/**
 * Observable workflow state. `set` publishes inside a handler; the
 * underlying `cell` drives the client side (`readStateCell`).
 *
 * @since 0.3.0
 * @category constructors
 */
export const defineState = <Value extends Schema.Top>(
  name: string,
  decl: { readonly value: Value },
) => {
  const cell = StateCell.make(name, { value: decl.value });
  return {
    name,
    cell,
    set: (value: Value["Type"]): Effect.Effect<void, never, WorkflowOps> =>
      Effect.flatMap(WorkflowOps, (runtime) => runtime.stateSet(cell, value)),
  } as const;
};

/**
 * Patch-marker version selection at a code site (see the versioning
 * guide): the newest name on fresh executions, the recorded name on
 * replays. Engines without replay always answer the newest.
 *
 * @since 0.3.0
 * @category combinators
 */
export const version = <const Names extends readonly [string, ...string[]]>(
  site: string,
  names: Names,
): Effect.Effect<Names[number], never, WorkflowOps> =>
  Effect.flatMap(WorkflowOps, (runtime) =>
    runtime.version(site, names),
  ) as Effect.Effect<Names[number], never, WorkflowOps>;

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
