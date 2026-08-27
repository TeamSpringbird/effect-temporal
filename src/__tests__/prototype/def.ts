// PROTOTYPE — throwaway design spike, NOT production code.
//
// Question: can ONE declaration (workflow + activities + messages + state)
// make the types flow to the handler, the worker implementation, and the
// client — with the handler fully engine-agnostic? This is the answer to
// the "TypedActivity / DurableDeferred leak": the workflow body should
// import nothing from engine-sandbox; every capability arrives as a typed
// `ops` toolkit derived from the single declaration, and the ENGINE decides
// how each op executes.

import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Workflow from "effect/unstable/workflow/Workflow";
import * as DurableDeferred from "effect/unstable/workflow/DurableDeferred";
import * as DurableMailbox from "../../mailbox.js";
import * as DurableUpdate from "../../update.js";
import * as StateCell from "../../state-cell.js";
import * as TypedActivity from "../../typed-activity.js";

// ── Declaration shapes ───────────────────────────────────────────────────────

export interface ActivityDecl {
  readonly payload: Schema.Top;
  readonly success?: Schema.Top;
  readonly error?: Schema.Top;
  readonly options?: TypedActivity.TypedActivityOptions;
}

export type MessageDecl =
  | { readonly deferred: Schema.Top }
  | { readonly mailbox: Schema.Top }
  | { readonly update: { readonly payload: Schema.Top; readonly success: Schema.Top; readonly error: Schema.Top } };

type SchemaType<S, Fallback> = S extends Schema.Top ? S["Type"] : Fallback;

// ── The typed ops toolkit the handler receives ───────────────────────────────

export interface UpdateRequestOf<P, S, E> {
  readonly payload: P;
  readonly respond: (exit: Exit.Exit<S, E>) => Effect.Effect<void>;
}

export type OpsOf<
  A extends Record<string, ActivityDecl>,
  M extends Record<string, MessageDecl>,
  St extends Record<string, Schema.Top>,
> = {
  readonly activity: {
    readonly [K in keyof A]: (
      payload: A[K]["payload"]["Type"],
    ) => Effect.Effect<SchemaType<A[K]["success"], void>, SchemaType<A[K]["error"], never>>;
  };
  readonly message: {
    readonly [K in keyof M]: M[K] extends { readonly deferred: infer S extends Schema.Top }
      ? { readonly await: Effect.Effect<S["Type"]> }
      : M[K] extends { readonly mailbox: infer P extends Schema.Top }
        ? {
            readonly take: Effect.Effect<P["Type"]>;
            readonly poll: Effect.Effect<Option.Option<P["Type"]>>;
          }
        : M[K] extends {
              readonly update: {
                readonly payload: infer P extends Schema.Top;
                readonly success: infer S extends Schema.Top;
                readonly error: infer E extends Schema.Top;
              };
            }
          ? { readonly take: Effect.Effect<UpdateRequestOf<P["Type"], S["Type"], E["Type"]>> }
          : never;
  };
  readonly state: {
    readonly [K in keyof St]: { readonly set: (value: St[K]["Type"]) => Effect.Effect<void> };
  };
};

// ── The one engine seam: an untyped runtime the typed ops dispatch through ──
// This is what each engine implements. The Temporal one wraps the existing
// engine-sandbox machinery; the memory one is plain queues and deferreds.

export interface OpsRuntime {
  readonly activity: (
    activity: TypedActivity.AnyTypedActivity,
    payload: unknown,
  ) => Effect.Effect<unknown, unknown>;
  readonly deferredAwait: (name: string) => Effect.Effect<unknown>;
  readonly mailboxTake: (name: string) => Effect.Effect<unknown>;
  readonly mailboxPoll: (name: string) => Effect.Effect<Option.Option<unknown>>;
  readonly updateTake: (
    name: string,
  ) => Effect.Effect<UpdateRequestOf<unknown, unknown, unknown>>;
  readonly stateSet: (name: string, value: unknown) => Effect.Effect<void>;
}

// ── Worker implementation typing: completeness-checked from the declaration ─

export type ImplementationsOf<A extends Record<string, ActivityDecl>> = {
  readonly [K in keyof A]: (
    payload: A[K]["payload"]["Type"],
  ) => Effect.Effect<SchemaType<A[K]["success"], void>, SchemaType<A[K]["error"], never>>;
};

// ── defineWorkflow ───────────────────────────────────────────────────────────

export const defineWorkflow = <
  const Tag extends string,
  const P extends Schema.Struct.Fields,
  S extends Schema.Top,
  E extends Schema.Top,
  const A extends Record<string, ActivityDecl>,
  const M extends Record<string, MessageDecl>,
  const St extends Record<string, Schema.Top>,
>(
  tag: Tag,
  decl: {
    readonly payload: P;
    readonly idempotencyKey: (payload: Schema.Struct<P>["Type"]) => string;
    readonly success?: S;
    readonly error?: E;
    readonly activities?: A;
    readonly messages?: M;
    readonly state?: St;
  },
) => {
  const workflow = Workflow.make(tag, {
    payload: decl.payload,
    idempotencyKey: decl.idempotencyKey,
    ...(decl.success === undefined ? {} : { success: decl.success }),
    ...(decl.error === undefined ? {} : { error: decl.error }),
  });

  // Materialize the existing primitives once, from the declaration. Names
  // are namespaced by tag so two definitions never collide.
  const activities = Object.fromEntries(
    Object.entries(decl.activities ?? {}).map(([key, a]) => [
      key,
      TypedActivity.make(`${tag}/${key}`, {
        payload: a.payload,
        ...(a.success === undefined ? {} : { success: a.success }),
        ...(a.error === undefined ? {} : { error: a.error }),
        ...(a.options === undefined ? {} : { options: a.options }),
      }),
    ]),
  ) as Record<string, TypedActivity.AnyTypedActivity>;

  const messages = decl.messages ?? ({} as M);
  const messageName = (key: string) => `${tag}/${key}`;
  const deferreds = Object.fromEntries(
    Object.entries(messages)
      .filter(([, m]) => "deferred" in m)
      .map(([key, m]) => [
        key,
        DurableDeferred.make(messageName(key), { success: (m as { deferred: Schema.Top }).deferred }),
      ]),
  );
  const mailboxes = Object.fromEntries(
    Object.entries(messages)
      .filter(([, m]) => "mailbox" in m)
      .map(([key, m]) => [
        key,
        DurableMailbox.make(messageName(key), { payload: (m as { mailbox: Schema.Top }).mailbox }),
      ]),
  );
  const updates = Object.fromEntries(
    Object.entries(messages)
      .filter(([, m]) => "update" in m)
      .map(([key, m]) => {
        const u = (m as { update: { payload: Schema.Top; success: Schema.Top; error: Schema.Top } }).update;
        return [key, DurableUpdate.make(messageName(key), { payload: u.payload, success: u.success, error: u.error })];
      }),
  );
  const cells = Object.fromEntries(
    Object.entries(decl.state ?? {}).map(([key, value]) => [
      key,
      StateCell.make(messageName(key), { value }),
    ]),
  );

  /** Build the TYPED ops toolkit over an untyped runtime — the single place
   * the unknown-seam casts live. */
  const makeOps = (runtime: OpsRuntime): OpsOf<A, M, St> => {
    const activity = Object.fromEntries(
      Object.entries(activities).map(([key, a]) => [
        key,
        (payload: unknown) => runtime.activity(a, payload),
      ]),
    );
    const message = Object.fromEntries(
      Object.keys(messages).map((key) => {
        const name = messageName(key);
        const m = messages[key]!;
        if ("deferred" in m) return [key, { await: runtime.deferredAwait(name) }];
        if ("mailbox" in m)
          return [key, { take: runtime.mailboxTake(name), poll: runtime.mailboxPoll(name) }];
        return [key, { take: runtime.updateTake(name) }];
      }),
    );
    const state = Object.fromEntries(
      Object.keys(cells).map((key) => [
        key,
        { set: (value: unknown) => runtime.stateSet(messageName(key), value) },
      ]),
    );
    // SAFETY (prototype): the runtime dispatches by the primitives built
    // from the same declaration the Ops type is derived from.
    return { activity, message, state } as unknown as OpsOf<A, M, St>;
  };

  type Payload = Schema.Struct<P>["Type"];
  type Success = SchemaType<S, void>;
  type Err = SchemaType<E, never>;

  return {
    tag,
    workflow,
    activities,
    deferreds,
    mailboxes,
    updates,
    cells,
    makeOps,
    /** Bind the engine-agnostic handler. The handler's R is `never`: it can
     * touch the outside world only through the typed ops. */
    handler: (
      body: (payload: Payload, ops: OpsOf<A, M, St>) => Effect.Effect<Success, Err>,
    ) => ({ definition: { tag, workflow, activities, deferreds, mailboxes, updates, cells, makeOps }, body }),
    /** Worker-side implementations, completeness-checked from the declaration. */
    implement: (impls: ImplementationsOf<A>): ImplementationsOf<A> => impls,
  };
};

export type AnyDefined = ReturnType<typeof defineWorkflow>;
