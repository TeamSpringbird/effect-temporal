/**
 * Wire contract shared by the client and sandbox halves: workflow argument,
 * workflow result, and the deferred-done signal, defined once so the two
 * sides cannot drift. Must stay free of `@temporalio/*` imports — it is part
 * of the workflow bundle's graph.
 *
 * Exits need an explicit JSON codec at every crossing: `Schema.Exit`'s
 * encoded form is still an `Exit` instance (its decoder begins with
 * `Exit.isExit`), which does not survive Temporal's JSON payload converter.
 * Everything crossing a signal or result boundary is therefore wrapped in
 * `Schema.toCodecJson` and decoded back to a genuine instance on receipt.
 *
 * **Advanced.** Applications do not import this module: the engine, the
 * `WorkflowClient` service, and the `testing` helpers consume it for you.
 * It is exported for engine-level composition — anything that must speak
 * the same wire (custom tooling, an alternative client surface).
 *
 * @since 0.1.0
 */

import type * as Exit from "effect/Exit";
import * as Schema from "effect/Schema";
import type * as Workflow from "effect/unstable/workflow/Workflow";

/**
 * Signal by which `deferredDone` reaches a running workflow.
 *
 * @since 0.1.0
 * @category wire
 */
export const DEFERRED_DONE_SIGNAL = "effect-workflow-deferred-done";

/**
 * Query by which the client half reads a deferred's current state: takes a
 * deferred name, returns its wire-encoded exit, or null while unresolved.
 *
 * @since 0.1.0
 * @category wire
 */
export const DEFERRED_STATE_QUERY = "effect-workflow-deferred-state";

/**
 * `ApplicationFailure.type` carrying a run's non-success exit in
 * `details[0]`: the run shows as FAILED in the Temporal UI while the reading
 * side decodes the exit back to the typed channel.
 *
 * @since 0.1.0
 * @category wire
 */
export const EXIT_FAILURE_TYPE = "EffectWorkflowExit";

/**
 * The attach bridge's activity name — see `activities.ts`.
 *
 * @since 0.1.0
 * @category wire
 */
export const BRIDGE_POLL_ACTIVITY = "effectWorkflowPollResult";

/**
 * What one attach poll of a foreign execution reports.
 *
 * @since 0.1.0
 * @category models
 */
export type EffectWorkflowBridgeResult =
  | { readonly kind: "running" }
  | { readonly kind: "wire"; readonly exit: unknown }
  | { readonly kind: "interrupted" }
  | { readonly kind: "defect"; readonly message: string };

/**
 * What one deferred-done signal carries: the target deferred and its exit.
 *
 * @since 0.1.0
 * @category models
 */
export interface DeferredDoneSignalPayload {
  readonly deferredName: string;
  /** `WireDeferredExit`-encoded exit (leaf values already schema-encoded). */
  readonly exit: unknown;
}

/**
 * Collapse a `Schema.Top`-held schema to a plain unknown⇄unknown codec —
 * the seam traffics in `unknown` by design. Shared with the sandbox half.
 *
 * @since 0.1.0
 * @category codecs
 */
export const asJsonCodec = (schema: Schema.Top): Schema.Codec<unknown, unknown> =>
  schema as unknown as Schema.Codec<unknown, unknown>;

/**
 * A typed encode/decode pair over one schema's JSON wire form.
 *
 * @since 0.1.0
 * @category models
 */
export interface WireValueCodec<T> {
  readonly encode: (value: T) => unknown;
  readonly decode: (wire: unknown) => T;
}

/**
 * Typed encode/decode pair over a schema's JSON codec — the one place the
 * unknown-seam casts around `Schema.toCodecJson` live.
 *
 * @since 0.1.0
 * @category codecs
 */
export const wireValueCodec = <S extends Schema.Top>(schema: S): WireValueCodec<S["Type"]> => {
  const codec = asJsonCodec(Schema.toCodecJson(schema));
  return {
    encode: Schema.encodeSync(codec) as (value: S["Type"]) => unknown,
    decode: Schema.decodeSync(codec) as (wire: unknown) => S["Type"],
  };
};

/**
 * What a thrown Temporal error means for the Effect channels: cancellation,
 * a wire-encoded typed exit riding an `ApplicationFailure`, or anything else.
 *
 * @since 0.1.0
 * @category models
 */
export type ThrownClassification =
  | { readonly kind: "interrupted" }
  | { readonly kind: "wire"; readonly exit: unknown }
  | { readonly kind: "other"; readonly error: unknown };

/**
 * Build a thrown-error classifier. The error classes are parameters because
 * each half must test against its own SDK module's identities
 * (`@temporalio/client` vs `@temporalio/workflow`) — this module stays free
 * of both.
 *
 * @since 0.1.0
 * @category errors
 */
export const makeClassifyThrown =
  (
    classes: {
      readonly CancelledFailure: abstract new (...args: never[]) => object;
      readonly ApplicationFailure: abstract new (...args: never[]) => {
        readonly type?: string | undefined | null;
        readonly details?: readonly unknown[] | null | undefined;
      };
    },
    exitType: string = EXIT_FAILURE_TYPE,
  ) =>
  (error: unknown): ThrownClassification => {
    let current: unknown = error;
    for (let depth = 0; depth < 8 && current !== undefined && current !== null; depth++) {
      if (current instanceof classes.CancelledFailure) return { kind: "interrupted" };
      if (current instanceof classes.ApplicationFailure && current.type === exitType) {
        return { kind: "wire", exit: current.details?.[0] };
      }
      // SAFETY: a read-only probe of an optional property — absent (or a
      // primitive receiver) yields undefined, which ends the loop.
      current = (current as { cause?: unknown }).cause;
    }
    return { kind: "other", error };
  };

/**
 * JSON codec for exits whose LEAF values are already encoded — the shape
 * `WorkflowEngine.makeUnsafe` hands to `deferredDone` and expects back from
 * `deferredResult`. Only the Exit/Cause STRUCTURE is (de)serialized here;
 * leaves pass through as `Unknown`.
 */
const wireDeferredExit = wireValueCodec(
  Schema.Exit(Schema.Unknown, Schema.Unknown, Schema.Defect()),
);

/**
 * Encode a leaf-encoded deferred exit to plain JSON for a signal crossing.
 *
 * @since 0.1.0
 * @category codecs
 */
export const encodeDeferredExit: (exit: Exit.Exit<unknown, unknown>) => unknown =
  wireDeferredExit.encode;
/**
 * Decode a signal-crossed deferred exit back to a genuine `Exit` instance
 * (leaves still schema-encoded).
 *
 * @since 0.1.0
 * @category codecs
 */
export const decodeDeferredExit: (wire: unknown) => Exit.Exit<unknown, unknown> =
  wireDeferredExit.decode;

/** The subset of a workflow definition the wire codecs need. */
type AnyWorkflowWithProps = Workflow.Any & {
  readonly payloadSchema: Schema.Top;
  readonly successSchema: Schema.Top;
  readonly errorSchema: Schema.Top;
};

/**
 * The per-workflow codecs for the two Temporal crossings: the workflow
 * argument (payload) and the run result (exit).
 *
 * @since 0.1.0
 * @category models
 */
export interface WorkflowWireCodecs {
  readonly encodePayload: (payload: unknown) => unknown;
  readonly decodePayload: (wire: unknown) => unknown;
  readonly encodeExit: (exit: Exit.Exit<unknown, unknown>) => unknown;
  readonly decodeExit: (wire: unknown) => Exit.Exit<unknown, unknown>;
}

/**
 * Per-workflow JSON codecs for the Temporal workflow argument and result.
 * A successful run's result is its encoded success exit; non-success exits
 * travel in an `EXIT_FAILURE_TYPE` failure's details instead (the run
 * fails), so `decodeExit` must handle both shapes.
 *
 * @since 0.1.0
 * @category codecs
 */
export const wireCodecsFor = (workflow: Workflow.Any): WorkflowWireCodecs => {
  // SAFETY: `Workflow.Any` carries these schema fields; the assertion only
  // names the subset the codecs read, with the field types it needs.
  const props = workflow as AnyWorkflowWithProps;
  const payload = wireValueCodec(props.payloadSchema);
  // SAFETY: widening — the schema's exit type narrows to
  // `Exit<unknown, unknown>`, which is all this type-erased seam exposes.
  const exit = wireValueCodec(
    Schema.Exit(props.successSchema, props.errorSchema, Schema.Defect()),
  ) as WireValueCodec<Exit.Exit<unknown, unknown>>;
  return {
    encodePayload: payload.encode,
    decodePayload: payload.decode,
    encodeExit: exit.encode,
    decodeExit: exit.decode,
  };
};
