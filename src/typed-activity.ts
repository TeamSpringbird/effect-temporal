/**
 * Schema-typed activity definitions: ONE declaration shared by the workflow
 * that calls an activity and the worker that implements it, so the two sides
 * cannot drift and the wire is validated at both boundaries. The definition
 * is temporal-free — it loads in the sandbox bundle and in client processes.
 *
 * ```ts
 * // definitions module
 * export const Reserve = TypedActivity.make("reserve", {
 *   payload: { requestId: Schema.String },
 *   success: Schema.String,
 *   error: Schema.TaggedStruct("OutOfStock", { sku: Schema.String }),
 *   options: { startToCloseTimeout: "5 minutes", retry: { maximumAttempts: 3 } },
 * });
 *
 * // workflow (engine-sandbox's `callActivity`)
 * const reservation = yield* callActivity(Reserve, { requestId });
 *
 * // worker (`implementActivities` over an `ActivityRunner` — the seam
 * // where your app's runtime, spans, and error reporting plug in)
 * ```
 *
 * Failure semantics: a TYPED failure (matching the `error` schema) is a
 * domain outcome — the worker throws it as a NON-RETRYABLE ApplicationFailure
 * carrying the encoded value, and the caller decodes it into the Effect error
 * channel. Everything else (defects, infra errors) stays a retryable activity
 * failure and surfaces to the workflow as a defect once retries exhaust.
 *
 * @since 0.1.0
 */

import * as Schema from "effect/Schema";
import { wireValueCodec, type WireValueCodec } from "./wire.js";

/**
 * `ApplicationFailure.type` carrying a typed activity failure in `details[0]`.
 *
 * @since 0.1.0
 * @category wire
 */
export const ACTIVITY_EXIT_TYPE = "EffectActivityExit";

/**
 * Applied when a definition declares no options of its own.
 *
 * @since 0.1.0
 * @category models
 */
export const DEFAULT_ACTIVITY_OPTIONS = {
  startToCloseTimeout: "10 minutes",
  retry: { maximumAttempts: 3 },
} as const;

/**
 * The Temporal activity options a definition carries — the subset of
 * `proxyActivities` options a typed activity pins at declaration time.
 *
 * @since 0.1.0
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
 * A typed activity definition: name, the three channel schemas, and the
 * Temporal options every call site honors.
 *
 * @since 0.1.0
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
 * Type-erased activity definition, for APIs that operate on any activity.
 *
 * @since 0.1.0
 * @category models
 */
export type AnyTypedActivity = TypedActivity<string, Schema.Top, Schema.Top, Schema.Top>;

/**
 * Extracts a definition's decoded payload type.
 *
 * @since 0.1.0
 * @category models
 */
export type PayloadOf<A> =
  A extends TypedActivity<string, infer P, Schema.Top, Schema.Top> ? P["Type"] : never;
/**
 * Extracts a definition's decoded success type.
 *
 * @since 0.1.0
 * @category models
 */
export type SuccessOf<A> =
  A extends TypedActivity<string, Schema.Top, infer S, Schema.Top> ? S["Type"] : never;
/**
 * Extracts a definition's decoded error type.
 *
 * @since 0.1.0
 * @category models
 */
export type ErrorOf<A> =
  A extends TypedActivity<string, Schema.Top, Schema.Top, infer E> ? E["Type"] : never;

/**
 * Declare a typed activity. `payload` accepts struct fields (wrapped in
 * `Schema.Struct` for you) or a full schema; `success` defaults to
 * `Schema.Void`, `error` to `Schema.Never`, and `options` to
 * `DEFAULT_ACTIVITY_OPTIONS`.
 *
 * @since 0.1.0
 * @category constructors
 */
export const make = <
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

/**
 * The wire codecs for a definition's three channels.
 *
 * @since 0.1.0
 * @category models
 */
export interface TypedActivityCodecs<A extends AnyTypedActivity> {
  readonly payload: WireValueCodec<PayloadOf<A>>;
  readonly success: WireValueCodec<SuccessOf<A>>;
  readonly error: WireValueCodec<ErrorOf<A>>;
}

/**
 * Build the wire codecs for an activity's payload, success, and error
 * channels from the definition's own schemas — the shared encoding used by
 * `callActivity` on the workflow side and `implementActivities` on the
 * worker side.
 *
 * @since 0.1.0
 * @category codecs
 */
// SAFETY: each codec is built from the definition's own schema, so the
// schema's Type is exactly what PayloadOf/SuccessOf/ErrorOf extract; the
// casts only restore what the type-erased `AnyTypedActivity` bound loses.
export const codecsFor = <A extends AnyTypedActivity>(activity: A): TypedActivityCodecs<A> => ({
  payload: wireValueCodec(activity.payloadSchema) as WireValueCodec<PayloadOf<A>>,
  success: wireValueCodec(activity.successSchema) as WireValueCodec<SuccessOf<A>>,
  error: wireValueCodec(activity.errorSchema) as WireValueCodec<ErrorOf<A>>,
});
