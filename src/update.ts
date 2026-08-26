/**
 * Request/response into a running workflow — Temporal updates with typed
 * channels: the caller gets the handler's typed success or typed failure
 * back, unlike a fire-and-forget mailbox message. The workflow body takes
 * requests with `takeUpdate` (engine-sandbox) and answers each through its
 * `respond`; clients call `executeUpdate` (engine-client) and receive the
 * response in the Effect error/success channels.
 *
 * The response always travels as a wire-encoded Exit in the update RESULT
 * (the update itself never fails), so typed failures round-trip without a
 * failure-conversion seam.
 *
 * @since 0.1.0
 */

import type * as Exit from "effect/Exit";
import * as Schema from "effect/Schema";
import { wireValueCodec, type WireValueCodec } from "./wire.js";

/**
 * The one Temporal update all `DurableUpdate` requests ride.
 *
 * @since 0.1.0
 * @category wire
 */
export const WORKFLOW_UPDATE = "effect-workflow-update";

/**
 * What one update request carries: the target update and its payload.
 *
 * @since 0.1.0
 * @category models
 */
export interface WorkflowUpdatePayload {
  readonly updateName: string;
  /** Wire-encoded request payload (already schema-encoded JSON). */
  readonly payload: unknown;
}

/**
 * An update definition: the name, request payload schema, and response
 * success/error schemas shared by the workflow body and every calling side.
 *
 * @since 0.1.0
 * @category models
 */
export interface DurableUpdate<P extends Schema.Top, S extends Schema.Top, E extends Schema.Top> {
  readonly name: string;
  readonly payloadSchema: P;
  readonly successSchema: S;
  readonly errorSchema: E;
}

/**
 * Declare an update: a name (unique within the workflows that use it), the
 * request payload schema, and the response's success/error schemas. Shared
 * by the workflow body and every calling side.
 *
 * @since 0.1.0
 * @category constructors
 */
export const make = <P extends Schema.Top, S extends Schema.Top, E extends Schema.Top>(
  name: string,
  options: { readonly payload: P; readonly success: S; readonly error: E },
): DurableUpdate<P, S, E> => ({
  name,
  payloadSchema: options.payload,
  successSchema: options.success,
  errorSchema: options.error,
});

/**
 * The encode/decode pairs for an update's two crossings: the request
 * payload, and the response exit that travels in the update result.
 *
 * @since 0.1.0
 * @category models
 */
export interface UpdateCodec<P extends Schema.Top, S extends Schema.Top, E extends Schema.Top> {
  readonly encodePayload: (payload: P["Type"]) => unknown;
  readonly decodePayload: (wire: unknown) => P["Type"];
  readonly encodeExit: (exit: Exit.Exit<S["Type"], E["Type"]>) => unknown;
  readonly decodeExit: (wire: unknown) => Exit.Exit<S["Type"], E["Type"]>;
}

/**
 * Build the wire codecs for an update — payload encoding for the request,
 * exit encoding for the response — from the definition's own schemas, so
 * `takeUpdate` and `executeUpdate` cannot disagree on the wire shape.
 *
 * @since 0.1.0
 * @category codecs
 */
export const updateCodec = <P extends Schema.Top, S extends Schema.Top, E extends Schema.Top>(
  update: DurableUpdate<P, S, E>,
): UpdateCodec<P, S, E> => {
  const payload = wireValueCodec(update.payloadSchema);
  // SAFETY: the codec is built from the update's own success/error schemas,
  // so the schema's Type is exactly Exit<S["Type"], E["Type"]> — the cast
  // only restores what `Schema.Exit`'s generic plumbing cannot carry here.
  const exit = wireValueCodec(
    Schema.Exit(update.successSchema, update.errorSchema, Schema.Defect()),
  ) as WireValueCodec<Exit.Exit<S["Type"], E["Type"]>>;
  return {
    encodePayload: payload.encode,
    decodePayload: payload.decode,
    encodeExit: exit.encode,
    decodeExit: exit.decode,
  };
};
