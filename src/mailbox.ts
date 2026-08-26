/**
 * A durable inbound message queue for a workflow: the repeated-signal
 * counterpart to the one-shot `DurableDeferred`. Deliveries ride a Temporal
 * signal (recorded in history, so consumption is deterministic on replay)
 * and buffer until the workflow takes them.
 *
 * This module holds the shared definition and codec; the operations live
 * with their process: `takeMailbox` / `offerMailbox` (workflow → workflow)
 * in `engine-sandbox`, `offerMailbox` (client → workflow) in
 * `engine-client`.
 *
 * @since 0.1.0
 */

import type * as Schema from "effect/Schema";
import { wireValueCodec, type WireValueCodec } from "./wire.js";

/**
 * Signal by which offers reach a running workflow.
 *
 * @since 0.1.0
 * @category wire
 */
export const MAILBOX_SIGNAL = "effect-workflow-mailbox";

/**
 * What one mailbox signal carries: the target mailbox and its message.
 *
 * @since 0.1.0
 * @category models
 */
export interface MailboxSignalPayload {
  readonly mailboxName: string;
  /** Wire-encoded payload (already schema-encoded JSON). */
  readonly payload: unknown;
}

/**
 * A mailbox definition: the name and payload schema shared by the
 * consuming workflow and every offering side.
 *
 * @since 0.1.0
 * @category models
 */
export interface DurableMailbox<S extends Schema.Top> {
  readonly name: string;
  readonly payloadSchema: S;
}

/**
 * Declare a mailbox: a name (unique within the workflows that use it) and
 * the payload schema. Shared by the workflow body and every offering side.
 *
 * @since 0.1.0
 * @category constructors
 */
export const make = <S extends Schema.Top>(
  name: string,
  options: { readonly payload: S },
): DurableMailbox<S> => ({ name, payloadSchema: options.payload });

/**
 * The wire codec for a mailbox's payload — how messages are encoded by
 * `offerMailbox` and decoded by `takeMailbox` / `pollMailbox`.
 *
 * @since 0.1.0
 * @category codecs
 */
export const mailboxCodec = <S extends Schema.Top>(
  mailbox: DurableMailbox<S>,
): WireValueCodec<S["Type"]> => wireValueCodec(mailbox.payloadSchema);
