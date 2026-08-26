// Message-passing demo definitions, mirroring the Temporal `message-passing`
// sample: a language service driven by an update (set language, returning
// the previous one or a typed failure), observed via a state cell, and
// finished by a one-shot approval.

import * as Schema from "effect/Schema";
import * as DurableDeferred from "effect/unstable/workflow/DurableDeferred";
import * as Workflow from "effect/unstable/workflow/Workflow";
import * as StateCell from "../../state-cell.js";
import * as DurableUpdate from "../../update.js";

export const SUPPORTED_LANGUAGES = ["english", "french", "spanish"] as const;

export const SetLanguage = DurableUpdate.make("set-language", {
  payload: Schema.Struct({ language: Schema.String }),
  success: Schema.String,
  error: Schema.String,
});

export const CurrentLanguage = StateCell.make("current-language", {
  value: Schema.String,
});

export const Approved = DurableDeferred.make("message-approved", {
  success: Schema.String,
});

/** An update the workflow NEVER takes — for the lifecycle-edge test where a
 * run completes with the request still pending. */
export const Orphan = DurableUpdate.make("orphan", {
  payload: Schema.Struct({ note: Schema.String }),
  success: Schema.String,
  error: Schema.String,
});

export const MessageDemo = Workflow.make("effectMessageDemo", {
  payload: { requestId: Schema.String },
  idempotencyKey: ({ requestId }) => requestId,
  success: Schema.String,
});

/** Completes a deferred on ANOTHER execution (workflow → workflow
 * `DurableDeferred.done`), then returns — for asserting that a target which
 * never existed (or already closed) is a no-op for the SENDER. */
export const DeferredPokeDemo = Workflow.make("effectDeferredPokeDemo", {
  payload: { requestId: Schema.String, targetExecutionId: Schema.String },
  idempotencyKey: ({ requestId }) => requestId,
  success: Schema.String,
});
