/**
 * Nexus handler-side bridge (Node, worker registration): expose a shim
 * workflow as a workflow-backed Nexus operation. The operation's input is
 * the workflow's typed payload; the started workflow keeps the digest
 * execution id (so the shim's addressing contract holds) and receives the
 * wire-encoded payload. The operation's raw output is the workflow's result
 * payload — the encoded success exit — which the caller-side
 * `callNexusWorkflowOperation` (engine-sandbox) decodes back to the typed
 * channels.
 *
 * @since 0.1.0
 */

import * as Effect from "effect/Effect";
import type * as Schema from "effect/Schema";
import type * as Workflow from "effect/unstable/workflow/Workflow";
import { startWorkflow, WorkflowRunOperationHandler } from "@temporalio/nexus";
import { wireCodecsFor } from "./wire.js";

/**
 * Build the Nexus operation handler for a shim workflow: register it in the
 * worker's Nexus service under the operation name callers pass to
 * `callNexusWorkflowOperation`.
 *
 * @since 0.1.0
 * @category constructors
 */
export const effectWorkflowRunOperation = <
  Tag extends string,
  Payload extends Workflow.AnyStructSchema,
  Success extends Schema.Top,
  Error extends Schema.Top,
>(
  workflow: Workflow.Workflow<Tag, Payload, Success, Error>,
): WorkflowRunOperationHandler<unknown, unknown> =>
  new WorkflowRunOperationHandler(async (ctx, input) => {
    // The operation input arrives WIRE-encoded (the caller side encodes it):
    // decode before computing the execution id, so `idempotencyKey` sees the
    // typed payload and the digest matches a client-side
    // `MyFlow.executionId(payload)` even for transforming schemas. The wire
    // form passes through as the workflow argument unchanged.
    // SAFETY: `wireCodecsFor` decodes through the workflow's own
    // payloadSchema, so the decoded value is exactly `Payload["Type"]`.
    const payload = wireCodecsFor(workflow).decodePayload(input) as Payload["Type"];
    const executionId = await Effect.runPromise(workflow.executionId(payload));
    return await startWorkflow<(wire: unknown) => Promise<unknown>>(ctx, workflow._tag, {
      workflowId: executionId,
      args: [input],
      // A repeated operation call with the same idempotency key attaches to
      // the RUNNING execution; against a CLOSED one it fails loudly rather
      // than silently starting a duplicate side-effecting run.
      workflowIdConflictPolicy: "USE_EXISTING",
      workflowIdReusePolicy: "REJECT_DUPLICATE",
    });
  });
