// Sliding-window batch demo definitions, mirroring the Temporal
// `batch-sliding-window` sample: an orchestrator starts record-processing
// children up to a window, children report completion back through a
// workflow-to-workflow mailbox, and the orchestrator continues-as-new
// mid-batch, carrying its in-flight set into the next run.

import * as Schema from "effect/Schema";
import * as Workflow from "effect/unstable/workflow/Workflow";
import * as DurableMailbox from "../../mailbox.js";

export const CompletionReports = DurableMailbox.make("record-complete", {
  payload: Schema.Struct({ index: Schema.Finite }),
});

export const BatchDemo = Workflow.make("effectBatchDemo", {
  payload: {
    requestId: Schema.String,
    total: Schema.Finite,
    window: Schema.Finite,
    offset: Schema.Finite,
    /** Records started but not yet reported complete — carried across
     * continue-as-new so a fresh run keeps waiting for them. */
    inFlight: Schema.Array(Schema.Finite),
  },
  idempotencyKey: ({ requestId }) => requestId,
  success: Schema.String,
});

export const RecordDemo = Workflow.make("effectRecordDemo", {
  payload: {
    requestId: Schema.String,
    index: Schema.Finite,
    /** The orchestrator's execution id, for the completion report. */
    batchExecutionId: Schema.String,
  },
  idempotencyKey: ({ requestId }) => requestId,
  success: Schema.String,
});
