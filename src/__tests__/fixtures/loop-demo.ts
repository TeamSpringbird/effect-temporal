// Looping workflow definition, mirroring the Temporal `continue-as-new`
// sample: each run records its iteration, then continues as a fresh run
// until the iteration cap.

import * as Schema from "effect/Schema";
import * as DurableDeferred from "effect/unstable/workflow/DurableDeferred";
import * as Workflow from "effect/unstable/workflow/Workflow";
import * as StateCell from "../../state-cell.js";

export const LoopDemo = Workflow.make("effectLoopDemo", {
  payload: { requestId: Schema.String, iteration: Schema.Finite },
  idempotencyKey: ({ requestId }) => requestId,
  success: Schema.String,
});

/** Snapshot published by `CellLoopDemo` — cells are PER-RUN, so the value
 * run 1 publishes must read as `None` after continue-as-new until run 2
 * republishes. */
export const LoopStage = StateCell.make("loop-stage", {
  value: Schema.String,
});

/** Gate awaited once per run, so the test controls exactly when run 1
 * continues-as-new and when run 2 republishes and finishes. */
export const LoopGate = DurableDeferred.make("loop-gate", {
  success: Schema.String,
});

/** Run 1 (iteration 0) publishes the cell, waits on the gate, and
 * continues-as-new; run 2 idles on the gate, republishes, and returns. */
export const CellLoopDemo = Workflow.make("effectCellLoopDemo", {
  payload: { requestId: Schema.String, iteration: Schema.Finite },
  idempotencyKey: ({ requestId }) => requestId,
  success: Schema.String,
});
