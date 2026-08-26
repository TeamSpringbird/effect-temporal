// Sliding-window batch demo — bundle entrypoint. Export names equal the
// workflow tags. Children are started discarded (ABANDON) so a
// continue-as-new never tears them down; each reports back to the
// orchestrator's stable workflow id, which survives the run change.

import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Activity from "effect/unstable/workflow/Activity";
import { proxyActivities } from "@temporalio/workflow";
import {
  callRawActivity,
  continueAsNew,
  makeTemporalWorkflow,
  offerMailbox,
  pollMailbox,
  takeMailbox,
} from "../../engine-sandbox.js";
import { BatchDemo, CompletionReports, RecordDemo } from "./batch-demo.js";

const acts = proxyActivities<{ processRecord(index: string): Promise<string> }>({
  startToCloseTimeout: "10 seconds",
});

/** How many children one run starts before continuing as new. */
const CHILDREN_PER_RUN = 2;

export const effectBatchDemo = makeTemporalWorkflow(BatchDemo, (payload) =>
  Effect.gen(function* () {
    const inFlight = new Set(payload.inFlight);
    let offset = payload.offset;
    let startedThisRun = 0;

    while (offset < payload.total && startedThisRun < CHILDREN_PER_RUN) {
      if (inFlight.size >= payload.window) {
        const report = yield* takeMailbox(CompletionReports);
        inFlight.delete(report.index);
      }
      yield* RecordDemo.execute(
        {
          requestId: `${payload.requestId}-rec-${offset}`,
          index: offset,
          batchExecutionId: yield* BatchDemo.executionId(payload),
        },
        { discard: true },
      );
      inFlight.add(offset);
      offset++;
      startedThisRun++;
    }

    if (offset < payload.total) {
      // Reports that landed during this run would be lost at
      // continue-as-new; drain them into the carried set first.
      while (true) {
        const report = yield* pollMailbox(CompletionReports);
        if (Option.isNone(report)) break;
        inFlight.delete(report.value.index);
      }
      return yield* continueAsNew(BatchDemo, { ...payload, offset, inFlight: [...inFlight] });
    }

    while (inFlight.size > 0) {
      const report = yield* takeMailbox(CompletionReports);
      inFlight.delete(report.index);
    }
    return `processed:${payload.total}`;
  }),
);

export const effectRecordDemo = makeTemporalWorkflow(RecordDemo, (payload) =>
  Effect.gen(function* () {
    yield* Activity.make({
      name: "process-record",
      success: Schema.String,
      execute: callRawActivity(() => acts.processRecord(String(payload.index))),
    });
    yield* offerMailbox(CompletionReports, {
      workflowId: payload.batchExecutionId,
      payload: { index: payload.index },
    });
    return `record:${payload.index}`;
  }),
);
