// DSL-interpreter demo definitions, mirroring the Temporal `dsl-interpreter`
// sample: the workflow payload carries a declarative program — sequential
// steps, each either a single activity call or a parallel fan-out — and the
// workflow interprets it. Effect is the interpreter host: sequence maps to
// `Effect.forEach`, parallelism to `Effect.all`.

import * as Schema from "effect/Schema";
import * as Workflow from "effect/unstable/workflow/Workflow";

export const DslStep = Schema.Union([
  Schema.Struct({ single: Schema.String }),
  Schema.Struct({ parallel: Schema.Array(Schema.String) }),
]);

export const DslDemo = Workflow.make("effectDslDemo", {
  payload: {
    requestId: Schema.String,
    steps: Schema.Array(DslStep),
  },
  idempotencyKey: ({ requestId }) => requestId,
  success: Schema.String,
});
