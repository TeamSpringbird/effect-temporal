// Nexus demo definitions, mirroring the Temporal `nexus-hello` sample: a
// service with a synchronous operation (`echo`) and a workflow-backed one
// (`greet`, served by a shim workflow). Loaded by the workflow bundle (the
// caller needs the service definition) and by the Node side (the handler).

import * as Schema from "effect/Schema";
import * as Workflow from "effect/unstable/workflow/Workflow";
import * as nexus from "nexus-rpc";

export interface EchoInput {
  readonly message: string;
}

/** Served by `effectWorkflowRunOperation(GreetDemo)`: the input is the
 * workflow's typed payload, the raw output its encoded exit. */
export const helloService = nexus.service("effectHello", {
  echo: nexus.operation<EchoInput, EchoInput>(),
  greet: nexus.operation<{ readonly name: string }, unknown>(),
});

export const GreetDemo = Workflow.make("effectGreetDemo", {
  payload: { name: Schema.String },
  idempotencyKey: ({ name }) => name,
  success: Schema.String,
  error: Schema.String,
});

export const CallerDemo = Workflow.make("effectNexusCallerDemo", {
  payload: { requestId: Schema.String, name: Schema.String, endpoint: Schema.String },
  idempotencyKey: ({ requestId }) => requestId,
  success: Schema.String,
  error: Schema.String,
});
