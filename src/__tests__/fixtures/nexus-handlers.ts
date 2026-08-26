// Nexus demo — Node-side service handler, registered on the worker. The
// sync operation echoes; the workflow-backed one starts the shim workflow
// through `effectWorkflowRunOperation`.

import * as nexus from "nexus-rpc";
import { effectWorkflowRunOperation } from "../../nexus.js";
import { GreetDemo, helloService, type EchoInput } from "./nexus-demo.js";

export const helloServiceHandler = nexus.serviceHandler(helloService, {
  echo: async (_ctx, input: EchoInput) => input,
  greet: effectWorkflowRunOperation(GreetDemo),
});
