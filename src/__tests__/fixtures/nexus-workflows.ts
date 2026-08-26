// Nexus demo — bundle entrypoint: the workflow-backed operation's target
// (`effectGreetDemo`) and the caller. Export names equal the workflow tags.

import * as Effect from "effect/Effect";
import * as DurableClock from "effect/unstable/workflow/DurableClock";
import { createNexusServiceClient } from "@temporalio/workflow";
import {
  callNexusWorkflowOperation,
  callRawActivity,
  makeTemporalWorkflow,
  type NexusOperationClient,
} from "../../engine-sandbox.js";
import { CallerDemo, GreetDemo, helloService } from "./nexus-demo.js";

export const effectGreetDemo = makeTemporalWorkflow(GreetDemo, (payload) =>
  Effect.gen(function* () {
    if (payload.name === "grinch") return yield* Effect.fail(`unwelcome:${payload.name}`);
    if (payload.name === "slow") {
      yield* DurableClock.sleep({ name: "slow-greeting", duration: "5 minutes" });
    }
    return `Hello, ${payload.name}!`;
  }),
);

export const effectNexusCallerDemo = makeTemporalWorkflow(CallerDemo, (payload) =>
  Effect.gen(function* () {
    const nexusClient = createNexusServiceClient({
      service: helloService,
      endpoint: payload.endpoint,
    });

    // Synchronous operation: a plain workflow-API promise.
    const echoed = yield* callRawActivity(() =>
      nexusClient.executeOperation(
        "echo",
        { message: payload.name },
        { scheduleToCloseTimeout: "10 seconds" },
      ),
    );

    // Workflow-backed operation: typed channels decoded from the exit.
    const greeting = yield* callNexusWorkflowOperation({
      client: nexusClient as NexusOperationClient,
      operation: "greet",
      workflow: GreetDemo,
      payload: { name: payload.name },
      scheduleToCloseTimeout: "10 seconds",
    });

    return `${echoed.message}|${greeting}`;
  }),
);
