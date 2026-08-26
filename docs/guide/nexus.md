# Nexus operations

[Temporal Nexus](https://docs.temporal.io/nexus) connects workflows across namespaces and clusters through typed service contracts. effect-temporal bridges both directions: serving a workflow **as** a Nexus operation, and calling one **from** a workflow body — with the typed success/error channels surviving the crossing.

Requires the optional peer `@temporalio/nexus`, and a Temporal server with Nexus enabled (the time-skipping test server has no Nexus support; use a local dev server in tests).

## Serving: a workflow as an operation

Register `effectWorkflowRunOperation(MyWorkflow)` in the Nexus service handler on the worker:

```ts
import * as nexus from "nexus-rpc";
import { effectWorkflowRunOperation } from "@springbird/effect-temporal/nexus";

export const provisioningHandler = nexus.serviceHandler(provisioningService, {
  // sync operations stay plain async functions
  lookup: async (_ctx, input) => findTenant(input),
  // a shim workflow served as a workflow-backed operation
  provision: effectWorkflowRunOperation(ProvisionFlow),
});

// worker: Worker.create({ ..., nexusServices: [provisioningHandler] })
```

The operation's input is the workflow's typed payload; the started workflow keeps its **digest execution id**, so the addressing contract holds across the Nexus boundary.

One deliberate exception to the [idempotency contract](/guide/defining-workflows#idempotency-and-execution-ids): a repeated operation call attaches to the **running** execution (`USE_EXISTING`), but against a **closed** one it fails loudly rather than silently starting a duplicate side-effecting run. Direct `execute` attaches to closed runs; the Nexus path refuses — a caller replaying an old operation months later should hear about it, not re-provision.

## Calling: from a workflow body

```ts
import { createNexusServiceClient } from "@temporalio/workflow";
import {
  callNexusWorkflowOperation,
  type NexusOperationClient,
} from "@springbird/effect-temporal/engine-sandbox";

const nexusClient = createNexusServiceClient({
  service: provisioningService,
  endpoint: "provisioning",
});

const result = yield* callNexusWorkflowOperation({
  client: nexusClient as NexusOperationClient,
  operation: "provision",
  workflow: ProvisionFlow,   // the target's definition, for the codecs
  payload: { tenantId },
  scheduleToCloseTimeout: "10 minutes",
});
// success/error channels: ProvisionFlow's typed channels
```

The payload is wire-encoded outward; the operation's result — the target workflow's encoded exit — decodes back into typed channels, **including across cancellation**: interrupting the calling fiber cancels the in-flight operation through the same per-call scope as activities, and the run's bounded settle wait keeps the caller open for the cancellation handshake.

## Synchronous operations

Operations *not* backed by a shim workflow are plain workflow-API promises — call them with `callRawActivity`:

```ts
const info = yield* callRawActivity(() =>
  nexusClient.executeOperation("lookup", { id }, { scheduleToCloseTimeout: "10 seconds" }),
);
```
