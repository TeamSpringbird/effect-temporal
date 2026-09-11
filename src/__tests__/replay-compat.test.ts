// =========================================================================
// Wire compatibility across releases: a history recorded on 0.3.0 replays
// through the current bundle
// =========================================================================
//
// `fixtures/histories/definition-order-0.3.0.history.b64` is a real
// `defOrder` run recorded with the 0.3.0 `definition-workflows` bundle
// (update, mailbox signal, patch marker, activities, deferred signal). The
// current bundle must replay it clean: activity types, signal and query
// names, update names, and patch-marker ids are byte-identical, and the
// `OrderFlow` handler still issues the same commands in the same order.
//
// Record a new fixture only when a release INTENDS a wire change — and then
// keep the old one too, so the drill covers every generation still in
// flight.

import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { temporal as proto } from "@temporalio/proto";
import { bundleWorkflowCode, Worker } from "@temporalio/worker";
import { describe, expect, it } from "vitest";

const workflowsPath = fileURLToPath(new URL("./fixtures/definition-workflows.ts", import.meta.url));

const loadHistory = (name: string) => {
  const path = fileURLToPath(new URL(`./fixtures/histories/${name}.history.b64`, import.meta.url));
  return proto.api.history.v1.History.decode(Buffer.from(readFileSync(path, "utf8"), "base64"));
};

describe("replay compatibility", { concurrent: false }, () => {
  it("replays a 0.3.0 definition-demo history through the current bundle", async () => {
    const history = loadHistory("definition-order-0.3.0");
    // The fixture is the real thing: activity, update, signal, and marker
    // events are all present, so a replay exercises every wire name.
    const kinds = new Set(history.events.map((event) => event.eventType));
    expect(kinds.has(proto.api.enums.v1.EventType.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED)).toBe(true);
    expect(kinds.has(proto.api.enums.v1.EventType.EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_ACCEPTED)).toBe(true);
    expect(kinds.has(proto.api.enums.v1.EventType.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED)).toBe(true);
    expect(kinds.has(proto.api.enums.v1.EventType.EVENT_TYPE_MARKER_RECORDED)).toBe(true);

    const workflowBundle = await bundleWorkflowCode({ workflowsPath });
    await expect(Worker.runReplayHistory({ workflowBundle }, history)).resolves.toBeUndefined();
  }, 120_000);
});
