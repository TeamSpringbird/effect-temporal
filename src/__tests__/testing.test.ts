// The fake client, exercised the way service tests consume it: typed start
// records, configured failures, wire-faithful results, and loud failure on
// everything unconfigured.

import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import { describe, expect, it } from "vitest";
import { makeWorkflowClient } from "../client.js";
import { decodeWorkflowResult, makeFakeTemporalClient, simulateAlreadyStarted } from "../testing.js";
import { wireCodecsFor } from "../wire.js";
import { Demo } from "./fixtures/demo.js";

const payload = { requestId: "fake-1", mode: "approve" } as const;

describe("makeFakeTemporalClient", () => {
  it("records starts with their wire payload, task queue, and memo", async () => {
    const fake = makeFakeTemporalClient();
    const wf = makeWorkflowClient({ client: fake.client, taskQueue: "queue-1" });
    await Effect.runPromise(
      wf.start(Demo, payload, { workflowId: "wf-1", memo: { key: "value" } }),
    );

    expect(fake.starts).toEqual([
      {
        workflowType: "effectDemo",
        workflowId: "wf-1",
        taskQueue: "queue-1",
        args: [payload],
        memo: { key: "value" },
      },
    ]);
  });

  it("surfaces a simulated duplicate start as the typed error", async () => {
    const fake = makeFakeTemporalClient({
      onStart: (start) => {
        throw simulateAlreadyStarted(start);
      },
    });
    const wf = makeWorkflowClient({ client: fake.client, taskQueue: "queue-1" });
    const exit = await Effect.runPromiseExit(wf.start(Demo, payload, { workflowId: "wf-dup" }));

    expect(exit._tag).toBe("Failure");
  });

  it("answers execute with a wire-faithful typed result", async () => {
    const fake = makeFakeTemporalClient({
      workflows: [Demo],
      result: (start) => `handled:${start.workflowId}`,
    });
    const wf = makeWorkflowClient({ client: fake.client, taskQueue: "queue-1" });
    const result = await Effect.runPromise(wf.execute(Demo, payload, { workflowId: "wf-2" }));

    expect(result).toBe("handled:wf-2");
  });

  it("records signals and terminations by id", async () => {
    const fake = makeFakeTemporalClient();
    const handle = fake.client.workflow.getHandle("wf-3");
    await handle.signal("some-signal", { n: 1 });
    await handle.terminate("rescheduled");

    expect(fake.signals).toEqual([
      { workflowId: "wf-3", signalName: "some-signal", args: [{ n: 1 }] },
    ]);
    expect(fake.terminations).toEqual([{ workflowId: "wf-3", reason: "rescheduled" }]);
  });

  it("dies loudly on any unconfigured surface", async () => {
    const fake = makeFakeTemporalClient();
    expect(() => (fake.client as { schedule: { create: unknown } }).schedule.create).toThrow(
      /FakeTemporalClient: client\.schedule\.create is not implemented/,
    );
    await expect(fake.client.workflow.getHandle("nobody").result()).rejects.toThrow(
      /nothing started under that id/,
    );
  });
});

describe("decodeWorkflowResult", () => {
  it("round-trips a typed success and rejects a non-success wire exit", () => {
    expect(decodeWorkflowResult(Demo, { _tag: "Success", value: "decoded" })).toBe("decoded");
    expect(() =>
      decodeWorkflowResult(Demo, {
        _tag: "Failure",
        cause: [{ _tag: "Fail", error: "typed-failure" }],
      }),
    ).toThrow(/did not succeed/);
  });

  it("preserves native defect details in the thrown message", () => {
    const wire = wireCodecsFor(Demo).encodeExit(Exit.die(new Error("boom")));

    expect(() => decodeWorkflowResult(Demo, wire)).toThrow(/boom/);
  });
});
