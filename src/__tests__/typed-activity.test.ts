// The typed-activity seam end-to-end: schema-validated payloads, decoded
// successes, typed failures that skip retries and land in the workflow's
// error channel, and `sleepUntil` under time skipping.

import * as Effect from "effect/Effect";
import { ApplicationFailure, WorkflowFailedError } from "@temporalio/client";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { handle, implementActivities, type ActivityRunner } from "../activities.js";
import { startWorkflowTestHarness, type WorkflowTestHarness } from "../testing.js";
import { codecsFor, type ErrorOf, type PayloadOf, type SuccessOf } from "../typed-activity.js";
import { Reserve, TypedActivityDemo } from "./fixtures/typed-activity-demo.js";

let harness: WorkflowTestHarness;
beforeAll(async () => {
  harness = await startWorkflowTestHarness();
}, 120_000);
afterAll(() => harness.teardown());

const workflowsPath = new URL("./fixtures/typed-activity-workflows.ts", import.meta.url).pathname;

// The barest possible runner — no runtime, no spans; a real worker builds
// one over its ManagedRuntime.
const plainRunner: ActivityRunner<never> = {
  run: (_name, _payload, effect) => Effect.runPromiseExit(effect),
};

const implementReserve = (
  fn: (
    payload: PayloadOf<typeof Reserve>,
  ) => Effect.Effect<SuccessOf<typeof Reserve>, ErrorOf<typeof Reserve>>,
) => implementActivities(plainRunner, [handle(Reserve, fn)]);

const run = (activities: Record<string, (wire: unknown) => Promise<unknown>>, sku: string) =>
  harness.withWorker({ activities, workflowsPath }, async (wf) => {
    const notBeforeISO = new Date((await harness.currentTimeMillis()) + 61_000).toISOString();
    return wf.execute(TypedActivityDemo, { requestId: `typed-${sku}`, sku, notBeforeISO });
  });

describe("typed activities", { concurrent: false }, () => {
  it("decodes a validated payload in, and a typed success out", async () => {
    const seen: Array<unknown> = [];
    const activities = implementReserve((payload) =>
      Effect.sync(() => {
        seen.push(payload);
        return `reserved:${payload.sku}x${payload.quantity}`;
      }),
    );

    // sleepUntil: the 61s not-before timer is durable and time-skipped.
    expect(await run(activities, "sku-1")).toBe("reserved:sku-1x2");
    expect(seen).toEqual([{ sku: "sku-1", quantity: 2 }]);
  }, 120_000);

  it("delivers a typed failure to the workflow's error channel without retrying", async () => {
    let attempts = 0;
    const activities = implementReserve((payload) =>
      Effect.suspend(() => {
        attempts++;
        return Effect.fail({ _tag: "OutOfStock", sku: payload.sku } as const);
      }),
    );

    expect(await run(activities, "sku-2")).toBe("backordered:sku-2");
    // Typed failures are domain outcomes: non-retryable by construction.
    expect(attempts).toBe(1);
  }, 120_000);

  it("retries defects as ordinary activity failures", async () => {
    let attempts = 0;
    const activities = implementReserve((payload) =>
      // Effect.sync turns the throw into a defect — the retryable path.
      Effect.sync(() => {
        attempts++;
        if (attempts < 3) throw new Error("transient blip");
        return `reserved:${payload.sku}`;
      }),
    );

    expect(await run(activities, "sku-3")).toBe("reserved:sku-3");
    expect(attempts).toBe(3);
  }, 120_000);

  it("rejects a wire payload that fails the schema before the impl runs, non-retryably", async () => {
    const activities = implementReserve(() => Effect.die(new Error("impl must not run")));
    const handler = activities["typedReserve"]!;
    const thrown = await handler({ sku: "x", quantity: "not-a-number" }).then(
      () => undefined,
      (error: unknown) => error,
    );
    expect(thrown).toBeInstanceOf(ApplicationFailure);
    // Deterministic decode failures must not burn Temporal retry attempts.
    expect((thrown as ApplicationFailure).nonRetryable).toBe(true);
  });

  it("fails the run — not the workflow task — on a malformed workflow payload", async () => {
    // A thrown decode error inside the workflow function would fail the
    // WORKFLOW TASK, which Temporal retries forever: the execution hangs
    // instead of failing. The engine must convert it to a non-retryable run
    // failure. Started raw to bypass the typed client's own encoding.
    await harness.withWorker(
      { activities: implementReserve(() => Effect.succeed("unused")), workflowsPath },
      async (wf, taskQueue) => {
        await expect(
          wf.raw.workflow.execute("effectTypedActivity", {
            workflowId: `malformed-${Date.now()}`,
            taskQueue,
            args: [{ requestId: 123, sku: null }],
          }),
        ).rejects.toThrow(WorkflowFailedError);
      },
    );
  }, 120_000);

  it("round-trips codecs symmetrically outside any server", () => {
    const codecs = codecsFor(Reserve);
    expect(codecs.payload.decode(codecs.payload.encode({ sku: "s", quantity: 1 }))).toEqual({
      sku: "s",
      quantity: 1,
    });
    expect(codecs.error.decode(codecs.error.encode({ _tag: "OutOfStock", sku: "s" }))).toEqual({
      _tag: "OutOfStock",
      sku: "s",
    });
  });
});
