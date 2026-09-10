// DurableMailbox end to end, mirroring the Temporal samples the primitive
// unblocks (see EXAMPLES.md):
//
//   `state` — repeated signals mutate a long-lived Map, applied in delivery
//     order, including overwrite and delete
//   `timer-examples` (UpdatableTimer) — a sleeping deadline replaced by
//     mailbox messages, racing the timer
//
// Offers are awaited one at a time so delivery order is defined; the final
// `execute` (idempotent attach) both fetches the result and drives the
// time-skipping clock.

import { fileURLToPath } from "node:url";
import * as Effect from "effect/Effect";
import * as WorkflowEngine from "effect/unstable/workflow/WorkflowEngine";
import { describe, expect, it } from "vitest";
import * as Option from "effect/Option";
import { makeTemporalClientEngine, offerMailbox, readStateCell } from "../engine-client.js";
import { MAILBOX_SIGNAL } from "../mailbox.js";
import {
  DeadlineUpdates,
  StateDemo,
  StateSnapshot,
  StateUpdates,
  UpdatableTimerDemo,
} from "./fixtures/mailbox-demo.js";
import { createWorkflowTestEnv, type TestActivities } from "./utils/workflow-test-env.js";

const workflowsPath = fileURLToPath(new URL("./fixtures/mailbox-workflows.ts", import.meta.url));

const temporal = createWorkflowTestEnv("effect-mailbox");

const activities: TestActivities = {};

describe("DurableMailbox over Temporal", { concurrent: false }, () => {
  it("applies repeated signals to long-lived state in delivery order", async () => {
    await temporal.withWorker({ activities, workflowsPath }, async (taskQueue) => {
      const client = temporal.env.client;
      const engine = makeTemporalClientEngine({ client, taskQueue });
      const run = <A, E>(effect: Effect.Effect<A, E, WorkflowEngine.WorkflowEngine>): Promise<A> =>
        Effect.runPromise(Effect.provideService(effect, WorkflowEngine.WorkflowEngine, engine));

      const payload = { requestId: "state-1" } as const;
      const executionId = await run(StateDemo.execute(payload, { discard: true }));

      const offer = (message: (typeof StateUpdates)["mailbox"]["payloadSchema"]["Type"]) =>
        Effect.runPromise(
          offerMailbox(StateUpdates.mailbox, { client, workflowId: executionId, payload: message }),
        );

      // A signal and a query race through separate workflow tasks, so
      // mid-flight reads poll until the published snapshot catches up.
      const readSnapshot = () =>
        Effect.runPromise(readStateCell(StateSnapshot.cell, { client, workflowId: executionId }));
      const readUntil = async (expected: Record<string, number>) => {
        for (let i = 0; i < 50; i++) {
          const snapshot = await readSnapshot();
          if (
            Option.isSome(snapshot) &&
            JSON.stringify(snapshot.value) === JSON.stringify(expected)
          ) {
            return snapshot.value;
          }
          await new Promise((resolve) => setTimeout(resolve, 100));
        }
        return Option.getOrNull(await readSnapshot());
      };

      // Unpublished cell reads as None.
      expect(Option.isNone(await readSnapshot())).toBe(true);

      await offer({ op: "set", key: "a", value: 1 });
      await offer({ op: "set", key: "b", value: 2 });
      expect(await readUntil({ a: 1, b: 2 })).toEqual({ a: 1, b: 2 });

      await offer({ op: "set", key: "a", value: 3 });
      await offer({ op: "del", key: "b" });
      await offer({ op: "set", key: "c", value: 4 });
      expect(await readUntil({ a: 3, c: 4 })).toEqual({ a: 3, c: 4 });

      await offer({ op: "finish" });
      expect(await run(StateDemo.execute(payload))).toBe("a=3,c=4");

      // The last published snapshot outlives the run.
      const closed = await readSnapshot();
      expect(Option.isSome(closed) && closed.value).toEqual({ a: 3, c: 4 });
    });
  }, 120_000);

  it("updates a sleeping timer's deadline via the mailbox", async () => {
    await temporal.withWorker({ activities, workflowsPath }, async (taskQueue) => {
      const client = temporal.env.client;
      const engine = makeTemporalClientEngine({ client, taskQueue });
      const run = <A, E>(effect: Effect.Effect<A, E, WorkflowEngine.WorkflowEngine>): Promise<A> =>
        Effect.runPromise(Effect.provideService(effect, WorkflowEngine.WorkflowEngine, engine));

      // Ten-minute initial deadline; two updates land while it sleeps, then
      // awaiting the result time-skips through the final two-minute one.
      const payload = { requestId: "timer-1", initialMillis: 600_000 } as const;
      const executionId = await run(UpdatableTimerDemo.execute(payload, { discard: true }));

      for (const millis of [300_000, 120_000]) {
        await Effect.runPromise(
          offerMailbox(DeadlineUpdates.mailbox, { client, workflowId: executionId, payload: { millis } }),
        );
      }

      expect(await run(UpdatableTimerDemo.execute(payload))).toBe("fired-after-updates:2");
    });
  }, 120_000);

  it("drops a malformed mailbox message instead of poisoning the run", async () => {
    await temporal.withWorker({ activities, workflowsPath }, async (taskQueue) => {
      const client = temporal.env.client;
      const engine = makeTemporalClientEngine({ client, taskQueue });
      const run = <A, E>(effect: Effect.Effect<A, E, WorkflowEngine.WorkflowEngine>): Promise<A> =>
        Effect.runPromise(Effect.provideService(effect, WorkflowEngine.WorkflowEngine, engine));

      const payload = { requestId: "state-poison" } as const;
      const executionId = await run(StateDemo.execute(payload, { discard: true }));

      // Raw signal under the KNOWN mailbox name with schema-violating junk:
      // `value` must be a finite number, and `op: "boom"` is no op at all.
      const handle = client.workflow.getHandle(executionId);
      await handle.signal(MAILBOX_SIGNAL, {
        mailboxName: StateUpdates.name,
        payload: { op: "set", key: "poison", value: "not-a-number" },
      });
      await handle.signal(MAILBOX_SIGNAL, {
        mailboxName: StateUpdates.name,
        payload: { op: "boom" },
      });

      // Well-formed messages after the junk still apply, in order.
      const offer = (message: (typeof StateUpdates)["mailbox"]["payloadSchema"]["Type"]) =>
        Effect.runPromise(
          offerMailbox(StateUpdates.mailbox, { client, workflowId: executionId, payload: message }),
        );
      await offer({ op: "set", key: "a", value: 1 });
      await offer({ op: "set", key: "b", value: 2 });
      await offer({ op: "finish" });

      // The run completes with ONLY the well-formed updates applied — the
      // malformed ones were dropped, not applied and not fatal.
      expect(await run(StateDemo.execute(payload))).toBe("a=1,b=2");
    });
  }, 120_000);

  it("drops offers to closed executions instead of failing", async () => {
    await temporal.withWorker({ activities, workflowsPath }, async () => {
      await Effect.runPromise(
        offerMailbox(StateUpdates.mailbox, {
          client: temporal.env.client,
          workflowId: "effect-mailbox-never-started",
          payload: { op: "finish" },
        }),
      );
    });
  }, 120_000);
});
