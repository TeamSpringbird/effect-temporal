// Mailbox demo — bundle entrypoint.

import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as DurableClock from "effect/unstable/workflow/DurableClock";
import { workflowBundle } from "../../engine-sandbox.js";
import {
  DeadlineUpdates,
  StateDemo,
  StateSnapshot,
  StateUpdates,
  UpdatableTimerDemo,
} from "./mailbox-demo.js";

const StateDemoLive = StateDemo.toLayer(() =>
  Effect.gen(function* () {
    const state = new Map<string, number>();
    while (true) {
      const update = yield* StateUpdates.take;
      if (update.op === "finish") break;
      if (update.op === "set") state.set(update.key, update.value);
      else state.delete(update.key);
      yield* StateSnapshot.set(Object.fromEntries(state));
    }
    return Array.from(state.entries())
      .toSorted(([a], [b]) => a.localeCompare(b))
      .map(([key, value]) => `${key}=${value}`)
      .join(",");
  }),
);

const UpdatableTimerDemoLive = UpdatableTimerDemo.toLayer((payload) =>
  Effect.gen(function* () {
    let deadlineMillis = payload.initialMillis;
    let updates = 0;
    while (true) {
      const winner = yield* Effect.raceFirst(
        DeadlineUpdates.take.pipe(
          Effect.map((update) => ({ kind: "update" as const, update })),
        ),
        DurableClock.sleep({
          // Clock deferred names must be unique per sleep within a run.
          name: `deadline-${updates}`,
          duration: `${deadlineMillis} millis`,
        }).pipe(Effect.map(() => ({ kind: "fired" as const }))),
      );
      if (winner.kind === "fired") return `fired-after-updates:${updates}`;
      deadlineMillis = winner.update.millis;
      updates++;
    }
  }),
);

export default workflowBundle(Layer.mergeAll(StateDemoLive, UpdatableTimerDemoLive));
