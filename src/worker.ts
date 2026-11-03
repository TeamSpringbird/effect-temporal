/** Worker-side integration. Never import this module into a workflow bundle. */
import { cancellationSignal, CancelledFailure, heartbeat } from "@temporalio/activity";
import * as Cause from "effect/Cause";
import type * as Context from "effect/Context";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Schedule from "effect/Schedule";
import type { ActivityRunner } from "./activities.js";

/**
 * Run activities with application services, tracing, and Temporal cancellation.
 * Heartbeats are opt-in; configure heartbeatTimeout on the activity separately.
 * A heartbeat reports worker liveness, not business progress.
 */
export const makeActivityRunner = <R>(options: {
  readonly context: Context.Context<R>;
  readonly heartbeatInterval?: Duration.Input;
  readonly onDefect?: ActivityRunner<R>["onDefect"];
}): ActivityRunner<R> => {
  const interval = options.heartbeatInterval === undefined
    ? undefined
    : Duration.toMillis(Duration.fromInputUnsafe(options.heartbeatInterval));
  if (interval !== undefined && (!Number.isFinite(interval) || interval <= 0)) {
    throw new RangeError("heartbeatInterval must be finite and greater than zero");
  }
  return {
    onDefect: (error) => options.onDefect === undefined ? error : options.onDefect(error),
    run: async (name, _payload, activity) => {
      const signal = cancellationSignal();
      const exit = await Effect.runPromiseExit(
        Effect.gen(function* () {
          if (interval !== undefined) {
            yield* Effect.forkScoped(
              Effect.repeat(Effect.sync(() => heartbeat()), Schedule.spaced(interval)),
            );
          }
          return yield* activity;
        }).pipe(Effect.scoped, Effect.withSpan(name), Effect.provideContext(options.context)),
        { signal },
      );
      if (Exit.isFailure(exit) && Cause.hasInterrupts(exit.cause) && signal.aborted) {
        throw new CancelledFailure("Activity was cancelled");
      }
      return exit;
    },
  };
};
