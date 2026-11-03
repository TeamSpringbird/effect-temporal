import { CancelledFailure } from "@temporalio/activity";
import { MockActivityEnvironment } from "@temporalio/testing";
import * as Context from "effect/Context";
import * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import { describe, expect, it } from "vitest";
import { makeActivityRunner } from "../worker.js";

class Greeting extends Context.Service<Greeting, string>()("Greeting") {}

describe("worker activity runner", () => {
  it("provides services and preserves typed failures", async () => {
    const environment = new MockActivityEnvironment();
    const runner = makeActivityRunner({ context: Context.make(Greeting, "hello") });
    expect(await environment.run(() => runner.run("greeting", {}, Greeting)))
      .toEqual(Exit.succeed("hello"));
    await environment.run(async () => {
      const failure = await runner.run("failure", {}, Effect.fail("expected"));
      expect(Exit.isFailure(failure)).toBe(true);
      if (Exit.isFailure(failure)) {
        expect(Cause.hasFails(failure.cause)).toBe(true);
        expect(Cause.squash(failure.cause)).toBe("expected");
      }
    });
  });

  it("interrupts and finalizes an activity on Temporal cancellation", async () => {
    const environment = new MockActivityEnvironment();
    const runner = makeActivityRunner({ context: Context.empty() });
    let finalized = false;
    const activity = Effect.gen(function* () {
      yield* Effect.addFinalizer(() => Effect.sync(() => { finalized = true; }));
      environment.cancel();
      yield* Effect.never;
    }).pipe(Effect.scoped);
    await expect(environment.run(() => runner.run("cancel", {}, activity)))
      .rejects.toBeInstanceOf(CancelledFailure);
    expect(finalized).toBe(true);
  });

  it("only emits heartbeats when explicitly enabled", async () => {
    const environment = new MockActivityEnvironment();
    let heartbeats = 0;
    environment.on("heartbeat", () => { heartbeats++; });
    const plain = makeActivityRunner({ context: Context.empty() });
    await environment.run(() => plain.run("plain", {}, Effect.void));
    expect(heartbeats).toBe(0);

    const runner = makeActivityRunner({ context: Context.empty(), heartbeatInterval: "10 millis" });
    const firstHeartbeat = Effect.promise(() => new Promise<void>((resolve) => {
      environment.once("heartbeat", () => resolve());
    }));
    await environment.run(() => runner.run("heartbeat", {}, firstHeartbeat));
    expect(heartbeats).toBeGreaterThan(0);
    // The scoped heartbeat fiber has terminated with the activity.
    const finishedCount = heartbeats;
    await environment.run(() => plain.run("after", {}, Effect.sleep("30 millis")));
    expect(heartbeats).toBe(finishedCount);
  });

  it("rejects invalid heartbeat intervals", () => {
    expect(() => makeActivityRunner({ context: Context.empty(), heartbeatInterval: 0 }))
      .toThrow(RangeError);
  });
});
