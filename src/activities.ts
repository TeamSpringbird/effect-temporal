/**
 * Attach bridge: a workflow cannot await the result of an execution it does
 * not own (`startChild` throws AlreadyStarted, external handles only signal
 * and cancel). The sandbox polls this activity instead, which reads the
 * foreign execution's status and result with a real Temporal client.
 *
 * Register in every worker that runs shim workflows:
 *
 * ```ts
 * const worker = await Worker.create({
 *   activities: { ...yourActivities, ...makeEffectWorkflowActivities(client) },
 * });
 * ```
 *
 * @since 0.1.0
 */

import * as Cause from "effect/Cause";
import type * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import { ApplicationFailure, WorkflowNotFoundError, type Client } from "@temporalio/client";
import { classifyThrown } from "./thrown.js";
import {
  ACTIVITY_EXIT_TYPE,
  codecsFor,
  type AnyTypedActivity,
  type ErrorOf,
  type PayloadOf,
  type SuccessOf,
} from "./typed-activity.js";
import type { EffectWorkflowBridgeResult } from "./wire.js";

const classifyFailure = (error: unknown): EffectWorkflowBridgeResult => {
  const thrown = classifyThrown(error);
  return thrown.kind === "other" ? { kind: "defect", message: String(thrown.error) } : thrown;
};

/**
 * The attach-bridge activity table (`effectWorkflowPollResult`), bound to a
 * real Temporal client. Spread into the `activities` of every worker that
 * runs shim workflows.
 *
 * @since 0.1.0
 * @category constructors
 */
export const makeEffectWorkflowActivities = (client: Client) => ({
  effectWorkflowPollResult: async (input: {
    readonly workflowId: string;
  }): Promise<EffectWorkflowBridgeResult> => {
    const handle = client.workflow.getHandle(input.workflowId);
    let status: string;
    try {
      status = (await handle.describe()).status.name;
    } catch (error) {
      if (error instanceof WorkflowNotFoundError) {
        return { kind: "defect", message: `no such execution: ${input.workflowId}` };
      }
      throw error;
    }
    if (status === "RUNNING") return { kind: "running" };
    if (status === "CANCELLED") return { kind: "interrupted" };
    try {
      return { kind: "wire", exit: await handle.result() };
    } catch (error) {
      return classifyFailure(error);
    }
  },
});

/**
 * How a worker executes the Effect behind an activity: the run function
 * itself — spans, error reporting, and your ManagedRuntime live here — and
 * an optional hook applied to defects before they are rethrown as retryable
 * failures. The simplest runner is
 * `{ run: (_name, _payload, effect) => Effect.runPromiseExit(effect) }`;
 * a real app builds one over its ManagedRuntime so handlers can require
 * application services (`R`).
 *
 * @since 0.1.0
 * @category models
 */
export interface ActivityRunner<R> {
  readonly run: <A, E>(
    activityName: string,
    payload: unknown,
    effect: Effect.Effect<A, E, R>,
  ) => Promise<Exit.Exit<A, E>>;
  readonly onDefect?: (error: unknown) => unknown;
}

/**
 * One activity definition paired with its handler, as produced by `handle`
 * and consumed by `implementActivities`.
 *
 * @since 0.1.0
 * @category models
 */
export interface BoundActivity<R, Name extends string = string> {
  readonly activity: AnyTypedActivity & { readonly name: Name };
  readonly execute: (
    payload: never,
    runner: ActivityRunner<R>,
  ) => Promise<Exit.Exit<unknown, unknown>>;
}

/**
 * Bind one definition to its Effect handler; register via `implementActivities`.
 *
 * @since 0.1.0
 * @category constructors
 */
export const handle = <A extends AnyTypedActivity, R>(
  activity: A,
  fn: (payload: PayloadOf<A>) => Effect.Effect<SuccessOf<A>, ErrorOf<A>, R>,
): BoundActivity<R, A["name"]> => ({
  activity,
  // SAFETY: `implementActivities` decodes the payload through this
  // definition's own codec before calling execute, so the value really is
  // PayloadOf<A>; the exit widens to unknown/unknown for the erased record.
  execute: (payload, runner) =>
    runner.run(activity.name, payload, fn(payload as PayloadOf<A>)) as Promise<
      Exit.Exit<unknown, unknown>
    >,
});

/**
 * Register a table of activity implementations under one runner:
 *
 * ```ts
 * implementActivities(runner, [
 *   handle(Reserve, ({ sku, quantity }) => reserveInventory(sku, quantity)),
 *   handle(Charge, chargeCardEffect),
 * ])
 * ```
 *
 * Payloads are schema-decoded (validated) before a handler runs — decode
 * failure throws non-retryable. A typed failure (the definition's `error`
 * schema) is thrown as a non-retryable ACTIVITY_EXIT failure the calling
 * workflow decodes into its error channel; every other failure is passed to
 * `runner.onDefect` and rethrown retryable.
 *
 * The returned record keeps each definition's name as a LITERAL key, so
 * spreading tables together preserves per-name lookups and a misspelled
 * name in a test is a compile error.
 *
 * @since 0.1.0
 * @category constructors
 */
export const implementActivities = <R, const B extends ReadonlyArray<BoundActivity<R, string>>>(
  runner: ActivityRunner<R>,
  bindings: B,
): Record<B[number]["activity"]["name"], (wire: unknown) => Promise<unknown>> => {
  const out: Record<string, (wire: unknown) => Promise<unknown>> = {};
  for (const binding of bindings) {
    const codecs = codecsFor(binding.activity);
    out[binding.activity.name] = async (wire: unknown) => {
      let payload: unknown;
      try {
        payload = codecs.payload.decode(wire);
      } catch (error) {
        throw ApplicationFailure.create({
          nonRetryable: true,
          message: `effect-activity: payload for "${binding.activity.name}" failed schema decode: ${String(error)}`,
        });
      }
      // SAFETY: `payload` was just decoded through this binding's own payload
      // codec, which is what `execute`'s (contravariant) `never` stands for.
      const exit = await binding.execute(payload as never, runner);
      if (Exit.isSuccess(exit)) return codecs.success.encode(exit.value);
      if (Cause.hasFails(exit.cause)) {
        throw ApplicationFailure.create({
          type: ACTIVITY_EXIT_TYPE,
          nonRetryable: true,
          message: "effect-activity: typed failure (error in details)",
          details: [codecs.error.encode(Cause.squash(exit.cause))],
        });
      }
      const squashed = Cause.squash(exit.cause);
      throw runner.onDefect === undefined ? squashed : runner.onDefect(squashed);
    };
  }
  // The loop builds string keys; the literal-keyed shape is restored here.
  return out as Record<B[number]["activity"]["name"], (wire: unknown) => Promise<unknown>>;
};
