/**
 * In-memory test doubles for the CLIENT side of the engine, so service
 * tests stop hand-rolling `{ workflow: { start } } as never` casts. The fake
 * records every start, signal, and termination as typed data, answers
 * results through the workflow's own wire codecs, and dies loudly on any
 * client surface a test did not configure — the same "unstubbed methods
 * fail fast" contract as a `makeFake` service stub.
 *
 * This module is for tests that treat Temporal as a SEAM. Tests that need
 * real workflow semantics (timers, retries, continue-as-new) should keep
 * driving a real `TestWorkflowEnvironment` and decode results with
 * `decodeWorkflowResult` below instead of asserting on wire exits.
 *
 * @since 0.1.0
 */

import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Queue from "effect/Queue";
import type * as Schema from "effect/Schema";
import type * as Workflow from "effect/unstable/workflow/Workflow";
import type { ActivityRunner, BoundActivity } from "./activities.js";
import { makeWorkflowClient, type WorkflowStartOptions } from "./client.js";
import { WorkflowExecutionAlreadyStartedError, type Client } from "@temporalio/client";
import type { PayloadOf, SuccessOf } from "./client.js";
import { WorkflowOps, type UpdateRequest, type WorkflowOpsRuntime } from "./definition.js";
import { codecsFor } from "./typed-activity.js";
import { wireCodecsFor } from "./wire.js";

/**
 * One recorded `workflow.start` call, as the fake captured it.
 *
 * @since 0.1.0
 * @category models
 */
export interface RecordedWorkflowStart {
  readonly workflowType: string;
  readonly workflowId: string;
  readonly taskQueue: string | undefined;
  readonly args: ReadonlyArray<unknown>;
  readonly memo: Record<string, unknown> | undefined;
}

/**
 * One recorded signal delivery, as the fake captured it.
 *
 * @since 0.1.0
 * @category models
 */
export interface RecordedWorkflowSignal {
  readonly workflowId: string;
  readonly signalName: string;
  readonly args: ReadonlyArray<unknown>;
}

/**
 * One recorded termination, as the fake captured it.
 *
 * @since 0.1.0
 * @category models
 */
export interface RecordedWorkflowTermination {
  readonly workflowId: string;
  readonly reason: string | undefined;
}

/**
 * What `makeFakeTemporalClient` is configured with — every option is
 * optional, and unconfigured surfaces die loudly when touched.
 *
 * @since 0.1.0
 * @category models
 */
export interface FakeTemporalClientOptions {
  /**
   * Definitions whose results this fake can answer: `handle.result()` for a
   * started workflow encodes the value from `result` through the matching
   * definition's wire codec, exactly as the engine would.
   */
  readonly workflows?: ReadonlyArray<Workflow.Any>;
  /**
   * Called for every `workflow.start`. Throw to simulate a start failure —
   * `simulateAlreadyStarted` builds the duplicate-start error.
   */
  readonly onStart?: (start: RecordedWorkflowStart) => void | Promise<void>;
  /**
   * The DECODED success value for a started workflow, consulted by
   * `handle.result()`. Leave undefined for fire-and-forget tests — awaiting
   * a result then dies loudly.
   */
  readonly result?: (start: RecordedWorkflowStart) => unknown;
}

/**
 * The fake: a `Client`-shaped double plus the typed records of everything
 * that was started, signalled, or terminated through it.
 *
 * @since 0.1.0
 * @category models
 */
export interface FakeTemporalClient {
  /** The `Client`-shaped double — wrap it for tests via
   * `makeWorkflowClient({ client: fake.client, taskQueue })`. */
  readonly client: Client;
  readonly starts: ReadonlyArray<RecordedWorkflowStart>;
  readonly signals: ReadonlyArray<RecordedWorkflowSignal>;
  readonly terminations: ReadonlyArray<RecordedWorkflowTermination>;
}

/**
 * The error a real duplicate start produces, for `onStart` simulations.
 *
 * @since 0.1.0
 * @category testing
 */
export const simulateAlreadyStarted = (start: RecordedWorkflowStart) =>
  new WorkflowExecutionAlreadyStartedError(
    "Workflow execution already started",
    start.workflowId,
    start.workflowType,
  );

const die = (surface: string): never => {
  throw new Error(
    `FakeTemporalClient: ${surface} is not implemented — configure it in makeFakeTemporalClient(options) or stub it explicitly`,
  );
};

/** Every property access on an unconfigured surface fails fast and names itself. */
const loudProxy = (path: string): object =>
  new Proxy(
    {},
    {
      get(_target, property) {
        return die(`${path}.${String(property)}`);
      },
    },
  );

/**
 * Build the in-memory client double. Wrap `fake.client` with
 * `makeWorkflowClient` to drive the real client seam in tests, then assert
 * on `starts` / `signals` / `terminations`.
 *
 * @since 0.1.0
 * @category testing
 */
export const makeFakeTemporalClient = (
  options: FakeTemporalClientOptions = {},
): FakeTemporalClient => {
  const starts: RecordedWorkflowStart[] = [];
  const signals: RecordedWorkflowSignal[] = [];
  const terminations: RecordedWorkflowTermination[] = [];
  const codecs = new Map(
    (options.workflows ?? []).map((workflow) => [workflow._tag, wireCodecsFor(workflow)]),
  );

  const resultFor = (workflowId: string): unknown => {
    const start = starts.find((s) => s.workflowId === workflowId);
    if (start === undefined) {
      return die(`workflow.getHandle("${workflowId}").result — nothing started under that id`);
    }
    if (options.result === undefined) {
      return die(`workflow.getHandle("${workflowId}").result — no result() configured`);
    }
    const codec = codecs.get(start.workflowType);
    if (codec === undefined) {
      return die(
        `workflow.getHandle("${workflowId}").result — "${start.workflowType}" is not in options.workflows`,
      );
    }
    return codec.encodeExit(Exit.succeed(options.result(start)));
  };

  // The "dies loudly" contract extends to handle members: `describe`,
  // `query`, `executeUpdate`, `cancel` and friends fail with a named error,
  // not a bare TypeError. `then` and symbols pass through as undefined so
  // resolving a promise WITH a handle (which probes `.then`) still works.
  const loudMembers = <T extends object>(path: string, implemented: T): T =>
    new Proxy(implemented, {
      get(target, property) {
        // SAFETY: guarded by the `in` check on the line itself.
        if (property in target) return target[property as keyof T];
        if (property === "then" || typeof property === "symbol") return undefined;
        return die(`${path}.${String(property)}`);
      },
    });

  const getHandle = (workflowId: string) =>
    loudMembers(`workflow.getHandle("${workflowId}")`, {
      result: async () => resultFor(workflowId),
      signal: async (signalName: string, ...args: ReadonlyArray<unknown>) => {
        signals.push({ workflowId, signalName, args });
      },
      terminate: async (reason?: string) => {
        terminations.push({ workflowId, reason });
      },
    });

  const workflow = loudMembers("client.workflow", {
    start: async (
      workflowType: string,
      startOptions: {
        workflowId: string;
        taskQueue?: string;
        args?: ReadonlyArray<unknown>;
        memo?: Record<string, unknown>;
      },
    ) => {
      const start: RecordedWorkflowStart = {
        workflowType,
        workflowId: startOptions.workflowId,
        taskQueue: startOptions.taskQueue,
        args: startOptions.args ?? [],
        memo: startOptions.memo,
      };
      starts.push(start);
      await options.onStart?.(start);
      return getHandle(start.workflowId);
    },
    getHandle,
  });

  // SAFETY: the proxy answers every `Client` member — the configured
  // surfaces for real, everything else via `loudProxy`, which dies with a
  // named error instead of a TypeError. That fail-fast behavior is this
  // module's documented contract; a structural `Client` cannot express it.
  const client = new Proxy(
    {
      workflow,
      // Deadlines are a transport concern the fake has no clock for.
      withDeadline: <T>(_deadline: number, fn: () => Promise<T>) => fn(),
    },
    {
      get(target, property) {
        // SAFETY: guarded by the `in` check on the line itself.
        if (property in target) return target[property as keyof typeof target];
        return loudProxy(`client.${String(property)}`);
      },
    },
  ) as unknown as Client;

  return { client, starts, signals, terminations };
};

/**
 * Decode a real run's wire result into the workflow's typed success — for
 * tests that drive a live `TestWorkflowEnvironment` handle and should assert
 * on domain values, never on the encoded-exit shape. Throws with the decoded
 * cause when the run did not succeed.
 *
 * @since 0.1.0
 * @category testing
 */
export const decodeWorkflowResult = <W extends Workflow.Any>(
  workflow: W,
  wire: unknown,
): SuccessOf<W> => {
  const exit = wireCodecsFor(workflow).decodeExit(wire);
  // SAFETY: the exit was decoded through the workflow's own success schema,
  // so the success value is SuccessOf<W> — the codec seam types it unknown.
  if (Exit.isSuccess(exit)) return exit.value as SuccessOf<W>;
  throw new Error(`workflow "${workflow._tag}" did not succeed: ${JSON.stringify(exit.cause)}`);
};

/**
 * Encode a payload the way a caller would put it on the wire — for tests
 * asserting what a start SHOULD have carried, without hand-writing the
 * encoded form.
 *
 * @since 0.1.0
 * @category testing
 */
export const encodeWorkflowPayload = <W extends Workflow.Any>(
  workflow: W,
  payload: PayloadOf<W>,
): unknown => wireCodecsFor(workflow).encodePayload(payload);

// ── Live test harness ────────────────────────────────────────────────────────

/**
 * Options for `startWorkflowTestHarness`. `local` mode runs a full dev
 * server (needed for Nexus and schedules); time-skipping is the default.
 *
 * @since 0.1.0
 * @category models
 */
export interface WorkflowTestHarnessOptions {
  readonly mode?: "timeSkipping" | "local";
  readonly serverArgs?: ReadonlyArray<string>;
}

/**
 * What one harness worker runs: the workflow bundle path and its activity
 * table.
 *
 * @since 0.1.0
 * @category models
 */
export interface HarnessWorkerOptions {
  readonly workflowsPath: string;
  readonly activities: Record<string, (wire: unknown) => Promise<unknown>>;
}

/**
 * The promise-flavored typed client a harness worker body receives.
 *
 * @since 0.1.0
 * @category models
 */
export interface HarnessClient {
  execute<
    Tag extends string,
    P extends Workflow.AnyStructSchema,
    S extends Schema.Top,
    E extends Schema.Top,
  >(
    workflow: Workflow.Workflow<Tag, P, S, E>,
    payload: P["Type"],
    options?: WorkflowStartOptions,
  ): Promise<S["Type"]>;
  start<
    Tag extends string,
    P extends Workflow.AnyStructSchema,
    S extends Schema.Top,
    E extends Schema.Top,
  >(
    workflow: Workflow.Workflow<Tag, P, S, E>,
    payload: P["Type"],
    options?: WorkflowStartOptions,
  ): Promise<void>;
  readonly raw: Client;
}

/**
 * A booted Temporal test server plus typed helpers for running workers and
 * workflows against it.
 *
 * @since 0.1.0
 * @category models
 */
export interface WorkflowTestHarness {
  /** Run `body` against a worker that lives only as long as the body does;
   * the body receives a typed client bound to the worker's task queue. */
  withWorker<A>(
    options: HarnessWorkerOptions,
    body: (client: HarnessClient, taskQueue: string) => Promise<A>,
  ): Promise<A>;
  /** The environment's current time — the base for absolute timestamps under
   * time skipping. */
  currentTimeMillis(): Promise<number>;
  /** The underlying TestWorkflowEnvironment, for surfaces the harness does
   * not model. */
  readonly env: import("@temporalio/testing").TestWorkflowEnvironment;
  teardown(): Promise<void>;
}

/**
 * Boot a Temporal test server and hand back a typed harness:
 *
 *   const h = await startWorkflowTestHarness();
 *   await h.withWorker({ workflowsPath, activities }, async (wf) => {
 *     expect(await wf.execute(Demo, payload)).toBe("done");
 *   });
 *   await h.teardown();
 *
 * Framework-agnostic on purpose — wire it into beforeAll/afterAll yourself.
 *
 * @since 0.1.0
 * @category testing
 */
export const startWorkflowTestHarness = async (
  options: WorkflowTestHarnessOptions = {},
): Promise<WorkflowTestHarness> => {
  const { TestWorkflowEnvironment } = await import("@temporalio/testing");
  const { Worker } = await import("@temporalio/worker");
  const env =
    options.mode === "local"
      ? await TestWorkflowEnvironment.createLocal({
          server: { extraArgs: [...(options.serverArgs ?? [])] },
        })
      : await TestWorkflowEnvironment.createTimeSkipping();
  let sequence = 0;

  return {
    env,
    currentTimeMillis: () => env.currentTimeMs(),
    teardown: () => env.teardown(),
    withWorker: async (workerOptions, body) => {
      const taskQueue = `effect-workflow-harness-${++sequence}-${Math.random().toString(36).slice(2, 10)}`;
      const worker = await Worker.create({
        connection: env.nativeConnection,
        ...(env.namespace === undefined ? {} : { namespace: env.namespace }),
        taskQueue,
        workflowsPath: workerOptions.workflowsPath,
        activities: workerOptions.activities,
      });
      const wf = makeWorkflowClient({ client: env.client, taskQueue });
      const client: HarnessClient = {
        execute: (workflow, payload, opts) =>
          Effect.runPromise(wf.execute(workflow, payload, opts)),
        start: (workflow, payload, opts) => Effect.runPromise(wf.start(workflow, payload, opts)),
        raw: env.client,
      };
      return worker.runUntil(() => body(client, taskQueue));
    },
  };
};

// ─── In-memory WorkflowOps ───────────────────────────────────────────────────

/**
 * A running in-memory world for handler unit tests: provide `layer` to a
 * workflow handler and drive its declared channels from the outside —
 * exactly what a client would do against the real engine, minus the engine.
 *
 * @since 0.3.0
 * @category models
 */
export interface TestWorkflowOps {
  /** Provides `WorkflowOps` backed by this world. */
  readonly layer: Layer.Layer<WorkflowOps>;
  /** Resolve a declared deferred, waking any handler blocked on `.await`. */
  readonly resolve: <A>(
    deferred: { readonly deferred: object; readonly await: Effect.Effect<A, never, WorkflowOps> },
    value: A,
  ) => Effect.Effect<void>;
  /** Deliver one mailbox message. */
  readonly offer: <P>(
    mailbox: { readonly mailbox: object; readonly take: Effect.Effect<P, never, WorkflowOps> },
    payload: P,
  ) => Effect.Effect<void>;
  /** Send an update request and await its typed response. */
  readonly request: <P, S, E>(
    update: {
      readonly update: object;
      readonly take: Effect.Effect<UpdateRequest<P, S, E>, never, WorkflowOps>;
    },
    payload: P,
  ) => Effect.Effect<S, E>;
  /** Read the last value a handler `set` on a declared state cell. */
  readonly stateOf: <V>(cell: {
    readonly cell: object;
    readonly set: (value: V) => Effect.Effect<void, never, WorkflowOps>;
  }) => Effect.Effect<Option.Option<V>>;
}

/**
 * Build an in-memory `WorkflowOps` runtime, so the SAME handler that runs
 * on Temporal runs in a plain unit test:
 *
 * ```ts
 * const world = yield* makeTestWorkflowOps({
 *   activities: [handle(Charge, () => Effect.succeed("receipt"))],
 * });
 * const fiber = yield* Effect.forkChild(
 *   orderHandler({ orderId: "o-1" }).pipe(Effect.provide(world.layer)),
 * );
 * yield* world.resolve(Approval, "ben");
 * ```
 *
 * Activity calls run their bound handlers with the payload round-tripped
 * through the declaration's schema (as the wire would); typed failures land
 * in the error channel, everything else is a defect. A call to an activity
 * with no binding dies loudly. `version` always answers the newest name —
 * there is no replay in memory.
 *
 * @since 0.3.0
 * @category constructors
 */
export const makeTestWorkflowOps = (options?: {
  readonly activities?: ReadonlyArray<BoundActivity<never, string>>;
}): Effect.Effect<TestWorkflowOps> =>
  Effect.sync(() => {
    const bindings = new Map((options?.activities ?? []).map((b) => [b.activity.name, b]));
    const runner: ActivityRunner<never> = {
      run: (_name, _payload, effect) => Effect.runPromiseExit(effect),
    };
    const deferreds = new Map<object, Deferred.Deferred<unknown>>();
    const mailboxes = new Map<object, Queue.Queue<unknown>>();
    const updates = new Map<object, Queue.Queue<UpdateRequest<unknown, unknown, unknown>>>();
    const cells = new Map<object, unknown>();

    const deferredFor = (key: object) => {
      const existing = deferreds.get(key);
      if (existing) return existing;
      const created = Deferred.makeUnsafe<unknown>();
      deferreds.set(key, created);
      return created;
    };
    const queueFor = <T>(map: Map<object, Queue.Queue<T>>, key: object) =>
      Effect.suspend(() => {
        const existing = map.get(key);
        if (existing) return Effect.succeed(existing);
        return Effect.map(Queue.unbounded<T>(), (created) => {
          const raced = map.get(key);
          if (raced) return raced;
          map.set(key, created);
          return created;
        });
      });

    const runtime: WorkflowOpsRuntime = {
      activity: (activity, payload) =>
        Effect.suspend(() => {
          const binding = bindings.get(activity.name);
          if (binding === undefined) {
            return Effect.die(
              `makeTestWorkflowOps: no binding for activity "${activity.name}" — pass it in \`activities\``,
            );
          }
          const codecs = codecsFor(binding.activity);
          const validated = codecs.payload.decode(codecs.payload.encode(payload));
          return Effect.flatten(
            Effect.promise(() => binding.execute(validated as never, runner)),
          );
        }),
      deferredAwait: (deferred) => Deferred.await(deferredFor(deferred)),
      mailboxTake: (mailbox) => Effect.flatMap(queueFor(mailboxes, mailbox), Queue.take),
      mailboxPoll: (mailbox) => Effect.flatMap(queueFor(mailboxes, mailbox), Queue.poll),
      updateTake: (update) => Effect.flatMap(queueFor(updates, update), Queue.take),
      stateSet: (cell, value) => Effect.sync(() => void cells.set(cell, value)),
      version: (_site, names) => Effect.succeed(names[names.length - 1]!),
    };

    const world: TestWorkflowOps = {
      layer: Layer.succeed(WorkflowOps, runtime),
      resolve: (wrapper, value) =>
        Effect.asVoid(Deferred.done(deferredFor(wrapper.deferred), Exit.succeed(value))),
      offer: (wrapper, payload) =>
        Effect.asVoid(
          Effect.flatMap(queueFor(mailboxes, wrapper.mailbox), (queue) =>
            Queue.offer(queue, payload),
          ),
        ),
      request: (wrapper, payload) =>
        Effect.flatMap(queueFor(updates, wrapper.update), (queue) => {
          const reply = Deferred.makeUnsafe<unknown, unknown>();
          return Effect.flatMap(
            Queue.offer(queue, {
              payload,
              respond: (exit) => Effect.asVoid(Deferred.done(reply, exit)),
            }),
            () => Deferred.await(reply),
          );
          // SAFETY: the reply deferred completes only through `respond`,
          // whose exit the wrapper's declaration types as S/E.
        }) as Effect.Effect<never, never>,
      stateOf: (wrapper) =>
        Effect.sync(() =>
          cells.has(wrapper.cell)
            ? Option.some(cells.get(wrapper.cell) as never)
            : Option.none(),
        ),
    };
    return world;
  });
