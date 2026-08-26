/**
 * The ONE client seam: a `WorkflowClient` service configured once (Temporal
 * client + default task queue) whose methods cover both start semantics —
 * digest execution ids with attach-on-duplicate (greenfield), and explicit
 * caller-chosen workflow ids (brownfield fleets whose ids are load-bearing).
 * Which one a call gets is decided by whether `workflowId` is passed, so the
 * choice is visible at every call site and nothing re-threads
 * `{ client, taskQueue }` through application code.
 *
 * ```ts
 * const wf = yield* WorkflowClient;
 * yield* wf.start(QueueDispatch, { dispatchId }, { workflowId });  // explicit id
 * const out = yield* wf.execute(Demo, payload);                    // digest + attach
 * yield* wf.terminate(workflowId, { reason: "rescheduled" });
 * ```
 *
 * @since 0.1.0
 */

import * as Clock from "effect/Clock";
import * as Context from "effect/Context";
import * as Data from "effect/Data";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import type * as Option from "effect/Option";
import type * as Schema from "effect/Schema";
import type * as DurableDeferred from "effect/unstable/workflow/DurableDeferred";
import type * as Workflow from "effect/unstable/workflow/Workflow";
import * as WorkflowEngine from "effect/unstable/workflow/WorkflowEngine";
import {
  WorkflowExecutionAlreadyStartedError,
  WorkflowNotFoundError,
  type Client,
  type ScheduleSpec,
} from "@temporalio/client";
import {
  createWorkflowSchedule,
  deferredState,
  executeUpdate,
  makeTemporalClientEngine,
  offerMailbox,
  readStateCell,
  ScheduleAlreadyExistsError,
} from "./engine-client.js";
import type { DurableMailbox } from "./mailbox.js";
import type { StateCell } from "./state-cell.js";
import { classifyThrown } from "./thrown.js";
import type { DurableUpdate } from "./update.js";
import { wireCodecsFor } from "./wire.js";

export {
  /**
   * Re-exported from `engine-client`: a schedule with this id already exists.
   *
   * @since 0.1.0
   * @category errors
   */
  ScheduleAlreadyExistsError,
};

/**
 * Extracts a workflow definition's decoded payload type.
 *
 * @since 0.1.0
 * @category models
 */
export type PayloadOf<W> = Workflow.PayloadSchema<W>["Type"];
/**
 * Extracts a workflow definition's decoded success type.
 *
 * @since 0.1.0
 * @category models
 */
export type SuccessOf<W> =
  W extends Workflow.Workflow<infer _N, infer _P, infer S, infer _E> ? S["Type"] : never;
/**
 * Extracts a workflow definition's decoded error type.
 *
 * @since 0.1.0
 * @category models
 */
export type ErrorOf<W> =
  W extends Workflow.Workflow<infer _N, infer _P, infer _S, infer E> ? E["Type"] : never;

/**
 * A start under an explicit `workflowId` found the id already taken — the
 * duplicate-start condition, surfaced typed so callers that treat "already
 * started" as success can catch the tag.
 *
 * @since 0.1.0
 * @category errors
 */
export class WorkflowAlreadyStartedError extends Data.TaggedError("WorkflowAlreadyStartedError")<{
  readonly workflowId: string;
  readonly workflowType: string;
}> {}

/**
 * Per-call options for `start` / `execute`.
 *
 * @since 0.1.0
 * @category models
 */
export interface WorkflowStartOptions {
  /** Present: start under this exact id (`WorkflowAlreadyStartedError` on a
   * duplicate). Absent: digest execution id with attach-on-duplicate. */
  readonly workflowId?: string;
  readonly memo?: Record<string, unknown>;
  /** gRPC caller-latency bound for the start call. */
  readonly deadlineMillis?: number;
  /** Override the service's default task queue for this call. */
  readonly taskQueue?: string;
}

/**
 * The `WorkflowClient` service surface.
 *
 * @since 0.1.0
 * @category models
 */
export interface WorkflowClientShape {
  /** Start without awaiting the result. */
  readonly start: <
    Tag extends string,
    P extends Workflow.AnyStructSchema,
    S extends Schema.Top,
    E extends Schema.Top,
  >(
    workflow: Workflow.Workflow<Tag, P, S, E>,
    payload: P["Type"],
    options?: WorkflowStartOptions,
  ) => Effect.Effect<void, WorkflowAlreadyStartedError>;
  /** Start (or attach) and await the typed result. */
  readonly execute: <
    Tag extends string,
    P extends Workflow.AnyStructSchema,
    S extends Schema.Top,
    E extends Schema.Top,
  >(
    workflow: Workflow.Workflow<Tag, P, S, E>,
    payload: P["Type"],
    options?: WorkflowStartOptions,
  ) => Effect.Effect<S["Type"], E["Type"]>;
  /** Cancel by id — the graceful counterpart to `terminate`: the run
   * unwinds (finalizers and compensation run) and records Cancelled.
   * Unknown or closed executions are a no-op. */
  readonly interrupt: (
    workflowId: string,
    options?: { readonly deadlineMillis?: number },
  ) => Effect.Effect<void>;
  /** Terminate by id — the hard stop: no unwind, no compensation. Unknown
   * executions are a no-op. */
  readonly terminate: (
    workflowId: string,
    options?: { readonly reason?: string; readonly deadlineMillis?: number },
  ) => Effect.Effect<void>;
  /** Offer to a running workflow's mailbox; closed/unknown is a no-op. */
  readonly offerMailbox: <S extends Schema.Top>(
    mailbox: DurableMailbox<S>,
    workflowId: string,
    payload: S["Type"],
  ) => Effect.Effect<void>;
  /** Send an update request and receive the workflow's typed response;
   * unknown executions and runs that end unanswered are defects. */
  readonly executeUpdate: <P extends Schema.Top, S extends Schema.Top, E extends Schema.Top>(
    update: DurableUpdate<P, S, E>,
    workflowId: string,
    payload: P["Type"],
  ) => Effect.Effect<S["Type"], E["Type"]>;
  /** Read the latest published snapshot of a state cell — `None` while the
   * execution is unknown or the cell unpublished, including after close. */
  readonly readStateCell: <S extends Schema.Top>(
    cell: StateCell<S>,
    workflowId: string,
  ) => Effect.Effect<Option.Option<S["Type"]>>;
  /** Read a deferred's state via query — `None` while pending or unknown,
   * `Some(typed exit)` once completed; never perturbs the signal path. */
  readonly deferredState: <Success extends Schema.Constraint, Error extends Schema.Constraint>(
    deferred: DurableDeferred.DurableDeferred<Success, Error>,
    workflowId: string,
  ) => Effect.Effect<Option.Option<Exit.Exit<Success["Type"], Error["Type"]>>>;
  /** Create a Temporal schedule firing the workflow with a fixed payload;
   * an existing schedule under the id fails typed. */
  readonly createSchedule: <
    Tag extends string,
    P extends Workflow.AnyStructSchema,
    S extends Schema.Top,
    E extends Schema.Top,
  >(options: {
    readonly scheduleId: string;
    readonly workflow: Workflow.Workflow<Tag, P, S, E>;
    readonly payload: P["Type"];
    readonly spec: ScheduleSpec;
    readonly taskQueue?: string;
  }) => Effect.Effect<void, ScheduleAlreadyExistsError>;
  /** The raw Temporal client, for surfaces this service does not model
   * (describe, handles, schedules administration). */
  readonly raw: Client;
}

/**
 * The client service tag — provide it with `layerWorkflowClient` and yield
 * it wherever application code starts, drives, or reads workflows.
 *
 * @since 0.1.0
 * @category services
 */
export class WorkflowClient extends Context.Service<WorkflowClient, WorkflowClientShape>()(
  "effect-temporal/WorkflowClient",
) {}

/**
 * What the service is configured with: the Temporal client and the default
 * task queue its calls target.
 *
 * @since 0.1.0
 * @category models
 */
export interface WorkflowClientConfig {
  readonly client: Client;
  readonly taskQueue: string;
}

// ── Explicit-id starts ───────────────────────────────────────────────────────
// The `workflowId` half of start/execute: CALLER-CHOSEN Temporal workflow ids
// instead of digest execution ids, for fleets whose ids are load-bearing
// (persisted to rows, stage-scoped, terminated by id).

interface ExplicitIdStart<W extends Workflow.Any> {
  readonly client: Client;
  readonly workflow: W;
  readonly workflowId: string;
  readonly payload: PayloadOf<W>;
  readonly taskQueue: string;
  readonly memo?: Record<string, unknown>;
  /** gRPC deadline for the start call — a caller-latency bound, not a
   * workflow timeout. */
  readonly deadlineMillis?: number;
}

const withDeadline = <T>(
  client: Client,
  deadlineMillis: number | undefined,
  run: (client: Client) => Promise<T>,
): Effect.Effect<T, never, never> =>
  deadlineMillis === undefined
    ? Effect.promise(() => run(client))
    : Effect.flatMap(Clock.currentTimeMillis, (now) =>
        Effect.promise(() => client.withDeadline(now + deadlineMillis, () => run(client))),
      );

/** Start under an explicit id, without awaiting the result. A second start
 * under the same id fails with `WorkflowAlreadyStartedError` while the first
 * run is open or (under the default reuse policy) rejects a completed id per
 * Temporal's dedupe window — callers that treat "already started" as success
 * catch the tag. */
const startById = <W extends Workflow.Any>(
  options: ExplicitIdStart<W>,
): Effect.Effect<void, WorkflowAlreadyStartedError> =>
  withDeadline(options.client, options.deadlineMillis, async (client) => {
    await client.workflow.start(options.workflow._tag, {
      workflowId: options.workflowId,
      taskQueue: options.taskQueue,
      args: [wireCodecsFor(options.workflow).encodePayload(options.payload)],
      ...(options.memo === undefined ? {} : { memo: options.memo }),
    });
  }).pipe(
    Effect.catchDefect((error) =>
      error instanceof WorkflowExecutionAlreadyStartedError
        ? Effect.fail(
            new WorkflowAlreadyStartedError({
              workflowId: options.workflowId,
              workflowType: options.workflow._tag,
            }),
          )
        : Effect.die(error),
    ),
  );

/** Start (or attach to) a workflow under an explicit id and await its typed
 * exit. The decoded exit is itself an Effect: typed failures surface in the
 * error channel, defects die. */
const executeById = <W extends Workflow.Any>(
  options: ExplicitIdStart<W>,
): Effect.Effect<SuccessOf<W>, ErrorOf<W>> => {
  const codecs = wireCodecsFor(options.workflow);
  return Effect.promise(async () => {
    const handle = await options.client.workflow
      .start(options.workflow._tag, {
        workflowId: options.workflowId,
        taskQueue: options.taskQueue,
        args: [codecs.encodePayload(options.payload)],
        ...(options.memo === undefined ? {} : { memo: options.memo }),
      })
      .catch((error: unknown) => {
        if (error instanceof WorkflowExecutionAlreadyStartedError) {
          return options.client.workflow.getHandle(options.workflowId);
        }
        throw error;
      });
    try {
      const wire = await handle.result();
      return codecs.decodeExit(wire);
    } catch (error) {
      const thrown = classifyThrown(error);
      switch (thrown.kind) {
        case "interrupted":
          return Exit.interrupt();
        case "wire":
          return codecs.decodeExit(thrown.exit);
        case "other":
          return Exit.die(thrown.error);
      }
    }
    // SAFETY: the exit was decoded through the workflow's own wire codecs
    // (its success/error schemas), so its channels are exactly SuccessOf<W> /
    // ErrorOf<W> — the codec seam types them as unknown.
  }).pipe(Effect.flatMap((exit) => exit as Exit.Exit<SuccessOf<W>, ErrorOf<W>>));
};

/** Cancel by id (the graceful stop); unknown/closed executions are a no-op. */
const interruptById = (options: {
  readonly client: Client;
  readonly workflowId: string;
  /** gRPC deadline for the cancel call. */
  readonly deadlineMillis?: number;
}): Effect.Effect<void> =>
  withDeadline(options.client, options.deadlineMillis, async (client) => {
    try {
      await client.workflow.getHandle(options.workflowId).cancel();
    } catch (error) {
      if (!(error instanceof WorkflowNotFoundError)) throw error;
    }
  });

/** Terminate by id; unknown executions are a no-op. */
const terminateById = (options: {
  readonly client: Client;
  readonly workflowId: string;
  readonly reason?: string;
  /** gRPC deadline for the terminate call. */
  readonly deadlineMillis?: number;
}): Effect.Effect<void> =>
  withDeadline(options.client, options.deadlineMillis, async (client) => {
    try {
      await client.workflow.getHandle(options.workflowId).terminate(options.reason);
    } catch (error) {
      if (!(error instanceof WorkflowNotFoundError)) throw error;
    }
  });

/**
 * Build a `WorkflowClientShape` directly — the plain-value form behind
 * `layerWorkflowClient`, also usable standalone (tests wrap a
 * `FakeTemporalClient` this way).
 *
 * @since 0.1.0
 * @category constructors
 */
export const makeWorkflowClient = (config: WorkflowClientConfig): WorkflowClientShape => {
  const engines = new Map<string, WorkflowEngine.WorkflowEngine["Service"]>();
  const engineFor = (taskQueue: string) => {
    let engine = engines.get(taskQueue);
    if (engine === undefined) {
      engine = makeTemporalClientEngine({ client: config.client, taskQueue });
      engines.set(taskQueue, engine);
    }
    return engine;
  };

  return {
    start: (workflow, payload, options) => {
      const taskQueue = options?.taskQueue ?? config.taskQueue;
      if (options?.workflowId !== undefined) {
        return startById({
          client: config.client,
          workflow,
          workflowId: options.workflowId,
          payload,
          taskQueue,
          ...(options.memo === undefined ? {} : { memo: options.memo }),
          ...(options.deadlineMillis === undefined
            ? {}
            : { deadlineMillis: options.deadlineMillis }),
        });
      }
      // The remaining R is the schemas' encoding/decoding services — always
      // `never` for wire-safe definitions (context-free schemas), which is
      // what every definition in this model is.
      return workflow
        .execute(payload, { discard: true })
        .pipe(
          Effect.provideService(WorkflowEngine.WorkflowEngine, engineFor(taskQueue)),
          Effect.asVoid,
        ) as Effect.Effect<void, never>;
    },

    execute: <
      Tag extends string,
      P extends Workflow.AnyStructSchema,
      S extends Schema.Top,
      E extends Schema.Top,
    >(
      workflow: Workflow.Workflow<Tag, P, S, E>,
      payload: P["Type"],
      options?: WorkflowStartOptions,
    ) => {
      const taskQueue = options?.taskQueue ?? config.taskQueue;
      if (options?.workflowId !== undefined) {
        return executeById({
          client: config.client,
          workflow,
          workflowId: options.workflowId,
          payload,
          taskQueue,
          ...(options.memo === undefined ? {} : { memo: options.memo }),
          ...(options.deadlineMillis === undefined
            ? {}
            : { deadlineMillis: options.deadlineMillis }),
        });
      }
      // SAFETY: as in `start` above, the remaining R is the schemas'
      // encoding services — `never` for the context-free schemas this model
      // uses — and the channels are the definition's own success/error types.
      return workflow
        .execute(payload)
        .pipe(
          Effect.provideService(WorkflowEngine.WorkflowEngine, engineFor(taskQueue)),
        ) as Effect.Effect<S["Type"], E["Type"]>;
    },

    terminate: (workflowId, options) =>
      terminateById({
        client: config.client,
        workflowId,
        ...(options?.reason === undefined ? {} : { reason: options.reason }),
        ...(options?.deadlineMillis === undefined
          ? {}
          : { deadlineMillis: options.deadlineMillis }),
      }),

    interrupt: (workflowId, options) =>
      interruptById({
        client: config.client,
        workflowId,
        ...(options?.deadlineMillis === undefined
          ? {}
          : { deadlineMillis: options.deadlineMillis }),
      }),

    offerMailbox: (mailbox, workflowId, payload) =>
      offerMailbox(mailbox, { client: config.client, workflowId, payload }),

    executeUpdate: (update, workflowId, payload) =>
      executeUpdate(update, { client: config.client, workflowId, payload }),

    readStateCell: (cell, workflowId) =>
      readStateCell(cell, { client: config.client, workflowId }),

    deferredState: (deferred, workflowId) =>
      deferredState(deferred, { client: config.client, workflowId }),

    createSchedule: (options) =>
      createWorkflowSchedule({
        client: config.client,
        scheduleId: options.scheduleId,
        workflow: options.workflow,
        payload: options.payload,
        taskQueue: options.taskQueue ?? config.taskQueue,
        spec: options.spec,
      }).pipe(Effect.asVoid),

    raw: config.client,
  };
};

/**
 * Provide `WorkflowClient` from a Temporal client and default task queue.
 *
 * @since 0.1.0
 * @category layers
 */
export const layerWorkflowClient = (config: WorkflowClientConfig): Layer.Layer<WorkflowClient> =>
  Layer.succeed(WorkflowClient, makeWorkflowClient(config));
