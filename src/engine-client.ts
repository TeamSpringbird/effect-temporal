/**
 * Client half: a `WorkflowEngine` whose operations translate to Temporal
 * client calls, so ordinary Effect code drives workflows through Effect's
 * own API — `execute` starts (or attaches) and awaits, `poll` describes,
 * `interrupt` cancels, `DurableDeferred.done` signals. The execution id (a
 * digest of tag + idempotencyKey(payload)) is the Temporal workflow id, so
 * AlreadyStarted is the dedupe working, not an error.
 *
 * `activityExecute`, `deferredResult`, and `scheduleClock` die loudly here:
 * they are only reachable from inside a workflow body.
 *
 * **Tier note.** Application code normally uses the `WorkflowClient` service
 * (the `client` module), which wraps this engine and these standalone
 * operations behind one configured surface. Import this module directly for
 * engine-level composition: providing `WorkflowEngine` yourself, tests that
 * drive the engine contract, or wiring without a service layer.
 *
 * @since 0.1.0
 */

import * as Data from "effect/Data";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Option from "effect/Option";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as DurableDeferred from "effect/unstable/workflow/DurableDeferred";
import * as Workflow from "effect/unstable/workflow/Workflow";
import * as WorkflowEngine from "effect/unstable/workflow/WorkflowEngine";
import {
  ScheduleAlreadyRunning,
  WorkflowExecutionAlreadyStartedError,
  WorkflowNotFoundError,
  type Client,
  type ScheduleHandle,
  type ScheduleSpec,
  type WorkflowHandle,
} from "@temporalio/client";
import {
  asJsonCodec,
  DEFERRED_DONE_SIGNAL,
  DEFERRED_STATE_QUERY,
  decodeDeferredExit,
  encodeDeferredExit,
  wireCodecsFor,
} from "./wire.js";
import { MAILBOX_SIGNAL, mailboxCodec, type DurableMailbox } from "./mailbox.js";
import { classifyThrown } from "./thrown.js";
import { STATE_CELL_QUERY, stateCellCodec, type StateCell } from "./state-cell.js";
import {
  updateCodec,
  WORKFLOW_UPDATE,
  type DurableUpdate,
  type WorkflowUpdatePayload,
} from "./update.js";

/**
 * What the client engine is configured with: the Temporal client and the
 * task queue its starts target.
 *
 * @since 0.1.0
 * @category models
 */
export interface TemporalClientEngineOptions {
  readonly client: Client;
  readonly taskQueue: string;
}

const workflowSideOnly = (method: string) =>
  Effect.die(
    `TemporalClientEngine.${method}: workflow-side only — an Activity or clock ran outside a workflow body`,
  );

/** Await the run and decode its wire exit: an `EXIT_FAILURE_TYPE`
 * failure carries the encoded typed exit in its details, cancellation
 * becomes an interrupted exit, anything else is a die exit. */
const awaitResult = async (
  workflow: Workflow.Any,
  handle: WorkflowHandle,
): Promise<Workflow.Result<unknown, unknown>> => {
  const codecs = wireCodecsFor(workflow);
  try {
    const wire = await handle.result();
    return new Workflow.Complete({ exit: codecs.decodeExit(wire) });
  } catch (error) {
    const thrown = classifyThrown(error);
    switch (thrown.kind) {
      case "interrupted":
        return new Workflow.Complete({ exit: Exit.interrupt() });
      case "wire":
        return new Workflow.Complete({ exit: codecs.decodeExit(thrown.exit) });
      case "other":
        return new Workflow.Complete({ exit: Exit.die(thrown.error) });
    }
  }
};

/**
 * Build the client-side `WorkflowEngine`, the seam `WorkflowClient` (and
 * `MyWorkflow.execute` run outside a sandbox) drives Temporal through.
 *
 * @since 0.1.0
 * @category constructors
 */
export const makeTemporalClientEngine = (
  options: TemporalClientEngineOptions,
): WorkflowEngine.WorkflowEngine["Service"] => {
  const { client, taskQueue } = options;

  /** Start, or attach to the execution this id already names.
   * REJECT_DUPLICATE is load-bearing: the default reuse policy would start
   * a fresh (duplicate, side-effecting) run once the first one closed,
   * where the idempotency contract requires the original result forever. */
  const startOrAttach = async (
    workflowType: string,
    executionId: string,
    wirePayload: unknown,
  ): Promise<WorkflowHandle> => {
    try {
      return await client.workflow.start(workflowType, {
        workflowId: executionId,
        taskQueue,
        args: [wirePayload],
        workflowIdReusePolicy: "REJECT_DUPLICATE",
      });
    } catch (error) {
      if (error instanceof WorkflowExecutionAlreadyStartedError) {
        return client.workflow.getHandle(executionId);
      }
      throw error;
    }
  };

  return WorkflowEngine.makeUnsafe({
    // The workflow bundle's export is the registration.
    register: () => Effect.void,

    execute: (workflow, { executionId, payload, discard }) => {
      // A payload that fails its own schema is a caller bug: die here.
      const wirePayload = wireCodecsFor(workflow).encodePayload(payload);
      // SAFETY: `makeUnsafe`'s unknown-seam — the engine contract types
      // `execute` against per-workflow generics it cannot thread through a
      // handler record; the promise produces exactly the Result-or-undefined
      // (undefined iff `discard`) shape the contract reads back.
      return Effect.promise(async () => {
        const handle = await startOrAttach(workflow._tag, executionId, wirePayload);
        if (discard) return undefined;
        return await awaitResult(workflow, handle);
      }) as Effect.Effect<Workflow.Result<unknown, unknown> | undefined> as never;
    },

    poll: (workflow, executionId) =>
      Effect.promise(async (): Promise<Option.Option<Workflow.Result<unknown, unknown>>> => {
        const handle = client.workflow.getHandle(executionId);
        let status: string;
        try {
          status = (await handle.describe()).status.name;
        } catch (error) {
          // An unknown execution polls as None, per the engine contract.
          if (error instanceof WorkflowNotFoundError) return Option.none();
          throw error;
        }
        if (status === "RUNNING") return Option.none();
        if (status === "CANCELLED") {
          return Option.some(new Workflow.Complete({ exit: Exit.interrupt() }));
        }
        return Option.some(await awaitResult(workflow, handle));
      }),

    interrupt: (_workflow, executionId) =>
      Effect.promise(async () => {
        try {
          await client.workflow.getHandle(executionId).cancel();
        } catch (error) {
          // Interrupting an unknown or closed execution is a no-op,
          // matching interruption of a completed fiber.
          if (!(error instanceof WorkflowNotFoundError)) throw error;
        }
      }),

    interruptUnsafe: (_workflow, executionId) =>
      Effect.promise(async () => {
        try {
          await client.workflow.getHandle(executionId).terminate("effect-workflow interruptUnsafe");
        } catch (error) {
          if (!(error instanceof WorkflowNotFoundError)) throw error;
        }
      }),

    // Nothing ever suspends under this engine, so there is nothing to
    // resume; Temporal owns continuation.
    resume: () => Effect.void,

    deferredDone: ({ executionId, deferredName, exit }) =>
      Effect.promise(async () => {
        try {
          await client.workflow.getHandle(executionId).signal(DEFERRED_DONE_SIGNAL, {
            deferredName,
            exit: encodeDeferredExit(exit),
          });
        } catch (error) {
          // An approval landing after the workflow closed is a normal race,
          // not an error; `deferredDone` never fails.
          if (!(error instanceof WorkflowNotFoundError)) throw error;
        }
      }),

    activityExecute: () => workflowSideOnly("activityExecute"),
    // Structurally sandbox-only (needs WorkflowInstance in context); the
    // client-side read is `deferredState`.
    deferredResult: () => workflowSideOnly("deferredResult"),
    scheduleClock: () => workflowSideOnly("scheduleClock"),
  });
};

/**
 * A schedule with this id already exists (and is not deleted) — the
 * routine re-registration condition, surfaced typed so deploy-time
 * idempotent setup can catch the tag.
 *
 * @since 0.1.0
 * @category errors
 */
export class ScheduleAlreadyExistsError extends Data.TaggedError("ScheduleAlreadyExistsError")<{
  readonly scheduleId: string;
  readonly workflowType: string;
}> {}

/**
 * Create a Temporal schedule whose action starts the given shim workflow
 * with the WIRE-encoded payload. Fired runs get schedule-generated workflow
 * ids (the base id plus a timestamp), so they are addressed by those ids —
 * e.g. via `MyWorkflow.poll` — rather than by the digest execution id.
 *
 * @since 0.1.0
 * @category client
 */
export const createWorkflowSchedule = <
  Tag extends string,
  Payload extends Workflow.AnyStructSchema,
  Success extends Schema.Top,
  Error extends Schema.Top,
>(options: {
  readonly client: Client;
  readonly scheduleId: string;
  readonly workflow: Workflow.Workflow<Tag, Payload, Success, Error>;
  readonly payload: Payload["Type"];
  readonly taskQueue: string;
  readonly spec: ScheduleSpec;
}): Effect.Effect<ScheduleHandle, ScheduleAlreadyExistsError> =>
  Effect.tryPromise({
    try: () =>
      options.client.schedule.create({
        scheduleId: options.scheduleId,
        spec: options.spec,
        action: {
          type: "startWorkflow",
          workflowType: options.workflow._tag,
          taskQueue: options.taskQueue,
          workflowId: options.scheduleId,
          args: [wireCodecsFor(options.workflow).encodePayload(options.payload)],
        },
      }),
    catch: (error) => {
      if (error instanceof ScheduleAlreadyRunning) {
        return new ScheduleAlreadyExistsError({
          scheduleId: options.scheduleId,
          workflowType: options.workflow._tag,
        });
      }
      throw error;
    },
  });

/**
 * Offer a message to a running workflow's mailbox. Offering to a closed or
 * unknown execution is a no-op — the workflow finishing first is a normal
 * race, matching `DurableDeferred.done`.
 *
 * @since 0.1.0
 * @category client
 */
export const offerMailbox = <S extends Schema.Top>(
  mailbox: DurableMailbox<S>,
  options: {
    readonly client: Client;
    readonly workflowId: string;
    readonly payload: S["Type"];
  },
): Effect.Effect<void> => {
  const wire = mailboxCodec(mailbox).encode(options.payload);
  return Effect.promise(async () => {
    try {
      await options.client.workflow.getHandle(options.workflowId).signal(MAILBOX_SIGNAL, {
        mailboxName: mailbox.name,
        payload: wire,
      });
    } catch (error) {
      if (!(error instanceof WorkflowNotFoundError)) throw error;
    }
  });
};

/**
 * Send an update request to a running workflow and receive its typed
 * response: the workflow's `respond` exit lands in this effect's
 * success/error channels. Unlike mailbox offers, an update expects an
 * answer, so an unknown execution is a defect rather than a no-op.
 *
 * @since 0.1.0
 * @category client
 */
export const executeUpdate = <P extends Schema.Top, S extends Schema.Top, E extends Schema.Top>(
  update: DurableUpdate<P, S, E>,
  options: {
    readonly client: Client;
    readonly workflowId: string;
    readonly payload: P["Type"];
  },
): Effect.Effect<S["Type"], E["Type"]> => {
  const codecs = updateCodec(update);
  const wire = codecs.encodePayload(options.payload);
  return Effect.flatMap(
    Effect.promise(async () => {
      try {
        return {
          outcome: "wire" as const,
          wire: await options.client.workflow
            .getHandle(options.workflowId)
            .executeUpdate<unknown, [WorkflowUpdatePayload]>(WORKFLOW_UPDATE, {
              args: [{ updateName: update.name, payload: wire }],
            }),
        };
      } catch (error) {
        return { outcome: "thrown" as const, error };
      }
    }),
    (result) => {
      if (result.outcome === "wire") return codecs.decodeExit(result.wire);
      // A run cancelled (or continued-as-new) out from under a pending
      // update surfaces as a cancellation inside the update failure: the
      // caller is interrupted, exactly as if it had been awaiting the
      // cancelled run itself. Everything else — unknown execution, a run
      // completing unanswered — stays a defect: an update expects an answer.
      const thrown = classifyThrown(result.error);
      // SAFETY: widening — `Exit.interrupt()` is `Exit<never, never>`,
      // assignable to any exit's channels.
      return thrown.kind === "interrupted"
        ? (Exit.interrupt() as Exit.Exit<S["Type"], E["Type"]>)
        : Effect.die(result.error);
    },
  );
};

/**
 * Read the latest snapshot a workflow published to `cell`: `None` while the
 * execution is unknown or the cell unpublished, `Some(typed value)`
 * otherwise — including after the run has closed.
 *
 * @since 0.1.0
 * @category client
 */
export const readStateCell = <S extends Schema.Top>(
  cell: StateCell<S>,
  options: {
    readonly client: Client;
    readonly workflowId: string;
  },
): Effect.Effect<Option.Option<S["Type"]>> =>
  Effect.promise(async () => {
    let wire: unknown;
    try {
      wire = await options.client.workflow
        .getHandle(options.workflowId)
        .query<unknown, [string]>(STATE_CELL_QUERY, cell.name);
    } catch (error) {
      if (error instanceof WorkflowNotFoundError) return Option.none();
      throw error;
    }
    if (wire === null || wire === undefined) return Option.none();
    return Option.some(stateCellCodec(cell).decode(wire));
  });

/**
 * Read a deferred's current state from outside the workflow: `None` while
 * the execution is unknown or the deferred unresolved, `Some(typed exit)`
 * once completed. Backed by a Temporal query, so it never perturbs the
 * signal path.
 *
 * @since 0.1.0
 * @category client
 */
export const deferredState = <Success extends Schema.Constraint, Error extends Schema.Constraint>(
  deferred: DurableDeferred.DurableDeferred<Success, Error>,
  options: {
    readonly client: Client;
    readonly workflowId: string;
  },
): Effect.Effect<Option.Option<Exit.Exit<Success["Type"], Error["Type"]>>> => {
  const decodeExit = Schema.decodeSync(asJsonCodec(deferred.exitSchema));
  return Effect.promise(async () => {
    const handle = options.client.workflow.getHandle(options.workflowId);
    let wire: unknown;
    try {
      wire = await handle.query<unknown, [string]>(DEFERRED_STATE_QUERY, deferred.name);
    } catch (error) {
      if (error instanceof WorkflowNotFoundError) return Option.none();
      throw error;
    }
    if (wire === null || wire === undefined) return Option.none();
    // Wire JSON → Exit instance with encoded leaves → typed leaves.
    // SAFETY: `decodeExit` runs the deferred's own exitSchema (built from its
    // success/error schemas), so the decoded exit has exactly these channels —
    // the `asJsonCodec` seam types it as unknown.
    return Option.some(
      decodeExit(decodeDeferredExit(wire)) as Exit.Exit<Success["Type"], Error["Type"]>,
    );
  });
};

/**
 * Layer form, for composition into an app runtime.
 *
 * @since 0.1.0
 * @category layers
 */
export const layerTemporalClientEngine = (
  options: TemporalClientEngineOptions,
): Layer.Layer<WorkflowEngine.WorkflowEngine> =>
  Layer.succeed(WorkflowEngine.WorkflowEngine, makeTemporalClientEngine(options));
