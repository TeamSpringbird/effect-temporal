// Static contracts for the whole public surface, enforced by the package's
// `tsc --noEmit` gate: `expectTypeOf` mismatches and vanished
// `@ts-expect-error`s are both compile errors. Assertions live in
// never-invoked thunks that RETURN what they construct — nothing runs, and
// the Effect language service's floating-effect rule stays satisfied; the
// one `it` exists so vitest accepts the file.
//
// Division of labor: `@ts-expect-error` pins the plain-TypeScript surface
// (payload shapes, literal unions, missing fields). Effect CHANNEL
// mismatches are pinned by exact positive `expectTypeOf` assertions
// instead — the Effect language service rewrites channel-assignability
// errors into its own diagnostics (TS377xxx), which `@ts-expect-error`
// cannot suppress or assert on.

import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import type * as Option from "effect/Option";
import type * as Workflow from "effect/unstable/workflow/Workflow";
import type * as WorkflowEngine from "effect/unstable/workflow/WorkflowEngine";
import { describe, expect, expectTypeOf, it } from "vitest";
// Type tests pin both halves' signatures in one place; nothing here runs in
// either process, so the sandbox/client separation the rule protects is moot.
// oxlint-disable-next-line effect-temporal/no-mixed-halves
import {
  deferredState,
  executeUpdate,
  makeTemporalClientEngine,
  offerMailbox,
  readStateCell,
} from "../engine-client.js";
import {
  callRawActivity,
  continueAsNew,
  makeTemporalWorkflow,
  offerMailbox as offerMailboxFromWorkflow,
  setStateCell,
  takeMailbox,
  takeUpdate,
  type SandboxRun,
} from "../engine-sandbox.js";
import * as Versioning from "../versioning.js";
import { ChildDemo } from "./fixtures/child-demo.js";
import { Demo } from "./fixtures/demo.js";
import { LoopDemo } from "./fixtures/loop-demo.js";
import { StateSnapshot, StateUpdates } from "./fixtures/mailbox-demo.js";
import { Approved, SetLanguage } from "./fixtures/message-demo.js";

declare const clientOptions: Parameters<typeof makeTemporalClientEngine>[0];

type DemoPayload = {
  readonly requestId: string;
  readonly mode: "approve" | "fail-after-step" | "long-activity" | "timeout-activity";
};
type StateUpdate =
  | { readonly op: "set"; readonly key: string; readonly value: number }
  | { readonly op: "del"; readonly key: string }
  | { readonly op: "finish" };

const _workflowChannels = () => {
  const executed = Demo.execute({ requestId: "r", mode: "approve" });
  expectTypeOf<Effect.Success<typeof executed>>().toEqualTypeOf<string>();
  expectTypeOf<Effect.Error<typeof executed>>().toEqualTypeOf<string>();
  expectTypeOf<Effect.Services<typeof executed>>().toEqualTypeOf<WorkflowEngine.WorkflowEngine>();

  // Discarded execution: the success is the execution id, never the result.
  const discarded = Demo.execute({ requestId: "r", mode: "approve" }, { discard: true });
  expectTypeOf<Effect.Success<typeof discarded>>().toEqualTypeOf<string>();
  expectTypeOf<Effect.Error<typeof discarded>>().toEqualTypeOf<never>();

  const polled = Demo.poll("execution-id");
  expectTypeOf<Effect.Success<typeof polled>>().toEqualTypeOf<
    Option.Option<Workflow.Result<string, string>>
  >();

  const negatives = [
    // @ts-expect-error unknown mode literal
    Demo.execute({ requestId: "r", mode: "nope" }),
    // @ts-expect-error missing payload field
    Demo.execute({ requestId: "r" }),
    // @ts-expect-error payload field of the wrong type
    Demo.executionId({ requestId: 1, mode: "approve" }),
  ];
  return { executed, discarded, polled, negatives };
};

const _handlerInference = () =>
  makeTemporalWorkflow(Demo, (payload, executionId) => {
    expectTypeOf(payload).toEqualTypeOf<DemoPayload>();
    expectTypeOf(executionId).toEqualTypeOf<string>();
    return Effect.succeed("ok");
  });

const _activities = () => {
  const called = callRawActivity(() => Promise.resolve(42));
  expectTypeOf<Effect.Success<typeof called>>().toEqualTypeOf<number>();
  expectTypeOf<Effect.Error<typeof called>>().toEqualTypeOf<never>();
  expectTypeOf<Effect.Services<typeof called>>().toEqualTypeOf<SandboxRun>();
  return called;
};

const _mailboxes = () => {
  const { client } = clientOptions;
  const taken = takeMailbox(StateUpdates);
  expectTypeOf<Effect.Success<typeof taken>>().toEqualTypeOf<StateUpdate>();
  expectTypeOf<Effect.Services<typeof taken>>().toEqualTypeOf<SandboxRun>();

  return [
    taken,
    offerMailbox(StateUpdates, { client, workflowId: "id", payload: { op: "finish" } }),
    offerMailboxFromWorkflow(StateUpdates, { workflowId: "id", payload: { op: "finish" } }),
    // @ts-expect-error unknown mailbox op
    offerMailbox(StateUpdates, { client, workflowId: "id", payload: { op: "reset" } }),
    // @ts-expect-error a set requires key and value
    offerMailboxFromWorkflow(StateUpdates, { workflowId: "id", payload: { op: "set" } }),
  ];
};

const _stateCells = () => {
  const read = readStateCell(StateSnapshot, {
    client: clientOptions.client,
    workflowId: "id",
  });
  expectTypeOf<Effect.Success<typeof read>>().toEqualTypeOf<
    Option.Option<{ readonly [key: string]: number }>
  >();

  return [
    read,
    setStateCell(StateSnapshot, { a: 1 }),
    // @ts-expect-error cell values are numbers
    setStateCell(StateSnapshot, { a: "one" }),
  ];
};

const _updates = () => {
  const { client } = clientOptions;
  const response = executeUpdate(SetLanguage, {
    client,
    workflowId: "id",
    payload: { language: "french" },
  });
  expectTypeOf<Effect.Success<typeof response>>().toEqualTypeOf<string>();
  expectTypeOf<Effect.Error<typeof response>>().toEqualTypeOf<string>();

  const negative =
    // @ts-expect-error payload must match the update's schema
    executeUpdate(SetLanguage, { client, workflowId: "id", payload: { lang: "x" } });

  const taken = takeUpdate(SetLanguage);
  expectTypeOf<Effect.Services<typeof taken>>().toEqualTypeOf<SandboxRun>();
  const served = Effect.andThen(taken, (request) => {
    expectTypeOf(request.payload).toEqualTypeOf<{ readonly language: string }>();
    expectTypeOf(request.respond).parameter(0).toEqualTypeOf<Exit.Exit<string, string>>();
    return Effect.all([
      request.respond(Exit.succeed("previous")),
      request.respond(Exit.fail("unsupported")),
      // @ts-expect-error the response is an Exit, not a bare value
      request.respond("previous"),
    ]);
  });
  return { response, negative, served };
};

const _continueAsNew = () => {
  const continued = continueAsNew(LoopDemo, { requestId: "r", iteration: 1 });
  expectTypeOf<Effect.Success<typeof continued>>().toEqualTypeOf<never>();

  return [
    continued,
    // @ts-expect-error payload must match the target workflow's schema
    continueAsNew(LoopDemo, { requestId: "r" }),
    // @ts-expect-error payload fields are typed
    continueAsNew(LoopDemo, { requestId: "r", iteration: "one" }),
  ];
};

const _deferreds = () => {
  const state = deferredState(Approved, { client: clientOptions.client, workflowId: "id" });
  expectTypeOf<Effect.Success<typeof state>>().toEqualTypeOf<
    Option.Option<Exit.Exit<string, never>>
  >();
  return state;
};

const _versioning = () => {
  const selected = Versioning.version("site", ["v1", "v2", "v3"]);
  expectTypeOf<Effect.Success<typeof selected>>().toEqualTypeOf<"v1" | "v2" | "v3">();

  const matched = Versioning.match("site", [
    { version: "v1", run: Effect.succeed(1) },
    { version: "v2", run: Effect.fail("legacy-error") },
    { version: "v3", run: Effect.succeed("three") },
  ]);
  expectTypeOf<Effect.Success<typeof matched>>().toEqualTypeOf<number | string>();
  expectTypeOf<Effect.Error<typeof matched>>().toEqualTypeOf<string>();

  // Heterogeneous cases reduce with full fidelity: distinct result shapes
  // union, an always-failing case contributes `never` to the success union,
  // and a case requiring services carries its requirement into the union.
  const heterogeneous = Versioning.match("pricing", [
    { version: "v1", run: Effect.succeed({ kind: "flat" as const, cents: 100 }) },
    { version: "v2", run: Effect.fail({ kind: "quote-required" as const }) },
    {
      version: "v3",
      run: callRawActivity(() => Promise.resolve({ kind: "dynamic" as const, quote: "q" })),
    },
  ]);
  expectTypeOf<Effect.Success<typeof heterogeneous>>().toEqualTypeOf<
    { kind: "flat"; cents: number } | { kind: "dynamic"; quote: string }
  >();
  expectTypeOf<Effect.Error<typeof heterogeneous>>().toEqualTypeOf<{ kind: "quote-required" }>();
  expectTypeOf<Effect.Services<typeof heterogeneous>>().toEqualTypeOf<SandboxRun>();

  // The selected literal narrows through ordinary control flow.
  const narrowed = Effect.map(selected, (name) => {
    if (name === "v1") {
      expectTypeOf(name).toEqualTypeOf<"v1">();
      return 0;
    }
    expectTypeOf(name).toEqualTypeOf<"v2" | "v3">();
    return 1;
  });
  return { selected, matched, heterogeneous, narrowed };
};

const _childWorkflows = () => {
  const childExecuted = ChildDemo.execute({ requestId: "r", outcome: "ok" });
  expectTypeOf<Effect.Error<typeof childExecuted>>().toEqualTypeOf<string>();
  return [
    childExecuted,
    // @ts-expect-error outcome is a literal union
    ChildDemo.execute({ requestId: "r", outcome: "explode" }),
  ];
};

describe("type safety", { concurrent: false }, () => {
  it("is enforced at typecheck time", () => {
    // The assertions above are compile-time only; this run exists so vitest
    // accepts the file.
    for (const thunk of [
      _workflowChannels,
      _handlerInference,
      _activities,
      _mailboxes,
      _stateCells,
      _updates,
      _continueAsNew,
      _deferreds,
      _versioning,
      _childWorkflows,
    ]) {
      expect(typeof thunk).toBe("function");
    }
  });
});
