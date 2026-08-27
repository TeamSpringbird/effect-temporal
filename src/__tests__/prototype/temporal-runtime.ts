// PROTOTYPE — throwaway. The Temporal implementation of the ops seam:
// every op dispatches into the EXISTING engine-sandbox machinery, so the
// prototype rides the proven engine underneath. Sandbox-only module.

import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as DurableDeferred from "effect/unstable/workflow/DurableDeferred";
import {
  callActivity,
  pollMailbox,
  setStateCell,
  takeMailbox,
  takeUpdate,
} from "../../engine-sandbox.js";
import type { DurableMailbox } from "../../mailbox.js";
import type { DurableUpdate } from "../../update.js";
import type { StateCell } from "../../state-cell.js";
import type * as Schema from "effect/Schema";
import type { OpsRuntime } from "./def.js";

interface DefinitionLike {
  readonly workflow: {
    readonly toLayer: (execute: (payload: any, executionId: string) => Effect.Effect<any, any, any>) => Layer.Layer<never, never, any>;
  };
  readonly deferreds: Record<string, DurableDeferred.DurableDeferred<any, any>>;
  readonly mailboxes: Record<string, DurableMailbox<Schema.Top>>;
  readonly updates: Record<string, DurableUpdate<Schema.Top, Schema.Top, Schema.Top>>;
  readonly cells: Record<string, StateCell<Schema.Top>>;
  readonly makeOps: (runtime: OpsRuntime) => any;
}

const byName = <T extends { readonly name: string }>(record: Record<string, T>) =>
  new Map(Object.values(record).map((item) => [item.name, item]));

const temporalRuntime = (def: DefinitionLike): OpsRuntime => {
  const deferreds = new Map(Object.entries(def.deferreds).map(([k, d]) => [d.name ?? k, d]));
  const mailboxes = byName(def.mailboxes);
  const updates = byName(def.updates);
  const cells = byName(def.cells);
  const missing = (kind: string, name: string) =>
    Effect.die(`prototype: no ${kind} named "${name}" in this definition`);
  // SAFETY (prototype): these ops require SandboxRun / the engine at the
  // type level; the per-run wrapper provides them. The seam types R=never
  // so handlers stay engine-agnostic.
  return {
    activity: (activity, payload) => callActivity(activity, payload as never) as never,
    deferredAwait: (name) => {
      const d = deferreds.get(name);
      return d ? (DurableDeferred.await(d) as never) : missing("deferred", name);
    },
    mailboxTake: (name) => {
      const m = mailboxes.get(name);
      return m ? (takeMailbox(m) as never) : missing("mailbox", name);
    },
    mailboxPoll: (name) => {
      const m = mailboxes.get(name);
      return m ? (pollMailbox(m) as never) : missing("mailbox", name);
    },
    updateTake: (name) => {
      const u = updates.get(name);
      return u ? (takeUpdate(u) as never) : missing("update", name);
    },
    stateSet: (name, value) => {
      const c = cells.get(name);
      return c ? (setStateCell(c, value) as never) : missing("cell", name);
    },
  };
};

/** Turn a bound handler into an upstream `toLayer` registration whose body
 * receives Temporal-backed typed ops — ready for `workflowBundle`. */
export const toTemporalLayer = (bound: {
  readonly definition: DefinitionLike;
  readonly body: (payload: any, ops: any) => Effect.Effect<any, any>;
}): Layer.Layer<never, never, any> => {
  const ops = bound.definition.makeOps(temporalRuntime(bound.definition));
  return bound.definition.workflow.toLayer((payload) => bound.body(payload, ops));
};
