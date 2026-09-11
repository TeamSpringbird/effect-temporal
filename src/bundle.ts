/**
 * The one file the Temporal worker points at. A workflow bundle's entry
 * module imports `workflowBundle` from here, passes it the merged
 * `Workflow.toLayer` registrations, and exports the result as its DEFAULT
 * export — every registered tag becomes a startable Temporal workflow type,
 * and every hosted handler receives the Temporal `WorkflowOps` runtime the
 * `definition` module's declarations dispatch through:
 *
 * ```ts
 * // workflows.ts — the bundle entry (Worker.create({ workflowsPath: ... }))
 * import * as Layer from "effect/Layer";
 * import { workflowBundle } from "@springbird/effect-temporal/bundle";
 * import { OrderFlow, orderHandler } from "./definitions.js";
 *
 * export default workflowBundle(OrderFlow.toLayer(orderHandler));
 * ```
 *
 * This is the whole public sandbox-side surface for applications authored
 * with the `definition` module: handlers import `definition` (and their own
 * modules), the bundle entry imports this. The one other thing here is the
 * Temporal-only retirement step of a version chain (`deprecateVersion`),
 * which belongs in a bundle, never in a handler. Everything else in the
 * sandbox half (`engine-sandbox`) is engine machinery — raw activity
 * proxies, the Nexus caller, workflow-to-workflow offers — for code that
 * deliberately steps below the declaration surface.
 *
 * @since 0.4.0
 */

import * as Effect from "effect/Effect";
import {
  deprecatePatch as temporalDeprecatePatch,
  patched as temporalPatched,
} from "@temporalio/workflow";

export {
  /**
   * Host `Workflow.toLayer` registrations behind one dynamic Temporal
   * workflow. See the module doc.
   *
   * @since 0.4.0
   * @category constructors
   */
  workflowBundle,
} from "./engine-sandbox.js";

/**
 * `true` on fresh executions (records the patch marker), `false` when
 * replaying a history recorded before this patch id existed — Temporal's
 * raw patch primitive, for one-off guards outside a `version` chain.
 *
 * @since 0.5.0
 * @category versioning
 */
export const patched = (patchId: string): Effect.Effect<boolean> =>
  Effect.sync(() => temporalPatched(patchId));

/**
 * Phase two of Temporal's patch lifecycle: keep a marker recognized for
 * histories that carry it while no longer branching. Deploy after every
 * pre-patch execution has drained.
 *
 * @since 0.5.0
 * @category versioning
 */
export const deprecatePatch = (patchId: string): Effect.Effect<void> =>
  Effect.sync(() => temporalDeprecatePatch(patchId));

/**
 * Retire one name of a `version` / `versioned` site: `deprecatePatch` for
 * its marker `${site}-${name}`. Remove the name from the chain and deploy
 * this in its place for one release, once every history carrying the marker
 * has closed.
 *
 * @since 0.5.0
 * @category versioning
 */
export const deprecateVersion = (site: string, name: string): Effect.Effect<void> =>
  deprecatePatch(`${site}-${name}`);
