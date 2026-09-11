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
 * modules), the bundle entry imports this. Everything else in the sandbox
 * half (`engine-sandbox`) is engine machinery — raw activity proxies, the
 * Nexus caller, workflow-to-workflow offers — for code that deliberately
 * steps below the declaration surface.
 *
 * @since 0.4.0
 */

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
