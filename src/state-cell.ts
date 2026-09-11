/**
 * Queryable workflow state: the workflow publishes typed snapshots to a
 * named cell, and clients read the latest one through a Temporal query —
 * including after the run has closed. Cells are per-run: a run started by
 * `continueAsNew` begins empty until it republishes.
 *
 * Temporal query handlers are synchronous and read-only, which is why the
 * abstraction is a published snapshot rather than an on-demand computation.
 * **Internal.** This module holds the shared wire contract (query name,
 * definition shape, codec) the engine halves consume. It has no package
 * export: applications declare cells with `defineState` from the
 * `definition` module.
 *
 * @since 0.1.0
 */

import type * as Schema from "effect/Schema";
import { wireValueCodec, type WireValueCodec } from "./wire.js";

/**
 * Query by which clients read a cell: takes a cell name, returns its
 * wire-encoded value, or null while unpublished.
 *
 * @since 0.1.0
 * @category wire
 */
export const STATE_CELL_QUERY = "effect-workflow-state-cell";

/**
 * A state cell definition: the name and value schema shared by the
 * publishing workflow and every reading side.
 *
 * @since 0.1.0
 * @category models
 */
export interface StateCell<S extends Schema.Top> {
  readonly name: string;
  readonly valueSchema: S;
}


/**
 * The wire codec for a cell's value — how snapshots are encoded when
 * published (`setStateCell`) and decoded when read (`readStateCell`).
 *
 * @since 0.1.0
 * @category codecs
 */
export const stateCellCodec = <S extends Schema.Top>(
  cell: StateCell<S>,
): WireValueCodec<S["Type"]> => wireValueCodec(cell.valueSchema);
