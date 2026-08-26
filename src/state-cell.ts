/**
 * Queryable workflow state: the workflow publishes typed snapshots to a
 * named cell, and clients read the latest one through a Temporal query —
 * including after the run has closed. Cells are per-run: a run started by
 * `continueAsNew` begins empty until it republishes.
 *
 * Temporal query handlers are synchronous and read-only, which is why the
 * abstraction is a published snapshot rather than an on-demand computation.
 * This module holds the shared definition and codec; the operations live
 * with their process: `setStateCell` in `engine-sandbox`, `readStateCell`
 * in `engine-client`.
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
 * Declare a state cell: a name (unique within the workflows that use it)
 * and the value schema. Shared by the workflow body and every reading side.
 *
 * @since 0.1.0
 * @category constructors
 */
export const make = <S extends Schema.Top>(
  name: string,
  options: { readonly value: S },
): StateCell<S> => ({ name, valueSchema: options.value });

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
