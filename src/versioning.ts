/**
 * Workflow versioning over Temporal patch markers, for changing workflow
 * code while executions are in flight.
 *
 * The composable form is a version CHAIN per code site: the first case is
 * the original behavior (guarded by no marker, so histories from before the
 * site adopted versioning replay through it), and each later case is
 * guarded by its own patch marker `${site}-${name}`. Fresh executions take
 * the newest case and record only its marker; replays take the case whose
 * marker their history carries.
 *
 * ```ts
 * const result = yield* Versioning.match("pricing", [
 *   { version: "v1", run: originalPricing },
 *   { version: "v2", run: revisedPricing },
 * ]);
 * ```
 *
 * Evolving a site is appending a case. Retiring one is removing it AFTER
 * every history carrying its marker has closed, then `deprecateVersion` —
 * replaying a removed version's history fails loudly rather than silently
 * running the wrong code.
 *
 * Evaluate a site's version at a deterministic point on the main workflow
 * fiber, never from racing fibers — marker order is part of history. As
 * with Temporal's native API, a site re-evaluated in a LATER workflow task
 * of the same run may advance to a newer version; evaluate once and reuse
 * the result where that matters.
 *
 * Sandbox-only: this module imports `@temporalio/workflow`.
 *
 * @since 0.1.0
 */

import * as Effect from "effect/Effect";
import {
  deprecatePatch as temporalDeprecatePatch,
  patched as temporalPatched,
} from "@temporalio/workflow";

/**
 * One case of a version chain: the version's name and its behavior.
 *
 * @since 0.1.0
 * @category models
 */
export interface Version<Name extends string, A, E, R> {
  readonly version: Name;
  readonly run: Effect.Effect<A, E, R>;
}

/**
 * Which version of `site` this execution runs, as a literal of `names`
 * (ordered oldest first). Checked newest-first, so a fresh execution
 * records only the newest marker.
 *
 * @since 0.1.0
 * @category combinators
 */
export const version = <const Names extends readonly [string, ...string[]]>(
  site: string,
  names: Names,
): Effect.Effect<Names[number]> =>
  Effect.sync(() => {
    for (let i = names.length - 1; i >= 1; i--) {
      if (temporalPatched(`${site}-${names[i]!}`)) return names[i]!;
    }
    return names[0];
  });

/**
 * Run the case `version(site, ...)` selects — the effectful form of the
 * patch branch, with the result, error, and service channels unioned across
 * cases.
 *
 * @since 0.1.0
 * @category combinators
 */
export const match = <
  const Cases extends readonly [
    Version<string, unknown, unknown, unknown>,
    ...Version<string, unknown, unknown, unknown>[],
  ],
>(
  site: string,
  cases: Cases,
): Effect.Effect<
  Effect.Success<Cases[number]["run"]>,
  Effect.Error<Cases[number]["run"]>,
  Effect.Services<Cases[number]["run"]>
> => {
  // SAFETY: `cases` is a non-empty tuple, so mapping it yields a non-empty
  // array of version names.
  const names = cases.map((entry) => entry.version) as unknown as readonly [string, ...string[]];
  // SAFETY: `version` returns one of the cases' names, so `find` always
  // matches, and the matched case's run is covered by the channels unioned
  // over `Cases[number]`.
  return Effect.flatMap(
    version(site, names),
    (name) => cases.find((entry) => entry.version === name)!.run,
  ) as Effect.Effect<
    Effect.Success<Cases[number]["run"]>,
    Effect.Error<Cases[number]["run"]>,
    Effect.Services<Cases[number]["run"]>
  >;
};

/**
 * `true` on fresh executions (records the patch marker), `false` when
 * replaying a history recorded before this patch id existed. The boolean
 * primitive `match` is built on; useful for one-off guards.
 *
 * @since 0.1.0
 * @category combinators
 */
export const patched = (patchId: string): Effect.Effect<boolean> =>
  Effect.sync(() => temporalPatched(patchId));

/**
 * Phase two of Temporal's patch lifecycle: keep a marker recognized for
 * histories that carry it while no longer branching. Deploy after every
 * pre-patch execution has drained.
 *
 * @since 0.1.0
 * @category combinators
 */
export const deprecatePatch = (patchId: string): Effect.Effect<void> =>
  Effect.sync(() => temporalDeprecatePatch(patchId));

/**
 * `deprecatePatch` for a version-chain case's marker.
 *
 * @since 0.1.0
 * @category combinators
 */
export const deprecateVersion = (site: string, name: string): Effect.Effect<void> =>
  deprecatePatch(`${site}-${name}`);
