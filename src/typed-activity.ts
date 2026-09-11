/**
 * **Deprecated module** — superseded by the `definition` module in 0.3.0 and
 * scheduled for removal in 0.5.0. Everything here is a re-export or a thin
 * alias so existing imports keep compiling while you migrate:
 *
 * - `TypedActivity.make(name, decl)` → `defineActivity(name, decl)` from
 *   `@springbird/effect-temporal/definition` (a declared activity IS its
 *   `TypedActivity` projection, and is callable inside handlers).
 * - the type helpers `PayloadOf`, `SuccessOf`, `ErrorOf`, `AnyTypedActivity`,
 *   `TypedActivity`, `TypedActivityOptions` → the same names from
 *   `@springbird/effect-temporal/definition`.
 * - `codecsFor`, `ACTIVITY_EXIT_TYPE` → the same names from
 *   `@springbird/effect-temporal/wire`.
 *
 * The `prefer-definition` lint rule reports every import from this module.
 *
 * @deprecated Import from `definition` (types, `defineActivity`) or `wire`
 * (`codecsFor`, `ACTIVITY_EXIT_TYPE`) instead. Removed in 0.5.0.
 * @since 0.1.0
 */

import { defineActivity, makeTypedActivity } from "./definition.js";

export {
  /**
   * @deprecated Import `ACTIVITY_EXIT_TYPE` from `wire`. Removed in 0.5.0.
   * @since 0.1.0
   * @category wire
   */
  ACTIVITY_EXIT_TYPE,
  /**
   * @deprecated Import `codecsFor` from `wire`. Removed in 0.5.0.
   * @since 0.1.0
   * @category codecs
   */
  codecsFor,
  /**
   * @deprecated Import `TypedActivityCodecs` from `wire`. Removed in 0.5.0.
   * @since 0.1.0
   * @category models
   */
  type TypedActivityCodecs,
} from "./wire.js";

export {
  /**
   * @deprecated Import `DEFAULT_ACTIVITY_OPTIONS` from `definition`. Removed in 0.5.0.
   * @since 0.1.0
   * @category models
   */
  DEFAULT_ACTIVITY_OPTIONS,
  /**
   * @deprecated Import `AnyTypedActivity` from `definition`. Removed in 0.5.0.
   * @since 0.1.0
   * @category models
   */
  type AnyTypedActivity,
  /**
   * @deprecated Import `ErrorOf` from `definition`. Removed in 0.5.0.
   * @since 0.1.0
   * @category models
   */
  type ErrorOf,
  /**
   * @deprecated Import `PayloadOf` from `definition`. Removed in 0.5.0.
   * @since 0.1.0
   * @category models
   */
  type PayloadOf,
  /**
   * @deprecated Import `SuccessOf` from `definition`. Removed in 0.5.0.
   * @since 0.1.0
   * @category models
   */
  type SuccessOf,
  /**
   * @deprecated Import `TypedActivity` from `definition`. Removed in 0.5.0.
   * @since 0.1.0
   * @category models
   */
  type TypedActivity,
  /**
   * @deprecated Import `TypedActivityOptions` from `definition`. Removed in 0.5.0.
   * @since 0.1.0
   * @category models
   */
  type TypedActivityOptions,
} from "./definition.js";

/**
 * Declare a typed activity — the pre-0.3.0 spelling of `defineActivity`.
 * Returns exactly what `defineActivity` returns (the projection plus the
 * in-handler callable), so migrating is renaming the import.
 *
 * @deprecated Use `defineActivity` from `definition`. Removed in 0.5.0.
 * @since 0.1.0
 * @category constructors
 */
export const make: typeof defineActivity = defineActivity;

export {
  /**
   * The projection-only constructor, for engine-level code that needs a
   * `TypedActivity` without the callable.
   *
   * @internal
   */
  makeTypedActivity,
};
