/**
 * Classification of errors thrown by Temporal client calls, bound to the
 * client module's error-class identities. The sandbox half binds its own
 * instance over `@temporalio/workflow`'s classes.
 *
 * @since 0.1.0
 */

import { ApplicationFailure, CancelledFailure } from "@temporalio/client";
import { makeClassifyThrown, type ThrownClassification } from "./wire.js";

export type {
  /**
   * What a thrown Temporal error means for the Effect channels — re-exported
   * from `wire` for client-side consumers.
   *
   * @since 0.1.0
   * @category models
   */
  ThrownClassification,
};

/**
 * Classify an error thrown by a `@temporalio/client` call: cancellation, a
 * wire-encoded typed exit riding an `ApplicationFailure`, or anything else.
 *
 * @since 0.1.0
 * @category errors
 */
export const classifyThrown = makeClassifyThrown({ ApplicationFailure, CancelledFailure });
