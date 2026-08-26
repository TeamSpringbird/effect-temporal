/**
 * The workflow sandbox exposes none of `crypto`, `TextEncoder` or
 * `performance`, all three of which effect reaches for in-sandbox: starting a
 * child workflow computes the child's execution id via `makeHashDigest` →
 * `crypto.subtle.digest`, and the nanosecond clock reads `performance.now()`.
 * Every polyfill here is a pure function of its input or of the SDK's own
 * replay-safe `Date.now()`; the hashing pair is pinned byte-for-byte against
 * node's implementations by sandbox-polyfills.test.ts so in-sandbox child ids
 * always equal `MyChild.executionId(payload)` outside.
 *
 * @since 0.1.0
 */

import { sha256 } from "@noble/hashes/sha2.js";

/** Lone surrogates → U+FFFD, matching `TextEncoder`'s WTF-8 handling:
 * `encodeURIComponent` THROWS on unpaired surrogates where the real encoder
 * substitutes, and the two sides of a child-id digest must agree even on
 * ill-formed input (a truncated emoji in an idempotency key). */
const toWellFormed = (input: string): string => {
  // Fast path: well-formed strings (the overwhelming case) pass through.
  if (!/[\uD800-\uDFFF]/.test(input)) return input;
  let out = "";
  for (let i = 0; i < input.length; i++) {
    const code = input.charCodeAt(i);
    if (code >= 0xd800 && code <= 0xdbff) {
      const next = i + 1 < input.length ? input.charCodeAt(i + 1) : 0;
      if (next >= 0xdc00 && next <= 0xdfff) {
        out += input[i]! + input[i + 1]!;
        i++;
        continue;
      }
      out += "�";
    } else if (code >= 0xdc00 && code <= 0xdfff) {
      out += "�";
    } else {
      out += input[i]!;
    }
  }
  return out;
};

/**
 * UTF-8 encoding without `TextEncoder` or `Buffer`, composed from the two
 * encoding built-ins ECMAScript itself guarantees: `encodeURIComponent`
 * percent-encodes to UTF-8 bytes, `unescape` (Annex B, spec-frozen)
 * collapses them to one char per byte.
 *
 * @since 0.1.0
 * @category polyfills
 */
export const utf8Encode = (input: string): Uint8Array =>
  Uint8Array.from(unescape(encodeURIComponent(toWellFormed(input))), (char) => char.charCodeAt(0));

/**
 * Install the polyfills onto the current `globalThis`. Called at the start
 * of every run, not at module load: under `reuseV8Context` the module
 * evaluates once per context while `globalThis` is swapped per workflow
 * instance.
 *
 * @since 0.1.0
 * @category polyfills
 */
export const ensureSandboxPolyfills = (): void => {
  // SAFETY: a structural view of the globals this module assigns — the
  // package compiles under `"types": []` with no DOM or node globals in
  // scope, so the shape must be spelled out here.
  const globals = globalThis as {
    crypto?: { subtle?: { digest?: unknown } };
    TextEncoder?: unknown;
    performance?: { now: () => number; timeOrigin: number };
  };

  // Effect's nanosecond clock — `Clock.currentTimeNanos`, and the timing on
  // every span, including the one `Workflow.execute` wraps a child start in —
  // reads `performance.now()` whenever `process.hrtime` is absent, as it is
  // here. Assigned rather than guarded: the bundle's OTel interceptor modules
  // leave a real `performance` behind on the first instance a V8 context
  // serves and none on the rest, so effect binds a wall clock that every later
  // instance then fails to resolve at all. `now()` returning `Date.now()`
  // collapses the origin effect caches from the pair (once per context, on
  // whichever instance reads the clock first) to zero, keeping every instance
  // that shares that cache on one clock. `timeOrigin: 0` is not read by
  // effect; it keeps the Performance-API invariant `timeOrigin + now()` =
  // wall clock for any other in-sandbox consumer.
  globals.performance = { now: () => Date.now(), timeOrigin: 0 };

  if (globals.crypto?.subtle?.digest === undefined) {
    globals.crypto = {
      subtle: {
        digest: (algorithm: string, data: ArrayBuffer | Uint8Array): Promise<ArrayBuffer> => {
          if (algorithm !== "SHA-256") {
            return Promise.reject(
              new Error(
                `effect-workflow sandbox crypto polyfill: unsupported algorithm ${algorithm}`,
              ),
            );
          }
          const bytes = data instanceof Uint8Array ? data : new Uint8Array(data);
          const digest = sha256(bytes);
          const out = new ArrayBuffer(digest.byteLength);
          new Uint8Array(out).set(digest);
          return Promise.resolve(out);
        },
      },
    };
  }
  if (globals.TextEncoder === undefined) {
    globals.TextEncoder = class {
      encode(input = ""): Uint8Array {
        return utf8Encode(input);
      }
    };
  }
};

// In the sandbox (`process` is absent there), also install at module eval:
// effect caches its clock origin on the first read, so a read during bundle
// evaluation — before any run — would otherwise lock the origin to whatever
// `performance` an interceptor module left behind. Outside the sandbox this
// module is a plain library import and must not touch node's real globals.
// Read off `globalThis` rather than as a bare identifier: consumers of this
// package compile under `"types": []` with no node typings in scope, where
// `process` alone is an unresolvable name.
if ((globalThis as { process?: unknown }).process === undefined) ensureSandboxPolyfills();
