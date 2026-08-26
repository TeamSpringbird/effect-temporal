// Pins the sandbox polyfills against the real implementations. If these ever
// disagree, a parent workflow would derive a DIFFERENT child execution id
// than `MyChild.executionId(payload)` reports outside — silently breaking
// child idempotency and addressing.

import { createHash } from "node:crypto";
import { sha256 } from "@noble/hashes/sha2.js";
import { describe, expect, it, vi } from "vitest";
import { ensureSandboxPolyfills, utf8Encode } from "../sandbox-polyfills.js";

const reference = (bytes: Uint8Array): string => createHash("sha256").update(bytes).digest("hex");

const hex = (bytes: Uint8Array): string =>
  Array.from(bytes, (byte) => byte.toString(16).padStart(2, "0")).join("");

describe("sandbox polyfills", () => {
  it("noble sha256 matches node:crypto across block-boundary lengths", () => {
    // 55/56/64 straddle the padding boundaries where SHA-256 bugs live.
    for (const length of [0, 1, 3, 31, 55, 56, 57, 63, 64, 65, 127, 128, 1000]) {
      const bytes = new Uint8Array(length).map((_, i) => (i * 37 + length) % 256);
      expect(hex(sha256(bytes)), `length ${length}`).toBe(reference(bytes));
    }
  });

  it("utf8Encode matches TextEncoder, and digests agree end to end", () => {
    const samples = [
      "",
      "effectShimDemo-req-1",
      "ascii only",
      "umlauts äöü",
      "emoji 🌍🚀 and CJK 漢字",
      "surrogate pair 𝄞",
    ];
    for (const sample of samples) {
      const encoded = utf8Encode(sample);
      expect(hex(encoded), sample).toBe(hex(new TextEncoder().encode(sample)));
      expect(hex(sha256(encoded)), sample).toBe(reference(new TextEncoder().encode(sample)));
    }
  });

  it("puts `performance` on the sandbox clock, at a zero origin", () => {
    const realPerformance = globalThis.performance;
    // The sandbox freezes `Date.now()` for the length of a workflow task.
    const now = vi.spyOn(Date, "now").mockReturnValue(1_760_000_000_000);
    try {
      ensureSandboxPolyfills();

      // Effect's clock-origin formula, pinned at zero — the property the
      // polyfill's comment explains.
      const origin =
        BigInt(Date.now()) * 1_000_000n - BigInt(Math.round(performance.now() * 1_000_000));
      expect(origin).toBe(0n);
    } finally {
      now.mockRestore();
      globalThis.performance = realPerformance;
    }
  });
});
