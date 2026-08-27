// PROTOTYPE — throwaway. The data half of versioning: can a declaration's
// schema EVOLVE (add/change fields) while old runs are in flight?
//
// Every boundary in the engine is schema-encoded JSON, and every decode
// happens deterministically on replay — so the whole problem reduces to:
// the CURRENT schema must decode the wire that OLD code wrote. `evolved`
// makes that a declaration-level concern: newest schema first, legacy
// schemas behind pure migrations, one Type coming out — so handler types
// only ever see the newest shape.

import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as SchemaGetter from "effect/SchemaGetter";
import { describe, expect, expectTypeOf, it } from "vitest";
import { wireValueCodec } from "../../wire.js";

/** Newest-first schema evolution: decode tries `current`, then each legacy
 * schema migrated forward by a PURE function (pure = deterministic on
 * replay). Encoding always writes the newest shape. */
const evolved = <Current extends Schema.Top, Legacy extends Schema.Top>(
  current: Current,
  legacy: Legacy,
  migrate: (value: Legacy["Type"]) => Current["Type"],
) =>
  Schema.Union([
    current,
    legacy.pipe(
      Schema.decodeTo(current, {
        decode: SchemaGetter.transform(migrate),
        encode: SchemaGetter.forbidden(() => "legacy shapes are never written"),
      }),
    ),
  ]);

// V1 shipped without `priority`; V2 adds it. In-flight runs hold V1 wire in
// their histories (start events, activity results, buffered signals).
const OrderV1 = Schema.Struct({ orderId: Schema.String });
const OrderV2 = Schema.Struct({ orderId: Schema.String, priority: Schema.Finite });
const OrderPayload = evolved(OrderV2, OrderV1, (v1) => ({ ...v1, priority: 0 }));

describe("prototype: schema evolution across in-flight versions", () => {
  it("decodes V1 wire (old histories) and V2 wire to ONE newest Type", () => {
    const codec = wireValueCodec(OrderPayload);

    // What old code wrote into history before the deploy:
    const v1Wire = wireValueCodec(OrderV1).encode({ orderId: "a" });
    expect(codec.decode(v1Wire)).toEqual({ orderId: "a", priority: 0 });

    // What new code writes and reads:
    const v2Wire = codec.encode({ orderId: "b", priority: 3 });
    expect(codec.decode(v2Wire)).toEqual({ orderId: "b", priority: 3 });

    // Encoding never produces the legacy shape.
    expect(v2Wire).toEqual({ orderId: "b", priority: 3 });

    // The handler-facing Type is ONLY the newest shape.
    expectTypeOf<(typeof OrderPayload)["Type"]>().toEqualTypeOf<
      { readonly orderId: string; readonly priority: number }
    >();
  });

  it("rejects wire that matches NO version, instead of guessing", () => {
    const codec = wireValueCodec(OrderPayload);
    expect(() => codec.decode({ nonsense: true })).toThrow();
  });

  it("migrations may not be effectful by accident", async () => {
    // A migration is a plain function — if someone smuggles a failing
    // transform in, decode fails loudly rather than silently corrupting.
    const Bad = evolved(OrderV2, OrderV1, () => {
      throw new Error("impure migration");
    });
    const v1Wire = wireValueCodec(OrderV1).encode({ orderId: "x" });
    expect(() => wireValueCodec(Bad).decode(v1Wire)).toThrow();
    await Effect.runPromise(Effect.void); // keep vitest's async shape happy
  });
});
