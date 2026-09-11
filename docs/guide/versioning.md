# Versioning

Changing workflow code while executions are in flight is the sharpest knife in any durable-execution system: Temporal **replays** a workflow's history through your current code, and code that would produce different commands than the history records fails the replay — or worse, silently corrupts it.

Versioning has two halves. **Logic** changes at a code site use `version`; **data** changes in a declared schema use `evolved`. Both live in the [definition module](/guide/declaring-capabilities).

## Logic: `version`

`version(site, names)` selects one of the named behaviors at a code site, backed by Temporal patch markers:

```ts
import { version } from "@springbird/effect-temporal/definition";

const pricing = yield* version("pricing", ["v1", "v2"]);
const result = pricing === "v2" ? yield* revisedPricing : yield* originalPricing;
```

- the **first name is the original behavior**, guarded by no marker — histories from before the site adopted versioning replay through it;
- each later name is guarded by its own patch marker (`pricing-v2`, …);
- **fresh executions answer the newest name** and record only its marker;
- **replays answer the name their history's marker selects**;
- engines without replay — the [in-memory test runtime](/guide/testing#the-in-memory-runtime) — always answer the newest name.

The result is typed as the literal union of exactly the names given, so a `switch` over it is exhaustive.

Evolving a site is appending a name. Adopting `version` on an existing workflow is safe.

### The run-table form: `versioned`

Branching by hand on the returned name is error-prone at exactly the site where determinism matters. `versioned` takes the behaviors themselves — one effect per name, keyed **oldest first**:

```ts
import { versioned } from "@springbird/effect-temporal/definition";

const result = yield* versioned("pricing", {
  v1: originalPricing,
  v2: revisedPricing,
});
```

The **key order is the chain order**: the first key is the original, unguarded behavior; each later key is guarded by its own marker (`pricing-v2`, …). The selected case runs, and result, error, and service channels are unioned across cases. Same markers, same lifecycle, same replay semantics as `version` — it is `version` plus the lookup. The [lint rule](/guide/lint-rules) `versioning-on-main-fiber` covers both calls.

### The lifecycle of a name

1. **Append** `"v3"`. Deploy. Fresh runs take v3; in-flight runs keep replaying their recorded name.
2. **Retire** an old name only after every history carrying its marker has closed: remove it from the list and deploy `deprecateVersion(site, name)` in its place for one release. Replaying a *removed* version's history fails loudly rather than silently running the wrong code. (`deprecateVersion` lives in the `versioning` module today — the one non-deprecated reason to import it; it moves to `bundle` in 0.5.0.)

::: warning `Versioning.match` is deprecated
The pre-0.4.0 `versioning` module's `match(site, [{ version, run }])` is the Temporal-only ancestor of `versioned` — same markers, same semantics, but it imports `@temporalio/workflow` and so cannot run in the in-memory runtime. It is deprecated and removed in 0.5.0; `versioned(site, { v1: run1, v2: run2 })` is the drop-in replacement, and the `prefer-definition` lint rule points at it.
:::

### Rules

**Evaluate a site's version at a deterministic point on the main workflow fiber** — never inside `Effect.fork*`, `Effect.race*`, or `Effect.all` branches. Marker order is part of history; racing fibers make it nondeterministic. The [lint rule](/guide/lint-rules) `versioning-on-main-fiber` catches this shape.

**A site re-evaluated in a later workflow task of the same run may advance** to a newer version (Temporal semantics). Evaluate once, keep the result, where consistency across the run matters:

```ts
const pricingVersion = yield* version("pricing", ["v1", "v2"]);
// ... branch on pricingVersion wherever needed, without re-evaluating
```

**Version discipline applies even where replay passes.** Temporal's replay check compares command kind and activity type, not arguments — an argument-only change replays "clean" against old histories while silently corrupting them. If a change alters what an activity is asked to do, it needs a version even though the replayer won't complain.

## Schema evolution: `evolved`

The data half: a declaration's schema can evolve — add or change fields — while old runs are in flight. Every boundary is schema-encoded JSON, and every decode happens deterministically on replay, so the whole problem reduces to: the **current schema must decode the wire old code wrote**. `evolved` makes that a declaration-level concern:

```ts
import { evolved } from "@springbird/effect-temporal/definition";

// V1 shipped without `priority`; V2 adds it. In-flight runs hold V1 wire
// in their histories (start events, activity results, buffered signals).
const OrderV1 = Schema.Struct({ orderId: Schema.String });
const OrderV2 = Schema.Struct({ orderId: Schema.String, priority: Schema.Finite });

const OrderPayload = evolved(OrderV2, OrderV1, (v1) => ({ ...v1, priority: 0 }));
```

Use `OrderPayload` anywhere a schema goes — a workflow payload, an activity's `success`, a mailbox's `payload`. The contract:

- decode tries `current` first, then `legacy` migrated forward through the function;
- the migration is a **pure** function — purity is what keeps replay deterministic;
- encoding always writes the newest shape; the legacy shape is never written again;
- handler types only ever see the newest `Type`;
- wire that matches *no* generation fails loudly instead of guessing.

Chain `evolved` calls for further generations: `evolved(V3, evolved(V2, V1, migrate12), migrate23)`.
