# Versioning

Changing workflow code while executions are in flight is the sharpest knife in any durable-execution system: Temporal **replays** a workflow's history through your current code, and code that would produce different commands than the history records fails the replay — or worse, silently corrupts it.

effect-temporal wraps Temporal's patch markers in a composable, Effect-shaped form: a **version chain** per code site.

```ts
import * as Versioning from "@springbird/effect-temporal/versioning";

const result = yield* Versioning.match("pricing", [
  { version: "v1", run: originalPricing },
  { version: "v2", run: revisedPricing },
]);
```

- the **first case is the original behavior**, guarded by no marker — histories from before the site adopted versioning replay through it;
- each later case is guarded by its own patch marker (`pricing-v2`, …);
- **fresh executions take the newest case** and record only its marker;
- **replays take the case their history's marker selects**;
- result, error, and service channels are unioned across cases.

Evolving a site is appending a case. Adopting `match` on an existing workflow is safe.

## The lifecycle of a case

1. **Append** `{ version: "v3", run: newBehavior }`. Deploy. Fresh runs take v3; in-flight runs keep replaying their recorded case.
2. **Retire** an old case only after every history carrying its marker has closed: remove it from the chain and deploy `deprecateVersion(site, name)` in its place for one release. Replaying a *removed* version's history fails loudly rather than silently running the wrong code.

The primitives underneath are exported too: `Versioning.version(site, names)` returns the selected name as a literal for branching by hand, `patched(id)` is the raw boolean guard, and `deprecatePatch(id)` is Temporal's phase-two marker.

## Rules

**Evaluate a site's version at a deterministic point on the main workflow fiber** — never inside `Effect.fork*`, `Effect.race*`, or `Effect.all` branches. Marker order is part of history; racing fibers make it nondeterministic. The [lint rule](/guide/lint-rules) `versioning-on-main-fiber` catches this shape.

**A site re-evaluated in a later workflow task of the same run may advance** to a newer version (Temporal semantics). Evaluate once, keep the result, where consistency across the run matters:

```ts
const pricingVersion = yield* Versioning.version("pricing", ["v1", "v2"]);
// ... branch on pricingVersion wherever needed, without re-evaluating
```

**Version discipline applies even where replay passes.** Temporal's replay check compares command kind and activity type, not arguments — an argument-only change replays "clean" against old histories while silently corrupting them. If a change alters what an activity is asked to do, it needs a version case even though the replayer won't complain.
