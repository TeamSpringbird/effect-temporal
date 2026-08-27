# Lint rules

The workflow sandbox has authoring rules that a linter can see; this package ships them as an ESLint-compatible plugin, loadable by [oxlint](https://oxc.rs/docs/guide/usage/linter)'s `jsPlugins`.

## The authoring rules

The whole Effect program runs inside the Temporal workflow sandbox. `Activity.make` is a typed seam, not a Temporal Activity — durability comes from the Temporal activity proxies, timers, and signals the effects call, each memoized in history. Hence:

1. **All I/O goes through a Temporal activity — a [declared activity](/guide/declaring-capabilities) call (`yield* Charge(payload)`) or `callRawActivity`.** Anything else is nondeterministic on replay. A raw `Effect.promise(() => acts.foo())` works but is not cancelled on interrupt.
2. **`Effect.promise` callbacks must be zero-arity** — non-zero arity makes Effect allocate an `AbortController` per call, which the sandbox does not provide.
3. **No module-level mutable state in workflow code** — under the worker's default `reuseV8Context`, module-level variables are shared across every workflow instance on a thread. Keep run state inside the handler.
4. **Never mix the halves** — a module must not import both the sandbox half (`@temporalio/workflow`, `engine-sandbox`) and the client half (`@temporalio/client`, `engine-client`): they can never share a process.
5. **Evaluate versions on the main fiber** — [version](/guide/versioning) markers evaluated inside forks or races make marker order nondeterministic.

## Setup

Extend a shipped preset:

```jsonc
// .oxlintrc.json
{
  "extends": ["./node_modules/@springbird/effect-temporal/oxlint-presets/recommended.json"]
}
```

Or configure the rules directly:

```jsonc
{
  "jsPlugins": ["@springbird/effect-temporal/lint"],
  "rules": {
    "effect-temporal/zero-arity-effect-promise": "error",
    "effect-temporal/no-module-level-mutable": "error",
    "effect-temporal/no-mixed-halves": "error",
    "effect-temporal/prefer-call-temporal-activity": "warn",
    "effect-temporal/versioning-on-main-fiber": "error"
  }
}
```

Two presets ship: `recommended` (all five rules, `prefer-call-temporal-activity` as a warning) and `correctness` (only the hard-error rules).

## Scope

A file counts as workflow code when it imports `@temporalio/workflow` or the `engine-sandbox` module — the rules are inert elsewhere, so enabling them repo-wide is safe. `no-mixed-halves` applies everywhere by nature. `versioning-on-main-fiber` has one more trigger: importing `version` from the [definition module](/guide/declaring-capabilities) marks the file for that rule (alias-aware), since definition-authored handler modules deliberately import nothing engine-shaped. The other sandbox rules cannot see such modules — a handler that needs them linted can live next to its bundle entry, which imports `engine-sandbox`.

The remaining footguns — drain mailboxes before `continueAsNew`, respond to updates before completion — are runtime-shaped and covered by runtime guards and the guide instead.

| Rule | Catches |
| --- | --- |
| `zero-arity-effect-promise` | `Effect.promise((signal) => ...)` in workflow code |
| `no-module-level-mutable` | module-level `let`/`var` in workflow code |
| `no-mixed-halves` | one module importing both process halves |
| `prefer-call-temporal-activity` | raw `Effect.promise` where a cancellable call belongs |
| `versioning-on-main-fiber` | `Versioning.*` inside `fork` / `race` / `all` |
