// The lint plugin, consumed the way users consume it — real oxlint
// extending the shipped `recommended` preset — against fixture files: every
// rule must fire on the violation written for it, and a clean workflow
// module must produce no findings from this plugin.

import { spawnSync } from "node:child_process";
import { mkdtempSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";

const presetPath = fileURLToPath(new URL("../../oxlint-presets/recommended.json", import.meta.url));

const BAD = `
import * as Effect from "effect/Effect";
import { proxyActivities } from "@temporalio/workflow";
import { callRawActivity } from "@springbird/effect-temporal/engine-sandbox";
import { makeTemporalClientEngine } from "@springbird/effect-temporal/engine-client";
import * as Versioning from "@springbird/effect-temporal/versioning";
import { version } from "@springbird/effect-temporal/definition";

let counter = 0;
const acts = proxyActivities<{ foo(): Promise<string> }>({ startToCloseTimeout: "10 seconds" });

export const a = Effect.promise((signal) => acts.foo());
export const b = Effect.promise(() => acts.foo());
export const c = callRawActivity(() => acts.foo());
export const d = Effect.forkChild(Versioning.patched("x"));
export const e = Effect.forkChild(version("y", ["v1", "v2"]));
`;

const GOOD = `
import { callRawActivity } from "@springbird/effect-temporal/engine-sandbox";
import { version } from "@springbird/effect-temporal/definition";

declare const acts: { foo(): Promise<string> };
export const c = callRawActivity(() => acts.foo());
export const v = version("site", ["v1", "v2"]);
`;

// A definition-authored handler module imports NO engine module — the
// `version` / `versioned` imports alone must mark it for the versioning rule.
const DEFINITION_ONLY = `
import * as Effect from "effect/Effect";
import { version as pickVersion, versioned } from "@springbird/effect-temporal/definition";

export const bad = Effect.forkChild(pickVersion("site", ["v1", "v2"]));
export const badTable = Effect.raceFirst(
  versioned("site", { v1: Effect.succeed(1), v2: Effect.succeed(2) }),
  Effect.succeed(3),
);
export const fine = pickVersion("site", ["v1", "v2"]);
export const fineTable = versioned("site", { v1: Effect.succeed(1), v2: Effect.succeed(2) });
`;

// The removed (0.5.0) authoring surface, every shape the rule must catch:
// named imports of the old engine-sandbox ops, the typed-activity and
// versioning modules (named and namespace), the primitive constructors —
// and `workflowBundle` from `engine-sandbox`, whose home is `bundle`.
const DEPRECATED_IMPORTS = `
import { callActivity, takeMailbox, sleepUntil, workflowBundle, type UpdateRequest } from "@springbird/effect-temporal/engine-sandbox";
import * as TypedActivity from "@springbird/effect-temporal/typed-activity";
import { codecsFor, make as makeActivity } from "@springbird/effect-temporal/typed-activity";
import * as Versioning from "@springbird/effect-temporal/versioning";
import { make as makeMailbox, MAILBOX_SIGNAL } from "@springbird/effect-temporal/mailbox";
import { make as makeUpdate } from "@springbird/effect-temporal/update";
import { make as makeCell } from "../state-cell.js";
export const all = [callActivity, takeMailbox, sleepUntil, workflowBundle, TypedActivity, codecsFor, makeActivity, Versioning, makeMailbox, MAILBOX_SIGNAL, makeUpdate, makeCell];
export type R = UpdateRequest<never, never, never>;
`;

// The current authoring surface — nothing here may be reported.
const MODERN = `
import { workflowBundle } from "@springbird/effect-temporal/bundle";
import { callRawActivity, offerMailbox } from "@springbird/effect-temporal/engine-sandbox";
import { defineActivity, sleep, continueAsNew, executeChild, versioned, type PayloadOf } from "@springbird/effect-temporal/definition";
import { codecsFor, ACTIVITY_EXIT_TYPE } from "@springbird/effect-temporal/wire";
import { MAILBOX_SIGNAL } from "some-other-lib/mailbox";
export const all = [workflowBundle, callRawActivity, offerMailbox, defineActivity, sleep, continueAsNew, executeChild, versioned, codecsFor, ACTIVITY_EXIT_TYPE, MAILBOX_SIGNAL];
export type P = PayloadOf<never>;
`;

const runOxlint = (directory: string, files: string[]) => {
  const config = join(directory, ".oxlintrc.json");
  writeFileSync(config, JSON.stringify({ extends: [presetPath] }));
  const result = spawnSync("oxlint", ["--config", config, ...files], { encoding: "utf8" });
  return `${result.stdout}\n${result.stderr}`;
};

describe("lint plugin", { concurrent: false }, () => {
  it("flags each footgun and passes clean workflow code", () => {
    const directory = mkdtempSync(join(tmpdir(), "effect-workflow-lint-"));
    const bad = join(directory, "bad.ts");
    const good = join(directory, "good.ts");
    const definitionOnly = join(directory, "definition-only.ts");
    writeFileSync(bad, BAD);
    writeFileSync(good, GOOD);
    writeFileSync(definitionOnly, DEFINITION_ONLY);

    const output = runOxlint(directory, [bad, good, definitionOnly]);

    for (const rule of [
      "zero-arity-effect-promise",
      "no-module-level-mutable",
      "no-mixed-halves",
      "prefer-call-temporal-activity",
      "versioning-on-main-fiber",
    ]) {
      expect(output).toContain(`effect-temporal(${rule})`);
    }
    // The clean module produces no findings from this plugin.
    const goodFindings = output
      .split("\n")
      .filter((line) => line.includes("good.ts") && line.includes("effect-temporal("));
    expect(goodFindings).toEqual([]);

    // The definition-only module (no engine imports, aliased `version`) is
    // still covered by the versioning rule — exactly two findings: the
    // forked `version` and the raced `versioned`.
    const definitionOutput = runOxlint(directory, [definitionOnly]);
    expect(definitionOutput).toContain("effect-temporal(versioning-on-main-fiber)");
    expect(definitionOutput.match(/effect-temporal\(/g)).toHaveLength(2);
  }, 60_000);

  it("prefer-definition reports every removed import with its replacement, and nothing modern", () => {
    const directory = mkdtempSync(join(tmpdir(), "effect-workflow-lint-"));
    const deprecated = join(directory, "deprecated.ts");
    const modern = join(directory, "modern.ts");
    writeFileSync(deprecated, DEPRECATED_IMPORTS);
    writeFileSync(modern, MODERN);

    const output = runOxlint(directory, [deprecated]);
    const findings = output.match(/effect-temporal\(prefer-definition\)/g) ?? [];
    // callActivity, takeMailbox, sleepUntil, workflowBundle (→ bundle),
    // UpdateRequest, * as TypedActivity, codecsFor, make (typed-activity),
    // * as Versioning, make (mailbox), MAILBOX_SIGNAL, make (update),
    // make (state-cell)
    expect(findings).toHaveLength(13);
    for (const replacement of [
      "call the declared activity directly",
      "`defineActivity` from `definition`",
      "`codecsFor` from `wire`",
      "`version` / `versioned` from `definition`",
      "`defineMailbox` from `definition`",
      "`defineUpdate` from `definition`",
      "`defineState` from `definition`",
      "`sleepUntil` from `definition`",
      "`UpdateRequest<Payload, Success, Error>` from `definition`",
      "`offer` / `offersTo`",
    ]) {
      expect(output).toContain(replacement);
    }
    // oxlint exits non-zero on an error-level finding: this is what stops a
    // consumer regressing onto the deprecated surface.
    const config = join(directory, ".oxlintrc.json");
    const status = spawnSync("oxlint", ["--config", config, deprecated], { encoding: "utf8" }).status;
    expect(status).not.toBe(0);

    const modernOutput = runOxlint(directory, [modern]);
    const modernFindings = modernOutput
      .split("\n")
      .filter((line) => line.includes("modern.ts") && line.includes("effect-temporal("));
    expect(modernFindings).toEqual([]);
  }, 60_000);
});
