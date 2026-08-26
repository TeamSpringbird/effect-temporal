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

let counter = 0;
const acts = proxyActivities<{ foo(): Promise<string> }>({ startToCloseTimeout: "10 seconds" });

export const a = Effect.promise((signal) => acts.foo());
export const b = Effect.promise(() => acts.foo());
export const c = callRawActivity(() => acts.foo());
export const d = Effect.forkChild(Versioning.patched("x"));
`;

const GOOD = `
import { callRawActivity } from "@springbird/effect-temporal/engine-sandbox";

declare const acts: { foo(): Promise<string> };
export const c = callRawActivity(() => acts.foo());
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
    writeFileSync(bad, BAD);
    writeFileSync(good, GOOD);

    const output = runOxlint(directory, [bad, good]);

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
  }, 60_000);
});
