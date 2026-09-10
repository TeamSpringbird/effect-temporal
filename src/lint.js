// ESLint-compatible lint rules for this package's workflow-authoring
// footguns, loadable by oxlint (`jsPlugins`) and ESLint alike. The rules are
// syntactic: they key off a file's imports to decide whether it is workflow
// (sandbox) code, client code, or both.
//
//   // .oxlintrc.json
//   { "jsPlugins": ["@springbird/effect-temporal/lint"],
//     "rules": { "effect-temporal/zero-arity-effect-promise": "error", ... } }

const isSandboxSource = (source) =>
  source === "@temporalio/workflow" || source.endsWith("/engine-sandbox");
const isClientSource = (source) =>
  source === "@temporalio/client" || source.endsWith("/engine-client");

const isEffectCall = (node, names) =>
  node.callee.type === "MemberExpression" &&
  node.callee.object.type === "Identifier" &&
  node.callee.object.name === "Effect" &&
  node.callee.property.type === "Identifier" &&
  names.includes(node.callee.property.name);

/** Buffers reports until Program:exit so a file's imports — wherever they
 * sit — decide whether the file is sandbox code. */
const sandboxRule = (visit) => ({
  create(context) {
    const state = { sandbox: false, reports: [] };
    const visitors = visit(context, state);
    return {
      ...visitors,
      ImportDeclaration(node) {
        if (isSandboxSource(node.source.value)) state.sandbox = true;
        visitors.ImportDeclaration?.(node);
      },
      "Program:exit"() {
        if (state.sandbox) for (const report of state.reports) context.report(report);
      },
    };
  },
});

const rules = {
  "zero-arity-effect-promise": {
    meta: {
      type: "problem",
      docs: {
        description:
          "Effect.promise/tryPromise callbacks in workflow code must take no parameters: " +
          "a (signal) parameter makes Effect allocate an AbortController, which the " +
          "Temporal sandbox does not provide.",
      },
      messages: { arity: "This callback must take no parameters inside the workflow sandbox." },
      schema: [],
    },
    ...sandboxRule((_context, state) => ({
      CallExpression(node) {
        if (isEffectCall(node, ["promise"])) {
          const callback = node.arguments[0];
          if (callback?.params?.length > 0) {
            state.reports.push({ node: callback, messageId: "arity" });
          }
        }
        if (isEffectCall(node, ["tryPromise"])) {
          const options = node.arguments[0];
          const tryProperty = options?.properties?.find((property) => property.key?.name === "try");
          if (tryProperty?.value?.params?.length > 0) {
            state.reports.push({ node: tryProperty.value, messageId: "arity" });
          }
        }
      },
    })),
  },

  "no-module-level-mutable": {
    meta: {
      type: "problem",
      docs: {
        description:
          "Module-level mutable state in workflow code is shared across every workflow " +
          "instance on a worker thread (reuseV8Context); keep run state inside the handler.",
      },
      messages: { mutable: "Module-level `{{kind}}` is shared across workflow instances." },
      schema: [],
    },
    ...sandboxRule((_context, state) => ({
      VariableDeclaration(node) {
        if (node.parent?.type === "Program" && (node.kind === "let" || node.kind === "var")) {
          state.reports.push({ node, messageId: "mutable", data: { kind: node.kind } });
        }
      },
    })),
  },

  "no-mixed-halves": {
    meta: {
      type: "problem",
      docs: {
        description:
          "A module must not import both the sandbox half (@temporalio/workflow, " +
          "engine-sandbox) and the client half (@temporalio/client, engine-client): " +
          "they can never share a process.",
      },
      messages: { mixed: "This module imports both the sandbox and client halves." },
      schema: [],
    },
    create(context) {
      let sandbox;
      let client;
      return {
        ImportDeclaration(node) {
          if (isSandboxSource(node.source.value)) sandbox ??= node;
          if (isClientSource(node.source.value)) client ??= node;
        },
        "Program:exit"() {
          if (sandbox !== undefined && client !== undefined) {
            context.report({ node: client, messageId: "mixed" });
          }
        },
      };
    },
  },

  "prefer-call-temporal-activity": {
    meta: {
      type: "suggestion",
      docs: {
        description:
          "In workflow code, prefer a defined activity call (defineActivity) or " +
          "callRawActivity over raw Effect.promise: raw promises are not cancelled " +
          "when the workflow is interrupted.",
      },
      messages: {
        prefer:
          "Prefer a defined activity (or callRawActivity) — this call is not cancelled on interrupt.",
      },
      schema: [],
    },
    ...sandboxRule((_context, state) => {
      let insideActivityCall = 0;
      return {
        CallExpression(node) {
          if (
            node.callee.type === "Identifier" &&
            (node.callee.name === "callRawActivity" || node.callee.name === "callActivity")
          ) {
            insideActivityCall++;
            return;
          }
          if (insideActivityCall === 0 && isEffectCall(node, ["promise", "tryPromise"])) {
            state.reports.push({ node, messageId: "prefer" });
          }
        },
        "CallExpression:exit"(node) {
          if (
            node.callee.type === "Identifier" &&
            (node.callee.name === "callRawActivity" || node.callee.name === "callActivity")
          ) {
            insideActivityCall--;
          }
        },
      };
    }),
  },

  "versioning-on-main-fiber": {
    meta: {
      type: "problem",
      docs: {
        description:
          "Versioning.match/version/patched — and the definition module's version() — must " +
          "run at a deterministic point on the main workflow fiber: inside forks and races, " +
          "marker order becomes nondeterministic.",
      },
      messages: { fiber: "Do not evaluate versions inside a fork or race." },
      schema: [],
    },
    ...sandboxRule((_context, state) => {
      const FORKING = ["forkChild", "forkDetach", "raceFirst", "race", "raceAll", "all"];
      let forkDepth = 0;
      let definedVersionLocal = null;
      return {
        // Definition-authored handler modules import no engine module on
        // purpose — importing `version` from the definition module is what
        // marks the file as workflow code for THIS rule (alias-aware).
        ImportDeclaration(node) {
          const source = node.source.value;
          if (
            typeof source === "string" &&
            (source.endsWith("/definition") || source.endsWith("/definition.js"))
          ) {
            for (const specifier of node.specifiers ?? []) {
              if (specifier.type === "ImportSpecifier" && specifier.imported?.name === "version") {
                definedVersionLocal = specifier.local.name;
                state.sandbox = true;
              }
            }
          }
        },
        CallExpression(node) {
          if (isEffectCall(node, FORKING)) {
            forkDepth++;
            return;
          }
          const isVersioningMember =
            node.callee.type === "MemberExpression" &&
            node.callee.object.type === "Identifier" &&
            node.callee.object.name === "Versioning";
          // The definition module's bare `version(site, names)` call.
          const isDefinedVersion =
            node.callee.type === "Identifier" &&
            node.callee.name === (definedVersionLocal ?? "version");
          if (forkDepth > 0 && (isVersioningMember || isDefinedVersion)) {
            state.reports.push({ node, messageId: "fiber" });
          }
        },
        "CallExpression:exit"(node) {
          if (isEffectCall(node, FORKING)) forkDepth--;
        },
      };
    }),
  },
};

export default {
  meta: { name: "effect-temporal", version: "0.1.0" },
  rules,
};
