// Package-local scaffolding for tests that run real workflows against a
// Temporal test server — time-skipping by default, or a local dev server for
// features the time-skipping server lacks (Nexus). Each test names its
// fixture entrypoint; there is no default workflow bundle.

import { randomUUID } from "node:crypto";
import { afterAll, beforeAll } from "vitest";
import type { DataConverter } from "@temporalio/client";
import { TestWorkflowEnvironment } from "@temporalio/testing";
import { Worker, type WorkerOptions } from "@temporalio/worker";

// `never[]` parameters make this the top type for functions, so typed
// activity implementations (e.g. the package's attach bridge) assign without
// casts.
export type TestActivity = (...args: never[]) => unknown;
export type TestActivities = Record<string, TestActivity>;

export interface WorkerRunOptions {
  readonly workflowsPath: string;
  readonly activities: TestActivities;
  readonly dataConverter?: DataConverter;
  readonly nexusServices?: WorkerOptions["nexusServices"];
  /** Fixed task queue, for tests that must know it before the worker exists
   * (e.g. to point a Nexus endpoint at it). Random by default. */
  readonly taskQueue?: string;
}

export interface TestEnvOptions {
  /** `local` runs the dev server instead of the time-skipping server —
   * required for Nexus, at the cost of real-time timers. */
  readonly mode?: "timeSkipping" | "local";
  readonly serverArgs?: readonly string[];
}

/**
 * Boots the test server for the calling file and hands back the handles a
 * workflow test needs. Call it at module scope: it registers its own
 * `beforeAll`/`afterAll`.
 */
export function createWorkflowTestEnv(queuePrefix: string, envOptions: TestEnvOptions = {}) {
  let env: TestWorkflowEnvironment;

  beforeAll(async () => {
    env =
      envOptions.mode === "local"
        ? await TestWorkflowEnvironment.createLocal({
            server: { extraArgs: [...(envOptions.serverArgs ?? [])] },
          })
        : await TestWorkflowEnvironment.createTimeSkipping();
  }, 120_000);

  afterAll(async () => {
    await env?.teardown();
  });

  /** Run `body` against a worker that lives only as long as the body does. */
  const withWorker = async <A>(
    options: WorkerRunOptions,
    body: (taskQueue: string) => Promise<A>,
  ) => {
    const taskQueue = options.taskQueue ?? `${queuePrefix}-${randomUUID()}`;
    const worker = await Worker.create({
      connection: env.nativeConnection,
      ...(env.namespace === undefined ? {} : { namespace: env.namespace }),
      taskQueue,
      workflowsPath: options.workflowsPath,
      activities: options.activities,
      ...(options.dataConverter ? { dataConverter: options.dataConverter } : {}),
      ...(options.nexusServices ? { nexusServices: options.nexusServices } : {}),
    });
    return worker.runUntil(() => body(taskQueue));
  };

  return {
    /** The running test server, for cases that need its clock or a handle. */
    get env() {
      return env;
    },
    withWorker,
  };
}
