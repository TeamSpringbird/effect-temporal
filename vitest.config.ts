import { defineConfig } from "vitest/config";

// Every test file boots its own Temporal test-server process (time-skipping
// or local). Unbounded fork parallelism has crashed workers under the full
// fan-out; four concurrent servers is comfortably stable and barely slower
// than the free-for-all.
export default defineConfig({
  test: {
    environment: "node",
    include: ["src/**/*.test.ts"],
    // Hang guards, not perf budgets: a first run also downloads the Temporal
    // test-server binary.
    testTimeout: 120_000,
    hookTimeout: 120_000,
    maxWorkers: 4,
  },
});
