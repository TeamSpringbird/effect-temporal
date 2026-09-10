// Custom payload codecs end to end, mirroring the Temporal `encryption`
// sample (see EXAMPLES.md): a byte-level codec installed on both client and
// worker, with every shim surface exercised through it — workflow argument
// and result, update request/response, mailbox-style signals, state-cell
// queries, and a typed failure riding `ApplicationFailure` details.
//
// Non-vacuousness: raw history is read through the env's codec-LESS client
// and asserted to contain the codec's marker encoding — proving payloads on
// the wire were actually transformed, not just that the codec was ignored.

import { fileURLToPath } from "node:url";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Option from "effect/Option";
import * as Result from "effect/Result";
import * as DurableDeferred from "effect/unstable/workflow/DurableDeferred";
import * as WorkflowEngine from "effect/unstable/workflow/WorkflowEngine";
import { Client, type DataConverter } from "@temporalio/client";
import { describe, expect, it } from "vitest";
import { executeUpdate, makeTemporalClientEngine, readStateCell } from "../engine-client.js";
import { ChildDemo, ParentDemo } from "./fixtures/child-demo.js";
import { Approved, CurrentLanguage, MessageDemo, SetLanguage } from "./fixtures/message-demo.js";
import { createWorkflowTestEnv, type TestActivities } from "./utils/workflow-test-env.js";

const messageWorkflowsPath = fileURLToPath(
  new URL("./fixtures/message-workflows.ts", import.meta.url),
);
const childWorkflowsPath = fileURLToPath(new URL("./fixtures/child-workflows.ts", import.meta.url));

const temporal = createWorkflowTestEnv("effect-codec");

const MARKER_ENCODING = "binary/effect-marker";

type PayloadCodec = NonNullable<DataConverter["payloadCodecs"]>[number];
type Payloads = Parameters<PayloadCodec["encode"]>[0];

const utf8 = (value: string) => new TextEncoder().encode(value);
const fromUtf8 = (bytes: Uint8Array) => new TextDecoder().decode(bytes);

const base64 = (bytes: Uint8Array) => Buffer.from(bytes).toString("base64");
const fromBase64 = (text: string) => new Uint8Array(Buffer.from(text, "base64"));

/** Wraps every payload in a marked envelope, counting both directions. */
const makeMarkerCodec = () => {
  const counts = { encoded: 0, decoded: 0 };
  const codec: PayloadCodec = {
    async encode(payloads: Payloads) {
      counts.encoded += payloads.length;
      return payloads.map((payload) => ({
        metadata: { encoding: utf8(MARKER_ENCODING) },
        data: utf8(
          JSON.stringify({
            metadata: Object.fromEntries(
              Object.entries(payload.metadata ?? {}).map(([key, bytes]) => [
                key,
                base64(bytes as Uint8Array),
              ]),
            ),
            data: base64((payload.data as Uint8Array | undefined) ?? new Uint8Array()),
          }),
        ),
      }));
    },
    async decode(payloads: Payloads) {
      counts.decoded += payloads.length;
      return payloads.map((payload) => {
        const encoding = payload.metadata?.["encoding"];
        if (encoding === undefined || fromUtf8(encoding as Uint8Array) !== MARKER_ENCODING) {
          return payload;
        }
        const wrapped = JSON.parse(fromUtf8(payload.data as Uint8Array)) as {
          metadata: Record<string, string>;
          data: string;
        };
        return {
          metadata: Object.fromEntries(
            Object.entries(wrapped.metadata).map(([key, text]) => [key, fromBase64(text)]),
          ),
          data: fromBase64(wrapped.data),
        };
      });
    },
  };
  return { codec, counts };
};

const activities: TestActivities = {
  reserve: async (requestId: unknown) => `reservation:${String(requestId)}`,
  release: async () => "released",
  echo: async (value: unknown) => `echo:${String(value)}`,
};

describe("payload codecs", { concurrent: false }, () => {
  it("runs every shim surface through an installed codec", async () => {
    const { codec, counts } = makeMarkerCodec();
    const dataConverter: DataConverter = { payloadCodecs: [codec] };

    await temporal.withWorker(
      { activities: {}, workflowsPath: messageWorkflowsPath, dataConverter },
      async (taskQueue) => {
        const client = new Client({
          connection: temporal.env.connection,
          ...(temporal.env.namespace === undefined ? {} : { namespace: temporal.env.namespace }),
          dataConverter,
        });
        const engine = makeTemporalClientEngine({ client, taskQueue });
        const run = <A, E>(
          effect: Effect.Effect<A, E, WorkflowEngine.WorkflowEngine>,
        ): Promise<A> =>
          Effect.runPromise(Effect.provideService(effect, WorkflowEngine.WorkflowEngine, engine));

        const payload = { requestId: "codec-1" } as const;
        const executionId = await run(MessageDemo.execute(payload, { discard: true }));

        // Update request/response, typed both ways.
        const previous = await run(
          executeUpdate(SetLanguage.update, {
            client,
            workflowId: executionId,
            payload: { language: "french" },
          }),
        );
        expect(previous).toBe("english");
        const rejected = await Effect.runPromise(
          Effect.result(
            executeUpdate(SetLanguage.update, {
              client,
              workflowId: executionId,
              payload: { language: "klingon" },
            }),
          ),
        );
        expect(Result.isFailure(rejected) && rejected.failure).toBe("unsupported:klingon");

        // State-cell query.
        const snapshot = await Effect.runPromise(
          readStateCell(CurrentLanguage.cell, { client, workflowId: executionId }),
        );
        expect(Option.isSome(snapshot) && snapshot.value).toBe("french");

        // Deferred-done signal + final result.
        await run(
          DurableDeferred.done(Approved.deferred, {
            token: DurableDeferred.tokenFromExecutionId(Approved.deferred, {
              workflow: MessageDemo,
              executionId,
            }),
            exit: Exit.succeed("uri"),
          }),
        );
        expect(await run(MessageDemo.execute(payload))).toBe("approved:french by uri");

        // The wire really was transformed: raw history via the env's
        // codec-less client shows the marker encoding on the start input.
        const events =
          (await temporal.env.client.workflow.getHandle(executionId).fetchHistory()).events ?? [];
        const input = events[0]?.workflowExecutionStartedEventAttributes?.input?.payloads?.[0];
        expect(input?.metadata?.["encoding"]).toBeDefined();
        expect(fromUtf8(input!.metadata!["encoding"] as Uint8Array)).toBe(MARKER_ENCODING);
        expect(counts.encoded).toBeGreaterThan(0);
        expect(counts.decoded).toBeGreaterThan(0);
      },
    );
  }, 120_000);

  it("round-trips a run-level typed failure's details through the codec", async () => {
    const { codec } = makeMarkerCodec();
    const dataConverter: DataConverter = { payloadCodecs: [codec] };

    await temporal.withWorker(
      { activities, workflowsPath: childWorkflowsPath, dataConverter },
      async (taskQueue) => {
        const client = new Client({
          connection: temporal.env.connection,
          ...(temporal.env.namespace === undefined ? {} : { namespace: temporal.env.namespace }),
          dataConverter,
        });
        const engine = makeTemporalClientEngine({ client, taskQueue });
        const run = <A, E>(
          effect: Effect.Effect<A, E, WorkflowEngine.WorkflowEngine>,
        ): Promise<A> =>
          Effect.runPromise(Effect.provideService(effect, WorkflowEngine.WorkflowEngine, engine));

        const result = await run(
          Effect.result(
            ParentDemo.execute({ requestId: "codec-2", childOutcome: "fail", discardChild: false }),
          ),
        );
        expect(Result.isFailure(result) && result.failure).toBe("child-failed:codec-2-child");

        const childId = await run(
          ChildDemo.executionId({ requestId: "codec-2-child", outcome: "fail" }),
        );
        expect((await temporal.env.client.workflow.getHandle(childId).describe()).status.name).toBe(
          "FAILED",
        );
      },
    );
  }, 120_000);
});
