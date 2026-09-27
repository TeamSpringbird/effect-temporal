import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import { describe, expect, it } from "vitest";
import { handle, implementActivities, type ActivityRunner } from "../activities.js";
import { defineActivity } from "../definition.js";

const runner: ActivityRunner<never> = {
  run: (_name, _payload, effect) => Effect.runPromiseExit(effect),
};

const CreateEvaluation = defineActivity("createEvaluation", {
  payload: { evaluationId: Schema.String },
  success: Schema.String,
});

const ReminderWithDuplicateName = defineActivity("createEvaluation", {
  payload: { reviewerId: Schema.String },
  success: Schema.String,
});

const ReminderWithUniqueName = defineActivity("sendReviewerReminder", {
  payload: { reviewerId: Schema.String },
  success: Schema.String,
});

const createEvaluationHandler = handle(
  CreateEvaluation,
  ({ evaluationId }) => Effect.succeed(evaluationId),
);

const duplicateReminderHandler = handle(
  ReminderWithDuplicateName,
  ({ reviewerId }) => Effect.succeed(reviewerId),
);

const uniqueReminderHandler = handle(
  ReminderWithUniqueName,
  ({ reviewerId }) => Effect.succeed(reviewerId),
);

describe("implementActivities", () => {
  it("rejects two handlers with the same activity name", () => {
    expect(() =>
      implementActivities(runner, [
        createEvaluationHandler,
        duplicateReminderHandler,
      ]),
    ).toThrow(
      /^implementActivities: duplicate activity name "createEvaluation"$/,
    );
  });

  it("registers handlers with different activity names", () => {
    const activities = implementActivities(runner, [
      createEvaluationHandler,
      uniqueReminderHandler,
    ]);

    expect(Object.keys(activities)).toEqual([
      "createEvaluation",
      "sendReviewerReminder",
    ]);
  });
});
