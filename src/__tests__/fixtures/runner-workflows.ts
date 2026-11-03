import { ActivityCancellationType, proxyActivities } from "@temporalio/workflow";

const activities = proxyActivities<{ waitForCancellation(payload: null): Promise<void> }>({
  startToCloseTimeout: "1 minute",
  heartbeatTimeout: "5 seconds",
  cancellationType: ActivityCancellationType.WAIT_CANCELLATION_COMPLETED,
});

export async function runnerCancellation(): Promise<void> {
  await activities.waitForCancellation(null);
}
