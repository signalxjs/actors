/**
 * Process-wide engine counters — the MECHANISM half of a workflow
 * measurement, reported per host through `ops()` (`server.mjs` registers
 * them as the `workflow` section, exactly like the socket counters) and
 * summed across pods by `wf-load.mjs`.
 *
 * The loadgen sees OUTCOMES (runs completed, latency); these say what the
 * runtime did to get there: how many wakes rode a volatile timer versus a
 * durable reminder, how often arming a reminder lost its CAS, how many
 * wakes were lost outright and recovered by a touch. Every one of them is
 * a monotonic count, so a before/after delta over a run is meaningful.
 */
export const workflowCounters = {
    runsStarted: 0,
    runsFinished: 0,
    transitions: 0,
    saves: 0,
    defReads: 0,
    defCacheHits: 0,
    taskAttempts: 0,
    taskFailures: 0,
    timersArmed: 0,
    timersRearmed: 0,
    timersFired: 0,
    remindersSet: 0,
    reminderSetFailures: 0,
    remindersFired: 0,
    wakesLost: 0,
    wakesStale: 0,
    signalsDelivered: 0,
    signalsBuffered: 0,
    signalsLate: 0,
    signalTimeouts: 0,
    childStarts: 0,
    childStartFailures: 0,
    childDoneCalls: 0,
    childDoneDuplicates: 0,
    childDoneRetries: 0,
    joinChecks: 0,
    joinRepairs: 0,
    compensations: 0,
    publishes: 0,
    publishFailures: 0,
    /** Events the aggregator accepted. */
    statsEvents: 0
};

export type WorkflowCounters = typeof workflowCounters;

export function snapshotCounters(): WorkflowCounters {
    return { ...workflowCounters };
}

/** Tests reset between cases; production never calls this. */
export function resetCounters(): void {
    for (const key of Object.keys(workflowCounters) as (keyof WorkflowCounters)[]) {
        workflowCounters[key] = 0;
    }
}
