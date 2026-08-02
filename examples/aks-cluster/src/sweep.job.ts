// The long-running worker under test: a durable job that walks N steps,
// checkpointing every one (a Redis CAS each) — so a crash mid-run must
// resume from the last completed step on whichever host picks the actor
// up, and observed progress may regress at most one step per resume.
// Scenario (k) of the runbook kills hosts under a fleet of these.
import { defineJob } from '@sigx/actors/job';

export interface SweepInput {
    steps: number;
    stepMs: number;
}

export interface SweepResult {
    steps: number;
    attempts: number;
}

/** The durable resume point: the next step to run from. */
interface SweepCheckpoint {
    step: number;
}

export const SweepJob = defineJob<SweepInput, SweepResult, SweepCheckpoint>({
    type: 'SweepJob',
    // Public on purpose, like the Counter: the load generator drives it bare.
    unguarded: true,
    // Generous: scenario (k) kills hosts repeatedly and every kill that
    // lands on a running sweep costs an attempt.
    maxAttempts: 10,
    // Keep terminal records long enough for the verify pass, then forget.
    retainMs: 3_600_000,
    run: async (job, input) => {
        const from = job.resumedFrom?.step ?? 0;
        for (let step = from; step < input.steps; step++) {
            job.signal.throwIfAborted();
            await new Promise((resolve) => setTimeout(resolve, input.stepMs));
            await job.checkpoint({ step: step + 1 });
            await job.progress({ done: step + 1, total: input.steps });
        }
        // attempts > 1 in the result is the proof a crash-resume happened.
        return { steps: input.steps, attempts: job.attempt };
    }
});
