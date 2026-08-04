// A long-running job for the cluster demo: N steps of "work", a progress
// tick per step, and a durable checkpoint every few steps — the shape any
// real sync/import/AI-workflow job has. `defineJob` supplies the rest:
// state machine, idempotent start, cancel, crash-resume, watch().
import { defineJob } from '@sigx/actors/job';

export interface CrunchInput {
    steps: number;
    stepMs: number;
}
export interface CrunchCheckpoint {
    next: number;
    sum: number;
}

export interface CrunchResult {
    sum: number;
    attempts: number;
}

export const CrunchJob = defineJob<CrunchInput, CrunchResult, CrunchCheckpoint>({
    type: 'Crunch',
    // Demo-public, like the Counter. Real jobs declare `use: [...]`.
    allowAnonymous: true,
    maxAttempts: 5,
    run: async (job, input: CrunchInput) => {
        // Resume from the last checkpoint — attempt 1 starts at 0.
        let { next, sum } = job.resumedFrom ?? { next: 0, sum: 0 };
        for (let i = next; i < input.steps; i++) {
            job.signal.throwIfAborted();
            await new Promise((r) => setTimeout(r, input.stepMs)); // the "work"
            sum += i;
            await job.progress({ done: i + 1, total: input.steps });
            if ((i + 1) % 3 === 0) await job.checkpoint({ next: i + 1, sum });
        }
        return { sum, attempts: job.attempt };
    }
});
