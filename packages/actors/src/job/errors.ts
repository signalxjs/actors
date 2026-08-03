/**
 * Job-layer errors. Plain Errors on purpose: in-process callers get them
 * as-is; over the wire they follow the endpoint's normal masking rules
 * (the STATUS is what a remote client should branch on — `status()` tells
 * the whole story without throwing).
 */
import type { JobStatus } from './types';

/** An operation that needs a different job state (`resume` on a job that
 *  is not paused, `discard` on one that is not terminal). */
export class JobStateError extends Error {
    constructor(message: string) {
        super(`[sigx actors] ${message}`);
        this.name = 'JobStateError';
    }
}

/** `result()` on a job that has not completed (and not failed/cancelled). */
export class JobNotDoneError extends Error {
    readonly status: JobStatus;
    constructor(status: JobStatus) {
        super(`[sigx actors] job result() while status is "${status}" — poll status() first.`);
        this.name = 'JobNotDoneError';
        this.status = status;
    }
}

/** `result()` on a failed job — carries the run's failure message. */
export class JobFailedError extends Error {
    constructor(message: string | undefined) {
        super(`[sigx actors] job failed: ${message ?? 'unknown error'}`);
        this.name = 'JobFailedError';
    }
}

/** `result()` on a cancelled job. */
export class JobCancelledError extends Error {
    constructor() {
        super('[sigx actors] job was cancelled.');
        this.name = 'JobCancelledError';
    }
}
