/**
 * `@sigx/actors/job` — durable long-running operations on virtual actors.
 *
 * One job = one actor (key = your run id): start it from a request
 * handler and return immediately; poll `status()`, subscribe to
 * `watch()`, `cancel()` it, fetch `result()` — from anywhere in the
 * cluster. Crash-resume, keep-alive and cancellation come from the
 * runtime's `tasks:` primitive; this layer is the state machine and the
 * client surface around it.
 */
export { defineJob } from './define-job';
// `checkpoint(cp, options)` takes the root entry's `SaveOptions`; re-exported
// so a helper typed around `checkpoint` needs only this entry (#320).
export type { SaveOptions } from '../types';
export { JobCancelledError, JobFailedError, JobNotDoneError, JobStateError } from './errors';
export {
    JOB_PAUSED,
    type JobControl,
    type JobHandle,
    type JobInfo,
    type JobMethodTable,
    type JobOptions,
    type JobPaused,
    type JobProgress,
    type JobStatus,
    type JobStreamTable
} from './types';
