/**
 * The activity workers — where a task node's work actually happens.
 *
 * Two `defineWorker` pools, one per kind of work a real activity does:
 * `WfCompute` burns CPU for the requested duration (a sha256 chain that
 * checks the clock, so a 20 ms task is 20 ms of THIS core), `WfIo` awaits a
 * timer for it (holds a pool slot and no CPU — a downstream call). A run
 * dispatches to them with `ctx.actor(...)` on a fixed key: workers are
 * always local and a key has up to `maxLocal` concurrent members, so the
 * key is a task queue name rather than a placement decision.
 *
 * Failure is DETERMINISTIC per (run, node, attempt): hashed from the seed,
 * so a scenario at `failureRate=0.1` fails the same attempts on every run
 * and a retry can succeed. A worker never retries — that is the run's
 * decision, recorded in its state.
 */
import { createHash } from 'node:crypto';
import { defineWorker } from '../actors.app.ts';
import { config } from './config.ts';
import type { WorkerKind } from './types.ts';

export interface TaskCall {
    /** `${runId}:${nodeId}:${attempt}` — the failure seed. */
    seed: string;
    ms: number;
    failureRate: number;
}

export interface TaskResult {
    /** Wall time the worker actually spent, ms. */
    ms: number;
    digest: string;
}

/** FNV-1a over the seed, mapped to [0, 1). Exported so a test can pick a
 *  run id whose attempts fail and succeed in a chosen order. */
export function roll(seed: string): number {
    let h = 0x811c9dc5;
    for (let i = 0; i < seed.length; i++) {
        h ^= seed.charCodeAt(i);
        h = Math.imul(h, 0x01000193);
    }
    return (h >>> 0) / 4294967296;
}

export class TaskFailedError extends Error {
    constructor(seed: string) {
        super(`task failed (injected) ${seed}`);
        this.name = 'TaskFailedError';
    }
}

function failIfRolled(call: TaskCall): void {
    if (call.failureRate > 0 && roll(call.seed) < call.failureRate) {
        throw new TaskFailedError(call.seed);
    }
}

/** One slice of hashing before the loop is yielded to. */
const SLICE_MS = 2;

/**
 * Hash until `ms` of CPU has been spent, yielding to the event loop every
 * `SLICE_MS`. The yield is what keeps an overloaded host answering its
 * health probes and its peers' calls between tasks: without it a rung past
 * the CPU knee starved the loop for seconds at a time, liveness failed, and
 * the restart moved every activation on that host. The CPU cost is the
 * same; only its fairness changes.
 */
async function burn(ms: number, seed: string): Promise<string> {
    const started = performance.now();
    let digest = createHash('sha256').update(seed).digest();
    let spent = 0;
    while (spent < ms) {
        const sliceStart = performance.now();
        do {
            for (let i = 0; i < 64; i++) digest = createHash('sha256').update(digest).digest();
        } while (performance.now() - sliceStart < SLICE_MS && spent + (performance.now() - sliceStart) < ms);
        spent += performance.now() - sliceStart;
        if (spent < ms) await new Promise((resolve) => setImmediate(resolve));
    }
    void started;
    return digest.toString('hex').slice(0, 16);
}

export const ComputeWorker = defineWorker({
    type: 'WfCompute',
    ...(config.computeMaxLocal !== undefined ? { maxLocal: config.computeMaxLocal } : {}),
    methods: () => ({
        async run(call: TaskCall): Promise<TaskResult> {
            const started = performance.now();
            const digest = await burn(call.ms, call.seed);
            failIfRolled(call);
            return { ms: Math.round(performance.now() - started), digest };
        }
    })
});

export const IoWorker = defineWorker({
    type: 'WfIo',
    ...(config.ioMaxLocal !== undefined ? { maxLocal: config.ioMaxLocal } : {}),
    methods: () => ({
        async run(call: TaskCall): Promise<TaskResult> {
            const started = performance.now();
            await new Promise((resolve) => setTimeout(resolve, call.ms));
            failIfRolled(call);
            return { ms: Math.round(performance.now() - started), digest: '' };
        }
    })
});

export const workerFor = (kind: WorkerKind) => (kind === 'compute' ? ComputeWorker : IoWorker);

/** One pool per kind per host — the task-queue name. */
export const WORKER_KEY = 'default';
