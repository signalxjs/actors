/**
 * Tier-1 `defineJob` fixtures (#227) — the first in the bench suite. The
 * `jobs/*` scenarios exist because #124's remaining serialize cost lives on
 * the job path specifically: a checkpoint is a full durable save of growing
 * state, and every `status()` read clones the whole state (checkpoint and
 * result included) to build an 8-field `JobInfo`.
 *
 * `BenchStatusJob` parks holding a ladder-sized checkpoint: `run()` pauses
 * immediately, so the job idles durably with exactly `rows` rows in
 * `state.checkpoint` and `status()` can be closed-looped against a stable
 * shape. No timers, no clocks — safe under `manualScheduler()`.
 *
 * `BenchStepJob` is the reporter's `WorkflowRun` shape: a run body that
 * appends one step's output per released step and checkpoints after each.
 * The driver gates it through a module-scope command channel (plain
 * deferred queues, no timers) because a job's `run()` is detached — there
 * is no method call that reaches inside it. `sendStep()` resolves when that
 * step's checkpoint save has completed, which is what the scenario times.
 */
import { defineJob } from '@sigx/actors/job';
import { makeRows } from './actors.ts';
import type { LargeState } from './actors.ts';

export const BenchStatusJob = defineJob<number, number, LargeState['rows']>({
    type: 'BenchStatusJob',
    allowAnonymous: true,
    run: async (job, rows) => job.pause(makeRows(rows))
});

type StepRow = { id: number; label: string; output: string; at: Date };
type StepCommand = 'step' | 'stop';

interface StepChannel {
    commands: StepCommand[];
    commandWaiters: ((cmd: StepCommand) => void)[];
    ackWaiters: (() => void)[];
}

/**
 * Keyed by job key. Scenarios mint a fresh key per arm per round and call
 * `resetStepChannel` in their `finally`, so nothing leaks across rounds.
 */
const channels = new Map<string, StepChannel>();

function channel(key: string): StepChannel {
    let ch = channels.get(key);
    if (!ch) {
        ch = { commands: [], commandWaiters: [], ackWaiters: [] };
        channels.set(key, ch);
    }
    return ch;
}

function deliver(key: string, cmd: StepCommand): void {
    const ch = channel(key);
    const waiter = ch.commandWaiters.shift();
    if (waiter) waiter(cmd);
    else ch.commands.push(cmd);
}

function nextCommand(key: string): Promise<StepCommand> {
    const ch = channel(key);
    const queued = ch.commands.shift();
    if (queued !== undefined) return Promise.resolve(queued);
    return new Promise((resolve) => ch.commandWaiters.push(resolve));
}

/** Release one step; resolves once that step's checkpoint turn completed —
 *  the durable save for `BenchStepJob`, the deferred mark for
 *  `BenchEventualStepJob`. */
export function sendStep(key: string): Promise<void> {
    const ch = channel(key);
    const ack = new Promise<void>((resolve) => ch.ackWaiters.push(resolve));
    deliver(key, 'step');
    return ack;
}

/** Let the run body return. Safe to call more than once. */
export function sendStop(key: string): void {
    deliver(key, 'stop');
}

export function resetStepChannel(key: string): void {
    channels.delete(key);
}

function stepJob(type: string, durability: 'immediate' | 'eventual') {
    return defineJob<null, number, StepRow[]>({
        type,
        allowAnonymous: true,
        run: async (job) => {
            const steps: StepRow[] = [];
            for (;;) {
                const cmd = await nextCommand(job.key);
                if (cmd === 'stop') return steps.length;
                const id = steps.length;
                // The date derives from the step index, not a clock: state a
                // benchmark measures must not vary run to run.
                steps.push({
                    id,
                    label: `step-${id}`,
                    output: `output of step ${id}`,
                    at: new Date(1_700_000_000_000 + id)
                });
                await job.checkpoint(steps, { durability });
                channel(job.key).ackWaiters.shift()?.();
            }
        }
    });
}

export const BenchStepJob = stepJob('BenchStepJob', 'immediate');

/**
 * `BenchStepJob` with `{ durability: 'eventual' }` checkpoints (#320): a
 * step marks the state for the write-behind debounce and acks at once, so
 * under `manualScheduler` — where the debounce never fires — the whole run
 * is one save at deactivation and every step is the turn alone. The
 * `watch=0,eventual` arm reads against `watch=0`: the gap is the per-step
 * encode+CAS an eventual checkpoint defers.
 */
export const BenchEventualStepJob = stepJob('BenchEventualStepJob', 'eventual');

/**
 * The lifecycle fixture (#307): a job whose body does nothing, so a run is
 * the runtime's own bookkeeping and nothing else — `start` (state save,
 * task-ledger CAS, liveness reminder), the terminal transition, and the
 * ledger + reminder clears behind it.
 *
 * `onSettled` is the completion signal: it fires inside the terminal turn,
 * after the save that makes the status durable, which is the moment a
 * caller polling `status()` would see `completed`. The ledger and reminder
 * clears run AFTER it, detached — they are part of the run's cost but not
 * of its latency, exactly as in production.
 */
const settleWaiters = new Map<string, () => void>();

/** Resolves when the job under `key` reaches a terminal status. */
export function settledLifecycle(key: string): Promise<void> {
    return new Promise((resolve) => settleWaiters.set(key, resolve));
}

export const BenchLifecycleJob = defineJob<null, number>({
    type: 'BenchLifecycleJob',
    allowAnonymous: true,
    run: async () => 0,
    onSettled: (_control, info) => {
        const waiter = settleWaiters.get(info.key);
        if (waiter) {
            settleWaiters.delete(info.key);
            waiter();
        }
    }
});

/**
 * A job that stays `running` until its host tears it down: the body parks
 * on the run's abort signal and returns once it fires, which the runtime
 * treats as a wind-down (no terminal write, ledger entry kept). Parking N
 * of these puts N entries into the 16 reminder shard tables — the state
 * `jobs/many-running` measures a fresh `start()` against.
 */
export const BenchParkedJob = defineJob<null, number>({
    type: 'BenchParkedJob',
    allowAnonymous: true,
    run: (job) =>
        new Promise<number>((resolve) => {
            if (job.signal.aborted) resolve(0);
            else job.signal.addEventListener('abort', () => resolve(0), { once: true });
        })
});
