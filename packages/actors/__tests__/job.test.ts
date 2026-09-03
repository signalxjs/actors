import { afterEach, describe, expect, it, vi } from 'vitest';
import {
    defineJob,
    JobCancelledError,
    JobFailedError,
    JobNotDoneError,
    JobStateError,
    type JobInfo
} from '@sigx/actors/job';
import {
    createHost,
    manualScheduler,
    memoryStorage,
    ROSTER_INDEX_KEY,
    ROSTER_TYPE,
    TASKS_TYPE,
    type Host
} from '@sigx/actors/host';
import { reminderShardOf } from '../src/host/reminder-shards';
import { actor } from '@sigx/actors';
import { stubServerApp } from '@sigx/server/testing';

const quiet = { sweepIntervalMs: 600_000, reminderTickMs: 600_000, callTimeoutMs: 0 };

function within<T>(promise: Promise<T>, ms: number): Promise<T> {
    return Promise.race([
        promise,
        new Promise<never>((_, reject) =>
            setTimeout(() => reject(new Error(`did not settle within ${ms}ms`)), ms).unref?.()
        )
    ]);
}

function gate(): { open: () => void; opened: Promise<void> } {
    let open!: () => void;
    const opened = new Promise<void>((r) => (open = r));
    return { open, opened };
}

function aborted(signal: AbortSignal): Promise<void> {
    return signal.aborted
        ? Promise.resolve()
        : new Promise((r) => signal.addEventListener('abort', () => r(), { once: true }));
}

async function until(cond: () => boolean | Promise<boolean>, ms = 3000): Promise<void> {
    for (let waited = 0; !(await cond()); waited += 5) {
        if (waited > ms) throw new Error('condition not reached');
        await new Promise((r) => setTimeout(r, 5));
    }
}

let running: Host | null = null;
afterEach(async () => {
    vi.restoreAllMocks();
    await running?.stop({ timeoutMs: 1000 });
    running = null;
});

describe('defineJob: the principal snapshot (rfc-server-v4 §7)', () => {
    it('records the ENQUEUEING caller and hands it to the detached run', async () => {
        // A job outlives the request that started it: the run body is
        // detached and a crash-resume can happen on another host hours
        // later, with nobody waiting. So authorization happens once at
        // enqueue and the run reads the snapshot rather than re-deciding —
        // `ctx.principal` would be null in a task body by design.
        const seen: unknown[] = [];
        const restore = stubServerApp({
            authenticate: () => ({ id: 'ada' }),
            codec: {
                encode: (u) => (u as { id: string }).id,
                decode: (e) => (e === '' ? null : { id: e })
            }
        });
        try {
            const Attributed = defineJob({
                type: 'Attributed',
                allowAnonymous: true,
                run: async (job) => {
                    seen.push(job.principal);
                    return 'done';
                }
            });
            running = createHost({ actors: [Attributed], storage: memoryStorage(), defaults: quiet });
            await running.start();
            await actor(Attributed, 'r1').start(undefined as never);
            await until(async () => (await actor(Attributed, 'r1').status()).status === 'completed');
            expect(seen).toEqual([{ id: 'ada' }]);
        } finally {
            restore();
        }
    });

    it('is null for an anonymous starter — unattributed, never a default', async () => {
        const seen: unknown[] = [];
        const restore = stubServerApp({ authenticate: () => null });
        try {
            const Anon = defineJob({
                type: 'AnonJob',
                allowAnonymous: true,
                run: async (job) => {
                    seen.push(job.principal);
                    return 'done';
                }
            });
            running = createHost({ actors: [Anon], storage: memoryStorage(), defaults: quiet });
            await running.start();
            await actor(Anon, 'r1').start(undefined as never);
            await until(async () => (await actor(Anon, 'r1').status()).status === 'completed');
            expect(seen).toEqual([null]);
        } finally {
            restore();
        }
    });
});

describe('defineJob: the state machine', () => {
    it('runs start → running (with progress) → completed → result; start is idempotent', async () => {
        const step = gate();
        const SumJob = defineJob({
            type: 'Sum',
            allowAnonymous: true,
            run: async (job, input: { upTo: number }) => {
                let sum = 0;
                for (let i = 1; i <= input.upTo; i++) {
                    sum += i;
                    await job.progress({ done: i, total: input.upTo });
                    if (i === 1) await step.opened; // hold mid-run for asserts
                }
                return { sum };
            }
        });
        const host = createHost({ actors: [SumJob], defaults: quiet });
        running = host;
        const client = host.actor(SumJob, 'run-1');

        const started = await client.start({ upTo: 3 });
        expect(started.status).toBe('running');
        expect(started.attempts).toBe(1);

        // Idempotent under retry: the second start neither restarts nor throws.
        const again = await client.start({ upTo: 999 });
        expect(again.status).toBe('running');

        await until(async () => (await client.status()).progress?.done === 1);
        step.open();
        await until(async () => (await client.status()).status === 'completed');
        const info = await client.status();
        expect(info.progress).toBeNull();
        expect(info.finishedAt).not.toBeNull();
        expect(await client.result()).toEqual({ sum: 6 });
    });

    it('watch() streams the lifecycle live', async () => {
        const JobDef = defineJob({
            type: 'Watched',
            allowAnonymous: true,
            run: async (job, input: number) => {
                await job.progress({ done: 1, total: 2 });
                await job.progress({ done: 2, total: 2 });
                return input * 2;
            }
        });
        const host = createHost({ actors: [JobDef], defaults: quiet });
        running = host;
        const client = host.actor(JobDef, 'run-1');

        const seen: JobInfo[] = [];
        const done = gate();
        void (async () => {
            for await (const info of client.watch()) {
                seen.push(info);
                if (info.status === 'completed') break;
            }
            done.open();
        })();
        await new Promise((r) => setTimeout(r, 20)); // let the body subscribe
        await client.start(21);
        await within(done.opened, 2000);
        expect(seen[0]!.status).toBe('pending'); // initial: true seed
        expect(seen.at(-1)!.status).toBe('completed');
        expect(seen.some((s) => s.progress !== null)).toBe(true);
    });

    it('a throwing run fails the job; result() rethrows as JobFailedError', async () => {
        vi.spyOn(console, 'error').mockImplementation(() => {});
        const BadJob = defineJob({
            type: 'Bad',
            allowAnonymous: true,
            run: async () => {
                throw new Error('provider exploded');
            }
        });
        const host = createHost({ actors: [BadJob], defaults: quiet });
        running = host;
        const client = host.actor(BadJob, 'run-1');
        await client.start(undefined as never);
        await until(async () => (await client.status()).status === 'failed');
        expect((await client.status()).error?.message).toBe('provider exploded');
        await expect(client.result()).rejects.toThrow(JobFailedError);
    });

    it('result() before completion throws JobNotDoneError', async () => {
        const release = gate();
        const SlowJob = defineJob({
            type: 'Slow',
            allowAnonymous: true,
            run: async () => {
                await release.opened;
                return 1;
            }
        });
        const host = createHost({ actors: [SlowJob], defaults: quiet });
        running = host;
        const client = host.actor(SlowJob, 'run-1');
        await expect(client.result()).rejects.toThrow(JobNotDoneError); // pending
        await client.start(undefined as never);
        await expect(client.result()).rejects.toThrow(JobNotDoneError); // running
        release.open();
    });
});

describe('defineJob: cancellation', () => {
    it('cancel marks cancelled immediately, aborts the run, and blocks late writes', async () => {
        const observed: string[] = [];
        const CancelJob = defineJob({
            type: 'Cancellable',
            allowAnonymous: true,
            run: async (job) => {
                await aborted(job.signal);
                observed.push(String(job.signal.reason));
                // A late result must NOT overwrite the cancelled status.
                return 'too late';
            }
        });
        const host = createHost({ actors: [CancelJob], defaults: quiet });
        running = host;
        const client = host.actor(CancelJob, 'run-1');
        await client.start(undefined as never);
        const info = await client.cancel();
        expect(info.status).toBe('cancelled'); // immediate, not eventual
        await until(() => observed.length === 1);
        expect(observed).toEqual(['cancelled']);
        await new Promise((r) => setTimeout(r, 20)); // let the late return race
        expect((await client.status()).status).toBe('cancelled');
        await expect(client.result()).rejects.toThrow(JobCancelledError);
        expect(await client.cancel().then((i) => i.status)).toBe('cancelled'); // no-op
    });
});

describe('defineJob: late writes', () => {
    it('update() and progress() from a winding-down body cannot mutate a cancelled job', async () => {
        const tried = gate();
        const LateJob = defineJob({
            type: 'LateWriter',
            allowAnonymous: true,
            state: () => ({ notes: [] as string[] }),
            run: async (job) => {
                await job.update((extra) => extra.notes.push('before'));
                await aborted(job.signal);
                // Winding down — none of these may land.
                await job.update((extra) => extra.notes.push('after'));
                await job.progress({ done: 99 });
                tried.open();
                return 'late';
            }
        });
        const host = createHost({ actors: [LateJob], defaults: quiet });
        running = host;
        const client = host.actor(LateJob, 'run-1');
        await client.start(undefined as never);
        await until(async () => (await client.status()).extra.notes.length === 1);
        await client.cancel();
        await within(tried.opened, 1000);
        const info = await client.status();
        expect(info.status).toBe('cancelled');
        expect(info.extra.notes).toEqual(['before']);
        expect(info.progress).toBeNull();
    });
});

describe('defineJob: crash-resume', () => {
    it('a host death resumes the run with attempts bumped and the checkpoint delivered', async () => {
        const attempts: { attempt: number; resumedFrom: unknown }[] = [];
        const storage = memoryStorage();
        const ResumeJob = defineJob({
            type: 'Resumable',
            allowAnonymous: true,
            run: async (job, input: { rows: number }) => {
                attempts.push({ attempt: job.attempt, resumedFrom: job.resumedFrom });
                if (job.attempt === 1) {
                    await job.checkpoint({ cursor: 40 });
                    await aborted(job.signal); // park until the host dies
                    return undefined as never; // interrupted — never completes
                }
                return { rows: input.rows, from: (job.resumedFrom as { cursor: number }).cursor };
            }
        });
        const hostA = createHost({ actors: [ResumeJob], storage, defaults: quiet });
        await hostA.actor(ResumeJob, 'run-1').start({ rows: 100 });
        await until(() => attempts.length === 1);
        await within(hostA.stop({ timeoutMs: 5000 }), 2000);

        const hostB = createHost({ actors: [ResumeJob], storage, defaults: quiet });
        running = hostB;
        const client = hostB.actor(ResumeJob, 'run-1');
        await until(async () => (await client.status()).status === 'completed');
        expect(attempts).toHaveLength(2);
        expect(attempts[1]).toEqual({ attempt: 2, resumedFrom: { cursor: 40 } });
        expect((await client.status()).attempts).toBe(2);
        expect(await client.result()).toEqual({ rows: 100, from: 40 });
    });

    it('a crash-looping job fails after maxAttempts', async () => {
        const storage = memoryStorage();
        const LoopJob = defineJob({
            type: 'CrashLoop',
            allowAnonymous: true,
            maxAttempts: 2,
            run: async (job) => {
                await aborted(job.signal); // parks forever, every attempt
                return undefined as never;
            }
        });
        let host = createHost({ actors: [LoopJob], storage, defaults: quiet });
        await host.actor(LoopJob, 'run-1').start(undefined as never);
        // Attempt 1 interrupted, attempt 2 interrupted, attempt 3 refused.
        for (let i = 0; i < 2; i++) {
            await within(host.stop({ timeoutMs: 5000 }), 2000);
            host = createHost({ actors: [LoopJob], storage, defaults: quiet });
            await host.actor(LoopJob, 'run-1').status(); // activate → resume
        }
        running = host;
        const client = host.actor(LoopJob, 'run-1');
        await until(async () => (await client.status()).status === 'failed');
        expect((await client.status()).error?.message).toMatch(/gave up after 2 attempts/);
    });
});

describe('defineJob: pause / resume', () => {
    it('pause parks durably (no task), resume re-runs with data; attempts unchanged', async () => {
        const runs: { attempt: number; resumeData: unknown }[] = [];
        const storage = memoryStorage();
        const Approval = defineJob({
            type: 'Approval',
            allowAnonymous: true,
            run: async (job, input: { doc: string }) => {
                runs.push({ attempt: job.attempt, resumeData: job.resumeData });
                if (job.resumeData === undefined) {
                    return job.pause({ stage: 'awaiting-approval' });
                }
                return { doc: input.doc, approved: job.resumeData };
            }
        });
        const hostA = createHost({ actors: [Approval], storage, defaults: quiet });
        const a = hostA.actor(Approval, 'run-1');
        await a.start({ doc: 'contract' });
        await until(async () => (await a.status()).status === 'paused');
        // Paused = no live task; survives a full host restart with no resume.
        await within(hostA.stop({ timeoutMs: 2000 }), 1500);
        const hostB = createHost({ actors: [Approval], storage, defaults: quiet });
        running = hostB;
        const b = hostB.actor(Approval, 'run-1');
        expect((await b.status()).status).toBe('paused');
        expect(runs).toHaveLength(1);

        await b.resume({ by: 'alice' });
        await until(async () => (await b.status()).status === 'completed');
        expect(runs).toHaveLength(2);
        expect(runs[1]).toEqual({ attempt: 1, resumeData: { by: 'alice' } }); // pause-resume is free
        expect((await b.status()).attempts).toBe(1);
        expect(await b.result()).toEqual({ doc: 'contract', approved: { by: 'alice' } });
    });

    it('resume on a non-paused job throws JobStateError', async () => {
        const NopJob = defineJob({
            type: 'Nop',
            allowAnonymous: true,
            run: async () => 1
        });
        const host = createHost({ actors: [NopJob], defaults: quiet });
        running = host;
        await expect(host.actor(NopJob, 'run-1').resume()).rejects.toThrow(JobStateError);
    });

    it('a paused wait can time out via job.reminders + onReminder control', async () => {
        const storage = memoryStorage();
        const Hitl = defineJob({
            type: 'Hitl',
            allowAnonymous: true,
            run: async (job, input: { question: string }) => {
                if (job.resumeData === undefined) {
                    await job.reminders.set('timeout', { due: 40 });
                    return job.pause({ asked: input.question });
                }
                await job.reminders.clear('timeout');
                return { answer: job.resumeData };
            },
            onReminder: async (control, name) => {
                if (name === 'timeout') await control.resume({ timedOut: true });
            }
        });
        const host = createHost({
            actors: [Hitl],
            storage,
            defaults: { ...quiet, reminderTickMs: 25 }
        });
        running = host;
        await host.start();
        const client = host.actor(Hitl, 'run-1');
        await client.start({ question: 'approve?' });
        await until(async () => (await client.status()).status === 'completed', 5000);
        expect(await client.result()).toEqual({ answer: { timedOut: true } });
    });
});

describe('defineJob: extra state, retention, discard', () => {
    it('job.update() maintains extra fields visible in JobInfo', async () => {
        const EventfulJob = defineJob({
            type: 'Eventful',
            allowAnonymous: true,
            state: () => ({ events: [] as string[] }),
            run: async (job) => {
                await job.update((extra) => extra.events.push('node:a'));
                await job.update((extra) => extra.events.push('node:b'));
                return 1;
            }
        });
        const host = createHost({ actors: [EventfulJob], defaults: quiet });
        running = host;
        const client = host.actor(EventfulJob, 'run-1');
        await client.start(undefined as never);
        await until(async () => (await client.status()).status === 'completed');
        expect((await client.status()).extra.events).toEqual(['node:a', 'node:b']);
    });

    it('retainMs clears a terminal job after the retention reminder fires', async () => {
        const storage = memoryStorage();
        const Ephemeral = defineJob({
            type: 'Ephemeral',
            allowAnonymous: true,
            retainMs: 40,
            run: async () => 'done'
        });
        const host = createHost({
            actors: [Ephemeral],
            storage,
            defaults: { ...quiet, reminderTickMs: 25 }
        });
        running = host;
        await host.start();
        const client = host.actor(Ephemeral, 'run-1');
        await client.start(undefined as never);
        await until(async () => (await client.status()).status === 'completed');
        // The one-shot retention reminder clears the record and deactivates.
        await until(async () => (await client.status()).status === 'pending', 5000);
        expect(await storage.load('Ephemeral', 'run-1')).toBeNull();
    });

    it('discard forgets a terminal job; a live one refuses', async () => {
        const release = gate();
        const DiscardJob = defineJob({
            type: 'Discardable',
            allowAnonymous: true,
            run: async () => {
                await release.opened;
                return 1;
            }
        });
        const host = createHost({ actors: [DiscardJob], defaults: quiet });
        running = host;
        const client = host.actor(DiscardJob, 'run-1');
        await client.start(undefined as never);
        await expect(client.discard()).rejects.toThrow(JobStateError);
        release.open();
        await until(async () => (await client.status()).status === 'completed');
        await client.discard();
        expect((await client.status()).status).toBe('pending'); // fresh state
    });
});

describe('defineJob: onSettled', () => {
    it('fires once with the final state when the body completes', async () => {
        const settled: JobInfo[] = [];
        const Completing = defineJob({
            type: 'SettledComplete',
            allowAnonymous: true,
            run: async () => 'result',
            onSettled: (_control, info) => {
                settled.push(info);
            }
        });
        const host = createHost({ actors: [Completing], defaults: quiet });
        running = host;
        const client = host.actor(Completing, 'run-1');
        await client.start(undefined as never);
        await until(() => settled.length === 1);
        expect(settled[0]!.key).toBe('run-1');
        expect(settled[0]!.status).toBe('completed');
        expect(settled[0]!.error).toBeNull();
        // The save happens BEFORE the hook, so the state the hook observes is
        // already the durable one — not a transition it could still lose.
        expect((await client.status()).status).toBe('completed');
        await new Promise((r) => setTimeout(r, 20)); // nothing fires a second time
        expect(settled).toHaveLength(1);
    });

    it('fires with the error when the body throws', async () => {
        const settled: JobInfo[] = [];
        const Throwing = defineJob({
            type: 'SettledThrow',
            allowAnonymous: true,
            run: async () => {
                throw new Error('node exploded');
            },
            onSettled: (_control, info) => {
                settled.push(info);
            }
        });
        const host = createHost({ actors: [Throwing], defaults: quiet });
        running = host;
        await host.actor(Throwing, 'run-1').start(undefined as never);
        await until(() => settled.length === 1);
        expect(settled[0]!.status).toBe('failed');
        expect(settled[0]!.error?.message).toBe('node exploded');
    });

    it('fires on a cancel that lands while the job is PAUSED — no body turn happens', async () => {
        // The case the run body structurally cannot cover: a paused job holds
        // no task, so `doCancel`'s `tasks.cancel()` is a no-op and nothing in
        // the body ever observes the transition.
        const settled: JobInfo[] = [];
        const Parking = defineJob({
            type: 'SettledCancelParked',
            allowAnonymous: true,
            run: async (job) => job.pause({ at: 'node-a' }),
            onSettled: (_control, info) => {
                settled.push(info);
            }
        });
        const host = createHost({ actors: [Parking], defaults: quiet });
        running = host;
        const client = host.actor(Parking, 'run-1');
        await client.start(undefined as never);
        await until(async () => (await client.status()).status === 'paused');
        expect(settled).toHaveLength(0); // a pause is not a settle
        await client.cancel();
        expect(settled).toHaveLength(1);
        expect(settled[0]!.status).toBe('cancelled');
    });

    it('fires on the maxAttempts give-up, which the run body never sees', async () => {
        // The motivating case (flow#1058): the runtime REFUSES the restart and
        // finishes the job itself, so a projection maintained only from inside
        // run() would assert "still running" forever.
        const storage = memoryStorage();
        const settled: JobInfo[] = [];
        const Looping = defineJob({
            type: 'SettledGiveUp',
            allowAnonymous: true,
            maxAttempts: 2,
            run: async (job) => {
                await aborted(job.signal); // parks forever, every attempt
                return undefined as never;
            },
            onSettled: (_control, info) => {
                settled.push(info);
            }
        });
        let host = createHost({ actors: [Looping], storage, defaults: quiet });
        await host.actor(Looping, 'run-1').start(undefined as never);
        // Attempt 1 interrupted, attempt 2 interrupted, attempt 3 refused.
        for (let i = 0; i < 2; i++) {
            await within(host.stop({ timeoutMs: 5000 }), 2000);
            host = createHost({ actors: [Looping], storage, defaults: quiet });
            await host.actor(Looping, 'run-1').status(); // activate → resume
        }
        running = host;
        const client = host.actor(Looping, 'run-1');
        await until(async () => (await client.status()).status === 'failed');
        await until(() => settled.length === 1);
        expect(settled[0]!.status).toBe('failed');
        expect(settled[0]!.error?.message).toMatch(/gave up after 2 attempts/);
    });

    it('a throwing handler is swallowed — the terminal transition stands', async () => {
        const errors = vi.spyOn(console, 'error').mockImplementation(() => {});
        const Hostile = defineJob({
            type: 'SettledThrowingHook',
            allowAnonymous: true,
            run: async () => 'result',
            onSettled: () => {
                throw new Error('projection database is down');
            }
        });
        const host = createHost({ actors: [Hostile], defaults: quiet });
        running = host;
        const client = host.actor(Hostile, 'run-1');
        await client.start(undefined as never);
        await until(async () => (await client.status()).status === 'completed');
        expect(await client.result()).toBe('result');
        expect(errors).toHaveBeenCalledWith(
            expect.stringContaining('onSettled threw for job "SettledThrowingHook/run-1"'),
            expect.any(Error)
        );
    });

    it('the supplied JobControl reads the settled job', async () => {
        const seen: string[] = [];
        const Controlled = defineJob({
            type: 'SettledControl',
            allowAnonymous: true,
            state: () => ({ note: 'kept' }),
            run: async () => 'result',
            onSettled: (control) => {
                const info = control.info();
                seen.push(`${info.status}:${info.extra.note}`);
            }
        });
        const host = createHost({ actors: [Controlled], defaults: quiet });
        running = host;
        await host.actor(Controlled, 'run-1').start(undefined as never);
        await until(() => seen.length === 1);
        expect(seen).toEqual(['completed:kept']);
    });
});

describe('defineJob: the ledger lives in the state record (#309)', () => {
    const ID = 'Ledgerless\u0000run-1';

    /** Count every storage call — the same probe-counter idea the bench uses. */
    function countingStorage(inner: ReturnType<typeof memoryStorage>) {
        const counts = { loads: 0, saves: 0, clears: 0 };
        const storage: ReturnType<typeof memoryStorage> = {
            load: (t, k) => (counts.loads++, inner.load(t, k)),
            save: (t, k, s, e) => (counts.saves++, inner.save(t, k, s, e)),
            clear: (t, k, e) => (counts.clears++, inner.clear(t, k, e))
        };
        return { storage, counts, inner };
    }

    it('a running job writes NO $sigx:tasks record — its state record is the ledger', async () => {
        const storage = memoryStorage();
        const Job = defineJob({
            type: 'Ledgerless',
            allowAnonymous: true,
            run: async (job) => {
                await aborted(job.signal);
                return undefined as never;
            }
        });
        const host = createHost({ actors: [Job], storage, defaults: quiet });
        running = host;
        await host.actor(Job, 'run-1').start(undefined as never);
        expect((await host.actor(Job, 'run-1').status()).status).toBe('running');
        // Durable, but in ONE record: the job's own state carries
        // status/input/attempts, so a second record would be a second CAS
        // per start and per finish saying the same thing.
        expect(await storage.load(TASKS_TYPE, ID)).toBeNull();
        expect(await storage.load('Ledgerless', 'run-1')).not.toBeNull();
    });

    it('resumes from the state record alone after a host death, attempts bumped', async () => {
        const attempts: number[] = [];
        const storage = memoryStorage();
        const Job = defineJob({
            type: 'Ledgerless',
            allowAnonymous: true,
            run: async (job, input: { n: number }) => {
                attempts.push(job.attempt);
                if (job.attempt === 1) {
                    await aborted(job.signal);
                    return undefined as never;
                }
                return input.n * 2;
            }
        });
        const hostA = createHost({ actors: [Job], storage, defaults: quiet });
        await hostA.actor(Job, 'run-1').start({ n: 21 });
        await until(() => attempts.length === 1);
        await within(hostA.stop({ timeoutMs: 5000 }), 2000);
        expect(await storage.load(TASKS_TYPE, ID)).toBeNull();

        const hostB = createHost({ actors: [Job], storage, defaults: quiet });
        running = hostB;
        const client = hostB.actor(Job, 'run-1');
        await until(async () => (await client.status()).status === 'completed');
        expect(attempts).toEqual([1, 2]);
        expect((await client.status()).attempts).toBe(2);
        expect(await client.result()).toBe(42);
        expect(await storage.load(TASKS_TYPE, ID)).toBeNull();
    });

    it('a whole lifecycle costs 1 load, 4 saves and 0 clears', async () => {
        // The probe-counter form of `jobs/lifecycle`'s exact metrics, in
        // steady state: the state load on activation; the state CAS and the
        // task-roster CAS on start; the state CAS on finish and the roster
        // CAS on forget. Gone since #309: the ledger's load on activation,
        // load + CAS on start, load + clear on finish. Gone since #310: the
        // reminder shard's load + CAS on start and again on forget. The
        // host's one-time roster registration (an index load + CAS on its
        // FIRST task) is paid by the warm-up run below, not counted.
        const { storage, counts, inner } = countingStorage(memoryStorage());
        const Job = defineJob({
            type: 'Ledgerless',
            allowAnonymous: true,
            run: async () => 1
        });
        const host = createHost({ actors: [Job], storage, defaults: quiet });
        running = host;
        const lifecycle = async (key: string): Promise<void> => {
            const client = host.actor(Job, key);
            await client.start(undefined as never);
            await until(async () => (await client.status()).status === 'completed');
            // The roster untrack is the run's last detached write. Polled on
            // the INNER store — the poll is not one of the runtime's ops.
            const hostId = Object.keys(
                ((await inner.load(ROSTER_TYPE, ROSTER_INDEX_KEY))?.state ?? {}) as object
            )[0]!;
            await until(async () => {
                const record = await inner.load(ROSTER_TYPE, `${hostId}/${reminderShardOf(`Ledgerless\u0000${key}`)}`);
                return !(`Ledgerless\u0000${key}` in ((record?.state ?? {}) as object));
            });
        };
        await lifecycle('warm-up');
        const before = { ...counts };
        await lifecycle('run-1');
        expect({
            loads: counts.loads - before.loads,
            saves: counts.saves - before.saves,
            clears: counts.clears - before.clears
        }).toEqual({ loads: 1, saves: 4, clears: 0 });
    });
});

describe('defineJob: a rejected task start takes the running transition back (#316)', () => {
    /**
     * Reject the roster write that TRACKS `id` (the run's liveness), `n`
     * times. The host's index registration and the untrack never carry the
     * id, so they pass — this is the `ctx.tasks.start` failure alone. With
     * `thenJobSave`, the job's OWN next save after that failure rejects
     * too — the revert's save, which exercises `launch()`'s fallback.
     */
    function trackFailingStorage(id: string) {
        const inner = memoryStorage();
        const [jobType, jobKey] = id.split('\u0000') as [string, string];
        let failures = 0;
        let jobSaveFailures = 0;
        let thenJobSave = false;
        const storage: ReturnType<typeof memoryStorage> = {
            load: (t, k) => inner.load(t, k),
            save: (t, k, st, e) => {
                if (t === ROSTER_TYPE && failures > 0 && id in (st as object)) {
                    failures--;
                    if (thenJobSave) jobSaveFailures++;
                    return Promise.reject(new Error('roster transiently down'));
                }
                if (t === jobType && k === jobKey && jobSaveFailures > 0) {
                    jobSaveFailures--;
                    return Promise.reject(new Error('job record transiently down'));
                }
                return inner.save(t, k, st, e);
            },
            clear: (t, k, e) => inner.clear(t, k, e)
        };
        return {
            storage,
            inner,
            fail: (n: number, opts: { thenJobSave?: boolean } = {}) => {
                failures = n;
                thenJobSave = opts.thenJobSave ?? false;
            }
        };
    }

    it('start(): the job reads pending again, no host resumes it, and a retry starts it', async () => {
        const { storage, inner, fail } = trackFailingStorage('Revert\u0000run-1');
        const runs: number[] = [];
        const Job = defineJob({
            type: 'Revert',
            allowAnonymous: true,
            run: async (job, input: { n: number }) => {
                runs.push(job.attempt);
                return input.n * 2;
            }
        });
        const hostA = createHost({ actors: [Job], storage, defaults: quiet });
        running = hostA;
        const a = hostA.actor(Job, 'run-1');
        fail(1);
        await expect(a.start({ n: 21 })).rejects.toThrow(/transiently down/);
        // The caller saw the rejection, so nothing may say the run exists:
        // not the live state…
        expect(await a.status()).toMatchObject({ status: 'pending', attempts: 0, startedAt: null });
        // …and not the durable record, which is the ledger the next
        // activation derives its resumes from (#309).
        const record = (await inner.load('Revert', 'run-1'))?.state as { status: string };
        expect(record.status).toBe('pending');
        expect(runs).toEqual([]);
        // A host that activates the record resumes nothing.
        await within(hostA.stop({ timeoutMs: 2000 }), 1500);
        const hostB = createHost({ actors: [Job], storage, defaults: quiet });
        running = hostB;
        const b = hostB.actor(Job, 'run-1');
        expect((await b.status()).status).toBe('pending');
        await new Promise((r) => setTimeout(r, 50));
        expect(runs).toEqual([]);
        // Once the roster is back, the same start goes through.
        await b.start({ n: 21 });
        await until(async () => (await b.status()).status === 'completed');
        expect(await b.result()).toBe(42);
        expect(runs).toEqual([1]);
    });

    it('resume(): the job reads paused again, holds no resumeData, and a retry resumes it', async () => {
        const { storage, inner, fail } = trackFailingStorage('RevertPaused\u0000run-1');
        const runs: unknown[] = [];
        const Job = defineJob({
            type: 'RevertPaused',
            allowAnonymous: true,
            run: async (job, input: { doc: string }) => {
                runs.push(job.resumeData);
                if (job.resumeData === undefined) return job.pause({ stage: 'awaiting-approval' });
                return { doc: input.doc, approved: job.resumeData };
            }
        });
        const host = createHost({ actors: [Job], storage, defaults: quiet });
        running = host;
        const a = host.actor(Job, 'run-1');
        await a.start({ doc: 'contract' });
        await until(async () => (await a.status()).status === 'paused');
        fail(1);
        await expect(a.resume({ by: 'alice' })).rejects.toThrow(/transiently down/);
        expect((await a.status()).status).toBe('paused');
        const record = (await inner.load('RevertPaused', 'run-1'))?.state as {
            status: string;
            resumeData: unknown;
        };
        // Paused, with the rejected resume's data taken back too — a later
        // resume must not find a stale answer waiting.
        expect(record).toMatchObject({ status: 'paused', resumeData: null });
        expect(runs).toHaveLength(1);
        await a.resume({ by: 'bob' });
        await until(async () => (await a.status()).status === 'completed');
        expect(await a.result()).toEqual({ doc: 'contract', approved: { by: 'bob' } });
        expect(runs).toEqual([undefined, { by: 'bob' }]);
        expect((await a.status()).attempts).toBe(1); // pause-resume stays free
    });

    it('when the revert save fails too, live state stays in step with the durable running record and the run resumes at-least-once', async () => {
        const { storage, inner, fail } = trackFailingStorage('RevertStuck\u0000run-1');
        const runs: number[] = [];
        const Job = defineJob({
            type: 'RevertStuck',
            allowAnonymous: true,
            run: async (job, input: { n: number }) => {
                runs.push(job.attempt);
                return input.n * 2;
            }
        });
        const hostA = createHost({ actors: [Job], storage, defaults: quiet });
        running = hostA;
        const a = hostA.actor(Job, 'run-1');
        fail(1, { thenJobSave: true });
        // The caller still sees the LAUNCH's rejection, not the revert's.
        await expect(a.start({ n: 21 })).rejects.toThrow(/roster transiently down/);
        // The durable record could not be taken back, so it says `running`
        // — and the live state must agree with it rather than read
        // `pending` over a `running` ledger: the previous contract stands.
        const record = (await inner.load('RevertStuck', 'run-1'))?.state as { status: string };
        expect(record.status).toBe('running');
        expect(await a.status()).toMatchObject({ status: 'running', attempts: 1 });
        expect(runs).toEqual([]);
        // …which is at-least-once: the next activation derives the run
        // from the record and resumes it.
        await within(hostA.stop({ timeoutMs: 2000 }), 1500);
        const hostB = createHost({ actors: [Job], storage, defaults: quiet });
        running = hostB;
        const b = hostB.actor(Job, 'run-1');
        await until(async () => (await b.status()).status === 'completed');
        expect(await b.result()).toBe(42);
        expect(runs).toEqual([2]); // a resumed run is its own attempt
    });
});

describe('defineJob: eventual checkpoints ride the write-behind debounce (#320)', () => {
    /** Count the JOB's state saves only — the roster's writes are not the
     *  cost under test — and let a test cut the store off ("host death"). */
    function jobStorage(type: string) {
        const inner = memoryStorage();
        const counts = { saves: 0 };
        let dead = false;
        const storage: ReturnType<typeof memoryStorage> = {
            load: (t, k) => inner.load(t, k),
            save: (t, k, s, e) => {
                if (dead) return Promise.reject(new Error('host is dead'));
                if (t === type) counts.saves++;
                return inner.save(t, k, s, e);
            },
            clear: (t, k, e) => (dead ? Promise.reject(new Error('host is dead')) : inner.clear(t, k, e))
        };
        return { storage, inner, counts, kill: () => (dead = true) };
    }

    async function storedCheckpoint(inner: ReturnType<typeof memoryStorage>, type: string): Promise<unknown> {
        return ((await inner.load(type, 'run-1'))?.state as { checkpoint: unknown } | undefined)?.checkpoint;
    }

    it('50 eventual checkpoints then finish cost one terminal save, which carries the last checkpoint', async () => {
        const { storage, inner, counts } = jobStorage('Burst');
        const Burst = defineJob({
            type: 'Burst',
            allowAnonymous: true,
            run: async (job) => {
                for (let i = 1; i <= 50; i++) await job.checkpoint({ cursor: i }, { durability: 'eventual' });
                return 'done';
            }
        });
        // A hand-driven clock, never advanced: the debounce cannot fire
        // inside the burst, so the count is exact rather than "2 or 3
        // depending on how long 50 checkpoints took on this runner".
        const host = createHost({ actors: [Burst], storage, scheduler: manualScheduler(), defaults: quiet });
        running = host;
        const client = host.actor(Burst, 'run-1');
        await client.start(undefined as never);
        await until(async () => (await client.status()).status === 'completed');
        // `start` and the terminal save — 50 immediate checkpoints would
        // have added 50 more. (eventual-save.test.ts drives the debounce
        // itself and proves the window flushes.)
        expect(counts.saves).toBe(2);
        // The terminal record is never eventual: it is durable when
        // `status()` says completed, and it carries the last checkpoint.
        const record = (await inner.load('Burst', 'run-1'))?.state as { status: string; checkpoint: unknown };
        expect(record).toMatchObject({ status: 'completed', checkpoint: { cursor: 50 } });
    });

    it('a crash mid-burst resumes from the last FLUSHED checkpoint, not the last taken', async () => {
        vi.spyOn(console, 'error').mockImplementation(() => {}); // the dead store's final-flush report
        const { storage, inner, kill } = jobStorage('Crashy');
        const flushed = gate();
        const attempts: { attempt: number; resumedFrom: unknown }[] = [];
        let taken = 0;
        const Crashy = defineJob({
            type: 'Crashy',
            allowAnonymous: true,
            run: async (job) => {
                attempts.push({ attempt: job.attempt, resumedFrom: job.resumedFrom });
                if (job.attempt > 1) return (job.resumedFrom as { cursor: number }).cursor;
                await job.checkpoint({ cursor: 10 }, { durability: 'eventual' });
                await flushed.opened; // the test saw cursor 10 land in storage
                for (let i = 11; i <= 20; i++) {
                    await job.checkpoint({ cursor: i }, { durability: 'eventual' });
                    taken++;
                }
                await aborted(job.signal); // park until the host dies
                return undefined as never;
            }
        });
        const hostA = createHost({ actors: [Crashy], storage, defaults: quiet });
        await hostA.actor(Crashy, 'run-1').start(undefined as never);
        await until(async () => (await storedCheckpoint(inner, 'Crashy') as { cursor?: number } | undefined)?.cursor === 10);
        // The host "dies" here: nothing it writes from now on lands — not
        // the debounce the burst below arms, not the deactivation flush —
        // which is exactly the window a crash loses. Ten more checkpoints
        // are TAKEN after that, none durable.
        kill();
        flushed.open();
        await until(() => taken === 10);
        await within(hostA.stop({ timeoutMs: 5000 }), 2000);
        expect(await storedCheckpoint(inner, 'Crashy')).toEqual({ cursor: 10 });

        // The successor host has a live store — the death was host A's.
        const hostB = createHost({ actors: [Crashy], storage: inner, defaults: quiet });
        running = hostB;
        const client = hostB.actor(Crashy, 'run-1');
        await until(async () => (await client.status()).status === 'completed');
        expect(attempts[1]).toEqual({ attempt: 2, resumedFrom: { cursor: 10 } });
        expect(await client.result()).toBe(10);
    });

    it('a graceful stop mid-burst flushes the pending checkpoint — deactivation is not a crash', async () => {
        const { storage, inner } = jobStorage('Drained');
        const attempts: unknown[] = [];
        const Drained = defineJob({
            type: 'Drained',
            allowAnonymous: true,
            run: async (job) => {
                attempts.push(job.resumedFrom);
                if (job.attempt > 1) return 'done';
                for (let i = 1; i <= 20; i++) await job.checkpoint({ cursor: i }, { durability: 'eventual' });
                await aborted(job.signal);
                return undefined as never;
            }
        });
        const hostA = createHost({ actors: [Drained], storage, defaults: quiet });
        await hostA.actor(Drained, 'run-1').start(undefined as never);
        await until(() => attempts.length === 1);
        await within(hostA.stop({ timeoutMs: 5000 }), 2000);
        expect(await storedCheckpoint(inner, 'Drained')).toEqual({ cursor: 20 });

        const hostB = createHost({ actors: [Drained], storage, defaults: quiet });
        running = hostB;
        const client = hostB.actor(Drained, 'run-1');
        await until(async () => (await client.status()).status === 'completed');
        expect(attempts).toEqual([undefined, { cursor: 20 }]);
    });
});

describe('defineJob: job.append(entry) — O(delta) checkpoints on the append seam (#312)', () => {
    type Row = { id: number; out: string };
    /** The reducer every fixture shares: a checkpoint that starts undefined is CREATED by returning one. */
    const apply = (cp: Row[] | undefined, row: Row): Row[] => {
        (cp ??= []).push(row);
        return cp;
    };
    /** The job's record as storage holds it. */
    async function record(storage: ReturnType<typeof memoryStorage>, type: string) {
        const r = await storage.load(type, 'run-1');
        return r && { checkpoint: (r.state as { checkpoint: unknown }).checkpoint, log: r.log };
    }

    it('appends per step; a crash resumes with resumedFrom = the folded checkpoint', async () => {
        const storage = memoryStorage();
        const attempts: { attempt: number; resumedFrom: unknown }[] = [];
        let appended = 0;
        const Appending = defineJob({
            type: 'Appending',
            allowAnonymous: true,
            apply,
            run: async (job, input: number) => {
                attempts.push({ attempt: job.attempt, resumedFrom: job.resumedFrom });
                if (job.attempt > 1) return job.resumedFrom!.map((r) => r.out);
                for (let id = 0; id < input; id++) {
                    await job.append({ id, out: `step-${id}` });
                    appended++;
                }
                await aborted(job.signal);
                return undefined as never;
            }
        });
        const hostA = createHost({ actors: [Appending], storage, defaults: quiet });
        await hostA.actor(Appending, 'run-1').start(3);
        await until(() => appended === 3);
        // The snapshot is what `start` saved (no checkpoint); the steps are the log.
        expect(await record(storage, 'Appending')).toEqual({
            checkpoint: null,
            log: [
                { id: 0, out: 'step-0' },
                { id: 1, out: 'step-1' },
                { id: 2, out: 'step-2' }
            ]
        });
        await within(hostA.stop({ timeoutMs: 5000 }), 2000);

        const hostB = createHost({ actors: [Appending], storage, defaults: quiet });
        running = hostB;
        const client = hostB.actor(Appending, 'run-1');
        await until(async () => (await client.status()).status === 'completed');
        expect(attempts[1]).toEqual({
            attempt: 2,
            resumedFrom: [
                { id: 0, out: 'step-0' },
                { id: 1, out: 'step-1' },
                { id: 2, out: 'step-2' }
            ]
        });
        expect(await client.result()).toEqual(['step-0', 'step-1', 'step-2']);
        // The terminal save compacted: the fold is the snapshot, the log is empty.
        expect(await record(storage, 'Appending')).toEqual({
            checkpoint: [
                { id: 0, out: 'step-0' },
                { id: 1, out: 'step-1' },
                { id: 2, out: 'step-2' }
            ],
            log: []
        });
    });

    it('checkpoint() after appends is the compaction — the log is empty and the snapshot holds the fold', async () => {
        const storage = memoryStorage();
        const compacted = gate();
        const Compacting = defineJob({
            type: 'Compacting',
            allowAnonymous: true,
            apply,
            run: async (job) => {
                const rows: Row[] = [];
                for (let id = 0; id < 3; id++) {
                    rows.push({ id, out: `s${id}` });
                    await job.append(rows[id]!);
                }
                // The body keeps its own copy — the handle has no getter for
                // the fold; the fold IS what a resume would hand back.
                await job.checkpoint(rows);
                await job.append({ id: 3, out: 's3' });
                compacted.open();
                await aborted(job.signal);
                return undefined as never;
            }
        });
        const host = createHost({ actors: [Compacting], storage, defaults: quiet });
        running = host;
        await host.actor(Compacting, 'run-1').start(undefined as never);
        await compacted.opened;
        expect(await record(storage, 'Compacting')).toEqual({
            checkpoint: [
                { id: 0, out: 's0' },
                { id: 1, out: 's1' },
                { id: 2, out: 's2' }
            ],
            log: [{ id: 3, out: 's3' }]
        });
    });

    it('pause(cp) compacts and resume keeps the fold — appends after the resume chain on it', async () => {
        const storage = memoryStorage();
        const seen: unknown[] = [];
        const Pausing = defineJob({
            type: 'Pausing',
            allowAnonymous: true,
            apply,
            run: async (job) => {
                seen.push(job.resumedFrom);
                if (job.resumeData === undefined) {
                    const rows: Row[] = [];
                    for (let id = 0; id < 2; id++) {
                        rows.push({ id, out: `p${id}` });
                        await job.append(rows[id]!);
                    }
                    return job.pause(rows);
                }
                await job.append({ id: 2, out: 'p2' });
                return job.resumedFrom!.length;
            }
        });
        const host = createHost({ actors: [Pausing], storage, defaults: quiet });
        running = host;
        const client = host.actor(Pausing, 'run-1');
        await client.start(undefined as never);
        await until(async () => (await client.status()).status === 'paused');
        expect(await record(storage, 'Pausing')).toEqual({
            checkpoint: [
                { id: 0, out: 'p0' },
                { id: 1, out: 'p1' }
            ],
            log: []
        });
        // Resume on a FRESH activation, so the checkpoint comes from storage.
        await host.deactivateType('Pausing');
        await client.resume('go');
        await until(async () => (await client.status()).status === 'completed');
        expect(seen).toEqual([
            undefined,
            [
                { id: 0, out: 'p0' },
                { id: 1, out: 'p1' }
            ]
        ]);
        expect(await client.result()).toBe(2);
        expect(await record(storage, 'Pausing')).toEqual({
            checkpoint: [
                { id: 0, out: 'p0' },
                { id: 1, out: 'p1' },
                { id: 2, out: 'p2' }
            ],
            log: []
        });
    });

    it('on a storage without the seam every append is a full save — the recipe still holds, at the old cost', async () => {
        const inner = memoryStorage();
        const counts = { saves: 0 };
        const storage: ReturnType<typeof memoryStorage> = {
            load: (t, k) => inner.load(t, k),
            save: (t, k, s, e) => {
                if (t === 'Seamless') counts.saves++;
                return inner.save(t, k, s, e);
            },
            clear: (t, k, e) => inner.clear(t, k, e)
        };
        const attempts: unknown[] = [];
        let appended = 0;
        const Seamless = defineJob({
            type: 'Seamless',
            allowAnonymous: true,
            apply,
            run: async (job) => {
                attempts.push(job.resumedFrom);
                if (job.attempt > 1) return job.resumedFrom!.length;
                for (let id = 0; id < 3; id++) {
                    await job.append({ id, out: `f${id}` });
                    appended++;
                }
                await aborted(job.signal);
                return undefined as never;
            }
        });
        const hostA = createHost({ actors: [Seamless], storage, defaults: quiet });
        await hostA.actor(Seamless, 'run-1').start(undefined as never);
        await until(() => appended === 3);
        // `start` + three full saves: each append carried the whole fold.
        expect(counts.saves).toBe(4);
        expect(await record(inner, 'Seamless')).toEqual({
            checkpoint: [
                { id: 0, out: 'f0' },
                { id: 1, out: 'f1' },
                { id: 2, out: 'f2' }
            ],
            log: []
        });
        await within(hostA.stop({ timeoutMs: 5000 }), 2000);
        const hostB = createHost({ actors: [Seamless], storage, defaults: quiet });
        running = hostB;
        const client = hostB.actor(Seamless, 'run-1');
        await until(async () => (await client.status()).status === 'completed');
        expect(attempts[1]).toEqual([
            { id: 0, out: 'f0' },
            { id: 1, out: 'f1' },
            { id: 2, out: 'f2' }
        ]);
        expect(await client.result()).toBe(3);
    });

    it('append() without apply throws — the run fails with the reducer named', async () => {
        const NoReducer = defineJob({
            type: 'NoReducer',
            allowAnonymous: true,
            run: async (job) => {
                // Uncallable by type (E = never); the runtime guard is what
                // catches an untyped caller.
                await (job as unknown as { append(e: unknown): Promise<void> }).append({ id: 0 });
                return 'unreachable';
            }
        });
        const host = createHost({ actors: [NoReducer], defaults: quiet });
        running = host;
        const client = host.actor(NoReducer, 'run-1');
        await client.start(undefined as never);
        await until(async () => (await client.status()).status === 'failed');
        expect((await client.status()).error?.message).toMatch(/apply/);
    });

    it('a late append from a winding-down body is dropped, like progress()', async () => {
        const storage = memoryStorage();
        const parked = gate();
        const Late = defineJob({
            type: 'Late',
            allowAnonymous: true,
            apply,
            run: async (job) => {
                await job.append({ id: 0, out: 'first' });
                parked.open();
                await aborted(job.signal);
                await job.append({ id: 1, out: 'late' }); // cancelled: must not land
                return undefined as never;
            }
        });
        const host = createHost({ actors: [Late], storage, defaults: quiet });
        running = host;
        const client = host.actor(Late, 'run-1');
        await client.start(undefined as never);
        await parked.opened;
        await client.cancel();
        await until(async () => (await client.status()).status === 'cancelled');
        await new Promise((r) => setTimeout(r, 20));
        expect(await record(storage, 'Late')).toEqual({
            checkpoint: [{ id: 0, out: 'first' }],
            log: []
        });
    });
});
