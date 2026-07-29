import { afterEach, describe, expect, it, vi } from 'vitest';
import {
    defineJob,
    JobCancelledError,
    JobFailedError,
    JobNotDoneError,
    JobStateError,
    type JobInfo
} from '@sigx/actors/job';
import { createSilo, memoryStorage, type Silo } from '@sigx/actors/silo';

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

let running: Silo | null = null;
afterEach(async () => {
    vi.restoreAllMocks();
    await running?.stop({ timeoutMs: 1000 });
    running = null;
});

describe('defineJob: the state machine', () => {
    it('runs start → running (with progress) → completed → result; start is idempotent', async () => {
        const step = gate();
        const SumJob = defineJob({
            type: 'Sum',
            unguarded: true,
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
        const silo = createSilo({ actors: [SumJob], defaults: quiet });
        running = silo;
        const client = silo.actor(SumJob, 'run-1');

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
            unguarded: true,
            run: async (job, input: number) => {
                await job.progress({ done: 1, total: 2 });
                await job.progress({ done: 2, total: 2 });
                return input * 2;
            }
        });
        const silo = createSilo({ actors: [JobDef], defaults: quiet });
        running = silo;
        const client = silo.actor(JobDef, 'run-1');

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
            unguarded: true,
            run: async () => {
                throw new Error('provider exploded');
            }
        });
        const silo = createSilo({ actors: [BadJob], defaults: quiet });
        running = silo;
        const client = silo.actor(BadJob, 'run-1');
        await client.start(undefined as never);
        await until(async () => (await client.status()).status === 'failed');
        expect((await client.status()).error?.message).toBe('provider exploded');
        await expect(client.result()).rejects.toThrow(JobFailedError);
    });

    it('result() before completion throws JobNotDoneError', async () => {
        const release = gate();
        const SlowJob = defineJob({
            type: 'Slow',
            unguarded: true,
            run: async () => {
                await release.opened;
                return 1;
            }
        });
        const silo = createSilo({ actors: [SlowJob], defaults: quiet });
        running = silo;
        const client = silo.actor(SlowJob, 'run-1');
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
            unguarded: true,
            run: async (job) => {
                await aborted(job.signal);
                observed.push(String(job.signal.reason));
                // A late result must NOT overwrite the cancelled status.
                return 'too late';
            }
        });
        const silo = createSilo({ actors: [CancelJob], defaults: quiet });
        running = silo;
        const client = silo.actor(CancelJob, 'run-1');
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
            unguarded: true,
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
        const silo = createSilo({ actors: [LateJob], defaults: quiet });
        running = silo;
        const client = silo.actor(LateJob, 'run-1');
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
    it('a silo death resumes the run with attempts bumped and the checkpoint delivered', async () => {
        const attempts: { attempt: number; resumedFrom: unknown }[] = [];
        const storage = memoryStorage();
        const ResumeJob = defineJob({
            type: 'Resumable',
            unguarded: true,
            run: async (job, input: { rows: number }) => {
                attempts.push({ attempt: job.attempt, resumedFrom: job.resumedFrom });
                if (job.attempt === 1) {
                    await job.checkpoint({ cursor: 40 });
                    await aborted(job.signal); // park until the silo dies
                    return undefined as never; // interrupted — never completes
                }
                return { rows: input.rows, from: (job.resumedFrom as { cursor: number }).cursor };
            }
        });
        const siloA = createSilo({ actors: [ResumeJob], storage, defaults: quiet });
        await siloA.actor(ResumeJob, 'run-1').start({ rows: 100 });
        await until(() => attempts.length === 1);
        await within(siloA.stop({ timeoutMs: 5000 }), 2000);

        const siloB = createSilo({ actors: [ResumeJob], storage, defaults: quiet });
        running = siloB;
        const client = siloB.actor(ResumeJob, 'run-1');
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
            unguarded: true,
            maxAttempts: 2,
            run: async (job) => {
                await aborted(job.signal); // parks forever, every attempt
                return undefined as never;
            }
        });
        let silo = createSilo({ actors: [LoopJob], storage, defaults: quiet });
        await silo.actor(LoopJob, 'run-1').start(undefined as never);
        // Attempt 1 interrupted, attempt 2 interrupted, attempt 3 refused.
        for (let i = 0; i < 2; i++) {
            await within(silo.stop({ timeoutMs: 5000 }), 2000);
            silo = createSilo({ actors: [LoopJob], storage, defaults: quiet });
            await silo.actor(LoopJob, 'run-1').status(); // activate → resume
        }
        running = silo;
        const client = silo.actor(LoopJob, 'run-1');
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
            unguarded: true,
            run: async (job, input: { doc: string }) => {
                runs.push({ attempt: job.attempt, resumeData: job.resumeData });
                if (job.resumeData === undefined) {
                    return job.pause({ stage: 'awaiting-approval' });
                }
                return { doc: input.doc, approved: job.resumeData };
            }
        });
        const siloA = createSilo({ actors: [Approval], storage, defaults: quiet });
        const a = siloA.actor(Approval, 'run-1');
        await a.start({ doc: 'contract' });
        await until(async () => (await a.status()).status === 'paused');
        // Paused = no live task; survives a full silo restart with no resume.
        await within(siloA.stop({ timeoutMs: 2000 }), 1500);
        const siloB = createSilo({ actors: [Approval], storage, defaults: quiet });
        running = siloB;
        const b = siloB.actor(Approval, 'run-1');
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
            unguarded: true,
            run: async () => 1
        });
        const silo = createSilo({ actors: [NopJob], defaults: quiet });
        running = silo;
        await expect(silo.actor(NopJob, 'run-1').resume()).rejects.toThrow(JobStateError);
    });

    it('a paused wait can time out via job.reminders + onReminder control', async () => {
        const storage = memoryStorage();
        const Hitl = defineJob({
            type: 'Hitl',
            unguarded: true,
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
        const silo = createSilo({
            actors: [Hitl],
            storage,
            defaults: { ...quiet, reminderTickMs: 25 }
        });
        running = silo;
        await silo.start();
        const client = silo.actor(Hitl, 'run-1');
        await client.start({ question: 'approve?' });
        await until(async () => (await client.status()).status === 'completed', 5000);
        expect(await client.result()).toEqual({ answer: { timedOut: true } });
    });
});

describe('defineJob: extra state, retention, discard', () => {
    it('job.update() maintains extra fields visible in JobInfo', async () => {
        const EventfulJob = defineJob({
            type: 'Eventful',
            unguarded: true,
            state: () => ({ events: [] as string[] }),
            run: async (job) => {
                await job.update((extra) => extra.events.push('node:a'));
                await job.update((extra) => extra.events.push('node:b'));
                return 1;
            }
        });
        const silo = createSilo({ actors: [EventfulJob], defaults: quiet });
        running = silo;
        const client = silo.actor(EventfulJob, 'run-1');
        await client.start(undefined as never);
        await until(async () => (await client.status()).status === 'completed');
        expect((await client.status()).extra.events).toEqual(['node:a', 'node:b']);
    });

    it('retainMs clears a terminal job after the retention reminder fires', async () => {
        const storage = memoryStorage();
        const Ephemeral = defineJob({
            type: 'Ephemeral',
            unguarded: true,
            retainMs: 40,
            run: async () => 'done'
        });
        const silo = createSilo({
            actors: [Ephemeral],
            storage,
            defaults: { ...quiet, reminderTickMs: 25 }
        });
        running = silo;
        await silo.start();
        const client = silo.actor(Ephemeral, 'run-1');
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
            unguarded: true,
            run: async () => {
                await release.opened;
                return 1;
            }
        });
        const silo = createSilo({ actors: [DiscardJob], defaults: quiet });
        running = silo;
        const client = silo.actor(DiscardJob, 'run-1');
        await client.start(undefined as never);
        await expect(client.discard()).rejects.toThrow(JobStateError);
        release.open();
        await until(async () => (await client.status()).status === 'completed');
        await client.discard();
        expect((await client.status()).status).toBe('pending'); // fresh state
    });
});
