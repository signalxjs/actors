/**
 * `job.watch({ throttleMs })` (#231) — the job door to #130's coalescing.
 *
 * A job that reports progress per step over state that grows through the
 * run pays one whole-state snapshot per step for every watcher, and the
 * watch stream hard-coded `ctx.changes({ initial: true })`. The knob is
 * per-subscriber (a dashboard redrawing at 4 Hz and a log tail can watch
 * the same run differently), and the default stays byte-for-byte the old
 * contract: one `JobInfo` per mutating turn.
 *
 * The probe counts snapshots exactly, the change-throttle way: a Marker
 * lives in the CHECKPOINT, so every whole-state clone encodes it once.
 * `status()` no longer clones (#229), so polling between assertions costs
 * nothing and the counts attribute cleanly.
 */
import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { defineJob } from '@sigx/actors/job';
import { actor } from '@sigx/actors';
import {
    createHost,
    manualScheduler,
    memoryStorage,
    type Host,
    type ManualScheduler
} from '@sigx/actors/host';
import type { TypeHandler } from '@sigx/serialize';

class Marker {}

let encodes = 0;

const probe: TypeHandler<Marker, number> = {
    name: 'marker',
    tag: '$marker',
    test: (value) => value instanceof Marker,
    serialize: () => {
        encodes++;
        return 1;
    },
    revive: () => new Marker()
};

const quiet = { sweepIntervalMs: 600_000, reminderTickMs: 600_000, callTimeoutMs: 0 };
const call = { callChain: [], callId: 'test' };

async function until(cond: () => boolean | Promise<boolean>, ms = 3000): Promise<void> {
    for (let waited = 0; !(await cond()); waited += 5) {
        if (waited > ms) throw new Error('condition not reached');
        await new Promise((r) => setTimeout(r, 5));
    }
}

/** The command channel into the detached run body, keyed by job key. */
type Cmd = 'tick' | 'stop';
const channels = new Map<string, { cmds: Cmd[]; waiters: ((c: Cmd) => void)[] }>();
function chan(key: string): { cmds: Cmd[]; waiters: ((c: Cmd) => void)[] } {
    let c = channels.get(key);
    if (!c) channels.set(key, (c = { cmds: [], waiters: [] }));
    return c;
}
function send(key: string, cmd: Cmd): void {
    const c = chan(key);
    const w = c.waiters.shift();
    if (w) w(cmd);
    else c.cmds.push(cmd);
}
function nextCmd(key: string): Promise<Cmd> {
    const c = chan(key);
    const queued = c.cmds.shift();
    if (queued !== undefined) return Promise.resolve(queued);
    return new Promise((r) => c.waiters.push(r));
}

/**
 * Checkpoints a Marker once, then reports one progress step per released
 * tick — mutation without persistence, the exact shape #130 was built for.
 */
const ThrottleJob = defineJob<null, number, Marker>({
    type: 'ThrottleJob',
    allowAnonymous: true,
    run: async (job) => {
        await job.checkpoint(new Marker());
        let done = 0;
        for (;;) {
            const cmd = await nextCmd(job.key);
            if (cmd === 'stop') return done;
            await job.progress({ done: ++done });
        }
    }
});

function reader(stream: AsyncIterable<unknown>): {
    next(what: string): Promise<{ status: string; progress: { done: number } | null }>;
    ended(what: string): Promise<boolean>;
} {
    const iterator = stream[Symbol.asyncIterator]();
    const pull = (what: string): Promise<IteratorResult<unknown>> =>
        Promise.race([
            iterator.next(),
            new Promise<never>((_, reject) =>
                setTimeout(() => reject(new Error(`${what}: nothing arrived within 2000ms`)), 2000).unref?.()
            )
        ]);
    return {
        async next(what) {
            const r = await pull(what);
            if (r.done) throw new Error(`${what}: the feed ended instead of yielding`);
            return r.value as { status: string; progress: { done: number } | null };
        },
        async ended(what) {
            return (await pull(what)).done === true;
        }
    };
}

let running: Host | null = null;
let clock: ManualScheduler;
let seq = 0;
let key = '';

beforeEach(() => {
    encodes = 0;
    key = `run-${seq++}`;
});
afterEach(async () => {
    send(key, 'stop');
    channels.delete(key);
    await running?.stop({ timeoutMs: 2000 });
    running = null;
});

async function start(): Promise<Host> {
    clock = manualScheduler();
    running = createHost({
        actors: [ThrottleJob],
        storage: memoryStorage(),
        scheduler: clock,
        types: [probe],
        defaults: quiet
    });
    await running.start();
    const client = actor(ThrottleJob, key);
    await client.start(null);
    // The run body's first checkpoint has landed once status is `running`
    // with the initial (null) progress — after that, ticks drive it.
    await until(async () => (await client.status()).status === 'running');
    return running;
}

const ref = () => ({ type: ThrottleJob.type, key });

describe('job.watch({ throttleMs }) (#231)', () => {
    it('collapses a burst of progress into leading + trailing', async () => {
        const host = await start();
        const r = reader(host.dispatchStream!(ref(), 'watch', [{ throttleMs: 1000 }], call));
        await r.next('seed');

        encodes = 0;
        for (let i = 0; i < 5; i++) send(key, 'tick');
        const client = actor(ThrottleJob, key);
        await until(async () => (await client.status()).progress?.done === 5);

        // The first tick opened the window and emitted; the other four were
        // absorbed — one snapshot, not five.
        expect((await r.next('leading emit')).progress).toEqual({ done: 1 });
        expect(encodes).toBe(1);

        // Closing the window emits the LATEST state, taken fresh.
        clock.advance(1000);
        expect((await r.next('trailing emit')).progress).toEqual({ done: 5 });
        expect(encodes).toBe(2);
    });

    it('a no-arg watch keeps the old contract: one JobInfo per mutating turn', async () => {
        const host = await start();
        const r = reader(host.dispatchStream!(ref(), 'watch', [], call));
        await r.next('seed');

        encodes = 0;
        for (let n = 1; n <= 3; n++) {
            send(key, 'tick');
            expect((await r.next(`tick ${n}`)).progress).toEqual({ done: n });
        }
        expect(encodes).toBe(3);
    });

    it('a job settling inside its window still delivers the terminal info', async () => {
        const host = await start();
        const r = reader(host.dispatchStream!(ref(), 'watch', [{ throttleMs: 60_000 }], call));
        await r.next('seed');

        send(key, 'tick');
        expect((await r.next('leading emit')).progress).toEqual({ done: 1 });

        // Completion lands inside the window: the terminal transition must
        // not be lost to the throttle — the deactivation flush delivers it
        // before the feed ends.
        send(key, 'stop');
        const client = actor(ThrottleJob, key);
        await until(async () => (await client.status()).status === 'completed');
        const stopping = running!.stop({ timeoutMs: 2000 });
        running = null;
        expect((await r.next('terminal flush')).status).toBe('completed');
        expect(await r.ended('feed end')).toBe(true);
        await stopping;
    });
});
