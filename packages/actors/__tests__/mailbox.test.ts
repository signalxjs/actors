import { describe, expect, it } from 'vitest';
import { Mailbox } from '@sigx/actors/host';
import { isActorError } from '@sigx/actors';

const tick = () => new Promise<void>((r) => setTimeout(r, 0));

describe('Mailbox', () => {
    it('runs turns strictly one at a time, in order', async () => {
        const box = new Mailbox();
        const log: string[] = [];
        let releaseFirst!: () => void;
        const first = box.run(async () => {
            log.push('first:start');
            await new Promise<void>((r) => (releaseFirst = r));
            log.push('first:end');
        });
        const second = box.run(async () => {
            log.push('second');
        });
        await tick();
        // second must not have started while first holds the mailbox
        expect(log).toEqual(['first:start']);
        releaseFirst();
        await Promise.all([first, second]);
        expect(log).toEqual(['first:start', 'first:end', 'second']);
    });

    it('a failed turn rejects its own caller but never poisons the queue', async () => {
        const box = new Mailbox();
        const boom = box.run(() => {
            throw new Error('boom');
        });
        const after = box.run(async () => 'ok');
        await expect(boom).rejects.toThrow('boom');
        await expect(after).resolves.toBe('ok');
    });

    it('tracks depth across queued turns', async () => {
        const box = new Mailbox();
        let release!: () => void;
        const gate = new Promise<void>((r) => (release = r));
        const a = box.run(() => gate);
        const b = box.run(() => gate);
        expect(box.depth).toBe(2);
        release();
        await Promise.all([a, b]);
        expect(box.depth).toBe(0);
    });

    it('close() rejects new turns with the host-shutdown brand, queued turns still run', async () => {
        const box = new Mailbox();
        let release!: () => void;
        const gate = new Promise<void>((r) => (release = r));
        const queued = box.run(() => gate.then(() => 'done'));
        box.close();
        const rejected = box.run(async () => 'nope');
        await expect(rejected).rejects.toSatisfy(
            (e: unknown) => isActorError(e) && e.kind === 'host-shutdown'
        );
        release();
        await expect(queued).resolves.toBe('done');
    });

    it('drain() resolves only when every queued turn settled', async () => {
        const box = new Mailbox();
        const log: string[] = [];
        void box.run(async () => {
            await tick();
            log.push('a');
        });
        void box.run(async () => {
            log.push('b');
        });
        box.close();
        await box.drain();
        expect(log).toEqual(['a', 'b']);
    });
});

describe('Mailbox — interleaved lane', () => {
    it('interleaved turns overlap each other AND a parked serial turn, in both directions', async () => {
        const box = new Mailbox();
        const log: string[] = [];
        let releaseSerial!: () => void;
        let releaseInterleaved!: () => void;
        const serial = box.run(async () => {
            log.push('serial:start');
            await new Promise<void>((r) => (releaseSerial = r));
            log.push('serial:end');
        });
        // Never waits for the parked serial turn…
        const i1 = box.run(async () => {
            log.push('i1');
        }, true);
        // …and a parked interleaved turn never blocks anyone.
        const i2 = box.run(async () => {
            log.push('i2:start');
            await new Promise<void>((r) => (releaseInterleaved = r));
            log.push('i2:end');
        }, true);
        const i3 = box.run(async () => {
            log.push('i3');
        }, true);
        await tick();
        expect(log).toEqual(['serial:start', 'i1', 'i2:start', 'i3']);
        releaseSerial();
        await serial;
        releaseInterleaved();
        await Promise.all([i1, i2, i3]);
        expect(log).toEqual(['serial:start', 'i1', 'i2:start', 'i3', 'serial:end', 'i2:end']);
    });

    it('serial turns still chain strictly among themselves while interleaved ones fly', async () => {
        const box = new Mailbox();
        const log: string[] = [];
        let release!: () => void;
        const s1 = box.run(async () => {
            log.push('s1:start');
            await new Promise<void>((r) => (release = r));
            log.push('s1:end');
        });
        const s2 = box.run(async () => {
            log.push('s2');
        });
        const i1 = box.run(async () => {
            log.push('i1');
        }, true);
        await tick();
        // s2 waits behind s1; i1 does not.
        expect(log).toEqual(['s1:start', 'i1']);
        release();
        await Promise.all([s1, s2, i1]);
        expect(log).toEqual(['s1:start', 'i1', 's1:end', 's2']);
    });

    it('an interleaved turn never starts synchronously from run()', async () => {
        const box = new Mailbox();
        const log: string[] = [];
        const turn = box.run(async () => {
            log.push('ran');
        }, true);
        expect(log).toEqual([]);
        await turn;
        expect(log).toEqual(['ran']);
    });

    it('depth counts both lanes; a rejecting interleaved turn neither poisons nor leaks', async () => {
        const box = new Mailbox();
        let release!: () => void;
        const gate = new Promise<void>((r) => (release = r));
        const s = box.run(() => gate);
        const i = box.run(() => gate, true);
        const boom = box.run(async () => {
            throw new Error('boom');
        }, true);
        expect(box.depth).toBe(3);
        await expect(boom).rejects.toThrow('boom');
        release();
        await Promise.all([s, i]);
        expect(box.depth).toBe(0);
        // The tail was not poisoned and the in-flight set is empty: a
        // close+drain settles immediately.
        box.close();
        await box.drain();
        await expect(box.run(async () => 'late', true)).rejects.toSatisfy(
            (e: unknown) => isActorError(e) && e.kind === 'host-shutdown'
        );
    });

    it('drain() covers interleaved turns still running at close', async () => {
        const box = new Mailbox();
        const log: string[] = [];
        let release!: () => void;
        void box.run(async () => {
            log.push('i:start');
            await new Promise<void>((r) => (release = r));
            log.push('i:end');
        }, true);
        await tick();
        box.close();
        const drained = box.drain().then(() => log.push('drained'));
        await tick();
        expect(log).toEqual(['i:start']);
        release();
        await drained;
        expect(log).toEqual(['i:start', 'i:end', 'drained']);
    });
});
