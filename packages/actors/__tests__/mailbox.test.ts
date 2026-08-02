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
