/**
 * The list-watch loop against the fake API server: NDJSON parsing across
 * chunk boundaries, delta application, bookmark bookkeeping, reconnect
 * after a dropped stream, and the classic trap — 410 Gone after
 * compaction, where only a fresh LIST can reconcile a missed deletion.
 */
import { afterEach, describe, expect, it } from 'vitest';
import { kubeClient } from '../src/client';
import { buildLease, type LeaseObject } from '../src/lease';
import { listWatchLoop, ndjsonLines, type ListWatchLoop } from '../src/watch';
import { fakeKube, type FakeKube } from './fake-kube';
import type { SiloDescriptor } from '@sigx/actors/cluster';

const LABELS = { 'sigx.dev/cluster': 'default' };
const SELECTOR = 'sigx.dev/cluster=default';

function descriptor(siloId: string): SiloDescriptor {
    return { siloId, epoch: 1, address: `http://${siloId}:7311`, status: 'active' };
}

function lease(fake: FakeKube, siloId: string): LeaseObject {
    return buildLease(descriptor(siloId), {
        name: `sigx-${siloId}`,
        namespace: 'test',
        labels: LABELS,
        ttlMs: 15_000,
        nowMs: Date.now()
    });
}

function mapTarget(): {
    map: Map<string, LeaseObject>;
    replaceAll(leases: readonly LeaseObject[]): void;
    apply(type: 'ADDED' | 'MODIFIED' | 'DELETED', object: LeaseObject): void;
} {
    const map = new Map<string, LeaseObject>();
    return {
        map,
        replaceAll(leases) {
            map.clear();
            for (const item of leases) map.set(item.metadata.name, item);
        },
        apply(type, object) {
            if (type === 'DELETED') map.delete(object.metadata.name);
            else map.set(object.metadata.name, object);
        }
    };
}

async function until(cond: () => boolean, ms = 2000): Promise<void> {
    const start = Date.now();
    while (!cond()) {
        if (Date.now() - start > ms) throw new Error('condition not reached in time');
        await new Promise((resolve) => setTimeout(resolve, 5));
    }
}

function streamOf(chunks: string[]): ReadableStream<Uint8Array> {
    const encoder = new TextEncoder();
    return new ReadableStream({
        start(controller) {
            for (const chunk of chunks) controller.enqueue(encoder.encode(chunk));
            controller.close();
        }
    });
}

describe('ndjsonLines', () => {
    it('splits events across arbitrary chunk boundaries', async () => {
        const events: unknown[] = [];
        for await (const event of ndjsonLines(
            streamOf(['{"a":1}\n{"b"', ':2}\n{"c":3}', '\n{"d":4}'])
        )) {
            events.push(event);
        }
        // Includes the final line missing its trailing newline.
        expect(events).toEqual([{ a: 1 }, { b: 2 }, { c: 3 }, { d: 4 }]);
    });
});

describe('listWatchLoop', () => {
    let loop: ListWatchLoop | null = null;

    afterEach(() => {
        loop?.stop();
        loop = null;
    });

    function startLoop(fake: FakeKube, target: ReturnType<typeof mapTarget>): ListWatchLoop {
        loop = listWatchLoop({
            client: kubeClient({ apiServer: 'http://fake', namespace: 'test', fetch: fake.fetch }),
            labelSelector: SELECTOR,
            target,
            backoff: { minMs: 5, maxMs: 20 }
        });
        loop.start();
        return loop;
    }

    it('seeds from LIST and applies ADDED/MODIFIED/DELETED deltas', async () => {
        const fake = fakeKube();
        const target = mapTarget();
        fake.leases.set('sigx-s.a', lease(fake, 's.a'));

        startLoop(fake, target);
        await until(() => target.map.has('sigx-s.a'));

        const client = kubeClient({ apiServer: 'http://fake', namespace: 'test', fetch: fake.fetch });
        await client.create(lease(fake, 's.b'));
        await until(() => target.map.has('sigx-s.b'));

        await client.patch('sigx-s.b', { spec: { renewTime: '2099-01-01T00:00:00.000000Z' } });
        await until(() => target.map.get('sigx-s.b')?.spec.renewTime === '2099-01-01T00:00:00.000000Z');

        await client.delete('sigx-s.b');
        await until(() => !target.map.has('sigx-s.b'));
        expect(target.map.has('sigx-s.a')).toBe(true);
    });

    it('bookmarks advance the resume point without touching the map', async () => {
        const fake = fakeKube();
        const target = mapTarget();
        startLoop(fake, target);
        await until(() => fake.pendingWatchCount() === 1);

        // A mutation the selector filters out: the resourceVersion moves,
        // but no event reaches this watch — only a bookmark can carry the
        // new resume point.
        const client = kubeClient({ apiServer: 'http://fake', namespace: 'test', fetch: fake.fetch });
        const foreign = lease(fake, 's.other');
        foreign.metadata.labels = { 'sigx.dev/cluster': 'other' };
        await client.create(foreign);

        fake.bookmark();
        await new Promise((resolve) => setTimeout(resolve, 20));
        expect(target.map.size).toBe(0);

        // Compact to the bookmarked version, then drop: a resume from the
        // pre-bookmark version would 410 and force a second LIST; the
        // bookmarked one sails through.
        fake.compact();
        fake.dropWatches();
        await until(() => fake.pendingWatchCount() === 1);
        const lists = fake.log.filter((line) => line.startsWith('GET /') && !line.includes('watch=1'));
        expect(lists.length).toBe(1);
    });

    it('reconnects after a dropped stream and catches up without re-listing', async () => {
        const fake = fakeKube();
        const target = mapTarget();
        fake.leases.set('sigx-s.a', lease(fake, 's.a'));
        startLoop(fake, target);
        await until(() => fake.pendingWatchCount() === 1);

        fake.dropWatches();
        const client = kubeClient({ apiServer: 'http://fake', namespace: 'test', fetch: fake.fetch });
        await client.create(lease(fake, 's.b'));

        await until(() => target.map.has('sigx-s.b'));
        const lists = fake.log.filter((line) => line.startsWith('GET /') && !line.includes('watch=1'));
        expect(lists.length).toBe(1);
    });

    it('relist() resyncs — LIST lands, the watch resumes from the snapshot', async () => {
        const fake = fakeKube();
        const target = mapTarget();
        fake.leases.set('sigx-s.a', lease(fake, 's.a'));
        const running = startLoop(fake, target);
        await until(() => target.map.has('sigx-s.a'));

        await running.relist();
        expect(target.map.has('sigx-s.a')).toBe(true);
        const lists = fake.log.filter((line) => line.startsWith('GET /') && !line.includes('watch=1'));
        expect(lists.length).toBe(2);

        // The resumed watch still delivers deltas after the resync.
        const client = kubeClient({ apiServer: 'http://fake', namespace: 'test', fetch: fake.fetch });
        await client.create(lease(fake, 's.b'));
        await until(() => target.map.has('sigx-s.b'));
        expect(target.map.size).toBe(2);
    });

    it('relist() rejects when the LIST fails', async () => {
        const fake = fakeKube();
        const target = mapTarget();
        const running = startLoop(fake, target);
        await until(() => fake.pendingWatchCount() === 1);

        fake.failAll(true);
        await expect(running.relist()).rejects.toThrow('lease list failed');
        fake.failAll(false);
    });

    it('410 Gone after compaction forces a re-list that reconciles a missed deletion', async () => {
        const fake = fakeKube();
        const target = mapTarget();
        fake.leases.set('sigx-s.a', lease(fake, 's.a'));
        fake.leases.set('sigx-s.b', lease(fake, 's.b'));
        startLoop(fake, target);
        await until(() => target.map.size === 2);

        // Hold the watch down while history moves past it, then compact.
        fake.blockWatches();
        fake.dropWatches();
        const client = kubeClient({ apiServer: 'http://fake', namespace: 'test', fetch: fake.fetch });
        await client.delete('sigx-s.a');
        await client.create(lease(fake, 's.c'));
        fake.compact();
        fake.blockWatches(false);

        await until(() => !target.map.has('sigx-s.a') && target.map.has('sigx-s.c'));
        expect([...target.map.keys()].sort()).toEqual(['sigx-s.b', 'sigx-s.c']);
        const lists = fake.log.filter((line) => line.startsWith('GET /') && !line.includes('watch=1'));
        expect(lists.length).toBe(2);
    });
});
