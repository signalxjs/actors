/**
 * `socketStats()` (#166): the counters a fleet needs to SEE its sockets,
 * recorded by sessions at the event sites they own, published by the app as
 * an ops section. Driven through real sessions over the fake link — the
 * numbers must come from behaviour, not from the recorder being poked.
 */
import { afterEach, describe, expect, it } from 'vitest';
import { ServerFnError } from '@sigx/server';
import { defineActor } from '@sigx/actors';
import { createHost, type Host } from '@sigx/actors/host';
import {
    createActorSocketSession,
    socketStats,
    type ActorSocketSession,
    type SocketStats
} from '@sigx/actors/server';

const quiet = { sweepIntervalMs: 60_000, reminderTickMs: 60_000, callTimeoutMs: 0 };

const Cart = defineActor({
    type: 'Cart',
    allowAnonymous: true,
    state: () => ({ items: [] as string[] }),
    methods: (ctx) => ({
        async add(item: string) {
            ctx.state.items.push(item);
            await ctx.save();
            return ctx.state.items.length;
        },
        async total() {
            return ctx.state.items.length;
        }
    })
});

const Refused = defineActor({
    type: 'Refused',
    authorize: [
        () => {
            throw new ServerFnError(401, 'nope');
        }
    ],
    state: () => ({}),
    methods: () => ({
        async peek() {
            return 1;
        }
    })
});

let host: Host | null = null;
const sessions: ActorSocketSession[] = [];

afterEach(async () => {
    for (const s of sessions) s.close();
    sessions.length = 0;
    await host?.stop();
    host = null;
});

async function start(): Promise<Host> {
    host = createHost({ actors: [Cart, Refused], defaults: quiet });
    await host.start();
    return host;
}

interface Link {
    sent: string[];
    closes: Array<[number, string]>;
    send(m: string): void;
    close(c: number, r: string): void;
}

const link = (): Link => {
    const l: Link = {
        sent: [],
        closes: [],
        send: (m) => l.sent.push(m),
        close: (c, r) => l.closes.push([c, r])
    };
    return l;
};

async function connect(
    s: Host,
    stats: SocketStats,
    overrides: Record<string, unknown> = {}
): Promise<{ session: ActorSocketSession; l: Link }> {
    const l = link();
    const session = await createActorSocketSession({
        host: s,
        request: new Request('http://actors.test/socket'),
        send: l.send,
        close: l.close,
        origin: false,
        pingMs: 0,
        stats,
        ...overrides
    });
    sessions.push(session);
    return { session, l };
}

async function until(predicate: () => boolean, ms = 2000): Promise<void> {
    const started = Date.now();
    while (!predicate()) {
        if (Date.now() - started > ms) throw new Error('timed out waiting');
        await new Promise((resolve) => setTimeout(resolve, 5));
    }
}

describe('socketStats', () => {
    it('counts the connection lifecycle, calls and failures from behaviour', async () => {
        const s = await start();
        const stats = socketStats();
        const a = await connect(s, stats);
        const b = await connect(s, stats);
        const early = stats.snapshot();
        expect(early).toMatchObject({ connectionsOpened: 2, open: 2 });
        // Nullability means "no data": nothing has completed yet.
        expect(early.lifetimeMs).toBeNull();

        a.session.handle(JSON.stringify({ i: 1, s: 'Cart#add', a: ['k', 'x'] }));
        a.session.handle(JSON.stringify({ i: 2, s: 'Refused#peek', a: ['k'] }));
        await until(() => a.l.sent.length >= 3); // v, d, e
        expect(stats.snapshot()).toMatchObject({ callsStarted: 2, callsFailed: 1 });

        b.session.close();
        b.session.close(); // idempotent — must not double-count
        const snap = stats.snapshot();
        expect(snap).toMatchObject({ connectionsClosed: 1, open: 1 });
        expect(snap.lifetimeMs?.count).toBe(1);
    });

    it('counts refused upgrades without a session existing', async () => {
        const s = await start();
        const stats = socketStats();
        const l = link();
        await expect(
            createActorSocketSession({
                host: s,
                request: new Request('http://actors.test/socket', {
                    // happy-dom drops a real Origin header; an origin-less
                    // request under 'same-origin' is refused just the same.
                }),
                send: l.send,
                close: l.close,
                origin: 'same-origin',
                stats
            })
        ).rejects.toThrow(/origin/);
        expect(stats.snapshot()).toMatchObject({ connectionsRefused: 1, open: 0 });
    });

    it('tracks subscriptions open/closed and the live gauges', async () => {
        const s = await start();
        const stats = socketStats();
        const { session } = await connect(s, stats);
        session.handle(JSON.stringify({ i: 1, sub: { t: 'Cart', k: 'w', m: 'total' } }));
        session.handle(JSON.stringify({ i: 2, sub: { t: 'Cart', k: 'v', m: 'total' } }));
        await until(() => stats.snapshot().subscriptions === 2);
        expect(stats.snapshot()).toMatchObject({ subscriptionsOpened: 2 });
        session.handle(JSON.stringify({ i: 1, uns: 1 }));
        await until(() => stats.snapshot().subscriptions === 1);
        // Teardown closes the remainder — counted once, by whichever path
        // reaches it first.
        session.close();
        await until(() => stats.snapshot().subscriptionsClosed === 2);
    });

    it('classifies protocol breaches and lifetime closes', async () => {
        const s = await start();
        const stats = socketStats();
        const breach = await connect(s, stats);
        breach.session.handle('not json');
        expect(stats.snapshot()).toMatchObject({ protocolBreaches: 1, connectionsClosed: 1 });

        await connect(s, stats, { maxConnectionMs: 25 });
        await until(() => stats.snapshot().lifetimeCloses === 1);
        expect(stats.snapshot()).toMatchObject({ connectionsClosed: 2, open: 0 });
    });

    it('counts subscription deliveries and their bytes, not calls or streams', async () => {
        // Deliveries is the closest thing the host has to a syscall counter
        // (#245: a delivery is 77% socket write), so it must count exactly
        // the fan-out frames — a unary call's `{i,v}` + `{i,d}` pair is not
        // a delivery, however similar the frame looks.
        const s = await start();
        const stats = socketStats();
        const { session } = await connect(s, stats);

        session.handle(JSON.stringify({ i: 1, s: 'Cart#total', a: ['k'] }));
        await until(() => stats.snapshot().callsStarted === 1);
        expect(stats.snapshot()).toMatchObject({ deliveries: 0, deliveryBytes: 0 });

        session.handle(JSON.stringify({ i: 2, sub: { t: 'Cart', k: 'k', m: 'total' } }));
        await until(() => stats.snapshot().deliveries === 1);

        // Two mutations, two more pushes — the seed plus one per change.
        session.handle(JSON.stringify({ i: 3, s: 'Cart#add', a: ['k', 'apple'] }));
        await until(() => stats.snapshot().deliveries === 2);

        const snap = stats.snapshot();
        expect(snap.deliveries).toBe(2);
        // Approximate by contract (code units, not UTF-8) but never a
        // placeholder: it has to track the frames actually written.
        expect(snap.deliveryBytes).toBeGreaterThan(0);
        expect(snap.deliveryBytes).toBe(
            JSON.stringify({ i: 2, v: 0 }).length + JSON.stringify({ i: 2, v: 1 }).length
        );
    });

    it('counts a quantized throttle only when the policy MOVED the request', async () => {
        const s = await start();
        const stats = socketStats();
        const { session } = await connect(s, stats);

        // 50 is a bucket under the default policy — asked and got it.
        session.handle(JSON.stringify({ i: 1, sub: { t: 'Cart', k: 'a', m: 'total', w: 50 } }));
        await until(() => stats.snapshot().subscriptionsOpened === 1);
        expect(stats.snapshot().throttleQuantized).toBe(0);

        // 300 is not — it is served at 1000.
        session.handle(JSON.stringify({ i: 2, sub: { t: 'Cart', k: 'b', m: 'total', w: 300 } }));
        await until(() => stats.snapshot().throttleQuantized === 1);
    });

    it('reports bufferedBytes as null when no adapter can answer, and sums when they can', async () => {
        // The #208 rule: null MEANS "nobody could tell us". A zero here
        // would read as "the hosts are not buffering", which is the
        // misreading that left #182 unresolved for two Tier-3 sessions.
        const s = await start();
        const stats = socketStats();
        await connect(s, stats);
        expect(stats.snapshot().bufferedBytes).toBeNull();

        let depth = 7;
        await connect(s, stats, { bufferedBytes: () => depth });
        expect(stats.snapshot().bufferedBytes).toBe(7);

        depth = 11;
        // Polled, not accumulated — it is a gauge.
        expect(stats.snapshot().bufferedBytes).toBe(11);

        // A second reporting session sums; the silent one still contributes
        // nothing rather than a zero.
        await connect(s, stats, { bufferedBytes: () => 5 });
        expect(stats.snapshot().bufferedBytes).toBe(16);
    });

    it('survives an adapter whose bufferedBytes throws', async () => {
        // It is polled from an ops request. A socket dying under the probe
        // must not take the whole snapshot down.
        const s = await start();
        const stats = socketStats();
        await connect(s, stats, {
            bufferedBytes: () => {
                throw new Error('socket is gone');
            }
        });
        expect(stats.snapshot().bufferedBytes).toBeNull();
    });

    it('digests with the runtime histogram layout so renderers can merge it', async () => {
        const s = await start();
        const stats = socketStats();
        const { session } = await connect(s, stats);
        session.close();
        const digest = stats.digest();
        expect(digest.layout).toBe('ll-4-26');
        expect(digest.lifetime?.count).toBe(1);
        expect(digest).toMatchObject({ connectionsOpened: 1, connectionsClosed: 1 });
    });
});
