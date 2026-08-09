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
