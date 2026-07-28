/**
 * The silo-to-silo transport conformance suite: what "implements
 * `SiloTransport` correctly" means, as runnable cases.
 *
 * Every transport runs the SAME cases against its own N-silo harness, so a
 * behaviour is a property of clustering rather than of whichever wire
 * happens to carry it. It ships against `httpTransport()` first, before any
 * second transport exists — the incumbent passing is what proves the suite
 * describes the real contract rather than the newcomer's habits.
 *
 * **The governing rule: assert on `kind`, never on an HTTP status.** A
 * status is one encoding of a kind; the kind is the contract. That is what
 * makes "what is 421 over TCP?" a question with an answer. The single place
 * a number still matters is the ops surface — `clusterStats` classifies
 * peer failures by numeric status — and that has its own case.
 *
 * No test framework is imported. Cases are descriptors with a `run()` that
 * throws on failure, so a transport package outside this repo can drive
 * them from whatever runner it uses.
 */
import { mintCallId } from '../call-id';
import { defineActor } from '../define';
import { ActorStateConflictError, isActorError, type ActorErrorShape } from '../errors';
import type { ActorCallContext, ActorContext, AnyActorDefinition, Silo } from '../types';
import type { ClusterPlacement } from './placement';
import { clusterStats } from './stats';
import type { PlacementPolicy, SiloDescriptor } from './types';

// ---------------------------------------------------------------------------
// The harness a transport supplies

export interface ConformanceClusterOptions {
    silos: number;
    actors: readonly AnyActorDefinition[];
    /** `null` = an unauthenticated cluster (no shared secret). */
    secret?: string | null;
    policy?: PlacementPolicy;
    retryBackoffMs?: number;
}

export interface TransportConformanceHarness {
    readonly placements: readonly ClusterPlacement[];
    readonly silos: readonly Silo[];
    /**
     * Wire-level unreachable while membership STILL lists the silo — the
     * partition probe, and the only way to exercise `unreachable` apart
     * from a crash.
     */
    unbind(index: number): void;
    /** Membership drops it AND the listener dies. No cleanup runs. */
    crash(index: number): void;
    /**
     * Speak this transport at a peer with the WRONG credentials. Optional
     * only because a transport may have no authenticated mode at all; one
     * that does must implement it, or the "a forged call is refused" case
     * cannot run.
     */
    impostor?(target: SiloDescriptor): Promise<{ ok: boolean; status?: number }>;
    /**
     * Live peer connections silo `index` currently holds. Absent for a
     * connectionless transport, and the link cases then skip visibly —
     * only a socket transport can fail them, which is exactly why they
     * belong in the shared suite rather than in one package's tests.
     */
    openLinks?(index: number): number;
    stop(): Promise<void>;
}

export type TransportConformanceFactory = (
    options: ConformanceClusterOptions
) => Promise<TransportConformanceHarness>;

export interface ConformanceSkip {
    skipped: string;
}

export interface ConformanceCase {
    readonly name: string;
    /** One line: what breaks in production when this case fails. */
    readonly why: string;
    run(create: TransportConformanceFactory): Promise<void | ConformanceSkip>;
}

// ---------------------------------------------------------------------------
// Assertions — deliberately framework-free

function assert(condition: unknown, message: string): asserts condition {
    if (!condition) throw new Error(`[transport conformance] ${message}`);
}

function assertEqual(actual: unknown, expected: unknown, what: string): void {
    const a = JSON.stringify(actual);
    const e = JSON.stringify(expected);
    assert(a === e, `${what}: expected ${e}, got ${a}`);
}

/** Poll until `check()` stops throwing, or give up. */
async function waitFor(check: () => void, timeoutMs = 2000): Promise<void> {
    const deadline = Date.now() + timeoutMs;
    let last: unknown;
    for (;;) {
        try {
            check();
            return;
        } catch (error) {
            last = error;
            if (Date.now() > deadline) throw last;
            await sleep(5);
        }
    }
}

function sleep(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
}

async function caught(run: () => Promise<unknown>): Promise<unknown> {
    try {
        await run();
    } catch (error) {
        return error;
    }
    throw new Error('[transport conformance] expected the call to throw, but it resolved');
}

function call(overrides: Partial<ActorCallContext> = {}): ActorCallContext {
    return { callChain: [], callId: mintCallId(), ...overrides };
}

/** Run `body` against a fresh cluster and always tear it down. */
async function withCluster<T>(
    create: TransportConformanceFactory,
    options: ConformanceClusterOptions,
    body: (harness: TransportConformanceHarness) => Promise<T>
): Promise<T> {
    const harness = await create(options);
    try {
        return await body(harness);
    } finally {
        await harness.stop().catch(() => {});
    }
}

// ---------------------------------------------------------------------------
// The actors every transport is tested with. Shared so a behaviour cannot
// differ because one transport's tests used a friendlier actor.

const ECHO = 'ConformanceEcho';
const STREAMER = 'ConformanceStreamer';
const CHAIN = 'ConformanceChain';

interface EchoState {
    count: number;
    activations: number;
}

/** Records activations so single-activation is observable, and can echo
 *  codec-carried values back so a bare `JSON.stringify` is detectable. */
function echoActor(log: string[] = []): AnyActorDefinition {
    return defineActor({
        type: ECHO,
        unguarded: true,
        state: (): EchoState => ({ count: 0, activations: 0 }),
        onActivate: (ctx) => {
            log.push(`activate:${ctx.key}`);
        },
        methods: (ctx) => ({
            async increment(by: number) {
                ctx.state.count += by;
                await ctx.save();
                return ctx.state.count;
            },
            get() {
                return ctx.state.count;
            },
            /** Round-trips whatever it is given — the codec probe. */
            echo(value: unknown) {
                return value;
            },
            /** Throws a BRANDED actor error, to prove the brand survives. */
            throwActorError() {
                throw new ActorStateConflictError(`${ECHO}/${ctx.key}`);
            },
            /** Throws an ordinary error, which must NOT gain an actor brand. */
            throwPlainError() {
                throw new Error('conformance: a plain failure');
            },
            async slow(ms: number) {
                await new Promise((r) => setTimeout(r, ms));
                return 'done';
            }
        })
    }) as unknown as AnyActorDefinition;
}

function streamerActor(log: string[]): AnyActorDefinition {
    return defineActor({
        type: STREAMER,
        unguarded: true,
        state: () => ({}),
        methods: () => ({
            warm() {
                return 'warm';
            }
        }),
        streams: () => ({
            async *countTo(to: number) {
                for (let i = 1; i <= to; i++) yield i;
            },
            async *forever() {
                try {
                    for (let i = 0; ; i++) {
                        yield i;
                        await new Promise((r) => setTimeout(r, 5));
                    }
                } finally {
                    log.push('forever:finally');
                }
            }
        })
    }) as unknown as AnyActorDefinition;
}

interface BackMethods {
    back(): Promise<string>;
}

/**
 * A→B→A across two silos. Re-entering the ORIGINAL activation while its turn
 * is still up-stack must be detected as a deadlock — which only works if the
 * call chain survived BOTH hops.
 */
function chainActors(): { alpha: AnyActorDefinition; beta: AnyActorDefinition } {
    const alpha = defineActor({
        type: `${CHAIN}Alpha`,
        unguarded: true,
        state: () => ({}),
        methods: (ctx: ActorContext<object>) => ({
            async poke() {
                return await (ctx.actor(beta, 'b') as unknown as { poke(): Promise<string> }).poke();
            },
            async back() {
                return 'alpha-back';
            },
            warm() {
                return 'warm';
            }
        })
    });
    const beta = defineActor({
        type: `${CHAIN}Beta`,
        unguarded: true,
        state: () => ({}),
        methods: (ctx: ActorContext<object>) => ({
            async poke() {
                return await (ctx.actor(alpha, 'a') as unknown as BackMethods).back();
            },
            warm() {
                return 'warm';
            }
        })
    });
    return {
        alpha: alpha as unknown as AnyActorDefinition,
        beta: beta as unknown as AnyActorDefinition
    };
}

// ---------------------------------------------------------------------------
// The cases

const unaryRoundTrip: ConformanceCase = {
    name: 'unary round-trip through a non-owner, with codec-carried values',
    why: 'a transport that JSON.stringifies raw values silently drops every registered codec handler and re-opens __proto__',
    run: (create) =>
        withCluster(create, { silos: 2, actors: [echoActor()], policy: selfSilo }, async (h) => {
            // selfSilo: whichever silo first touches a key owns it. Activate
            // on 1, then call from 0 so every call crosses the wire.
            await h.silos[1]!.dispatch({ type: ECHO, key: 'k' }, 'increment', [1], call());
            const total = await h.silos[0]!.dispatch(
                { type: ECHO, key: 'k' },
                'increment',
                [41],
                call()
            );
            assertEqual(total, 42, 'remote unary result');

            const when = new Date('2020-01-02T03:04:05.000Z');
            const back = (await h.silos[0]!.dispatch(
                { type: ECHO, key: 'k' },
                'echo',
                [{ when, tags: new Map([['a', 1]]) }],
                call()
            )) as { when: Date; tags: Map<string, number> };
            assert(back.when instanceof Date, 'a Date must survive the hop as a Date');
            assertEqual(back.when.toISOString(), when.toISOString(), 'Date value');
            assert(back.tags instanceof Map, 'a Map must survive the hop as a Map');
            assertEqual(back.tags.get('a'), 1, 'Map contents');
        })
};

const singleActivation: ConformanceCase = {
    name: 'single activation under a race from every silo',
    why: 'two activations of one key means two copies of the state, which is the invariant the whole system rests on',
    run: (create) => {
        const log: string[] = [];
        return withCluster(
            create,
            { silos: 3, actors: [echoActor(log)], policy: selfSilo },
            async (h) => {
                const results = (await Promise.all(
                    h.silos.map((s) => s.dispatch({ type: ECHO, key: 'race' }, 'increment', [1], call()))
                )) as number[];
                assertEqual([...results].sort((a, b) => a - b), [1, 2, 3], 'every caller succeeded');
                const activations = log.filter((e) => e === 'activate:race');
                assertEqual(activations.length, 1, 'exactly one activation of the key');
            }
        );
    }
};

const streamRoundTrip: ConformanceCase = {
    name: 'stream round-trip from a non-owner, in order, ending on the terminator',
    why: 'a transport that treats a closed connection as a clean end turns a truncated stream into a silently short one',
    run: (create) => {
        const log: string[] = [];
        return withCluster(
            create,
            { silos: 2, actors: [streamerActor(log)], policy: selfSilo },
            async (h) => {
                await h.silos[1]!.dispatch({ type: STREAMER, key: 's' }, 'warm', [], call());
                const seen: number[] = [];
                const stream = h.silos[0]!.dispatchStream!(
                    { type: STREAMER, key: 's' },
                    'countTo',
                    [4],
                    call()
                );
                for await (const chunk of stream) seen.push(chunk as number);
                assertEqual(seen, [1, 2, 3, 4], 'chunks arrive complete and in order');
            }
        );
    }
};

const streamCancellation: ConformanceCase = {
    name: "stream cancellation reaches the producer's finally",
    why: 'with N streams multiplexed on one connection, closing the socket is NOT the cancel signal — a leaked generator holds its activation forever',
    run: (create) => {
        const log: string[] = [];
        return withCluster(
            create,
            { silos: 2, actors: [streamerActor(log)], policy: selfSilo },
            async (h) => {
                await h.silos[1]!.dispatch({ type: STREAMER, key: 's' }, 'warm', [], call());
                const stream = h.silos[0]!.dispatchStream!(
                    { type: STREAMER, key: 's' },
                    'forever',
                    [],
                    call()
                );
                for await (const chunk of stream) {
                    if ((chunk as number) >= 2) break; // consumer gives up
                }
                await waitFor(() =>
                    assert(
                        log.includes('forever:finally'),
                        "the OWNER's generator must run its finally when the consumer breaks"
                    )
                );
            }
        );
    }
};

const deadlinePropagation: ConformanceCase = {
    name: 'the deadline crosses the hop and the caller gives up on time',
    why: 'sending an absolute timestamp instead of remaining-ms makes every deadline wrong by the clock skew between hosts',
    run: (create) =>
        withCluster(create, { silos: 2, actors: [echoActor()], policy: selfSilo }, async (h) => {
            await h.silos[1]!.dispatch({ type: ECHO, key: 'slowpoke' }, 'get', [], call());
            const started = Date.now();
            const error = await caught(() =>
                h.silos[0]!.dispatch(
                    { type: ECHO, key: 'slowpoke' },
                    'slow',
                    [600],
                    call({ deadline: Date.now() + 60 })
                )
            );
            assert(isActorError(error), `a blown deadline must be an actor error, got ${error}`);
            assertEqual((error as ActorErrorShape).kind, 'call-timeout', 'deadline error kind');
            // The caller gave up on ITS OWN budget. A transport that forwards
            // an absolute timestamp instead of remaining-ms fails here the
            // moment the two hosts' clocks disagree.
            assert(
                Date.now() - started < 400,
                'the caller must give up on its own deadline, not wait for the remote turn'
            );
        })
};

const wrongHostConvergence: ConformanceCase = {
    name: 'a lost activation race redirects with an owner hint and converges',
    why: 'losing the owner hint means the redirect cannot be followed, and the call fails where it should have re-routed',
    run: (create) =>
        withCluster(create, { silos: 2, actors: [echoActor()], policy: selfSilo }, async (h) => {
            // Racing front doors on one key: both silos try to claim, one
            // loses the directory race, throws wrong-host at itself, and
            // re-routes to the winner. Both calls must still succeed.
            const results = (await Promise.all([
                h.silos[0]!.dispatch({ type: ECHO, key: 'shared' }, 'increment', [1], call()),
                h.silos[1]!.dispatch({ type: ECHO, key: 'shared' }, 'increment', [1], call())
            ])) as number[];
            assertEqual([...results].sort((a, b) => a - b), [1, 2], 'both racing calls succeed');

            const all = h.placements.map((p) => p.counters());
            const sum = (pick: (c: (typeof all)[number]) => number): number =>
                all.reduce((total, c) => total + pick(c), 0);
            assertEqual(sum((c) => c.claimConflicts), 1, 'exactly one silo lost the claim');
            assertEqual(sum((c) => c.wrongHostRedirects), 1, 'the loser consumed a redirect');
            assertEqual(sum((c) => c.routingFailures), 0, 'routing converged');
        })
};

const unreachableClassification: ConformanceCase = {
    name: 'a dead peer surfaces as `unreachable`, not a raw wire error',
    why: 'the placement evicts, refreshes and retries on `unreachable`; anything it cannot classify is a hard failure instead',
    run: (create) =>
        withCluster(
            create,
            { silos: 2, actors: [echoActor()], policy: selfSilo, retryBackoffMs: 1 },
            async (h) => {
                await h.silos[1]!.dispatch({ type: ECHO, key: 'gone' }, 'increment', [1], call());
                // Membership still lists it; only the wire is dead. The
                // caller cannot re-place, so the error must reach it named.
                h.unbind(1);
                const error = await caught(() =>
                    h.silos[0]!.dispatch({ type: ECHO, key: 'gone' }, 'get', [], call())
                );
                assert(isActorError(error), `expected an actor error, got ${String(error)}`);
                const kind = (error as ActorErrorShape).kind;
                assert(
                    kind === 'unreachable' || kind === 'activation',
                    `a dead peer must classify as unreachable (or a converged activation failure), got "${kind}"`
                );
            }
        )
};

const crashReplacement: ConformanceCase = {
    name: 'a crashed owner is re-placed and its state comes back',
    why: 'if a crash is not distinguishable from a slow peer, a cluster either fails calls forever or double-activates',
    run: (create) =>
        withCluster(
            create,
            { silos: 2, actors: [echoActor()], policy: selfSilo, retryBackoffMs: 1 },
            async (h) => {
                await h.silos[1]!.dispatch({ type: ECHO, key: 'survivor' }, 'increment', [7], call());
                h.crash(1);
                const total = await h.silos[0]!.dispatch(
                    { type: ECHO, key: 'survivor' },
                    'increment',
                    [1],
                    call()
                );
                assertEqual(total, 8, 'state was recovered from storage on the survivor');
            }
        )
};

const errorRebranding: ConformanceCase = {
    name: 'actor errors keep their brand across the hop; plain errors do not gain one',
    why: 'a caller must not be able to tell a remote hop from a local dispatch — and a peer must not be able to fake a redirect',
    run: (create) =>
        withCluster(create, { silos: 2, actors: [echoActor()], policy: selfSilo }, async (h) => {
            await h.silos[1]!.dispatch({ type: ECHO, key: 'boom' }, 'get', [], call());

            const branded = await caught(() =>
                h.silos[0]!.dispatch({ type: ECHO, key: 'boom' }, 'throwActorError', [], call())
            );
            assert(isActorError(branded), 'a remote actor error must satisfy isActorError');
            assertEqual((branded as ActorErrorShape).kind, 'state-conflict', 'kind survives the hop');

            const plain = await caught(() =>
                h.silos[0]!.dispatch({ type: ECHO, key: 'boom' }, 'throwPlainError', [], call())
            );
            assert(
                !isActorError(plain),
                'an ordinary error must NOT arrive branded as an actor error — that brand is how a redirect is trusted'
            );
        })
};

const methodNotFound: ConformanceCase = {
    name: 'an unknown method answers `method-not-found`',
    why: 'a transport that 500s here turns a caller bug into an opaque server failure',
    run: (create) =>
        withCluster(create, { silos: 2, actors: [echoActor()], policy: selfSilo }, async (h) => {
            await h.silos[1]!.dispatch({ type: ECHO, key: 'nm' }, 'get', [], call());
            const error = await caught(() =>
                h.silos[0]!.dispatch({ type: ECHO, key: 'nm' }, 'noSuchMethod', [], call())
            );
            assert(isActorError(error), `expected an actor error, got ${String(error)}`);
            assertEqual(
                (error as ActorErrorShape).kind,
                'method-not-found',
                'unknown method kind'
            );
        })
};

const deadlockChain: ConformanceCase = {
    name: 'the call chain crosses hosts, so a cross-silo deadlock is detected',
    why: 'callChain is the first thing an ad-hoc frame format drops, and losing it turns a detected deadlock into a hang',
    run: (create) => {
        const { alpha, beta } = chainActors();
        return withCluster(
            create,
            { silos: 2, actors: [alpha, beta], policy: selfSilo },
            async (h) => {
                // Pin alpha/a to silo 0 and beta/b to silo 1, so a→b→a
                // crosses the wire twice and the chain must survive both.
                await h.silos[0]!.dispatch({ type: alpha.type, key: 'a' }, 'warm', [], call());
                await h.silos[1]!.dispatch({ type: beta.type, key: 'b' }, 'warm', [], call());
                const error = await caught(() =>
                    h.silos[0]!.dispatch({ type: alpha.type, key: 'a' }, 'poke', [], call())
                );
                assert(isActorError(error), `expected an actor error, got ${String(error)}`);
                assertEqual((error as ActorErrorShape).kind, 'deadlock', 're-entrant cycle kind');
                const message = (error as Error).message;
                assert(
                    message.includes(alpha.type) && message.includes(beta.type),
                    `the deadlock must name the FULL cross-host chain — got "${message}"`
                );
            }
        );
    }
};

const opsStatsChannel: ConformanceCase = {
    name: 'the ops channel answers, and reading it does not move the counters',
    why: 'a stats fan-out routed through normal dispatch makes every observation change what it observes',
    run: (create) =>
        withCluster(create, { silos: 3, actors: [echoActor()], policy: selfSilo }, async (h) => {
            await h.silos[0]!.dispatch({ type: ECHO, key: 'x' }, 'increment', [1], call());
            const before = h.placements.map((p) => p.counters().inboundDispatches);

            const report = await clusterStats(h.placements[0]!, { timeoutMs: 2000 });
            assertEqual(report.silos.length, 3, 'every silo answered');
            assertEqual(report.partial, false, 'no member was unreachable');
            for (const silo of report.silos) assertEqual(silo.v, 1, 'report payload version');

            const after = h.placements.map((p) => p.counters().inboundDispatches);
            assertEqual(after, before, 'reading stats must not count as an inbound dispatch');
        })
};

const authRejection: ConformanceCase = {
    name: 'a forged call is refused, counted, and classified `unauthorized`',
    why: 'the status-shaped code is load-bearing off HTTP too — drop it and every auth failure degrades to a bare error during a secret rotation',
    run: async (create) => {
        const harness = await create({ silos: 2, actors: [echoActor()], secret: 'right' });
        try {
            if (!harness.impostor) {
                return { skipped: 'the harness cannot speak this transport with wrong credentials' };
            }
            assertEqual(harness.placements[1]!.counters().authFailures, 0, 'clean start');
            const target = harness.placements[1]!.descriptor();
            const result = await harness.impostor(target);
            assert(!result.ok, 'a forged call must be refused');
            assertEqual(
                harness.placements[1]!.counters().authFailures,
                1,
                'the refusal must be counted — it is otherwise completely silent'
            );
            assert(
                result.status === 403,
                `the refusal must carry a status-shaped 403 so clusterStats classifies it as ` +
                    `"unauthorized" rather than "error" — got ${String(result.status)}`
            );
        } finally {
            await harness.stop().catch(() => {});
        }
    }
};

const gracefulHandoff: ConformanceCase = {
    name: 'the owner keeps serving through its drain and callers converge',
    why: 'a transport that goes silent on stop() turns every rolling deploy into dropped calls',
    run: (create) =>
        withCluster(
            create,
            { silos: 2, actors: [echoActor()], policy: selfSilo, retryBackoffMs: 5 },
            async (h) => {
                const keys = ['h1', 'h2', 'h3'];
                for (const key of keys) {
                    await h.silos[0]!.dispatch({ type: ECHO, key }, 'increment', [1], call());
                }
                // Traffic keeps arriving through silo 1 while silo 0 drains.
                const stopping = h.silos[0]!.stop({ timeoutMs: 2000 });
                const results = (await Promise.all(
                    keys.map((key) =>
                        h.silos[1]!.dispatch({ type: ECHO, key }, 'increment', [1], call())
                    )
                )) as number[];
                await stopping;
                assertEqual([...results].sort(), [2, 2, 2], 'every call landed, state intact');
            }
        )
};

const noLinkLeak: ConformanceCase = {
    name: 'no peer links survive stop()',
    why: 'a connection-oriented transport that forgets to close leaks a socket per peer per silo restart',
    run: async (create) => {
        const harness = await create({ silos: 2, actors: [echoActor()], policy: selfSilo });
        if (!harness.openLinks) {
            await harness.stop().catch(() => {});
            return { skipped: 'connectionless transport — it holds no links to leak' };
        }
        const links = harness.openLinks;
        await harness.silos[0]!.dispatch({ type: ECHO, key: 'l' }, 'increment', [1], call());
        await harness.silos[1]!.dispatch({ type: ECHO, key: 'l' }, 'increment', [1], call());
        await harness.stop();
        for (let i = 0; i < harness.silos.length; i++) {
            assertEqual(links(i), 0, `silo ${i} still holds peer links after stop()`);
        }
    }
};

const reapDeparted: ConformanceCase = {
    name: 'links to a departed silo are reaped on the membership change alone',
    why: 'silo ids are never reused, so a link to a departed peer can never be useful again — keeping it is a pure leak',
    run: async (create) => {
        const harness = await create({ silos: 2, actors: [echoActor()], policy: selfSilo });
        try {
            if (!harness.openLinks) {
                return { skipped: 'connectionless transport — it holds no links to reap' };
            }
            const links = harness.openLinks;
            await harness.silos[0]!.dispatch({ type: ECHO, key: 'r' }, 'increment', [1], call());
            await harness.silos[1]!.dispatch({ type: ECHO, key: 'r' }, 'increment', [1], call());
            harness.crash(1);
            // No call is made — the membership change alone must do it.
            await waitFor(() =>
                assertEqual(links(0), 0, 'links to the departed silo must be dropped')
            );
        } finally {
            await harness.stop().catch(() => {});
        }
    }
};

/** Deterministic placement: whichever silo first touches a key owns it. */
const selfSilo: PlacementPolicy = {
    name: 'conformance-self',
    choose: (_ref, _view, self) => self
};

/**
 * The suite. Order is deliberate — the cheapest, most fundamental cases
 * first, so a broken transport fails on "can it carry a value at all"
 * rather than on a subtle cancellation race.
 */
export const transportConformance: readonly ConformanceCase[] = [
    unaryRoundTrip,
    singleActivation,
    streamRoundTrip,
    streamCancellation,
    deadlinePropagation,
    wrongHostConvergence,
    unreachableClassification,
    crashReplacement,
    errorRebranding,
    methodNotFound,
    deadlockChain,
    opsStatsChannel,
    authRejection,
    gracefulHandoff,
    noLinkLeak,
    reapDeparted
];

export { selfSilo as conformancePolicy };
