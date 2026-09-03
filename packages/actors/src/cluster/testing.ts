/**
 * The host-to-host transport conformance suite: what "implements
 * `HostTransport` correctly" means, as runnable cases.
 *
 * Every transport runs the SAME cases against its own N-host harness, so a
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
 * throws on failure, so a transport package drives them from whatever runner
 * it uses. That is currently the packages in THIS workspace: the subpath is
 * wired by a tsconfig/vitest alias and deliberately absent from
 * `package.json` exports, so it cannot be imported from outside the repo
 * until it is promoted.
 */
import { mintCallId } from '../call-id';
import { defineActor } from '../define';
import { defineWorker } from '../define-worker';
import { ActorStateConflictError, isActorError, type ActorErrorShape } from '../errors';
import type { ActorCallContext, ActorContext, AnyActorDefinition, Host } from '../types';
import { consistentHashPolicy, type ClusterPlacement } from './placement';
import { clusterStats } from './stats';
import type { PlacementPolicy, HostDescriptor } from './types';

// ---------------------------------------------------------------------------
// The harness a transport supplies

export interface ConformanceClusterOptions {
    hosts: number;
    actors: readonly AnyActorDefinition[];
    /**
     * Per-host registration override (#214): host `i` registers THESE
     * instead of `actors` — the heterogeneous-cluster knob behind the
     * registration-aware cases. Optional for a harness to honor: those
     * cases verify the topology actually took effect (via
     * `descriptor().types`) and SKIP when it did not, so a harness that
     * ignores this reads as skipped rather than falsely green.
     */
    actorsFor?: (index: number) => readonly AnyActorDefinition[];
    /** `null` = an unauthenticated cluster (no shared secret). */
    secret?: string | null;
    policy?: PlacementPolicy;
    retryBackoffMs?: number;
}

export interface TransportConformanceHarness {
    readonly placements: readonly ClusterPlacement[];
    readonly hosts: readonly Host[];
    /**
     * Wire-level unreachable while membership STILL lists the host — the
     * partition probe, and the only way to exercise `unreachable` apart
     * from a crash.
     */
    unbind(index: number): void;
    /** Membership drops it AND the listener dies. No cleanup runs. */
    crash(index: number): void;
    /**
     * Remove host `index` from MEMBERSHIP ONLY, leaving every socket intact.
     *
     * Without this, the reaping case cannot test what it claims: a crashed
     * peer's sockets close on their own, so link counts fall for reasons that
     * have nothing to do with the membership view, and a transport that never
     * reaps still passes. Optional, but a connection-oriented transport whose
     * harness omits it gets a SKIP rather than a false pass.
     */
    dropMembership?(index: number): void;
    /**
     * Speak this transport at a peer with the WRONG credentials. Optional
     * only because a transport may have no authenticated mode at all; one
     * that does must implement it, or the "a forged call is refused" case
     * cannot run.
     */
    impostor?(target: HostDescriptor): Promise<{ ok: boolean; status?: number }>;
    /**
     * Live peer connections host `index` currently holds. Absent for a
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
    /**
     * Runner timeout, ms, for a case that legitimately outlasts a test
     * framework's default — a race that needs many fresh clusters to be
     * worth asserting. A runner passes it through; absent means the default.
     */
    readonly timeoutMs?: number;
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
        allowAnonymous: true,
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
            },
            /** The context-bag probe: a plain copy of what this turn sees. */
            bagOf() {
                return { ...ctx.bag };
            }
        })
    }) as unknown as AnyActorDefinition;
}

const WORKER = 'ConformanceWorker';

/** A stateless worker — the registration cases split registration by host,
 *  and a worker is what a heterogeneous edge host realistically registers. */
function workerActor(): AnyActorDefinition {
    return defineWorker({
        type: WORKER,
        allowAnonymous: true,
        methods: () => ({
            async ping(n: number) {
                return n + 1;
            }
        })
    }) as unknown as AnyActorDefinition;
}

function streamerActor(log: string[]): AnyActorDefinition {
    return defineActor({
        type: STREAMER,
        allowAnonymous: true,
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
 * A→B→A across two hosts. Re-entering the ORIGINAL activation while its turn
 * is still up-stack must be detected as a deadlock — which only works if the
 * call chain survived BOTH hops.
 */
function chainActors(): { alpha: AnyActorDefinition; beta: AnyActorDefinition } {
    const alpha = defineActor({
        type: `${CHAIN}Alpha`,
        allowAnonymous: true,
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
        allowAnonymous: true,
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

/**
 * The same A→B→A shape with an ALWAYS-reentrant alpha: the cycle must
 * complete as a concurrent turn instead of deadlocking — over the same
 * unchanged envelope, which is the point (reentrancy never rides the wire;
 * the owning host reads its own definition).
 */
function alwaysChainActors(): { alpha: AnyActorDefinition; beta: AnyActorDefinition } {
    const alpha = defineActor({
        type: `${CHAIN}AlwaysAlpha`,
        allowAnonymous: true,
        reentrant: 'always',
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
        type: `${CHAIN}AlwaysBeta`,
        allowAnonymous: true,
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
        withCluster(create, { hosts: 2, actors: [echoActor()], policy: selfHost }, async (h) => {
            // selfHost: whichever host first touches a key owns it. Activate
            // on 1, then call from 0 so every call crosses the wire.
            await h.hosts[1]!.dispatch({ type: ECHO, key: 'k' }, 'increment', [1], call());
            const total = await h.hosts[0]!.dispatch(
                { type: ECHO, key: 'k' },
                'increment',
                [41],
                call()
            );
            assertEqual(total, 42, 'remote unary result');

            const when = new Date('2020-01-02T03:04:05.000Z');
            const back = (await h.hosts[0]!.dispatch(
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
    name: 'single activation under a race from every host',
    why: 'two activations of one key means two copies of the state, which is the invariant the whole system rests on',
    run: (create) => {
        const log: string[] = [];
        return withCluster(
            create,
            { hosts: 3, actors: [echoActor(log)], policy: selfHost },
            async (h) => {
                const results = (await Promise.all(
                    h.hosts.map((s) => s.dispatch({ type: ECHO, key: 'race' }, 'increment', [1], call()))
                )) as number[];
                assertEqual([...results].sort((a, b) => a - b), [1, 2, 3], 'every caller succeeded');
                const activations = log.filter((e) => e === 'activate:race');
                assertEqual(activations.length, 1, 'exactly one activation of the key');
            }
        );
    }
};

/**
 * Every host sends each of its calls to a DIFFERENT peer, cycling, and never
 * to itself. With n hosts and n-1 calls per host every ordered pair of hosts
 * dials the other at the same instant — the simultaneous-dial arbitration a
 * connection-oriented transport has to survive with calls already on the
 * wire. `selfHost` can never reach it: the first caller owns the key and
 * nobody dials anyone until the second call, which is exactly the gap #353
 * fell through.
 */
function spreadingPolicy(): PlacementPolicy {
    const turns = new Map<string, number>();
    return {
        name: 'conformance-spread',
        choose: (_ref, view, self) => {
            const peers = view.hosts
                .filter((h) => h.hostId !== self.hostId)
                .sort((a, b) => (a.hostId < b.hostId ? -1 : 1));
            if (peers.length === 0) return self;
            const turn = turns.get(self.hostId) ?? 0;
            turns.set(self.hostId, turn + 1);
            return peers[turn % peers.length]!;
        }
    };
}

const concurrentFirstActivation: ConformanceCase = {
    name: 'a concurrent first activation reached through mutual dials applies every call exactly once',
    why: 'a call re-sent after its frame was on the wire runs a non-idempotent method twice — and a call left on a connection that closed underneath it never settles at all (#353)',
    // Twenty real-socket clusters: well inside a second here, but not
    // inside a 5 s default on a slow CI leg.
    timeoutMs: 60_000,
    run: async (create) => {
        // The race is a timing window on the first dial, so it needs a
        // FRESH cluster per attempt — once the links are up, nothing here
        // can dial again. ~1 in 7 attempts hit it unfixed; twenty make a
        // miss vanishingly unlikely while a fix passes every one.
        const attempts = 20;
        const perHost = 2;
        for (let attempt = 0; attempt < attempts; attempt++) {
            const log: string[] = [];
            await withCluster(
                create,
                { hosts: 3, actors: [echoActor(log)], policy: spreadingPolicy(), retryBackoffMs: 1 },
                async (h) => {
                    const key = `mutual${attempt}`;
                    const calls: Promise<unknown>[] = [];
                    for (const host of h.hosts) {
                        for (let i = 0; i < perHost; i++) {
                            calls.push(host.dispatch({ type: ECHO, key }, 'increment', [1], call()));
                        }
                    }
                    // Bounded: a call parked on a closed connection would
                    // otherwise hang the suite rather than fail it.
                    let timer: ReturnType<typeof setTimeout> | undefined;
                    const hung = new Promise<never>((_, reject) => {
                        timer = setTimeout(
                            () => reject(new Error(`[transport conformance] attempt ${attempt}: a call never settled`)),
                            5000
                        );
                    });
                    let results: number[];
                    try {
                        results = (await Promise.race([Promise.all(calls), hung])) as number[];
                    } finally {
                        clearTimeout(timer);
                    }
                    const expected = Array.from({ length: h.hosts.length * perHost }, (_, i) => i + 1);
                    assertEqual(
                        [...results].sort((a, b) => a - b),
                        expected,
                        `attempt ${attempt}: every increment applied exactly once`
                    );
                    const activations = log.filter((e) => e === `activate:${key}`);
                    assertEqual(activations.length, 1, `attempt ${attempt}: exactly one activation`);
                }
            );
        }
    }
};

const streamRoundTrip: ConformanceCase = {
    name: 'stream round-trip from a non-owner, in order, ending on the terminator',
    why: 'a transport that treats a closed connection as a clean end turns a truncated stream into a silently short one',
    run: (create) => {
        const log: string[] = [];
        return withCluster(
            create,
            { hosts: 2, actors: [streamerActor(log)], policy: selfHost },
            async (h) => {
                await h.hosts[1]!.dispatch({ type: STREAMER, key: 's' }, 'warm', [], call());
                const seen: number[] = [];
                const stream = h.hosts[0]!.dispatchStream!(
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
            { hosts: 2, actors: [streamerActor(log)], policy: selfHost },
            async (h) => {
                await h.hosts[1]!.dispatch({ type: STREAMER, key: 's' }, 'warm', [], call());
                const stream = h.hosts[0]!.dispatchStream!(
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

const watchRoundTrip: ConformanceCase = {
    name: 'a watch from a non-owner sees the owner mutate',
    why: 'the receiving host cannot tell a watch from a plain read by the method alone, so a transport that drops the intent silently serves a one-shot call instead of a subscription',
    run: (create) =>
        withCluster(create, { hosts: 2, actors: [echoActor()], policy: selfHost }, async (h) => {
            await h.hosts[1]!.dispatch({ type: ECHO, key: 'watched' }, 'get', [], call());
            const iterator = h.hosts[0]!
                .dispatchWatch!({ type: ECHO, key: 'watched' }, 'get', [], call())
                [Symbol.asyncIterator]();
            try {
                assertEqual((await iterator.next()).value, 0, 'the initial value');
                await h.hosts[1]!.dispatch({ type: ECHO, key: 'watched' }, 'increment', [7], call());
                assertEqual((await iterator.next()).value, 7, "the owner's mutation arrives");
            } finally {
                await iterator.return?.(undefined);
            }
        })
};

const watchCancellation: ConformanceCase = {
    name: "a watch disconnect releases the OWNER's keep-alive",
    why: 'the owner has no other way to learn the subscriber has gone — and an open watch counts as activity, so one never released pins that activation for the life of the process',
    run: (create) =>
        withCluster(create, { hosts: 2, actors: [echoActor()], policy: selfHost }, async (h) => {
            await h.hosts[1]!.dispatch({ type: ECHO, key: 'pinned' }, 'get', [], call());
            const iterator = h.hosts[0]!
                .dispatchWatch!({ type: ECHO, key: 'pinned' }, 'get', [], call())
                [Symbol.asyncIterator]();
            await iterator.next();

            const pinned = (): boolean =>
                h.hosts[1]!.activations().some((a) => a.key === 'pinned' && a.keptAlive);
            assert(pinned(), 'the open watch keeps the owner activation alive');

            // A QUIET actor on purpose: nothing will arrive to resume the
            // serving generator, so if the release depends on a value it
            // never happens at all.
            await iterator.return?.(undefined);
            await waitFor(() =>
                assert(!pinned(), "the owner's keep-alive is released when the consumer leaves")
            );
        })
};

const deadlinePropagation: ConformanceCase = {
    name: 'the deadline crosses the hop and the caller gives up on time',
    why: 'sending an absolute timestamp instead of remaining-ms makes every deadline wrong by the clock skew between hosts',
    run: (create) =>
        withCluster(create, { hosts: 2, actors: [echoActor()], policy: selfHost }, async (h) => {
            await h.hosts[1]!.dispatch({ type: ECHO, key: 'slowpoke' }, 'get', [], call());
            const started = Date.now();
            const error = await caught(() =>
                h.hosts[0]!.dispatch(
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
        withCluster(create, { hosts: 2, actors: [echoActor()], policy: selfHost }, async (h) => {
            // Racing front doors on one key: both hosts try to claim, one
            // loses the directory race, throws wrong-host at itself, and
            // re-routes to the winner. Both calls must still succeed.
            const results = (await Promise.all([
                h.hosts[0]!.dispatch({ type: ECHO, key: 'shared' }, 'increment', [1], call()),
                h.hosts[1]!.dispatch({ type: ECHO, key: 'shared' }, 'increment', [1], call())
            ])) as number[];
            assertEqual([...results].sort((a, b) => a - b), [1, 2], 'both racing calls succeed');

            const all = h.placements.map((p) => p.counters());
            const sum = (pick: (c: (typeof all)[number]) => number): number =>
                all.reduce((total, c) => total + pick(c), 0);
            assertEqual(sum((c) => c.claimConflicts), 1, 'exactly one host lost the claim');
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
            { hosts: 2, actors: [echoActor()], policy: selfHost, retryBackoffMs: 1 },
            async (h) => {
                await h.hosts[1]!.dispatch({ type: ECHO, key: 'gone' }, 'increment', [1], call());
                // Membership still lists it; only the wire is dead. The
                // caller cannot re-place, so the error must reach it named.
                h.unbind(1);
                const error = await caught(() =>
                    h.hosts[0]!.dispatch({ type: ECHO, key: 'gone' }, 'get', [], call())
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
            { hosts: 2, actors: [echoActor()], policy: selfHost, retryBackoffMs: 1 },
            async (h) => {
                await h.hosts[1]!.dispatch({ type: ECHO, key: 'survivor' }, 'increment', [7], call());
                h.crash(1);
                const total = await h.hosts[0]!.dispatch(
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
        withCluster(create, { hosts: 2, actors: [echoActor()], policy: selfHost }, async (h) => {
            await h.hosts[1]!.dispatch({ type: ECHO, key: 'boom' }, 'get', [], call());

            const branded = await caught(() =>
                h.hosts[0]!.dispatch({ type: ECHO, key: 'boom' }, 'throwActorError', [], call())
            );
            assert(isActorError(branded), 'a remote actor error must satisfy isActorError');
            assertEqual((branded as ActorErrorShape).kind, 'state-conflict', 'kind survives the hop');

            const plain = await caught(() =>
                h.hosts[0]!.dispatch({ type: ECHO, key: 'boom' }, 'throwPlainError', [], call())
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
        withCluster(create, { hosts: 2, actors: [echoActor()], policy: selfHost }, async (h) => {
            await h.hosts[1]!.dispatch({ type: ECHO, key: 'nm' }, 'get', [], call());
            const error = await caught(() =>
                h.hosts[0]!.dispatch({ type: ECHO, key: 'nm' }, 'noSuchMethod', [], call())
            );
            assert(isActorError(error), `expected an actor error, got ${String(error)}`);
            assertEqual(
                (error as ActorErrorShape).kind,
                'method-not-found',
                'unknown method kind'
            );
        })
};

const prototypeMemberNotFound: ConformanceCase = {
    name: 'an inherited `Object.prototype` member is not a method either',
    why: 'the method table is an object literal, so `toString`/`constructor` used to DISPATCH — a transport must not resurrect surface nobody declared',
    run: (create) =>
        withCluster(create, { hosts: 2, actors: [echoActor()], policy: selfHost }, async (h) => {
            await h.hosts[1]!.dispatch({ type: ECHO, key: 'proto' }, 'get', [], call());
            for (const member of ['toString', 'constructor', 'valueOf', '__proto__']) {
                const error = await caught(() =>
                    h.hosts[0]!.dispatch({ type: ECHO, key: 'proto' }, member, [], call())
                );
                assert(
                    isActorError(error),
                    `expected an actor error for "${member}", got ${String(error)}`
                );
                assertEqual(
                    (error as ActorErrorShape).kind,
                    'method-not-found',
                    `"${member}" kind`
                );
            }
        })
};

const deadlockChain: ConformanceCase = {
    name: 'the call chain crosses hosts, so a cross-host deadlock is detected',
    why: 'callChain is the first thing an ad-hoc frame format drops, and losing it turns a detected deadlock into a hang',
    run: (create) => {
        const { alpha, beta } = chainActors();
        return withCluster(
            create,
            { hosts: 2, actors: [alpha, beta], policy: selfHost },
            async (h) => {
                // Pin alpha/a to host 0 and beta/b to host 1, so a→b→a
                // crosses the wire twice and the chain must survive both.
                await h.hosts[0]!.dispatch({ type: alpha.type, key: 'a' }, 'warm', [], call());
                await h.hosts[1]!.dispatch({ type: beta.type, key: 'b' }, 'warm', [], call());
                const error = await caught(() =>
                    h.hosts[0]!.dispatch({ type: alpha.type, key: 'a' }, 'poke', [], call())
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

const alwaysChain: ConformanceCase = {
    name: "an always-reentrant cross-host cycle completes instead of deadlocking",
    why: "reentrant: 'always' never rides the wire — the owning host must admit the in-chain call from its own definition, on the unchanged envelope",
    run: (create) => {
        const { alpha, beta } = alwaysChainActors();
        return withCluster(
            create,
            { hosts: 2, actors: [alpha, beta], policy: selfHost },
            async (h) => {
                // Same pinning as deadlockChain: a→b→a crosses the wire
                // twice; the chain arrives intact and alpha's 'always' mode
                // turns the would-be deadlock into a concurrent turn.
                await h.hosts[0]!.dispatch({ type: alpha.type, key: 'a' }, 'warm', [], call());
                await h.hosts[1]!.dispatch({ type: beta.type, key: 'b' }, 'warm', [], call());
                const result = await h.hosts[0]!.dispatch(
                    { type: alpha.type, key: 'a' },
                    'poke',
                    [],
                    call()
                );
                assertEqual(result, 'alpha-back', 'always-reentrant cycle result');
            }
        );
    }
};

const opsStatsChannel: ConformanceCase = {
    name: 'the ops channel answers, and reading it does not move the counters',
    why: 'a stats fan-out routed through normal dispatch makes every observation change what it observes',
    run: (create) =>
        withCluster(create, { hosts: 3, actors: [echoActor()], policy: selfHost }, async (h) => {
            await h.hosts[0]!.dispatch({ type: ECHO, key: 'x' }, 'increment', [1], call());
            const before = h.placements.map((p) => p.counters().inboundDispatches);

            const report = await clusterStats(h.placements[0]!, { timeoutMs: 2000 });
            assertEqual(report.hosts.length, 3, 'every host answered');
            assertEqual(report.partial, false, 'no member was unreachable');
            for (const host of report.hosts) assertEqual(host.v, 1, 'report payload version');

            const after = h.placements.map((p) => p.counters().inboundDispatches);
            assertEqual(after, before, 'reading stats must not count as an inbound dispatch');
        })
};

const authRejection: ConformanceCase = {
    name: 'a forged call is refused, counted, and classified `unauthorized`',
    why: 'the status-shaped code is load-bearing off HTTP too — drop it and every auth failure degrades to a bare error during a secret rotation',
    run: async (create) => {
        const harness = await create({ hosts: 2, actors: [echoActor()], secret: 'right' });
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
            { hosts: 2, actors: [echoActor()], policy: selfHost, retryBackoffMs: 5 },
            async (h) => {
                const keys = ['h1', 'h2', 'h3'];
                for (const key of keys) {
                    await h.hosts[0]!.dispatch({ type: ECHO, key }, 'increment', [1], call());
                }
                // Traffic keeps arriving through host 1 while host 0 drains.
                const stopping = h.hosts[0]!.stop({ timeoutMs: 2000 });
                const results = (await Promise.all(
                    keys.map((key) =>
                        h.hosts[1]!.dispatch({ type: ECHO, key }, 'increment', [1], call())
                    )
                )) as number[];
                await stopping;
                assertEqual([...results].sort(), [2, 2, 2], 'every call landed, state intact');
            }
        )
};

const noLinkLeak: ConformanceCase = {
    name: 'no peer links survive stop()',
    why: 'a connection-oriented transport that forgets to close leaks a socket per peer per host restart',
    run: async (create) => {
        const harness = await create({ hosts: 2, actors: [echoActor()], policy: selfHost });
        if (!harness.openLinks) {
            await harness.stop().catch(() => {});
            return { skipped: 'connectionless transport — it holds no links to leak' };
        }
        const links = harness.openLinks;
        await harness.hosts[0]!.dispatch({ type: ECHO, key: 'l' }, 'increment', [1], call());
        await harness.hosts[1]!.dispatch({ type: ECHO, key: 'l' }, 'increment', [1], call());
        await harness.stop();
        for (let i = 0; i < harness.hosts.length; i++) {
            assertEqual(links(i), 0, `host ${i} still holds peer links after stop()`);
        }
    }
};

const reapDeparted: ConformanceCase = {
    name: 'links to a departed host are reaped on the membership change alone',
    why: 'host ids are never reused, so a link to a departed peer can never be useful again — keeping it is a pure leak',
    run: async (create) => {
        const harness = await create({ hosts: 2, actors: [echoActor()], policy: selfHost });
        try {
            if (!harness.openLinks) {
                return { skipped: 'connectionless transport — it holds no links to reap' };
            }
            if (!harness.dropMembership) {
                // Falling back to `crash` here would pass vacuously: the
                // crashed peer's sockets close by themselves, so the link
                // count falls whether or not anything reaps on membership.
                return {
                    skipped:
                        'harness cannot drop membership without also closing sockets, so this ' +
                        'case could only pass vacuously'
                };
            }
            const links = harness.openLinks;
            await harness.hosts[0]!.dispatch({ type: ECHO, key: 'r' }, 'increment', [1], call());
            await harness.hosts[1]!.dispatch({ type: ECHO, key: 'r' }, 'increment', [1], call());
            await waitFor(() =>
                assert(links(0) > 0, 'the driver must hold a link before this case means anything')
            );
            // Sockets stay UP. Only the view changes, so nothing but the
            // membership subscription can cause the link to be dropped.
            harness.dropMembership(1);
            await waitFor(() =>
                assertEqual(links(0), 0, 'links to the departed host must be dropped')
            );
        } finally {
            await harness.stop().catch(() => {});
        }
    }
};

const contextBagHop: ConformanceCase = {
    name: 'the context bag survives a host-to-host hop, intact and only when valid',
    why: 'a transport that drops or mangles the envelope bag silently turns an edge-authenticated call anonymous on every remote dispatch',
    run: (create) =>
        withCluster(create, { hosts: 2, actors: [echoActor()], policy: selfHost }, async (h) => {
            const ref = { type: ECHO, key: 'bagged' };
            // selfHost: activate on 1, call from 0 so the bag crosses the wire.
            await h.hosts[1]!.dispatch(ref, 'get', [], call());
            const bag = Object.freeze({ user: 'ada', 'corr-id': 'r-42' });
            const seen = await h.hosts[0]!.dispatch(ref, 'bagOf', [], call({ bag }));
            assertEqual(seen, bag, 'the bag arrives intact on the owning host');
            // And absence stays absence — a call with no bag must not
            // inherit the previous call's.
            const empty = await h.hosts[0]!.dispatch(ref, 'bagOf', [], call());
            assertEqual(empty, {}, 'a bagless call reads an empty bag');
        })
};

const oneWayAcceptance: ConformanceCase = {
    name: 'one-way dispatch acks on acceptance, delivers exactly once, drops the post-acceptance failure',
    why: 'a transport that answers a one-way call only after the turn turns fire-and-forget back into a blocking call, and one that retries after the ack delivers twice',
    run: (create) =>
        withCluster(create, { hosts: 2, actors: [echoActor()], policy: selfHost }, async (h) => {
            const ref = { type: ECHO, key: 'oneway' };
            // selfHost: activate on 1 so every call from 0 crosses the wire.
            await h.hosts[1]!.dispatch(ref, 'get', [], call());

            // Acceptance, not completion: the one-way resolves while the slow
            // turn is still holding the activation. Margins are generous — the
            // hop itself is milliseconds; only waiting for the turn could
            // cost the full 500.
            const started = Date.now();
            const result = await h.hosts[0]!.dispatch(ref, 'slow', [500], call({ oneWay: true }));
            const elapsed = Date.now() - started;
            assertEqual(result, undefined, 'a one-way call resolves undefined');
            assert(
                elapsed < 300,
                `the ack must come at acceptance, not turn completion (took ${elapsed}ms)`
            );

            // Exactly once: the increment lands (behind the slow turn), and
            // exactly one copy of it — a transport that retries after the
            // ack would double-deliver.
            await h.hosts[0]!.dispatch(ref, 'increment', [5], call({ oneWay: true }));
            const settled = async (): Promise<number> =>
                (await h.hosts[0]!.dispatch(ref, 'get', [], call())) as number;
            const deadline = Date.now() + 3000;
            for (;;) {
                const count = await settled();
                if (count === 5) break;
                assert(Date.now() < deadline, `the one-way increment never landed (count ${count})`);
                await sleep(10);
            }
            await sleep(25);
            assertEqual(await settled(), 5, 'delivered exactly once — no post-ack retry');

            // A post-acceptance failure is dropped, never delivered: the
            // await below must RESOLVE, and the queue is not poisoned.
            await h.hosts[0]!.dispatch(ref, 'throwPlainError', [], call({ oneWay: true }));
            assertEqual(
                await h.hosts[0]!.dispatch(ref, 'increment', [1], call()),
                6,
                'turns keep running after a dropped one-way failure'
            );
        })
};

const heterogeneousPlacement: ConformanceCase = {
    name: 'a heterogeneous cluster routes a type only to hosts that register it',
    why: 'membership without registration-aware placement (#212) lets a type land on a host that never registered it — the rolling-deploy 404, and the reason an edge role could not join the cluster',
    run: (create) =>
        withCluster(
            create,
            {
                hosts: 2,
                actors: [echoActor()],
                actorsFor: (i) => (i === 0 ? [echoActor()] : [workerActor()]),
                // Deterministic over the ELIGIBLE view. Deliberately not the
                // suite's selfHost: a self-answering policy on the
                // non-registering host would dispatch locally by contract,
                // which is the policy's bug to own, not this case's subject.
                policy: consistentHashPolicy()
            },
            async (h) => {
                const edge = h.placements[1]!.descriptor().types;
                if (edge === undefined || edge.includes(ECHO)) {
                    return {
                        skipped: 'the harness does not honor per-host registration (actorsFor)'
                    };
                }
                // Deltas against a baseline, not absolute counts: the case
                // pins what THESE calls did, whatever the harness dispatched
                // while warming up.
                const inbound0 = h.placements[0]!.counters().inboundDispatches;
                const remote1 = h.placements[1]!.counters().remoteDispatches;
                // From the NON-registering host, across a SPREAD of keys:
                // every call must cross the wire and be served by the host
                // that registers the type. Many keys on purpose — a single
                // key can rendezvous onto the right host by luck, and a case
                // that fails only probabilistically pins nothing.
                for (let i = 0; i < 16; i++) {
                    const result = await h.hosts[1]!.dispatch(
                        { type: ECHO, key: `het${i}` },
                        'increment',
                        [5],
                        call()
                    );
                    assertEqual(result, 5, `key het${i} from the non-registering host`);
                }
                assert(
                    h.placements[1]!.counters().remoteDispatches - remote1 >= 16,
                    'every call crossed the wire instead of activating locally'
                );
                assertEqual(
                    h.placements[0]!.counters().inboundDispatches - inbound0,
                    16,
                    'the registering host served all of them'
                );
                // And the worker registered only on the edge host is
                // reachable FROM the other side, landing where it is
                // registered — same key spread, same reason.
                const inbound1 = h.placements[1]!.counters().inboundDispatches;
                for (let i = 0; i < 8; i++) {
                    const pong = await h.hosts[0]!.dispatch(
                        { type: WORKER, key: `w${i}` },
                        'ping',
                        [1],
                        call()
                    );
                    assertEqual(pong, 2, `worker key w${i} answered`);
                }
                assertEqual(
                    h.placements[1]!.counters().inboundDispatches - inbound1,
                    8,
                    'the worker executed only on the host that registers it'
                );
            }
        )
};

const targetedWorkerCalls: ConformanceCase = {
    name: 'a targeted worker call lands on the chosen member; a non-registering target refuses wrong-host',
    why: 'dispatchOn carries the fan-out/delivery path (#213) over this wire — a transport that loses the refusal kind turns a stale target into an unbranded failure instead of an answer',
    run: (create) =>
        withCluster(
            create,
            {
                hosts: 2,
                actors: [echoActor()],
                actorsFor: (i) => (i === 0 ? [echoActor(), workerActor()] : [echoActor()]),
                policy: consistentHashPolicy()
            },
            async (h) => {
                const p0 = h.placements[0]!;
                const p1 = h.placements[1]!;
                if (!p0.dispatchOn || !p1.dispatchOn) {
                    return { skipped: 'the harness placement predates dispatchOn (#213)' };
                }
                const registrar = p0.descriptor().types;
                if (
                    registrar === undefined ||
                    !registrar.includes(WORKER) ||
                    p1.descriptor().types?.includes(WORKER) !== false
                ) {
                    return {
                        skipped: 'the harness does not honor per-host registration (actorsFor)'
                    };
                }
                // Delivered ON the chosen member, from a host that does not
                // register the worker at all. Delta against a baseline, so
                // harness warmup dispatches cannot satisfy the assertion.
                const inbound0 = p0.counters().inboundDispatches;
                const pong = await p1.dispatchOn(
                    p0.identity.hostId,
                    { type: WORKER, key: 't' },
                    'ping',
                    [1]
                );
                assertEqual(pong, 2, 'the targeted call answered');
                assert(
                    p0.counters().inboundDispatches - inbound0 >= 1,
                    'the targeted host served it'
                );
                // A target that does not register the type refuses with the
                // WRONG-HOST kind and NO owner hint — an answer, carried by
                // this wire, never consumed as a re-route.
                const retries0 = p0.counters().retries;
                const error = await caught(() =>
                    p0.dispatchOn!(p1.identity.hostId, { type: WORKER, key: 't' }, 'ping', [1])
                );
                assert(isActorError(error), `expected an actor error, got ${String(error)}`);
                assertEqual((error as ActorErrorShape).kind, 'wrong-host', 'refusal kind');
                assertEqual(
                    (error as { owner?: unknown }).owner,
                    undefined,
                    'the refusal carries no owner hint'
                );
                assertEqual(p0.counters().retries - retries0, 0, 'one attempt — never retried');
            }
        )
};

/** Deterministic placement: whichever host first touches a key owns it. */
const selfHost: PlacementPolicy = {
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
    concurrentFirstActivation,
    streamRoundTrip,
    streamCancellation,
    watchRoundTrip,
    watchCancellation,
    deadlinePropagation,
    wrongHostConvergence,
    unreachableClassification,
    crashReplacement,
    errorRebranding,
    methodNotFound,
    prototypeMemberNotFound,
    deadlockChain,
    alwaysChain,
    opsStatsChannel,
    oneWayAcceptance,
    contextBagHop,
    heterogeneousPlacement,
    targetedWorkerCalls,
    authRejection,
    gracefulHandoff,
    noLinkLeak,
    reapDeparted
];

export { selfHost as conformancePolicy };
