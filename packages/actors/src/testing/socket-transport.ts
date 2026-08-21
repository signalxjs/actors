/**
 * The CLIENT-transport conformance suite (#99): what "this `ActorTransport`
 * carries the client contract" means, as runnable cases.
 *
 * Written against `fetchTransport()` FIRST, before any second client
 * transport existed — the incumbent passing is what proves the suite
 * describes the real contract rather than the newcomer's habits (the same
 * argument `cluster/testing.ts` makes for `transportConformance`).
 *
 * **Assert the outcome, never the mechanism.** A fetch transport carries a
 * call as one POST and a socket transport as one frame on a shared
 * connection; a case that pinned either would be false for the other. What
 * both must deliver: values round-trip the codec, errors arrive branded
 * with the endpoint's classification, a failed or cancelled call costs
 * nothing else, and fire-and-forget lands.
 *
 * The pre-response connection retry (#55) is deliberately NOT a case here:
 * it is `fetchTransport`-only behaviour, not part of this contract. A
 * per-call fetch that rejects provably never dispatched, so a retry is
 * safe; on a multiplexed socket a dropped connection fails every in-flight
 * call UN-retried (#99) — the transport cannot know which frames the server
 * consumed before the drop.
 */
import { defineActor } from '../define';
import type { ActorTransport } from '../client/index';
import type { AnyActorDefinition, Host } from '../types';
import type { ConformanceCase, ConformanceSkip } from './conformance';

// ---------------------------------------------------------------------------
// The harness a transport package supplies

export interface SocketTransportHarness {
    /** The transport under test, wired to a host serving `actors`. */
    transport: ActorTransport;
    /** That host — for outcome checks on the server side. */
    host: Host;
    /** Tear everything down: transport, connection, host. */
    stop(): Promise<void>;
}

/**
 * Build one harness serving exactly these definitions. Called per case, so
 * state never leaks between cases.
 */
export type SocketTransportFactory = (
    actors: readonly AnyActorDefinition[]
) => Promise<SocketTransportHarness>;

// ---------------------------------------------------------------------------
// Fixtures — built per case, so a parked turn can always be released

interface Fixtures {
    actors: AnyActorDefinition[];
    /** Resolvers for turns parked in `slow()`. */
    gates: Array<() => void>;
    releaseAll(): void;
}

function fixtures(): Fixtures {
    const gates: Array<() => void> = [];
    const Counter = defineActor({
        type: 'ConformanceCounter',
        allowAnonymous: true,
        state: () => ({ items: [] as unknown[] }),
        methods: (ctx) => ({
            async add(item: unknown) {
                ctx.state.items.push(item);
                await ctx.save();
                return ctx.state.items.length;
            },
            async total() {
                return ctx.state.items.length;
            },
            async echo(value: unknown) {
                return value;
            },
            async slow() {
                await new Promise<void>((resolve) => gates.push(resolve));
                return 'released';
            }
        }),
        streams: () => ({
            async *countTo(to: number) {
                for (let i = 1; i <= to; i++) yield i;
            },
            async *forever() {
                let i = 0;
                for (;;) {
                    yield i++;
                    await new Promise((resolve) => setTimeout(resolve, 5));
                }
            }
        })
    }) as unknown as AnyActorDefinition;
    const Refused = defineActor({
        type: 'ConformanceRefused',
        authorize: [
            () => {
                // Strict-`true` policies: anything else denies. `false` keeps
                // the case store-agnostic; the endpoint classifies it.
                return false;
            }
        ],
        state: () => ({}),
        methods: () => ({
            async peek() {
                return 'secret';
            }
        })
    }) as unknown as AnyActorDefinition;
    return {
        actors: [Counter, Refused],
        gates,
        releaseAll() {
            for (const release of gates.splice(0)) release();
        }
    };
}

// ---------------------------------------------------------------------------
// Assertions — deliberately framework-free

function assert(condition: unknown, message: string): asserts condition {
    if (!condition) throw new Error(`[socket-transport conformance] ${message}`);
}

function assertEqual(actual: unknown, expected: unknown, message: string): void {
    assert(
        actual === expected,
        `${message} — expected ${String(expected)}, got ${String(actual)}`
    );
}

const statusOf = (error: unknown): number | undefined =>
    (error as { status?: number } | null)?.status;

async function rejectionOf(promise: Promise<unknown>): Promise<unknown> {
    try {
        await promise;
    } catch (error) {
        return error;
    }
    throw new Error('[socket-transport conformance] expected the call to reject');
}

async function until(predicate: () => boolean | Promise<boolean>, what: string): Promise<void> {
    const deadline = Date.now() + 2_000;
    for (;;) {
        if (await predicate()) return;
        if (Date.now() > deadline) {
            throw new Error(`[socket-transport conformance] timed out waiting for ${what}`);
        }
        await new Promise((resolve) => setTimeout(resolve, 10));
    }
}

async function withHarness(
    create: SocketTransportFactory,
    body: (harness: SocketTransportHarness, fx: Fixtures) => Promise<void | ConformanceSkip>
): Promise<void | ConformanceSkip> {
    const fx = fixtures();
    const harness = await create(fx.actors);
    try {
        return await body(harness, fx);
    } finally {
        fx.releaseAll();
        await harness.stop();
    }
}

// ---------------------------------------------------------------------------
// The cases

const unaryRoundTrip: ConformanceCase<SocketTransportFactory> = {
    name: 'unary calls round-trip the codec, key first',
    why: 'every actor call a page makes is this shape; a codec fork corrupts values silently',
    run: (create) =>
        withHarness(create, async ({ transport }) => {
            assertEqual(
                await transport.call('ConformanceCounter#add', ['k1', 'apple']),
                1,
                'add returns the new length'
            );
            assertEqual(
                await transport.call('ConformanceCounter#total', ['k1']),
                1,
                'a second call reads the first one’s effect'
            );
            const sent = new Date(86_400_000);
            const back = await transport.call('ConformanceCounter#echo', ['k1', sent]);
            assert(back instanceof Date, 'codec types survive the wire');
            assertEqual(
                (back as Date).getTime(),
                sent.getTime(),
                'the value is the SAME instant'
            );
        })
};

const errorClassification: ConformanceCase<SocketTransportFactory> = {
    name: 'failures arrive branded with the endpoint classification, and cost nothing else',
    why: 'a page must tell "not allowed" from "does not exist" from "server broke" — and one refused widget must not break the rest',
    run: (create) =>
        withHarness(create, async ({ transport }) => {
            const denied = await rejectionOf(transport.call('ConformanceRefused#peek', ['k']));
            assert(
                statusOf(denied) === 401 || statusOf(denied) === 403,
                `a policy refusal reads as 401/403, got ${String(statusOf(denied))}`
            );
            const missing = await rejectionOf(transport.call('ConformanceNope#x', ['k']));
            assertEqual(statusOf(missing), 404, 'an unknown type reads as 404');
            const keyless = await rejectionOf(transport.call('ConformanceCounter#total', []));
            assertEqual(statusOf(keyless), 400, 'a missing key reads as 400');
            // The transport is still whole after three failures.
            assertEqual(
                await transport.call('ConformanceCounter#total', ['k1']),
                0,
                'calls keep working after failures'
            );
        })
};

const streamRoundTrip: ConformanceCase<SocketTransportFactory> = {
    name: 'streams yield every value in order, then end',
    why: 'a truncated or reordered stream reads as data loss to the page',
    run: (create) =>
        withHarness(create, async ({ transport }) => {
            const seen: unknown[] = [];
            for await (const value of transport.stream('ConformanceCounter#countTo', [
                'k',
                3
            ])) {
                seen.push(value);
            }
            assertEqual(seen.length, 3, 'three values arrive');
            assertEqual(seen[0], 1, 'in order, first');
            assertEqual(seen[2], 3, 'in order, last');
        })
};

const streamCancellation: ConformanceCase<SocketTransportFactory> = {
    name: 'a consumer break releases the stream server-side',
    why: 'a departed tab must not pin an activation (and its keep-alive) forever',
    run: (create) =>
        withHarness(create, async ({ transport, host }) => {
            const seen: unknown[] = [];
            for await (const value of transport.stream('ConformanceCounter#forever', ['k'])) {
                seen.push(value);
                if (seen.length === 2) break;
            }
            // Outcome, not mechanism: the activation becomes collectable
            // again, however the transport carried the cancellation.
            await until(
                () => host.activations().every((a) => !a.keptAlive),
                'the activation to release its keep-alive'
            );
            assertEqual(
                await transport.call('ConformanceCounter#total', ['k']),
                0,
                'the connection (or the next one) still serves calls'
            );
        })
};

const abortReleasesTheCall: ConformanceCase<SocketTransportFactory> = {
    name: 'aborting one call rejects it and costs nothing else',
    why: 'cancellation is per call; anything coarser turns one impatient widget into a page outage',
    run: (create) =>
        withHarness(create, async ({ transport }, fx) => {
            const controller = new AbortController();
            const pending = transport.call('ConformanceCounter#slow', ['k1'], {
                signal: controller.signal
            });
            // Only abort a call whose turn genuinely started.
            await until(() => fx.gates.length === 1, 'the slow turn to park');
            controller.abort();
            await rejectionOf(pending);
            fx.releaseAll();
            assertEqual(
                await transport.call('ConformanceCounter#total', ['k1']),
                0,
                'later calls are untouched'
            );
        })
};

const oneWayLands: ConformanceCase<SocketTransportFactory> = {
    name: 'a one-way call resolves early and still lands',
    why: 'fire-and-forget exists so a mutation can be cheap; silently dropping it would corrupt state',
    run: (create) =>
        withHarness(create, async ({ transport }) => {
            await transport.call('ConformanceCounter#add', ['k1', 'x'], { oneWay: true });
            await until(
                async () => (await transport.call('ConformanceCounter#total', ['k1'])) === 1,
                'the one-way effect to land'
            );
        })
};

const liveDelivery: ConformanceCase<SocketTransportFactory> = {
    name: 'live: a subscription delivers the current value and every later one',
    why: 'this is the reason a socket transport exists; without it live pages poll',
    run: (create) =>
        withHarness(create, async ({ transport }): Promise<void | ConformanceSkip> => {
            if (!transport.live) {
                return { skipped: 'transport has no live channel (no live() implementation)' };
            }
            const channel = transport.live();
            const seen: unknown[] = [];
            const off = channel.subscribe(
                { type: 'ConformanceCounter', key: 'k1', method: 'total' },
                (value) => seen.push(value)
            );
            try {
                await until(() => seen.length >= 1, 'the seed value');
                assertEqual(seen[0], 0, 'the seed is the current value');
                await transport.call('ConformanceCounter#add', ['k1', 'x']);
                await until(() => seen.includes(1), 'the mutated value to push');
            } finally {
                off();
            }
        })
};

/**
 * The suite. Order is deliberate — the cheapest, most fundamental cases
 * first, so a broken transport fails on "can it carry a value at all"
 * rather than on a cancellation race.
 */
export const socketTransportConformance: readonly ConformanceCase<SocketTransportFactory>[] = [
    unaryRoundTrip,
    errorClassification,
    streamRoundTrip,
    streamCancellation,
    abortReleasesTheCall,
    oneWayLands,
    liveDelivery
];
