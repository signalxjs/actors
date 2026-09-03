/**
 * `durableObjectPlacement` — routing a ref to the object that holds it.
 *
 * No Workers runtime: a fake namespace maps object names to real hosts, each
 * behind the real internal endpoint (`handleHostRequestForRuntime` +
 * `hostEndpointRuntime`). So the wire under test is the shipped one —
 * envelope, codec, NDJSON, branded errors — and only the platform is faked.
 */
import { describe, expect, it, vi } from 'vitest';
import {
    defineActor,
    defineWorker,
    isActorError,
    type AnyActorDefinition
} from '@sigx/actors';
import { createHost, manualScheduler, type Host } from '@sigx/actors/host';
import {
    consistentHashPolicy,
    handleHostRequestForRuntime,
    hostEndpointRuntime
} from '@sigx/actors/cluster';
import { durableObjectPlacement, durableObjectStorage } from '@sigx/actors-cloudflare';
import type { DurableObjectNamespaceLike } from '@sigx/actors-cloudflare';

const SEP = '\u0000';
const BASE = '/_sigx/do';

const Counter = defineActor({
    type: 'Counter',
    allowAnonymous: true,
    state: () => ({ count: 0 }),
    methods: (ctx) => ({
        async increment(by: number) {
            ctx.state.count += by;
            await ctx.save();
            return ctx.state.count;
        },
        async read() {
            return ctx.state.count;
        },
        /** Calls ANOTHER actor — the path that used to corrupt state. */
        async bumpPeer(key: string) {
            return ctx.actor(Counter, key).increment(1);
        },
        /** Calls a stateless worker — which must run HERE, in this object. */
        async viaWorker(n: number) {
            return ctx.actor(Work, 'any').double(n);
        },
        async boom() {
            throw new Error('nope');
        }
    }),
    streams: (ctx) => ({
        async *watch() {
            yield* ctx.changes({ initial: true });
        }
    })
});

const Work = defineWorker({
    type: 'Work',
    allowAnonymous: true,
    maxLocal: 2,
    methods: () => ({
        async double(n: number) {
            return n * 2;
        }
    })
});

/**
 * A Durable Object namespace whose objects are real hosts.
 *
 * Each object gets its own storage map — which is the whole point: a test
 * can look at exactly which object's storage a write landed in.
 */
function fakeNamespace(): DurableObjectNamespaceLike & {
    readonly objects: Map<string, { host: Host; store: Map<string, unknown> }>;
    stop(): Promise<void>;
} {
    const objects = new Map<string, { host: Host; store: Map<string, unknown> }>();

    // Keyed by the IN-FLIGHT creation, not the finished object: two
    // concurrent calls for one name must yield one object, exactly as the
    // platform guarantees. Awaiting between the miss and the insert would
    // otherwise start two hosts and leak the one that loses.
    const creating = new Map<string, Promise<{ host: Host; store: Map<string, unknown> }>>();

    const build = async (name: string) => {
        const store = new Map<string, unknown>();
        const storage = durableObjectStorage({
            get: async <T,>(k: string) =>
                store.has(k) ? (structuredClone(store.get(k)) as T) : undefined,
            put: async <T,>(k: string, v: T) => void store.set(k, structuredClone(v)),
            delete: async (k: string) => store.delete(k)
        });
        const host = createHost({
            actors: [Counter, Work],
            storage,
            scheduler: manualScheduler(),
            // The object hosts exactly the actor its name encodes; anything
            // else goes back out to the object that owns it.
            placement: durableObjectPlacement({
                namespace: namespace,
                isSelf: (ref) => `${ref.type}${SEP}${ref.key}` === name,
                base: BASE
            }),
            defaults: { sweepIntervalMs: 0, callTimeoutMs: 0 }
        });
        await host.start();
        const entry = { host, store };
        objects.set(name, entry);
        return entry;
    };

    const hostFor = (name: string): Promise<{ host: Host; store: Map<string, unknown> }> =>
        creating.get(name) ?? (creating.set(name, build(name)), creating.get(name)!);

    const namespace: DurableObjectNamespaceLike & {
        readonly objects: typeof objects;
        stop(): Promise<void>;
    } = {
        objects,
        idFromName: (name: string) => ({ name, toString: () => name }),
        get: (id) => ({
            async fetch(input: string | Request, init?: RequestInit) {
                const { host } = await hostFor(id.name!);
                const request =
                    typeof input === 'string' ? new Request(input, init) : input;
                return handleHostRequestForRuntime(request, {
                    // The mount names its own base, exactly as the real
                    // object does: everything after it IS the symbol
                    // (signalxjs/core#563).
                    base: BASE,
                    runtime: hostEndpointRuntime(host)
                });
            }
        }),
        async stop() {
            for (const { host } of objects.values()) await host.stop({ timeoutMs: 1_000 });
        }
    };
    return namespace;
}

/** The Worker side: no `isSelf`, so every call is a stub fetch. */
function workerPlacement(namespace: DurableObjectNamespaceLike) {
    return durableObjectPlacement({ namespace, base: BASE });
}

async function workerHost(namespace: DurableObjectNamespaceLike): Promise<Host> {
    const host = createHost({
        actors: [Counter, Work],
        scheduler: manualScheduler(),
        placement: workerPlacement(namespace),
        defaults: { sweepIntervalMs: 0, callTimeoutMs: 0 }
    });
    await host.start();
    return host;
}

describe('durableObjectPlacement', () => {
    it('routes a call to the object the ref names, and state persists there', async () => {
        const ns = fakeNamespace();
        const worker = await workerHost(ns);
        try {
            await expect(worker.actor(Counter, 'a').increment(2)).resolves.toBe(2);
            await expect(worker.actor(Counter, 'a').increment(3)).resolves.toBe(5);

            // One object, named for the actor — and the state is in ITS store.
            expect([...ns.objects.keys()]).toEqual([`Counter${SEP}a`]);
            const store = ns.objects.get(`Counter${SEP}a`)!.store;
            expect([...store.keys()]).toEqual([`sigx:state${SEP}Counter${SEP}a`]);
        } finally {
            await worker.stop({ timeoutMs: 1_000 });
            await ns.stop();
        }
    });

    it('gives two keys two objects, and the same key one', async () => {
        const ns = fakeNamespace();
        const worker = await workerHost(ns);
        try {
            await worker.actor(Counter, 'a').increment(1);
            await worker.actor(Counter, 'b').increment(1);
            await worker.actor(Counter, 'a').increment(1);

            expect(new Set(ns.objects.keys())).toEqual(
                new Set([`Counter${SEP}a`, `Counter${SEP}b`])
            );
            await expect(worker.actor(Counter, 'a').read()).resolves.toBe(2);
            await expect(worker.actor(Counter, 'b').read()).resolves.toBe(1);
        } finally {
            await worker.stop({ timeoutMs: 1_000 });
            await ns.stop();
        }
    });

    it('a stateless worker runs in the calling isolate — no object is created', async () => {
        const ns = fakeNamespace();
        const worker = await workerHost(ns);
        try {
            // From the Worker (edge): dispatches locally, touches no namespace.
            await expect(worker.actor(Work, 'any').double(2)).resolves.toBe(4);
            expect(ns.objects.size).toBe(0);
            // From inside a Durable Object: the worker runs in THAT object's
            // isolate — still no `Work` object anywhere.
            await expect(worker.actor(Counter, 'a').viaWorker(5)).resolves.toBe(10);
            expect([...ns.objects.keys()]).toEqual([`Counter${SEP}a`]);
        } finally {
            await worker.stop({ timeoutMs: 1_000 });
            await ns.stop();
        }
    });

    it('sends an actor-to-actor call OUT to the callee object', async () => {
        // The regression this design exists for. With the plain local host
        // inside a Durable Object, `ctx.actor(Counter, 'b')` from inside
        // Counter/a would activate Counter/b *in a's object* and write b's
        // record into a's storage — single activation violated, silently.
        const ns = fakeNamespace();
        const worker = await workerHost(ns);
        try {
            await expect(worker.actor(Counter, 'a').bumpPeer('b')).resolves.toBe(1);

            const aStore = ns.objects.get(`Counter${SEP}a`)!.store;
            const bStore = ns.objects.get(`Counter${SEP}b`)!.store;
            // b's state is in b's object...
            expect([...bStore.keys()]).toEqual([`sigx:state${SEP}Counter${SEP}b`]);
            // ...and NOT in a's.
            expect([...aStore.keys()]).not.toContain(`sigx:state${SEP}Counter${SEP}b`);

            // And the value is visible through b's own object.
            await expect(worker.actor(Counter, 'b').read()).resolves.toBe(1);
        } finally {
            await worker.stop({ timeoutMs: 1_000 });
            await ns.stop();
        }
    });

    it('answers its OWN actor with the local dispatcher, deriving no stub', () => {
        // Asserted directly rather than through a call: a self-call via
        // `ctx.actor` is a re-entrant call and the runtime's deadlock
        // detection owns that behaviour, which would mask what is being
        // checked here — that `isSelf` short-circuits before any stub is
        // derived, so a Durable Object can never fetch itself.
        let derived = 0;
        const placement = durableObjectPlacement({
            namespace: {
                idFromName: (name) => {
                    derived++;
                    return { name, toString: () => name };
                },
                get: () => ({ fetch: async () => new Response('never') })
            },
            isSelf: (ref) => ref.key === 'mine'
        });
        const localDispatcher = { dispatch: async () => 'local' };
        placement.bind!(localDispatcher, undefined as never);

        expect(placement.dispatcherFor({ type: 'Counter', key: 'mine' })).toBe(
            localDispatcher
        );
        expect(derived).toBe(0);

        expect(placement.dispatcherFor({ type: 'Counter', key: 'other' })).not.toBe(
            localDispatcher
        );
        expect(derived).toBe(1);
    });

    it('streams NDJSON back through the stub', async () => {
        const ns = fakeNamespace();
        const worker = await workerHost(ns);
        try {
            const seen: unknown[] = [];
            for await (const chunk of worker.actor(Counter, 'a').watch()) {
                seen.push(chunk);
                if (seen.length === 1) break;
            }
            expect(seen).toEqual([{ count: 0 }]);
        } finally {
            await worker.stop({ timeoutMs: 1_000 });
            await ns.stop();
        }
    });

    it('keeps an ACTOR error branded across the hop', async () => {
        // `fromHostWireError` re-creates the error on this side, and the
        // brand surviving is a conformance requirement — a forked wire is
        // how a remote `state-conflict` quietly stops being one, and the
        // runtime stops discarding stale activations.
        //
        // A re-entrant self-call is the cheapest genuine actor error to
        // raise inside the object: `bumpPeer('a')` on Counter/a.
        const ns = fakeNamespace();
        const worker = await workerHost(ns);
        try {
            const error = await worker
                .actor(Counter, 'a')
                .bumpPeer('a')
                .catch((e: unknown) => e);
            expect(isActorError(error)).toBe(true);
        } finally {
            await worker.stop({ timeoutMs: 1_000 });
            await ns.stop();
        }
    });

    it('carries a plain method failure back with its message intact', async () => {
        const ns = fakeNamespace();
        const worker = await workerHost(ns);
        try {
            const error = await worker
                .actor(Counter, 'a')
                .boom()
                .catch((e: unknown) => e);
            expect((error as Error).message).toMatch(/nope/);
            // And it is NOT actor-branded — which is what makes the branding
            // assertion above mean something rather than hold for anything
            // that crosses the hop.
            expect(isActorError(error)).toBe(false);
        } finally {
            await worker.stop({ timeoutMs: 1_000 });
            await ns.stop();
        }
    });

    it('derives a fresh stub per dispatch, and never caches one', async () => {
        const ns = fakeNamespace();
        let ids = 0;
        const counting: DurableObjectNamespaceLike = {
            idFromName: (name) => {
                ids++;
                return ns.idFromName(name);
            },
            get: (id) => ns.get(id)
        };
        const worker = await workerHost(counting);
        try {
            await worker.actor(Counter, 'a').increment(1);
            await worker.actor(Counter, 'a').increment(1);
            await worker.actor(Counter, 'a').increment(1);
            // NOT 1. A Durable Object stub is an I/O object bound to the
            // request context that created it, and workerd refuses to use one
            // from a different request — so a cache that outlives a request
            // turns every call after the first into "unreachable". Measured
            // on workerd, not theorised: see __tests__/workers/. Rebuilding
            // costs one idFromName hash and a closure.
            expect(ids).toBe(3);
        } finally {
            await worker.stop({ timeoutMs: 1_000 });
            await ns.stop();
        }
    });

    it('names the type when a namespace resolver has no binding for it', async () => {
        const placement = durableObjectPlacement({ namespace: () => undefined });
        expect(() => placement.dispatcherFor({ type: 'Ghost', key: 'x' })).toThrow(
            /no Durable Object namespace for actor type "Ghost"/
        );
    });

    it('refuses a base that cannot match a pathname', () => {
        // It is compared against `new URL(...).pathname`, which always starts
        // with "/", so a base without one matches nothing and every dispatch
        // fails a long way from the cause.
        expect(() => durableObjectPlacement({ namespace: {} as never, base: '_sigx/do' })).toThrow(
            /must start with "\/"/
        );
    });

    it('refuses a jurisdiction the binding cannot provide', async () => {
        const ns = fakeNamespace();
        const placement = durableObjectPlacement({ namespace: ns, jurisdiction: 'eu' });
        expect(() => placement.dispatcherFor({ type: 'Counter', key: 'a' })).toThrow(
            /does not support jurisdictions/
        );
        await ns.stop();
    });
});

/**
 * The runtime floor for `defineActor({ placement })` on a DO-hosted actor
 * (#362). The type narrowing from #351 reaches only an app module that
 * installs the placement plugin itself; the documented `app` factory never
 * does, so a strategy declared through it compiles — and this is what
 * catches it.
 */
describe('durableObjectPlacement: the declared-placement floor (#362)', () => {
    const local = { dispatch: async () => 'local' };
    const namespace: DurableObjectNamespaceLike = {
        idFromName: (name) => ({ name, toString: () => name }),
        get: () => ({ fetch: async () => new Response('never') })
    };
    const base = {
        allowAnonymous: true as const,
        state: () => ({ n: 0 }),
        methods: () => ({
            async get() {
                return 0;
            }
        })
    };
    /** A host that resolves definitions the way a registry does — sync for
     *  a definitions array, a promise for the lazy `virtual:sigx-actors`. */
    const hostOf = (defs: readonly AnyActorDefinition[], lazy = false): Host =>
        ({
            definition(type: string) {
                const def = defs.find((d) => d.type === type) ?? null;
                return lazy ? Promise.resolve(def) : def;
            }
        }) as unknown as Host;
    /** One placement per side: a Durable Object (with `isSelf`) and the Worker. */
    const bound = (defs: readonly AnyActorDefinition[], lazy = false) => {
        const object = durableObjectPlacement({
            namespace,
            isSelf: (ref) => ref.key === 'mine'
        });
        object.bind!(local, hostOf(defs, lazy));
        const worker = durableObjectPlacement({ namespace });
        worker.bind!(local, hostOf(defs, lazy));
        return { object, worker };
    };

    it('warns ONCE per type that a declared strategy is ignored — on the self path, the remote path, and a second placement alike', () => {
        const Placed = defineActor({ ...base, type: 'Placed', placement: { name: 'prefer-local' } });
        const warn = vi.spyOn(console, 'warn').mockImplementation(() => {});
        try {
            const { object, worker } = bound([Placed]);
            // The SELF path first: `isSelf` used to short-circuit before any
            // definition read, so a check hung off the stateless lookup alone
            // would never see a Durable Object's own actor.
            expect(object.dispatcherFor({ type: 'Placed', key: 'mine' })).toBe(local);
            expect(warn).toHaveBeenCalledTimes(1);
            expect(warn.mock.calls[0]![0]).toBe(
                '[sigx actors-cloudflare] actor "Placed" declares placement "prefer-local" — ' +
                    'Durable Objects ignore it; a ref maps to its object by name'
            );
            // Then many dispatches, both paths, and the Worker's placement —
            // objects of one class share an isolate, so once per type per
            // module, not per placement.
            object.dispatcherFor({ type: 'Placed', key: 'other' });
            object.dispatcherFor({ type: 'Placed', key: 'mine' });
            worker.dispatcherFor({ type: 'Placed', key: 'other' });
            worker.dispatcherFor({ type: 'Placed', key: 'mine' });
            expect(warn).toHaveBeenCalledTimes(1);
        } finally {
            warn.mockRestore();
        }
    });

    it('warns through a lazy registry too, and still routes', async () => {
        const Lazy = defineActor({ ...base, type: 'LazyPlaced', placement: { name: 'sticky' } });
        const warn = vi.spyOn(console, 'warn').mockImplementation(() => {});
        try {
            const { object } = bound([Lazy], true);
            await expect(object.dispatcherFor({ type: 'LazyPlaced', key: 'mine' })).resolves.toBe(
                local
            );
            const remote = await object.dispatcherFor({ type: 'LazyPlaced', key: 'other' });
            expect(remote).not.toBe(local);
            // Memoized: the second round is synchronous and says nothing more.
            expect(object.dispatcherFor({ type: 'LazyPlaced', key: 'mine' })).toBe(local);
            expect(warn).toHaveBeenCalledTimes(1);
            expect(warn.mock.calls[0]![0]).toMatch(/actor "LazyPlaced" declares placement "sticky"/);
        } finally {
            warn.mockRestore();
        }
    });

    it('THROWS for a strategy tagged for the cluster, on every dispatch and on both paths', async () => {
        // A cluster policy cannot mean anything here — a ref maps to its
        // object by name — so it is unambiguously wrong, not merely ignored:
        // the same posture the cluster placement takes with a tag it does
        // not own (#350).
        const Clustered = defineActor({
            ...base,
            type: 'Clustered',
            placement: consistentHashPolicy()
        });
        const Tagged = defineActor({
            ...base,
            type: 'Tagged',
            // The tag alone decides — no `choose()` needed to be refused.
            placement: { name: 'hand-rolled', backend: 'cluster' }
        });
        const warn = vi.spyOn(console, 'warn').mockImplementation(() => {});
        try {
            const { object, worker } = bound([Clustered, Tagged]);
            const message =
                /actor "Clustered" declares placement "consistent-hash", a cluster PlacementPolicy/;
            expect(() => object.dispatcherFor({ type: 'Clustered', key: 'mine' })).toThrow(message);
            expect(() => object.dispatcherFor({ type: 'Clustered', key: 'other' })).toThrow(message);
            expect(() => worker.dispatcherFor({ type: 'Clustered', key: 'x' })).toThrow(message);
            // Not memoized as "checked": the second call fails the same way.
            expect(() => worker.dispatcherFor({ type: 'Clustered', key: 'x' })).toThrow(message);
            expect(() => worker.dispatcherFor({ type: 'Tagged', key: 'x' })).toThrow(
                /actor "Tagged" declares placement "hand-rolled"/
            );
            // A lazy registry rejects instead of throwing, same message.
            const lazy = bound([Clustered], true);
            await expect(lazy.object.dispatcherFor({ type: 'Clustered', key: 'mine' })).rejects.toThrow(
                message
            );
            expect(warn).not.toHaveBeenCalled();
        } finally {
            warn.mockRestore();
        }
    });

    it('says nothing for a definition without placement, or a type the host does not know', () => {
        const warn = vi.spyOn(console, 'warn').mockImplementation(() => {});
        try {
            const { object, worker } = bound([Counter, Work]);
            expect(object.dispatcherFor({ type: 'Counter', key: 'mine' })).toBe(local);
            expect(object.dispatcherFor({ type: 'Counter', key: 'other' })).not.toBe(local);
            expect(worker.dispatcherFor({ type: 'Work', key: 'any' })).toBe(local);
            expect(worker.dispatcherFor({ type: 'Ghost', key: 'x' })).not.toBe(local);
            expect(warn).not.toHaveBeenCalled();
        } finally {
            warn.mockRestore();
        }
    });
});
