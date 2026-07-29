/**
 * The whole round trip with NO Workers runtime.
 *
 * A fake namespace instantiates REAL `createSiloDurableObject` classes over a
 * `Map`-backed storage and a one-field alarm, and a real
 * `createWorkerHandler` sits in front. So everything under test is shipped
 * code — the public wire, the placement, the internal mount, the silo, the
 * reminder table — and only the platform is faked.
 */
import { describe, expect, it } from 'vitest';
import { defineActor } from '@sigx/actors';
import { defineActorApp, metrics } from '@sigx/actors/silo';
import { encodeEnvelope, SILO_CALL_HEADER } from '@sigx/actors/cluster';
import {
    createSiloDurableObject,
    createWorkerHandler,
    durableObjects,
    unhostedStorage,
    type DurableObjectNamespaceLike,
    type DurableObjectStateLike,
    type SiloDurableObjectInstance
} from '@sigx/actors-cloudflare';

const SEP = '\u0000';

const Counter = defineActor({
    type: 'Counter',
    unguarded: true,
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
        async bumpPeer(key: string) {
            return ctx.actor(Counter, key).increment(1);
        },
        async armIn(ms: number) {
            await ctx.reminders.set('wake', { due: ms });
            return true;
        },
        async woke() {
            return ctx.state.count;
        }
    }),
    onReminder(ctx) {
        ctx.state.count += 100;
        return ctx.save();
    },
    streams: (ctx) => ({
        async *watch() {
            yield* ctx.changes({ initial: true });
        }
    })
});

interface Env {
    ACTORS: DurableObjectNamespaceLike;
}

/** One Durable Object: its own storage map and its own alarm slot. */
function fakeState(name: string): DurableObjectStateLike & {
    readonly map: Map<string, unknown>;
    alarm: number | null;
} {
    const map = new Map<string, unknown>();
    let armed: number | null = null;
    const state = {
        map,
        get alarm(): number | null {
            return armed;
        },
        set alarm(at: number | null) {
            armed = at;
        },
        id: { name, toString: () => name },
        storage: {
            get: async <T,>(k: string) =>
                map.has(k) ? (structuredClone(map.get(k)) as T) : undefined,
            put: async <T,>(k: string, v: T) => void map.set(k, structuredClone(v)),
            delete: async (k: string) => map.delete(k),
            getAlarm: async () => armed,
            setAlarm: async (at: number) => void (armed = at),
            deleteAlarm: async () => void (armed = null)
        } as DurableObjectStateLike['storage'],
        // The real gate serializes and does not nest; that contract is
        // exercised properly in durable-objects.test.ts. Here it only has to
        // not reorder anything.
        blockConcurrencyWhile: <T,>(fn: () => Promise<T>) => fn()
    };
    return state;
}

function harness(appFactory?: Parameters<typeof createSiloDurableObject>[0]['app']) {
    const states = new Map<string, ReturnType<typeof fakeState>>();
    const hosts = new Map<string, SiloDurableObjectInstance>();

    const Host = createSiloDurableObject<Env>({
        actors: [Counter],
        namespace: (env) => env.ACTORS,
        ...(appFactory ? { app: appFactory } : {})
    });

    const namespace: DurableObjectNamespaceLike = {
        idFromName: (name) => ({ name, toString: () => name }),
        get: (id) => ({
            fetch(input: string | Request, init?: RequestInit) {
                const name = id.name!;
                let host = hosts.get(name);
                if (!host) {
                    const state = fakeState(name);
                    states.set(name, state);
                    host = new Host(state, env);
                    hosts.set(name, host);
                }
                return host.fetch(
                    typeof input === 'string' ? new Request(input, init) : input
                );
            }
        })
    };
    const env: Env = { ACTORS: namespace };
    const worker = createWorkerHandler<Env>({
        actors: [Counter],
        namespace: (e) => e.ACTORS,
        // A Worker's callers are not browsers posting a form; the public
        // mount has to be told so, exactly as examples/aks-cluster does for
        // Node clients.
        fetch: { origin: false }
    });

    const call = async (symbol: string, args: readonly unknown[]): Promise<Response> =>
        worker.fetch(
            new Request(`https://edge.test/_sigx/actor/${encodeURIComponent(symbol)}`, {
                method: 'POST',
                headers: { 'content-type': 'application/json' },
                body: JSON.stringify({ args })
            }),
            env
        );

    const invoke = async (symbol: string, args: readonly unknown[]): Promise<unknown> => {
        const res = await call(symbol, args);
        const body = (await res.json()) as { data?: unknown; error?: { message: string } };
        if (!res.ok || body.error) {
            throw new Error(body.error?.message ?? `HTTP ${res.status}`);
        }
        return body.data;
    };

    return { states, hosts, namespace, env, worker, call, invoke, Host };
}

describe('createSiloDurableObject + createWorkerHandler', () => {
    it('serves the public wire end to end, into the right object', async () => {
        const h = harness();
        await expect(h.invoke('Counter#increment', ['a', 2])).resolves.toBe(2);
        await expect(h.invoke('Counter#increment', ['a', 3])).resolves.toBe(5);

        expect([...h.hosts.keys()]).toEqual([`Counter${SEP}a`]);
        const map = h.states.get(`Counter${SEP}a`)!.map;
        expect([...map.keys()]).toContain(`sigx:state${SEP}Counter${SEP}a`);
    });

    it('gives two keys two objects, and the same key one', async () => {
        const h = harness();
        await h.invoke('Counter#increment', ['a', 1]);
        await h.invoke('Counter#increment', ['b', 1]);
        await h.invoke('Counter#increment', ['a', 1]);

        expect(new Set(h.hosts.keys())).toEqual(
            new Set([`Counter${SEP}a`, `Counter${SEP}b`])
        );
        await expect(h.invoke('Counter#read', ['a'])).resolves.toBe(2);
        await expect(h.invoke('Counter#read', ['b'])).resolves.toBe(1);
    });

    it('sends an actor-to-actor call out to the callee object', async () => {
        const h = harness();
        await expect(h.invoke('Counter#bumpPeer', ['a', 'b'])).resolves.toBe(1);

        const aMap = h.states.get(`Counter${SEP}a`)!.map;
        const bMap = h.states.get(`Counter${SEP}b`)!.map;
        expect([...bMap.keys()]).toContain(`sigx:state${SEP}Counter${SEP}b`);
        expect([...aMap.keys()]).not.toContain(`sigx:state${SEP}Counter${SEP}b`);
    });

    it('streams NDJSON from the object through the Worker', async () => {
        const h = harness();
        const res = await h.call('Counter#watch', ['a']);
        expect(res.status).toBe(200);

        const reader = res.body!.getReader();
        const decoder = new TextDecoder();
        let buffer = '';
        while (!buffer.includes('\n')) {
            const { value, done } = await reader.read();
            if (done) break;
            buffer += decoder.decode(value, { stream: true });
        }
        expect(JSON.parse(buffer.slice(0, buffer.indexOf('\n'))).chunk).toEqual({
            count: 0
        });
        await reader.cancel();
    });

    it('boots the silo from a COLD alarm and delivers the reminder', async () => {
        // The eviction path: the platform wakes an object that has no silo in
        // memory, and `onAlarm()` throws if the reminders were never bound.
        // Booting inside `alarm()` is what makes that survivable.
        const h = harness();
        // Due immediately: the host builds its reminders from the object's
        // own state and takes no clock seam, so the reminder has to be
        // genuinely due by the time the alarm fires — which is also the real
        // situation, since the platform only calls alarm() once it is.
        await expect(h.invoke('Counter#armIn', ['a', 0])).resolves.toBe(true);

        const name = `Counter${SEP}a`;
        const state = h.states.get(name)!;
        expect(state.alarm).not.toBeNull();

        // Evict: drop the instance, keep the durable state. The old silo held
        // a live activation whose in-memory count would otherwise answer the
        // read below and hide whether the reminder did anything.
        h.hosts.delete(name);
        const revived = new h.Host(state, h.env);
        h.hosts.set(name, revived);

        state.alarm = null; // the platform clears it before invoking alarm()
        await revived.alarm();

        await expect(h.invoke('Counter#woke', ['a'])).resolves.toBe(100);
    });

    it('refuses a call for an actor it does not host', async () => {
        // Not a race: ref -> object id is a pure function here, so this means
        // the two sides disagree about naming or bindings.
        const Host = createSiloDurableObject<Env>({
            actors: [Counter],
            namespace: (env) => env.ACTORS
        });
        const state = fakeState(`Counter${SEP}a`);
        const env: Env = {
            ACTORS: {
                idFromName: (name) => ({ name, toString: () => name }),
                get: () => ({ fetch: async () => new Response('never') })
            }
        };
        const host = new Host(state, env);
        const res = await host.fetch(
            new Request(
                `https://sigx.invalid/_sigx/do/${encodeURIComponent('Counter#read')}`,
                {
                    method: 'POST',
                    headers: {
                        'content-type': 'application/json',
                        [SILO_CALL_HEADER]: encodeEnvelope(
                            { callChain: [], callId: 'c1.t.0' },
                            'test'
                        )
                    },
                    body: JSON.stringify({ args: ['wrong-key'] })
                }
            )
        );
        expect(res.ok).toBe(false);
        expect(JSON.stringify(await res.json())).toMatch(/disagree about objectName/);
    });

    it('refuses to start on an id with no name', async () => {
        // Without a name nothing can be compared, so `isSelf` answers false
        // for EVERY ref — including the object's own actor — and the object
        // fetches itself forever. (Breaking `isSelf` deliberately OOMs this
        // suite, which is what that costs.) Refusing to start is the only
        // safe answer.
        const h = harness();
        const nameless = fakeState('ignored');
        (nameless as { id: unknown }).id = { toString: () => 'abc123' };
        const host = new h.Host(nameless as never, h.env);
        await expect(host.silo()).rejects.toThrow(/id has no name/);
    });

    it('404s a path that is not the actor mount', async () => {
        const h = harness();
        const state = fakeState(`Counter${SEP}a`);
        const host = new h.Host(state, h.env);
        const res = await host.fetch(new Request('https://sigx.invalid/nope'));
        expect(res.status).toBe(404);
        expect(JSON.stringify(await res.json())).toMatch(/actor mount/);
    });

    it('lets an app factory add plugins, and hands it the object seams', async () => {
        const seen: Array<Record<string, unknown>> = [];
        const h = harness((base) => {
            seen.push(base as unknown as Record<string, unknown>);
            return defineActorApp(base).use(metrics());
        });
        await expect(h.invoke('Counter#increment', ['a', 1])).resolves.toBe(1);

        expect(seen).toHaveLength(1);
        // The seams come from THIS object, not from a generic app: DO-backed
        // storage, and no idle sweeper (the platform evicts the whole silo).
        expect(seen[0]!.storage).toBeDefined();
        expect(seen[0]!.reminders).toBeDefined();
        expect((seen[0]!.defaults as { sweepIntervalMs?: number }).sweepIntervalMs).toBe(0);
        // ...and never a namespace binding, so the factory structurally
        // cannot build a placement of its own.
        expect(Object.keys(seen[0]!)).not.toContain('ACTORS');
    });

    it('refuses an app factory that claims the placement itself', async () => {
        // `setPlacement` is exclusive, and that exclusivity IS the recursion
        // guard: without it a Durable Object could be handed a placement with
        // no `isSelf` and quietly fetch itself forever.
        const h = harness((base) =>
            defineActorApp(base).use(
                durableObjects({
                    namespace: {
                        idFromName: (name) => ({ name, toString: () => name }),
                        get: () => ({ fetch: async () => new Response('never') })
                    }
                })
            )
        );
        // Asserted on the object itself: through the wire it would arrive as
        // a generic call failure, and the point is the message naming both
        // plugins.
        const host = new h.Host(fakeState(`Counter${SEP}a`), h.env);
        await expect(host.silo()).rejects.toThrow(/cloudflare:durable-objects/);
    });

});

describe('unhostedStorage', () => {
    it('refuses every operation, naming why', async () => {
        const storage = unhostedStorage();
        await expect(storage.load('Counter', 'a')).rejects.toThrow(/never hosts an activation/);
        await expect(storage.save('Counter', 'a', {}, null)).rejects.toThrow(
            /never hosts an activation/
        );
        await expect(storage.clear('Counter', 'a', null)).rejects.toThrow(
            /never hosts an activation/
        );
    });
});
