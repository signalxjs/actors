/**
 * Stateless workers (`defineWorker`): definition-time contract and the
 * pooled multi-activation runtime — interchangeable members per (type, key),
 * fewest-queued-turns dispatch, pressure-driven growth up to `maxLocal`, and
 * a ctx with the identity-bound surface guarded away.
 */
import { afterEach, describe, expect, it, vi } from 'vitest';
import {
    defineActor,
    defineWorker,
    isActorDefinition,
    isActorError,
    type ActorStorage
} from '@sigx/actors';
import { createHost, memoryStorage, type Host } from '@sigx/actors/host';

const quiet = { sweepIntervalMs: 60_000, reminderTickMs: 60_000, callTimeoutMs: 0 };

/** A promise with its resolver — the latch the pool tests park turns on. */
function deferred<T = void>(): { promise: Promise<T>; resolve: (v: T) => void } {
    let resolve!: (v: T) => void;
    const promise = new Promise<T>((r) => (resolve = r));
    return { promise, resolve };
}

const running: Host[] = [];
afterEach(async () => {
    for (const h of running.splice(0)) await h.stop({ timeoutMs: 1000 });
});

function track(host: Host): Host {
    running.push(host);
    return host;
}

describe('defineWorker definition', () => {
    it('returns a plain actor definition carrying the stateless marker', () => {
        const def = defineWorker({
            type: 'w',
            allowAnonymous: true as const,
            maxLocal: 2,
            methods: () => ({
                async run() {
                    return 1;
                }
            })
        });
        expect(isActorDefinition(def)).toBe(true);
        expect(def.type).toBe('w');
        expect(def.__sigxActor.stateless).toEqual({ maxLocal: 2 });
    });

    it('rejects a non-positive or fractional maxLocal in every build', () => {
        const bad = (maxLocal: number) => () =>
            defineWorker({ type: 'w', allowAnonymous: true as const, maxLocal, methods: () => ({}) });
        expect(bad(0)).toThrow(/positive integer/);
        expect(bad(-1)).toThrow(/positive integer/);
        expect(bad(1.5)).toThrow(/positive integer/);
    });

    it('enumerates stream names exactly like defineActor', () => {
        const def = defineWorker({
            type: 'w',
            allowAnonymous: true as const,
            methods: () => ({}),
            streams: () => ({
                async *ticks() {
                    yield 1;
                }
            })
        });
        expect(def.streamNames).toEqual(['ticks']);
    });

    it('defineActor refuses the stateless marker next to identity-bound options', () => {
        // The marker is @internal but reachable from a hand-written call;
        // each of these contradictions throws in every build.
        const base = {
            type: 'w',
            allowAnonymous: true as const,
            state: () => ({}),
            methods: () => ({}),
            stateless: {}
        };
        expect(() => defineActor({ ...base, persistence: 'explicit' })).toThrow(/persistence/);
        expect(() => defineActor({ ...base, tasks: () => ({}) })).toThrow(/tasks/);
        expect(() => defineActor({ ...base, subscriptions: { t: () => {} } })).toThrow(
            /subscriptions/
        );
        expect(() => defineActor({ ...base, onReminder: () => {} })).toThrow(/onReminder/);
        expect(() => defineActor({ ...base, placement: { name: 'x' } })).toThrow(/placement/);
        expect(() => defineActor({ ...base, reentrant: true })).toThrow(/reentrant/);
        expect(() => defineActor({ ...base, reentrant: 'always' })).toThrow(/reentrant/);
        expect(() => defineActor({ ...base, methodReentrancy: { m: 'always' } })).toThrow(
            /methodReentrancy/
        );
        expect(() => defineActor({ ...base, migrateState: (s: unknown) => s as object })).toThrow(
            /migrateState/
        );
        // The maxLocal validation holds on the marker path too — the cap is
        // reachable without going through defineWorker.
        expect(() => defineActor({ ...base, stateless: { maxLocal: 0 } })).toThrow(
            /positive integer/
        );
        expect(() => defineActor({ ...base, stateless: { maxLocal: 1.5 } })).toThrow(
            /positive integer/
        );
    });

    it('still applies the shared type-name and guard rules', () => {
        expect(() => defineWorker({ type: '$w', allowAnonymous: true as const, methods: () => ({}) })).toThrow(
            /reserved/
        );
        // rfc-server-v4 §1.2 REMOVED the pre-v4 contradiction throw, with no
        // analog. `unguarded` + `use` was a lie — the chain ran anyway.
        // This pair is coherent: the identity gate is waived, and the
        // declared policy still decides, against a nullable principal.
        expect(() =>
            defineWorker({
                type: 'w',
                allowAnonymous: true as const,
                authorize: [() => true],
                methods: () => ({})
            })
        ).not.toThrow();
    });
});

/**
 * Pool worker fixture: `enter(tag)` records entry and parks on the shared
 * gate, so tests observe true concurrency; `token()` identifies the member
 * (minted in onActivate); `who()` reads ctx.key.
 */
function poolWorker(opts: {
    maxLocal?: number;
    idleAfterMs?: number;
    gate: () => Promise<void>;
    entered: string[];
    events?: string[];
}) {
    let minted = 0;
    return defineWorker({
        type: 'Pool',
        allowAnonymous: true as const,
        ...(opts.maxLocal !== undefined ? { maxLocal: opts.maxLocal } : {}),
        ...(opts.idleAfterMs !== undefined ? { idleAfterMs: opts.idleAfterMs } : {}),
        onActivate(ctx) {
            opts.events?.push(`activate:${ctx.key}`);
        },
        onDeactivate(ctx, reason) {
            opts.events?.push(`deactivate:${ctx.key}:${reason}`);
        },
        methods: (ctx) => {
            const token = ++minted;
            return {
                async enter(tag: string) {
                    opts.entered.push(tag);
                    await opts.gate();
                    return token;
                },
                async token() {
                    return token;
                },
                async who() {
                    return ctx.key;
                }
            };
        }
    });
}

describe('worker pool runtime', () => {
    it('two calls to the SAME key run concurrently on different members', async () => {
        const gate = deferred();
        const entered: string[] = [];
        const def = poolWorker({ maxLocal: 2, gate: () => gate.promise, entered });
        const host = track(createHost({ actors: [def], defaults: quiet }));
        const client = host.actor(def, 'any');
        const a = client.enter('a');
        const b = client.enter('b');
        // Both bodies entered while neither has resolved — impossible under
        // the single activation.
        await vi.waitFor(() => expect(entered.sort()).toEqual(['a', 'b']));
        gate.resolve();
        const tokens = await Promise.all([a, b]);
        expect(new Set(tokens).size).toBe(2); // distinct members served them
    });

    it('growth stops exactly at maxLocal; excess calls queue and drain', async () => {
        const gate = deferred();
        const entered: string[] = [];
        const def = poolWorker({ maxLocal: 2, gate: () => gate.promise, entered });
        const host = track(createHost({ actors: [def], defaults: quiet }));
        const client = host.actor(def, 'any');
        const calls = Array.from({ length: 8 }, (_, i) => client.enter(`c${i}`));
        await vi.waitFor(() => expect(entered.length).toBe(2));
        // The pool is saturated: exactly the cap, not one more.
        expect(host.stats().perType['Pool']).toBe(2);
        await new Promise((r) => setTimeout(r, 20));
        expect(entered.length).toBe(2);
        gate.resolve();
        await Promise.all(calls);
        expect(host.stats().perType['Pool']).toBe(2); // drained, not grown
    });

    it('sequential calls stay on one member; concurrency spins the second', async () => {
        let open = deferred();
        const entered: string[] = [];
        const events: string[] = [];
        const def = poolWorker({ maxLocal: 4, gate: () => open.promise, entered, events });
        const host = track(createHost({ actors: [def], defaults: quiet }));
        const client = host.actor(def, 'k');
        open.resolve();
        await client.enter('s1');
        await client.enter('s2');
        await client.enter('s3');
        // No pressure — one member serves a sequential caller forever.
        expect(events.filter((e) => e.startsWith('activate'))).toHaveLength(1);
        open = deferred();
        const p1 = client.enter('p1');
        const p2 = client.enter('p2');
        await vi.waitFor(() =>
            expect(events.filter((e) => e.startsWith('activate'))).toHaveLength(2)
        );
        open.resolve();
        await Promise.all([p1, p2]);
    });

    it('an idle member is picked over spinning a new one', async () => {
        let open = deferred();
        const entered: string[] = [];
        const def = poolWorker({ maxLocal: 3, gate: () => open.promise, entered });
        const host = track(createHost({ actors: [def], defaults: quiet }));
        const client = host.actor(def, 'k');
        // Grow to two members, then let both go idle.
        const grow = [client.enter('g1'), client.enter('g2')];
        await vi.waitFor(() => expect(entered.length).toBe(2));
        open.resolve();
        await Promise.all(grow);
        // Occupy one member; a quick call must ride the idle one, not queue
        // behind the busy one and not spin a third.
        open = deferred();
        const busy = client.enter('busy');
        await vi.waitFor(() => expect(entered).toContain('busy'));
        const quickGate = open; // capture: quick call parks on the same gate
        const quick = client.enter('quick');
        await vi.waitFor(() => expect(entered).toContain('quick'));
        expect(host.stats().perType['Pool']).toBe(2); // no third member
        quickGate.resolve();
        await Promise.all([busy, quick]);
    });

    it('ctx.key is the addressed key on every member', async () => {
        const gate = deferred();
        const entered: string[] = [];
        const def = poolWorker({ maxLocal: 2, gate: () => gate.promise, entered });
        const host = track(createHost({ actors: [def], defaults: quiet }));
        const client = host.actor(def, 'the-key');
        const hold = client.enter('hold'); // occupy member 0
        await vi.waitFor(() => expect(entered).toContain('hold'));
        await expect(client.who()).resolves.toBe('the-key'); // member 1
        gate.resolve();
        await hold;
        // distinct keys build distinct pools with their own members
        await expect(host.actor(def, 'other').who()).resolves.toBe('other');
    });

    it('members idle-collect individually and the pool re-creates on demand', async () => {
        let open = deferred();
        const entered: string[] = [];
        const events: string[] = [];
        const def = poolWorker({
            maxLocal: 2,
            idleAfterMs: 30,
            gate: () => open.promise,
            entered,
            events
        });
        const host = track(
            createHost({ actors: [def], defaults: { ...quiet, sweepIntervalMs: 10 } })
        );
        await host.start();
        const client = host.actor(def, 'k');
        const grow = [client.enter('a'), client.enter('b')];
        await vi.waitFor(() => expect(entered.length).toBe(2));
        open.resolve();
        await Promise.all(grow);
        await vi.waitFor(
            () => {
                expect(events.filter((e) => e.startsWith('deactivate'))).toHaveLength(2);
                expect(host.stats().perType['Pool'] ?? 0).toBe(0);
            },
            { timeout: 2000 }
        );
        // A fresh call re-creates the pool from empty.
        open = deferred();
        open.resolve();
        await expect(client.who()).resolves.toBe('k');
    });

    it('never touches storage: no load, no save, across the whole lifecycle', async () => {
        const ops: string[] = [];
        const inner = memoryStorage();
        const spy: ActorStorage = {
            load: (t, k) => (ops.push(`load:${t}`), inner.load(t, k)),
            save: (t, k, s, e) => (ops.push(`save:${t}`), inner.save(t, k, s, e)),
            clear: (t, k, e) => (ops.push(`clear:${t}`), inner.clear(t, k, e))
        };
        const gate = deferred();
        gate.resolve();
        const entered: string[] = [];
        const def = poolWorker({ maxLocal: 2, gate: () => gate.promise, entered });
        const host = track(createHost({ actors: [def], storage: spy, defaults: quiet }));
        const client = host.actor(def, 'k');
        await client.enter('x');
        await host.deactivateType('Pool');
        expect(ops).toEqual([]);
    });

    it('guards the identity-bound ctx members with loud throws', async () => {
        const probes = defineWorker({
            type: 'Probes',
            allowAnonymous: true as const,
            methods: (ctx) => ({
                async probe(member: string) {
                    const c = ctx as unknown as Record<string, unknown>;
                    try {
                        const v = c[member];
                        if (typeof v === 'function') await (v as () => unknown).call(ctx);
                        return 'no-throw';
                    } catch (error) {
                        return (error as Error).message;
                    }
                }
            })
        });
        const host = track(createHost({ actors: [probes], defaults: quiet }));
        const client = host.actor(probes, 'k');
        for (const member of [
            'state',
            'save',
            'clearState',
            'reminders',
            'tasks',
            'snapshot',
            'changes'
        ]) {
            await expect(client.probe(member)).resolves.toMatch(/not available on stateless/);
        }
    });

    it('a same-key self-call is a deterministic deadlock; another key is not', async () => {
        const selfish = defineWorker({
            type: 'Selfish',
            allowAnonymous: true as const,
            maxLocal: 4,
            methods: (ctx) => ({
                async callSelf(key: string): Promise<unknown> {
                    return ctx.actor(selfish, key).depth();
                },
                async depth() {
                    return 1;
                }
            })
        });
        const host = track(createHost({ actors: [selfish], defaults: quiet }));
        await expect(host.actor(selfish, 'k').callSelf('k')).rejects.toSatisfy(
            (e: unknown) => isActorError(e) && e.kind === 'deadlock'
        );
        await expect(host.actor(selfish, 'k').callSelf('other')).resolves.toBe(1);
    });

    it('host.stop() drains every pool member', async () => {
        const gate = deferred();
        const entered: string[] = [];
        const events: string[] = [];
        const def = poolWorker({ maxLocal: 2, gate: () => gate.promise, entered, events });
        const host = createHost({ actors: [def], defaults: quiet });
        const client = host.actor(def, 'k');
        const calls = [client.enter('a'), client.enter('b')];
        await vi.waitFor(() => expect(entered.length).toBe(2));
        gate.resolve();
        await Promise.all(calls);
        await host.stop({ timeoutMs: 1000 });
        expect(events.filter((e) => e.startsWith('deactivate:k:shutdown'))).toHaveLength(2);
    });

    it('streams work on a worker; watch is refused', async () => {
        const streamy = defineWorker({
            type: 'Streamy',
            allowAnonymous: true as const,
            methods: () => ({ async noop() {} }),
            streams: () => ({
                async *chunks(n: number) {
                    for (let i = 0; i < n; i++) yield i;
                }
            })
        });
        const host = track(createHost({ actors: [streamy], defaults: quiet }));
        const seen: number[] = [];
        for await (const v of host.actor(streamy, 'k').chunks(3)) seen.push(v);
        expect(seen).toEqual([0, 1, 2]);
        const watching = host.dispatchWatch!({ type: 'Streamy', key: 'k' }, 'noop', [], {
            callChain: [],
            callId: 'w'
        })[Symbol.asyncIterator]();
        await expect(watching.next()).rejects.toThrow(/stateless/);
    });
});
