/**
 * The two things rfc-server-v4 §7 buys actors, pinned.
 *
 * 1. **Resource-based policies.** A policy sees the principal AND the
 *    INSTANCE, so "may this user read cart `u_123`?" is expressible. Under
 *    the pre-v4 guard chain it structurally was not: a guard received
 *    `(rq, { symbol, name })`, and the key was peeled off the wire args
 *    only after the guard had already run.
 *
 * 2. **Principal propagation.** Identity rides a first-class envelope slot,
 *    so it survives `ctx.actor` hops without anyone remembering to stamp
 *    anything — and, because it is NOT a bag key, it cannot be forged
 *    through `.with({ bag })`.
 *
 * Plus the boundary rule both rest on: authentication is per REQUEST,
 * authorization is per ENTRY POINT. A `ctx.actor` hop is neither, so a
 * callee's policy does not re-run against the calling actor.
 */
import { afterEach, describe, expect, it } from 'vitest';
import { actor, defineActor, type AnyActorDefinition } from '@sigx/actors';
import { createHost, type Host } from '@sigx/actors/host';
import { isServerFnError } from '@sigx/server';
import { stubServerApp } from '@sigx/server/testing';

const quiet = { sweepIntervalMs: 60_000, reminderTickMs: 60_000, callTimeoutMs: 0 };

interface User {
    readonly id: string;
}

let host: Host | null = null;
let restore: (() => void) | undefined;

afterEach(async () => {
    await host?.stop();
    host = null;
    restore?.();
    restore = undefined;
});

/** Stamp an app whose authenticated user is `id` (or anonymous for null). */
function signedInAs(id: string | null): void {
    restore = stubServerApp({
        authenticate: () => (id === null ? null : ({ id } satisfies User)),
        codec: {
            encode: (principal) => (principal as User).id,
            decode: (encoded) => (encoded === '' ? null : ({ id: encoded } satisfies User))
        }
    });
}

async function start(defs: readonly AnyActorDefinition[]) {
    host = createHost({ actors: defs, defaults: quiet });
    await host.start();
    return host;
}

const status = (error: unknown): number | undefined =>
    isServerFnError(error) ? error.status : undefined;

describe('resource-based policies (rfc-server-v4 §7)', () => {
    /** The canonical shape: you may only reach the cart that is yours. */
    const Cart = defineActor({
        type: 'Cart',
        authorize: (user: User | null, _rq, op) => op.resource?.key === user?.id,
        state: () => ({ items: 0 }),
        methods: (ctx) => ({
            async add() {
                ctx.state.items += 1;
                return ctx.state.items;
            }
        })
    });

    it('admits the owner and refuses everyone else — the same method, same args', async () => {
        signedInAs('u_123');
        await start([Cart]);
        await expect(actor(Cart, 'u_123').add()).resolves.toBe(1);
        // Nothing about the CALL differs; only the instance it names does.
        // That is the distinction a pre-v4 guard could not draw at all.
        const denied = await actor(Cart, 'u_999').add().catch((e: unknown) => e);
        expect(status(denied)).toBe(403);
    });

    it('gives the policy the whole resource, kind included', async () => {
        const seen: unknown[] = [];
        signedInAs('u_1');
        const Probe = defineActor({
            type: 'Probe',
            authorize: (_p, _rq, op) => {
                seen.push(op.resource);
                return true;
            },
            state: () => ({}),
            methods: () => ({
                async ping() {
                    return 'pong';
                }
            })
        });
        await start([Probe]);
        await actor(Probe, 'k1').ping();
        expect(seen).toEqual([{ kind: 'actor', type: 'Probe', key: 'k1', method: 'ping' }]);
    });

    it('denies 401 rather than 403 when there is no principal at all', async () => {
        signedInAs(null);
        await start([Cart]);
        const denied = await actor(Cart, 'u_123').add().catch((e: unknown) => e);
        // 401 says "who are you?", 403 says "not you" — a caller that can
        // still fix it by signing in must not be told 403.
        expect(status(denied)).toBe(401);
    });

    it('ANDs methodAuthorize AFTER authorize', async () => {
        const order: string[] = [];
        signedInAs('u_1');
        const Doc = defineActor({
            type: 'Doc',
            authorize: () => {
                order.push('actor');
                return true;
            },
            methodAuthorize: {
                publish: [
                    () => {
                        order.push('method');
                        return false;
                    }
                ]
            },
            state: () => ({}),
            methods: () => ({
                async read() {
                    return 'ok';
                },
                async publish() {
                    return 'published';
                }
            })
        });
        await start([Doc]);
        await expect(actor(Doc, 'd1').read()).resolves.toBe('ok');
        expect(order).toEqual(['actor']);
        order.length = 0;
        const denied = await actor(Doc, 'd1').publish().catch((e: unknown) => e);
        expect(status(denied)).toBe(403);
        expect(order).toEqual(['actor', 'method']);
    });

    it('is STRICT-true: a policy that forgets to return denies', async () => {
        signedInAs('u_1');
        const Sloppy = defineActor({
            type: 'Sloppy',
            authorize: (() => undefined) as never,
            state: () => ({}),
            methods: () => ({
                async go() {
                    return 'ran';
                }
            })
        });
        await start([Sloppy]);
        const denied = await actor(Sloppy, 'k').go().catch((e: unknown) => e);
        expect(status(denied)).toBe(403);
    });
});

describe('principal propagation (rfc-server-v4 §7)', () => {
    const Inner = defineActor({
        type: 'Inner',
        allowAnonymous: true,
        state: () => ({}),
        methods: (ctx) => ({
            async whoCalled() {
                return (ctx.principal as User | null)?.id ?? null;
            }
        })
    });

    const Outer = defineActor({
        type: 'Outer',
        allowAnonymous: true,
        state: () => ({}),
        methods: (ctx) => ({
            async direct() {
                return (ctx.principal as User | null)?.id ?? null;
            },
            /** One hop deeper — the case `stampCallBag` used to have to carry. */
            async viaHop() {
                return ctx.actor(Inner, 'i1').whoCalled();
            }
        })
    });

    it('reaches the called actor as ctx.principal', async () => {
        signedInAs('ada');
        await start([Outer, Inner]);
        await expect(actor(Outer, 'o1').direct()).resolves.toBe('ada');
    });

    it('survives a ctx.actor hop unchanged — the ORIGINAL caller, not the actor', async () => {
        signedInAs('ada');
        await start([Outer, Inner]);
        // Authentication is per REQUEST; a hop is not an entry point, so the
        // identity a downstream actor sees is whoever entered the system.
        await expect(actor(Outer, 'o1').viaHop()).resolves.toBe('ada');
    });

    it('is null for an anonymous caller, never a stale or guessed identity', async () => {
        signedInAs(null);
        await start([Outer, Inner]);
        await expect(actor(Outer, 'o1').direct()).resolves.toBeNull();
        await expect(actor(Outer, 'o1').viaHop()).resolves.toBeNull();
    });

    it('cannot be forged through the context bag', async () => {
        signedInAs('ada');
        await start([Outer, Inner]);
        // The bag is app DATA and remains caller-settable; identity is not in
        // it, so writing a `principal` entry changes nothing. This is the
        // structural reason the slot is separate rather than a bag key.
        await expect(
            actor(Outer, 'o1').with({ bag: { principal: 'root' } }).direct()
        ).resolves.toBe('ada');
    });

    it('propagates nothing when the app declares no codec — anonymous, not wrong', async () => {
        restore = stubServerApp({ authenticate: () => ({ id: 'ada' }) satisfies User });
        await start([Outer, Inner]);
        // Fail-closed at the READER: with no codec the identity cannot round
        // trip, so the callee sees null rather than something unverifiable.
        // The call itself still succeeds — the actor is allowAnonymous.
        await expect(actor(Outer, 'o1').direct()).resolves.toBeNull();
    });
});

describe('allowAnonymous', () => {
    it('waives the identity gate but still runs a declared policy', async () => {
        const saw: (User | null)[] = [];
        signedInAs(null);
        const Public = defineActor({
            type: 'Public',
            allowAnonymous: true,
            // Legal since v4 — `unguarded` + `use` used to throw. A nullable
            // principal reaching a declared policy is the whole point:
            // "anyone may read it, only the owner may read a private one".
            authorize: (user: User | null) => {
                saw.push(user);
                return true;
            },
            state: () => ({}),
            methods: () => ({
                async read() {
                    return 'ok';
                }
            })
        });
        await start([Public]);
        await expect(actor(Public, 'k').read()).resolves.toBe('ok');
        expect(saw).toEqual([null]);
    });

    it('a bare allowAnonymous actor skips the app default entirely', async () => {
        // The default would re-deny exactly the anonymity the literal granted.
        restore = stubServerApp({ authenticate: () => null, authorize: () => false });
        const Bare = defineActor({
            type: 'Bare',
            allowAnonymous: true,
            state: () => ({}),
            methods: () => ({
                async read() {
                    return 'ok';
                }
            })
        });
        await start([Bare]);
        await expect(actor(Bare, 'k').read()).resolves.toBe('ok');
    });
});

describe('the app default policy', () => {
    it('decides for an actor that declares nothing — the one-line auth story', async () => {
        restore = stubServerApp({
            authenticate: () => ({ id: 'u_1' }) satisfies User,
            authorize: (user) => (user as User).id === 'admin'
        });
        const Plain = defineActor({
            type: 'Plain',
            state: () => ({}),
            methods: () => ({
                async read() {
                    return 'ok';
                }
            })
        });
        await start([Plain]);
        const denied = await actor(Plain, 'k').read().catch((e: unknown) => e);
        expect(status(denied)).toBe(403);
    });

    it('is REPLACED by a declared policy, not ANDed with it (most-specific-wins)', async () => {
        restore = stubServerApp({
            authenticate: () => ({ id: 'u_1' }) satisfies User,
            authorize: () => false
        });
        const Override = defineActor({
            type: 'Override',
            authorize: () => true,
            state: () => ({}),
            methods: () => ({
                async read() {
                    return 'ok';
                }
            })
        });
        await start([Override]);
        await expect(actor(Override, 'k').read()).resolves.toBe('ok');
    });
});
