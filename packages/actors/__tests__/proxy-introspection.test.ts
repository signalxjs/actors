/**
 * The proxy `get` traps manufacture a live dispatcher for ANY string prop —
 * which is the feature (methods need no registration), but it meant
 * `String(ref)`, `JSON.stringify(ref)` or a library probing `.constructor`
 * silently issued a real call. Introspection names must be answered
 * locally: `toString` reads `[actor Type#key]`, every other
 * `Object.prototype` name (plus `toJSON` and Node's legacy `inspect`)
 * returns `undefined`, exactly like symbols and `then`.
 */
import { inspect } from 'node:util';
import { afterEach, describe, expect, it } from 'vitest';
import { __actorRef, configureActors } from '@sigx/actors/client';
import type { ActorCallInit, ActorTransport } from '@sigx/actors/client';
import { actor, defineActor } from '@sigx/actors';
import { createHost } from '@sigx/actors/host';

const ENDPOINT = 'http://actors.test/_sigx/actor';
const quiet = { sweepIntervalMs: 60_000, reminderTickMs: 60_000, callTimeoutMs: 0 };

interface Recorded {
    symbol: string;
    args: unknown[];
    init: ActorCallInit | undefined;
}

function recordingTransport(): ActorTransport & { calls: Recorded[] } {
    const calls: Recorded[] = [];
    return {
        name: 'recording',
        calls,
        async call(symbol, args, init) {
            calls.push({ symbol, args, init });
            return { ok: symbol };
        },
        stream() {
            return (async function* () {})();
        }
    };
}

const wireRef = (key: string): Record<string, unknown> =>
    (__actorRef('Cart', ENDPOINT, ['watch']) as never as ClientRefLike).__sigxActorProxy(key);

interface ClientRefLike {
    __sigxActorProxy: (key: string) => Record<string, unknown>;
}

afterEach(() => {
    configureActors(null);
});

describe('wire client proxy introspection', () => {
    it('String(ref) reads a label and never dispatches', () => {
        const transport = recordingTransport();
        configureActors(transport);
        const cart = wireRef('c1');
        expect(String(cart)).toBe('[actor Cart#c1]');
        expect(`${cart}`).toBe('[actor Cart#c1]');
        expect(transport.calls).toHaveLength(0);
    });

    it('JSON.stringify and inspect never dispatch', () => {
        const transport = recordingTransport();
        configureActors(transport);
        const cart = wireRef('c1');
        expect(JSON.stringify(cart)).toBe('{}');
        inspect(cart);
        expect(transport.calls).toHaveLength(0);
    });

    it('Object.prototype names read undefined; then stays undefined', () => {
        const transport = recordingTransport();
        configureActors(transport);
        const cart = wireRef('c1');
        expect(cart.constructor).toBeUndefined();
        expect(cart.hasOwnProperty).toBeUndefined();
        expect(cart.valueOf).toBeUndefined();
        expect(cart.then).toBeUndefined();
        expect(transport.calls).toHaveLength(0);
    });

    it('real methods still dispatch — the guard blocks only introspection names', async () => {
        const transport = recordingTransport();
        configureActors(transport);
        const cart = wireRef('c1') as { add(item: string): Promise<unknown> };
        await expect(cart.add('apple')).resolves.toEqual({ ok: 'Cart#add' });
        expect(transport.calls).toHaveLength(1);
        expect(transport.calls[0].symbol).toBe('Cart#add');
    });
});

describe('server client proxy introspection (actor())', () => {
    const Probe = defineActor({
        type: 'IntroServer',
        allowAnonymous: true,
        state: () => ({}),
        methods: () => ({
            async ping() {
                return 'pong';
            }
        })
    });

    it('String(ref) reads a label without a running host', () => {
        const ref = actor(Probe, 'k1') as unknown as Record<string, unknown>;
        expect(String(ref)).toBe('[actor IntroServer#k1]');
        expect(ref.constructor).toBeUndefined();
        expect(JSON.stringify(ref)).toBe('{}');
    });
});

describe('host client proxy introspection', () => {
    it('logging a ref does not activate the actor; real calls still do', async () => {
        let activations = 0;
        const Probe = defineActor({
            type: 'IntroHost',
            allowAnonymous: true,
            state: () => {
                activations += 1;
                return {};
            },
            methods: () => ({
                async ping() {
                    return 'pong';
                }
            })
        });
        const host = createHost({ actors: [Probe], defaults: quiet });
        const ref = host.actor(Probe, 'p1');
        const loose = ref as unknown as Record<string, unknown>;
        expect(String(ref)).toBe('[actor IntroHost#p1]');
        expect(JSON.stringify(ref)).toBe('{}');
        inspect(ref);
        expect(loose.constructor).toBeUndefined();
        expect(activations).toBe(0);
        await expect(host.actor(Probe, 'p1').ping()).resolves.toBe('pong');
        expect(activations).toBe(1);
        await host.stop({ timeoutMs: 1000 });
    });
});
