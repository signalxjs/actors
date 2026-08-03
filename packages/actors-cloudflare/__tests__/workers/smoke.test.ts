/**
 * Does any of this run on workerd at all?
 *
 * The most boring assertions in the repo and, until now, the only completely
 * unverified ones: that `@sigx/actors/host` and this package LOAD on the
 * platform, that a host boots inside a Durable Object, and that a call
 * survives the public wire.
 */
import { env, SELF } from 'cloudflare:test';
import { describe, expect, it } from 'vitest';
import { durableObjectStorage, type DurableAlarms, type DurableStorage } from '@sigx/actors-cloudflare';
import type { Env } from './fixture-worker';

const SEP = '\u0000';

declare module 'cloudflare:test' {
    interface ProvidedEnv extends Env {}
}

async function call(symbol: string, args: readonly unknown[]): Promise<Response> {
    return SELF.fetch(`https://edge.test/_sigx/actor/${encodeURIComponent(symbol)}`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ args })
    });
}

async function invoke(symbol: string, args: readonly unknown[]): Promise<unknown> {
    const res = await call(symbol, args);
    const body = (await res.json()) as { data?: unknown; error?: { message?: string } };
    if (!res.ok || body.error) {
        throw new Error(body.error?.message ?? `HTTP ${res.status}`);
    }
    return body.data;
}

describe('workerd smoke', () => {
    it('serves an actor call over the public wire', async () => {
        await expect(invoke('Counter#increment', ['a', 2])).resolves.toBe(2);
        await expect(invoke('Counter#increment', ['a', 3])).resolves.toBe(5);
        await expect(invoke('Counter#read', ['a'])).resolves.toBe(5);
    });

    it('gives two keys two objects, and the same key one', async () => {
        await invoke('Counter#increment', ['x', 1]);
        await invoke('Counter#increment', ['y', 1]);
        await invoke('Counter#increment', ['x', 1]);
        await expect(invoke('Counter#read', ['x'])).resolves.toBe(2);
        await expect(invoke('Counter#read', ['y'])).resolves.toBe(1);
    });

    it('keeps working past the FIRST request through an isolate', async () => {
        // The regression. A Durable Object stub is an I/O object owned by the
        // request context that created it, and workerd refuses to use one
        // from another request — so caching stubs made request 1 succeed and
        // every later one fail with `unreachable`. Sequential, because that
        // is what exposed it: the app is memoized, so only the second request
        // onwards uses anything the first one built.
        for (let i = 1; i <= 5; i++) {
            await expect(invoke('Counter#increment', ['isolate', 1])).resolves.toBe(i);
        }
    });

    it('serves concurrent requests without crossing their bindings', async () => {
        // The namespace binding IS captured once at boot and shared. That is
        // safe precisely because no stub outlives a dispatch — but a Worker
        // isolate does serve requests concurrently, so it is worth pinning.
        const keys = ['c1', 'c2', 'c3', 'c4', 'c5', 'c6'];
        const results = await Promise.all(
            keys.map((k) => invoke('Counter#increment', [k, 1]))
        );
        expect(results).toEqual(keys.map(() => 1));
        const reads = await Promise.all(keys.map((k) => invoke('Counter#read', [k])));
        expect(reads).toEqual(keys.map(() => 1));
    });

    it('accepts a NUL byte in idFromName', () => {
        // The placement derives object names from the runtime's actor id,
        // whose separator is NUL. If workerd refused it, every actor would be
        // unreachable — and the decided fallback is JSON.stringify([type,key]).
        const id = env.ACTORS.idFromName('Counter\u0000a');
        expect(id.toString()).toMatch(/^[0-9a-f]+$/);
        // Same name, same object; different name, different object.
        expect(env.ACTORS.idFromName('Counter\u0000a').toString()).toBe(id.toString());
        expect(env.ACTORS.idFromName('Counter\u0000b').toString()).not.toBe(id.toString());
    });

    it('sends a cross-actor call out to the callee object', async () => {
        await expect(invoke('Counter#bumpPeer', ['p', 'q'])).resolves.toBe(1);
        await expect(invoke('Counter#read', ['q'])).resolves.toBe(1);
        // The caller's own state is untouched — it never hosted the callee.
        await expect(invoke('Counter#read', ['p'])).resolves.toBe(0);
    });

    it('streams NDJSON from the object through the Worker', async () => {
        const res = await call('Counter#watch', ['s']);
        expect(res.status).toBe(200);
        const reader = res.body!.getReader();
        const decoder = new TextDecoder();
        let buffer = '';
        while (!buffer.includes('\n')) {
            const { value, done } = await reader.read();
            if (done) break;
            buffer += decoder.decode(value, { stream: true });
        }
        expect(JSON.parse(buffer.slice(0, buffer.indexOf('\n'))).chunk).toMatchObject({
            count: 0
        });
        await reader.cancel();
    });
});

describe('the real DurableObjectStorage satisfies our interfaces', () => {
    it('is assignable, and works, in a real object', async () => {
        // Until now `@cloudflare/workers-types` was a devDependency used in
        // ZERO type positions, so even the compile-time claim was absent. The
        // real `put` takes an options arg and has a multi-key overload; `get`
        // has a Map-returning overload; `delete` has an array form.
        const id = env.ACTORS.idFromName(`Counter${SEP}conformance`);
        const stub = env.ACTORS.get(id);
        // Proven from inside the object in storage.test.ts; here we assert the
        // TYPES line up, which is a compile-time claim this file carries.
        const check = (state: DurableObjectState): void => {
            const asStorage: DurableStorage = state.storage;
            const asAlarms: DurableAlarms = state.storage;
            void durableObjectStorage(asStorage);
            void asAlarms;
        };
        expect(typeof check).toBe('function');
        expect(stub).toBeDefined();
    });
});
