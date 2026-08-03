/**
 * `cluster()` must not serve an unauthenticated internal mount by omission.
 *
 * The internal mount runs NO guards — the public edge is supposed to have run
 * them already — and it is contributed as an ordinary route, so it lands on
 * the same listener as the public actor endpoint. Without a secret,
 * `handleHostRequestForRuntime`'s guard returns early and every registered
 * type, key and method is reachable unauthenticated, including the
 * `$sigx:*` symbols the public mount deliberately refuses.
 *
 * That is strictly more exposure than `ops()`, which has thrown outside
 * `__DEV__` since it shipped. These tests hold `cluster()` to the same
 * posture, and pin the two cases that must NOT throw: the deliberate opt-out
 * for a trusted network, and `hostEndpointRuntime()`, whose caller (a
 * Cloudflare Durable Object) is not network-reachable and has no peer to
 * authenticate against.
 */
import { describe, expect, it, vi } from 'vitest';
import { defineActor } from '@sigx/actors';
import { cluster, hostEndpointRuntime, memoryClusterHub } from '@sigx/actors/cluster';
import { createHost, memoryStorage } from '@sigx/actors/host';

const Counter = defineActor({
    type: 'Counter',
    unguarded: true,
    state: () => ({ count: 0 }),
    methods: (ctx) => ({
        increment(by: number) {
            ctx.state.count += by;
            return ctx.state.count;
        }
    })
});

/** The smallest `cluster()` that constructs; `secret` is supplied per test. */
const clusterWith = (secret?: string | null) =>
    cluster({
        providers: memoryClusterHub().providers(),
        advertise: 'http://127.0.0.1:7311',
        ...(secret === undefined ? {} : { secret })
    });

describe('cluster(): the secret is not optional by omission', () => {
    it('refuses to construct without a secret when __DEV__ is false', () => {
        vi.stubGlobal('__DEV__', false);
        try {
            expect(() => clusterWith()).toThrow(/requires a secret outside development/);
        } finally {
            vi.unstubAllGlobals();
        }
    });

    it('constructs unauthenticated in dev, but says so once', () => {
        const warn = vi.spyOn(console, 'warn').mockImplementation(() => {});
        try {
            clusterWith();
            expect(warn).toHaveBeenCalledTimes(1);
            expect(warn.mock.calls[0]![0]).toMatch(/UNAUTHENTICATED/);
        } finally {
            warn.mockRestore();
        }
    });

    it.each([
        ['empty', ''],
        ['whitespace only', '   ']
    ])('refuses a %s secret, in dev as well — that shape is never deliberate', (_label, secret) => {
        // `secret: process.env.CLUSTER_SECRET ?? ''` is the motivating bug: it
        // reads as configured and authenticates with an empty key. Unlike a
        // missing secret there is no development story for it, so it throws
        // whatever __DEV__ says.
        expect(() => clusterWith(secret)).toThrow(/must not be empty/);

        vi.stubGlobal('__DEV__', false);
        try {
            expect(() => clusterWith(secret)).toThrow(/must not be empty/);
        } finally {
            vi.unstubAllGlobals();
        }
    });

    it('accepts an explicit null as the deliberate opt-out, silently', () => {
        const warn = vi.spyOn(console, 'warn').mockImplementation(() => {});
        vi.stubGlobal('__DEV__', false);
        try {
            expect(() => clusterWith(null)).not.toThrow();
            // Silent by design: the caller said so on purpose, and a warning
            // that fires on every correct boot is a warning nobody reads.
            expect(warn).not.toHaveBeenCalled();
        } finally {
            vi.unstubAllGlobals();
            warn.mockRestore();
        }
    });

    it('accepts a real secret without warning', () => {
        const warn = vi.spyOn(console, 'warn').mockImplementation(() => {});
        vi.stubGlobal('__DEV__', false);
        try {
            expect(() => clusterWith('a-shared-cluster-secret')).not.toThrow();
            expect(warn).not.toHaveBeenCalled();
        } finally {
            vi.unstubAllGlobals();
            warn.mockRestore();
        }
    });
});

describe('hostEndpointRuntime(): the no-cluster carve-out still works', () => {
    it('needs no secret, even when __DEV__ is false', async () => {
        // A Durable Object holds one actor, the platform guarantees the single
        // instance, and the Worker→object hop is a stub call rather than a
        // network request. Gating `cluster()` must not reach this path.
        vi.stubGlobal('__DEV__', false);
        const host = createHost({
            actors: [Counter],
            storage: memoryStorage(),
            defaults: { sweepIntervalMs: 60_000, reminderTickMs: 60_000 }
        });
        try {
            expect(() => hostEndpointRuntime(host)).not.toThrow();
        } finally {
            vi.unstubAllGlobals();
            await host.stop?.({ timeoutMs: 1000 });
        }
    });
});
