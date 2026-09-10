/**
 * The `HostHmac` seam (#440): the hash is pluggable, the wire is not.
 *
 * `webCryptoHmac` (the default, asynchronous) and `nodeHmac()` (synchronous,
 * `node:crypto`) must produce byte-identical headers so a fleet can carry
 * both during a roll — and the synchronous one must answer WITHOUT a
 * promise, since that is the whole reason it exists: `crypto.subtle.sign`
 * hops the threadpool on Node, and the callers branch on the result rather
 * than awaiting it.
 */
import { describe, expect, it } from 'vitest';
import { defineActor } from '@sigx/actors';
import {
    signAuth,
    signAuthWith,
    verifyAuth,
    verifyAuthWith,
    webCryptoHmac,
    type HostHmac
} from '@sigx/actors/cluster';
import { nodeHmac } from '@sigx/actors/node';
import { createCluster } from './harness';

const SECRET = 'a-shared-secret';

const Counter = defineActor({
    type: 'Counter',
    allowAnonymous: true,
    state: () => ({ count: 0 }),
    methods: (ctx) => ({
        increment(by: number) {
            ctx.state.count += by;
            return ctx.state.count;
        }
    })
});

describe('HostHmac: one wire, two implementations', () => {
    it('nodeHmac() answers synchronously; webCryptoHmac answers with a promise', async () => {
        const sync = signAuthWith(nodeHmac(), SECRET, 'Counter#increment', 'call-1');
        expect(typeof sync).toBe('string');
        const async = signAuthWith(webCryptoHmac, SECRET, 'Counter#increment', 'call-1');
        expect(async).toBeInstanceOf(Promise);
        expect(typeof (await async)).toBe('string');
    });

    it('both produce the same header grammar and the same digest for the same message', async () => {
        // Pin the timestamp by computing the hex directly: the header carries
        // `Date.now()`, so two signatures a millisecond apart differ by design.
        const message = 'sigx-host/1\nCounter#increment\ncall-1\n1757500000000';
        const fromNode = nodeHmac().hex(SECRET, message);
        const fromWeb = await webCryptoHmac.hex(SECRET, message);
        expect(fromNode).toMatch(/^[0-9a-f]{64}$/);
        expect(fromNode).toBe(fromWeb);
    });

    it('signs with one implementation and verifies with the other, both ways', async () => {
        const web = webCryptoHmac;
        const node = nodeHmac();
        const symbol = 'Counter#increment';

        const signedByNode = signAuthWith(node, SECRET, symbol, 'c-1') as string;
        expect(await verifyAuthWith(web, SECRET, signedByNode, symbol, 'c-1')).toBe(true);

        const signedByWeb = await signAuthWith(web, SECRET, symbol, 'c-2');
        const verdict = verifyAuthWith(node, SECRET, signedByWeb, symbol, 'c-2');
        // The synchronous verifier answers a boolean, not a promise.
        expect(verdict).toBe(true);
    });

    it('binds the signature to symbol, callId and secret under either implementation', async () => {
        for (const hmac of [webCryptoHmac, nodeHmac()]) {
            const header = await signAuthWith(hmac, SECRET, 'Counter#increment', 'c-1');
            expect(await verifyAuthWith(hmac, SECRET, header, 'Counter#increment', 'c-1')).toBe(true);
            expect(await verifyAuthWith(hmac, SECRET, header, 'Counter#reset', 'c-1')).toBe(false);
            expect(await verifyAuthWith(hmac, SECRET, header, 'Counter#increment', 'c-2')).toBe(false);
            expect(await verifyAuthWith(hmac, 'other', header, 'Counter#increment', 'c-1')).toBe(false);
        }
    });

    it('rejects a malformed header synchronously, whichever implementation is in use', () => {
        let asked = 0;
        const counting: HostHmac = {
            hex: (secret, message) => {
                asked++;
                return nodeHmac().hex(secret, message);
            }
        };
        for (const header of [null, '', 'v1.123', 'v2.123.' + 'a'.repeat(64), 'v1.abc.' + 'a'.repeat(64), 'v1.123.' + 'g'.repeat(64)]) {
            expect(verifyAuthWith(counting, SECRET, header, 'Counter#increment', 'c-1')).toBe(false);
        }
        // A stale timestamp is refused before the hash is ever computed.
        expect(verifyAuthWith(counting, SECRET, `v1.${Date.now() - 6 * 60_000}.${'a'.repeat(64)}`, 'S', 'c')).toBe(false);
        expect(asked).toBe(0);
    });

    it('the promise-returning signAuth/verifyAuth keep their contract and accept an implementation', async () => {
        const p = signAuth(SECRET, 'Counter#increment', 'c-1', nodeHmac());
        expect(p).toBeInstanceOf(Promise);
        const header = await p;
        const v = verifyAuth(SECRET, header, 'Counter#increment', 'c-1');
        expect(v).toBeInstanceOf(Promise);
        expect(await v).toBe(true);
    });
});

describe('a mixed fleet authenticates end to end', () => {
    it('host 0 on nodeHmac() and host 1 on the default call each other over the secured mount', async () => {
        const harness = await createCluster(2, {
            actors: [Counter],
            secret: SECRET,
            hmacFor: (i) => (i === 0 ? nodeHmac() : undefined)
        });
        try {
            const [a, b] = harness.hosts as [(typeof harness.hosts)[number], (typeof harness.hosts)[number]];
            // Two keys, so each host owns one and every second call crosses the wire.
            expect(await a.actor(Counter, 'owned-by-b-first').increment(1)).toBe(1);
            expect(await b.actor(Counter, 'owned-by-b-first').increment(1)).toBe(2);
            expect(await b.actor(Counter, 'owned-by-a-first').increment(1)).toBe(1);
            expect(await a.actor(Counter, 'owned-by-a-first').increment(1)).toBe(2);
            const remote = harness.placements.reduce(
                (n, p) => n + p.report().counters.dispatchesRemote,
                0
            );
            expect(remote).toBeGreaterThan(0);
        } finally {
            await harness.stop();
        }
    });

    it('a host on nodeHmac() with the WRONG secret is refused by a default host', async () => {
        const harness = await createCluster(2, {
            actors: [Counter],
            secret: SECRET,
            hmacFor: (i) => (i === 0 ? { hex: (_secret, message) => nodeHmac().hex('not-the-secret', message) } : undefined)
        });
        try {
            const [a, b] = harness.hosts as [(typeof harness.hosts)[number], (typeof harness.hosts)[number]];
            await b.actor(Counter, 'k').increment(1); // b owns it
            await expect(a.actor(Counter, 'k').increment(1)).rejects.toThrow();
        } finally {
            await harness.stop();
        }
    });
});
