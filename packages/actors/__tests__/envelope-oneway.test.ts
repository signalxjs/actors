/**
 * The envelope's optional `ow` (one-way) flag — issue #246.
 *
 * Same two postures as `tp` (see envelope-traceparent.test.ts). **Additive**:
 * a normal call's envelope is byte-identical to pre-field builds, and both
 * peers of a mixed-version cluster interop. **Lossy, never loud**: a
 * malformed `ow` degrades the call to a normal awaited delivery — the caller
 * waits longer, nothing is lost — never a 400.
 */
import { describe, expect, it } from 'vitest';
import { decodeEnvelope, encodeEnvelope } from '@sigx/actors/cluster';

const call = (oneWay?: true) => ({
    callChain: ['a'],
    callId: 'c1.x.1',
    ...(oneWay !== undefined ? { oneWay } : {})
});

describe('envelope one-way flag', () => {
    it('round-trips the flag', () => {
        const decoded = decodeEnvelope(encodeEnvelope(call(true), 'h1'));
        expect(decoded.call.oneWay).toBe(true);
        expect(decoded.call.callId).toBe('c1.x.1');
        expect(decoded.from).toBe('h1');
    });

    it('emits no ow key at all for a normal call', () => {
        // Byte-compat with pre-field builds: the key SET must not change.
        const parsed = JSON.parse(encodeEnvelope(call(), 'h1')) as Record<string, unknown>;
        expect(Object.keys(parsed)).toEqual(['v', 'callId', 'chain', 'from', 'hops']);
    });

    it('decodes an envelope from a peer that predates the flag', () => {
        const legacy = JSON.stringify({
            v: 1,
            callId: 'c2.x.1',
            chain: [],
            from: 'old-host',
            hops: 1
        });
        const decoded = decodeEnvelope(legacy);
        expect(decoded.call.oneWay).toBeUndefined();
        expect(decoded.call.callId).toBe('c2.x.1');
    });

    it.each([
        ['a wrong number', 2],
        ['a string', '1'],
        ['a boolean', true],
        ['an object', { oneWay: true }],
        ['null', null]
    ])('decodes but omits a malformed ow (%s) — the call degrades to awaited', (_label, ow) => {
        const header = JSON.stringify({
            v: 1,
            callId: 'c3.x.1',
            chain: [],
            from: 'h2',
            hops: 1,
            ow
        });
        const decoded = decodeEnvelope(header);
        expect(decoded.call.callId).toBe('c3.x.1');
        expect(decoded.call.oneWay).toBeUndefined();
    });

    it('still ignores unknown extra keys (a NEWER peer than this build)', () => {
        const future = JSON.stringify({
            v: 1,
            callId: 'c4.x.1',
            chain: [],
            from: 'h3',
            hops: 1,
            ow: 1,
            someFutureField: { nested: true }
        });
        const decoded = decodeEnvelope(future);
        expect(decoded.call.oneWay).toBe(true);
    });
});
