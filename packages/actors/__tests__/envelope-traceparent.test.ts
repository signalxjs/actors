/**
 * The envelope's optional `tp` (traceparent) field — issue #245.
 *
 * Two postures are pinned here. **Additive**: an envelope without the field
 * is byte-identical to what this build shipped before the field existed, and
 * a decoder accepts envelopes both with and without it — a rolling deploy is
 * a mixed-version cluster. **Lossy, never loud**: `callId`/`hops` are
 * load-bearing and malformed values 400, but a malformed traceparent may
 * only cost the trace, never the call.
 */
import { describe, expect, it } from 'vitest';
import { decodeEnvelope, encodeEnvelope } from '@sigx/actors/cluster';

const TP = '00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01';

const call = (traceparent?: string) => ({
    callChain: ['a'],
    callId: 'c1.x.1',
    ...(traceparent !== undefined ? { traceparent } : {})
});

describe('envelope traceparent', () => {
    it('round-trips a valid traceparent', () => {
        const decoded = decodeEnvelope(encodeEnvelope(call(TP), 'h1'));
        expect(decoded.call.traceparent).toBe(TP);
        expect(decoded.call.callId).toBe('c1.x.1');
        expect(decoded.from).toBe('h1');
    });

    it('emits no tp key at all when the call carries no traceparent', () => {
        // Byte-compat with pre-field builds: the key SET must not change,
        // not merely the value — an old peer's positive validation never
        // sees the field, and a diff of two captures stays clean.
        const parsed = JSON.parse(encodeEnvelope(call(), 'h1')) as Record<string, unknown>;
        expect(Object.keys(parsed)).toEqual(['v', 'callId', 'chain', 'from', 'hops']);
    });

    it('drops a malformed traceparent at encode, keeping the envelope clean', () => {
        const parsed = JSON.parse(
            encodeEnvelope(call('not-a-traceparent'), 'h1')
        ) as Record<string, unknown>;
        expect('tp' in parsed).toBe(false);
    });

    it('decodes an envelope from a peer that predates the field', () => {
        const legacy = JSON.stringify({
            v: 1,
            callId: 'c2.x.1',
            chain: [],
            from: 'old-host',
            hops: 1
        });
        const decoded = decodeEnvelope(legacy);
        expect(decoded.call.traceparent).toBeUndefined();
        expect(decoded.call.callId).toBe('c2.x.1');
    });

    it.each([
        ['a number', 123],
        ['the wrong shape', 'zz-nope'],
        ['uppercase hex', TP.toUpperCase()],
        ['an object', { traceId: 'x' }],
        // The spec marks these INVALID, not merely unknown.
        ['the reserved ff version', `ff${TP.slice(2)}`],
        ['an all-zero trace-id', `00-${'0'.repeat(32)}-00f067aa0ba902b7-01`],
        ['an all-zero parent-id', `00-4bf92f3577b34da6a3ce929d0e0e4736-${'0'.repeat(16)}-01`]
    ])('decodes but omits a malformed tp (%s) — the call must survive', (_label, tp) => {
        const header = JSON.stringify({
            v: 1,
            callId: 'c3.x.1',
            chain: [],
            from: 'h2',
            hops: 1,
            tp
        });
        const decoded = decodeEnvelope(header);
        expect(decoded.call.callId).toBe('c3.x.1');
        expect(decoded.call.traceparent).toBeUndefined();
    });

    it('still ignores unknown extra keys (a NEWER peer than this build)', () => {
        const future = JSON.stringify({
            v: 1,
            callId: 'c4.x.1',
            chain: [],
            from: 'h3',
            hops: 1,
            tp: TP,
            someFutureField: { nested: true }
        });
        const decoded = decodeEnvelope(future);
        expect(decoded.call.traceparent).toBe(TP);
    });
});
