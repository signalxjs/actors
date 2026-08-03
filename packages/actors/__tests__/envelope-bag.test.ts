/**
 * The envelope's optional `bag` field (#246) — the request-context bag on
 * the host-to-host wire.
 *
 * Same postures as `tp`/`ow` (see envelope-traceparent.test.ts): additive
 * within v1 — a bagless envelope is byte-identical to pre-field builds —
 * and lenient at decode: a malformed bag is dropped WHOLE (a partial
 * identity is worse than none), never a 400. Plus one posture of its own:
 * the decoded bag is the one OBJECT crossing the envelope's plain
 * JSON.parse, so it must be rebuilt, never aliased, and a `__proto__` key
 * must not reach Object.prototype.
 */
import { describe, expect, it } from 'vitest';
import { decodeEnvelope, encodeEnvelope } from '@sigx/actors/cluster';

const call = (bag?: Readonly<Record<string, string>>) => ({
    callChain: ['a'],
    callId: 'c1.x.1',
    ...(bag !== undefined ? { bag } : {})
});

const legacyFields = { v: 1, callId: 'c2.x.1', chain: [], from: 'h1', hops: 1 };

describe('envelope context bag', () => {
    it('round-trips a valid bag, frozen and rebuilt', () => {
        const bag = Object.freeze({ user: 'ada', 'corr-id': 'r-42' });
        const decoded = decodeEnvelope(encodeEnvelope(call(bag), 'h1'));
        expect(decoded.call.bag).toEqual(bag);
        expect(decoded.call.bag).not.toBe(bag);
        expect(Object.isFrozen(decoded.call.bag)).toBe(true);
        expect(decoded.call.callId).toBe('c1.x.1');
    });

    it('emits no bag key at all when the call carries none — and none for an empty bag', () => {
        // Byte-compat with pre-field builds: the key SET must not change.
        const bare = JSON.parse(encodeEnvelope(call(), 'h1')) as Record<string, unknown>;
        expect(Object.keys(bare)).toEqual(['v', 'callId', 'chain', 'from', 'hops']);
        const empty = JSON.parse(encodeEnvelope(call(Object.freeze({})), 'h1')) as Record<
            string,
            unknown
        >;
        expect('bag' in empty).toBe(false);
    });

    it('drops a middleware-injected invalid bag at encode, keeping the envelope clean', () => {
        // A useDispatch middleware can put anything on the context; the
        // envelope validates rather than shipping bytes the peer discards.
        const parsed = JSON.parse(
            encodeEnvelope(call({ k: 42 } as unknown as Record<string, string>), 'h1')
        ) as Record<string, unknown>;
        expect('bag' in parsed).toBe(false);
    });

    it('decodes an envelope from a peer that predates the field', () => {
        const decoded = decodeEnvelope(JSON.stringify(legacyFields));
        expect(decoded.call.bag).toBeUndefined();
        expect(decoded.call.callId).toBe('c2.x.1');
    });

    it.each([
        ['an array', ['x']],
        ['a string', 'user=ada'],
        ['a number', 7],
        ['a non-string value', { user: 42 }],
        ['a nested object', { user: { name: 'ada' } }],
        ['nine entries', Object.fromEntries(Array.from({ length: 9 }, (_, i) => [`k${i}`, 'v']))],
        ['an oversize value', { k: 'v'.repeat(300) }],
        ['a __proto__ key beside a valid one', JSON.parse('{"__proto__": "x", "user": "ada"}')]
    ])('decodes but omits a malformed bag (%s) WHOLE — the call must survive', (_label, bag) => {
        const decoded = decodeEnvelope(JSON.stringify({ ...legacyFields, bag }));
        expect(decoded.call.callId).toBe('c2.x.1');
        expect(decoded.call.bag).toBeUndefined();
    });

    it('a decoded __proto__ bag leaves Object.prototype unpolluted', () => {
        decodeEnvelope(
            JSON.stringify({ ...legacyFields }).replace(
                '"hops":1',
                '"hops":1,"bag":{"__proto__":{"polluted":true}}'
            )
        );
        expect(({} as { polluted?: boolean }).polluted).toBeUndefined();
    });

    it('a non-ASCII value round-trips and the encoded header stays pure ASCII', () => {
        const bag = Object.freeze({ user: 'Åsa Ödegård' });
        const header = encodeEnvelope(call(bag), 'h1');
        // eslint-disable-next-line no-control-regex
        expect(/^[\x00-\x7f]*$/.test(header)).toBe(true);
        expect(decodeEnvelope(header).call.bag).toEqual(bag);
    });

    it('still ignores unknown extra keys (a NEWER peer than this build)', () => {
        const decoded = decodeEnvelope(
            JSON.stringify({ ...legacyFields, bag: { user: 'ada' }, someFutureField: 1 })
        );
        expect(decoded.call.bag).toEqual({ user: 'ada' });
    });
});
