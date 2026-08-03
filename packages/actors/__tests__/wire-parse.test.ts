/**
 * The guarded wire parse (#218): `parseWire` skips the prototype-pollution
 * reviver — which disables V8's fast JSON parser — whenever no dangerous key
 * can be present in the text. These tests pin the safety contract the guard
 * must preserve: byte-for-byte equivalence with `JSON.parse(text, reviver)`.
 */
import { describe, expect, it } from 'vitest';
import { parseWire, parseWireWith, reviver } from '../src/wire-shared';
import { decodeFrameBody, encodeFrameBody, FrameReader, FrameType } from '../src/cluster/frames';
import { readNdjson } from '../src/wire-shared';

const parseSlow = (text: string): unknown => JSON.parse(text, reviver);

describe('parseWire', () => {
    it('drops literal dangerous keys — top-level, nested, and inside arrays', () => {
        expect(parseWire('{"__proto__": {"pwned": 1}, "ok": 2}')).toEqual({ ok: 2 });
        expect(parseWire('{"a": {"constructor": {"x": 1}, "keep": 3}}')).toEqual({
            a: { keep: 3 }
        });
        expect(parseWire('{"a": {"b": {"prototype": 1, "c": 2}}}')).toEqual({
            a: { b: { c: 2 } }
        });
        expect(parseWire('[{"__proto__": {"pwned": 1}}, {"ok": true}]')).toEqual([
            {},
            { ok: true }
        ]);
        // The point of the reviver: nothing leaked onto Object.prototype.
        expect(({} as { pwned?: unknown }).pwned).toBeUndefined();
    });

    it('drops a unicode-escaped "__proto__" key — the \\u clause is load-bearing', () => {
        // '{"\u005f\u005fproto\u005f\u005f": 1}' contains none of the three
        // literals, so any \u escape at all must force the reviver path.
        const text = '{"\\u005f\\u005fproto\\u005f\\u005f": {"pwned": 1}, "ok": 2}';
        expect(parseWire(text)).toEqual({ ok: 2 });
        expect(({} as { pwned?: unknown }).pwned).toBeUndefined();
    });

    it('a key merely containing a dangerous substring parses unchanged', () => {
        expect(parseWire('{"my__proto__ish": 1}')).toEqual({ my__proto__ish: 1 });
        expect(parseWire('{"constructor_arg": 2, "prototypes": 3}')).toEqual({
            constructor_arg: 2,
            prototypes: 3
        });
    });

    it('a clean payload (the fast path) parses identically to the reviver path', () => {
        const text = '{"a": [1, 2.5, true, null, "s"], "b": {"c": "d"}}';
        expect(parseWire(text)).toEqual(parseSlow(text));
    });

    it('matches JSON.parse(text, reviver) over a mixed corpus', () => {
        const corpus = [
            '42',
            '"plain string"',
            'null',
            '[]',
            '{}',
            '{"n": -1.5e10, "s": "x", "b": false}',
            '{"nested": {"deep": [{"k": "v"}]}}',
            '{"__proto__": 1}',
            '{"constructor": {"prototype": {"pwned": true}}}',
            '{"value is __proto__": "the VALUE, not the key, mentions it"}',
            '{"s": "a string containing constructor and prototype"}',
            '{"u": "\\u0041\\u00e5"}',
            '{"q": "quote \\" backslash \\\\ path c:\\\\utils"}',
            '{"mixed": ["\\u005f", {"__proto__": []}, "tail"]}',
            '{"empty": "", "zero": 0, "arr": [[], [[]]]}'
        ];
        for (const text of corpus) {
            expect(parseWire(text), text).toEqual(parseSlow(text));
        }
    });
});

describe('parseWireWith', () => {
    it('takes the guarded path for the default reviver (by identity)', () => {
        expect(parseWireWith('{"__proto__": {"pwned": 1}, "ok": 2}', reviver)).toEqual({ ok: 2 });
        expect(parseWireWith('{"clean": true}', reviver)).toEqual({ clean: true });
    });

    it('a custom reviver is always invoked — never downgraded to the fast path', () => {
        const seen: string[] = [];
        const custom = (key: string, value: unknown): unknown => {
            seen.push(key);
            return value;
        };
        // A payload the guard would fast-path for the default reviver.
        expect(parseWireWith('{"clean": true}', custom)).toEqual({ clean: true });
        expect(seen).toEqual(['clean', '']);
    });
});

describe('frame decode pollution safety', () => {
    const frameWith = (payload: unknown): Uint8Array =>
        encodeFrameBody({ type: FrameType.CALL, flags: 0, status: 0, corrId: 1, payload });

    it('drops a hostile key from a frame payload under the default reviver', () => {
        // Craft the hostile body by hand: encodeFrameBody would need the
        // hostile object to exist first, which JSON.stringify cannot emit for
        // __proto__ — so splice the JSON text in directly.
        const clean = frameWith({ marker: true });
        const header = clean.subarray(0, 8);
        const hostile = new TextEncoder().encode('{"__proto__": {"pwned": 1}, "ok": 2}');
        const body = new Uint8Array(header.length + hostile.length);
        body.set(header, 0);
        body.set(hostile, header.length);
        expect(decodeFrameBody(body, reviver).payload).toEqual({ ok: 2 });
        expect(({} as { pwned?: unknown }).pwned).toBeUndefined();
    });

    it('FrameReader applies a custom reviver to every decoded payload', () => {
        let calls = 0;
        const counting = (_key: string, value: unknown): unknown => {
            calls++;
            return value;
        };
        const body = frameWith({ a: 1, b: 2 });
        const framed = new Uint8Array(4 + body.length);
        new DataView(framed.buffer).setUint32(0, body.length, false);
        framed.set(body, 4);
        const reader = new FrameReader(1024, counting);
        reader.push(framed);
        const frames = [...reader.drain()];
        expect(frames).toHaveLength(1);
        expect(frames[0]?.payload).toEqual({ a: 1, b: 2 });
        expect(calls).toBeGreaterThan(0);
    });
});

describe('NDJSON pollution safety', () => {
    it('drops a hostile key from a streamed chunk line', async () => {
        const lines =
            '{"chunk": {"__proto__": {"pwned": 1}, "ok": 2}}\n' + '{"done": 1}\n';
        const res = new Response(lines, { status: 200 });
        const seen: unknown[] = [];
        for await (const chunk of readNdjson(res, 'test#stream')) {
            seen.push(chunk);
        }
        expect(seen).toEqual([{ ok: 2 }]);
        expect(({} as { pwned?: unknown }).pwned).toBeUndefined();
    });
});
