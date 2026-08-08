/**
 * `@sigx/actors/socket-wire` (#99): the published client-socket vocabulary.
 *
 * Two things are pinned here. The subpath must hand an out-of-repo adapter
 * the SAME codec and pollution-safe parse the built-in transports use —
 * a copy is how two wires drift. And the `{i,v}` / `{i,e}` reply shapes must
 * stay assignable from the `$live` endpoint's `LiveFrame`, because "a live
 * value over the socket is byte-identical to one over `$live`" is a protocol
 * promise, not a coincidence.
 */
import { describe, expect, expectTypeOf, it } from 'vitest';
import {
    encodeWire,
    parseWire,
    reviveWire,
    wireFail,
    type SocketReply,
    type SocketRequest
} from '@sigx/actors/socket-wire';
import * as wireShared from '../src/wire-shared';
import * as wireParse from '../src/wire-parse';
import type { LiveFrame } from '../src/wire-shared';

describe('socket-wire', () => {
    it('re-exports the one true codec and parse, not copies', () => {
        expect(encodeWire).toBe(wireShared.encodeWire);
        expect(reviveWire).toBe(wireShared.reviveWire);
        expect(wireFail).toBe(wireShared.wireFail);
        expect(parseWire).toBe(wireParse.parseWire);
    });

    it('parses socket input pollution-safely', () => {
        expect(parseWire('{"i":1,"v":{"__proto__":{"pwned":1},"ok":2}}')).toEqual({
            i: 1,
            v: { ok: 2 }
        });
    });

    it('every LiveFrame is a valid SocketReply', () => {
        expectTypeOf<LiveFrame>().toExtend<SocketReply>();
    });

    it('the request vocabulary matches the documented shapes', () => {
        expectTypeOf<{ i: number; s: string; a: unknown[]; k?: 1; w?: 1 }>().toExtend<
            SocketRequest
        >();
        expectTypeOf<{ i: number; c: 1 }>().toExtend<SocketRequest>();
        expectTypeOf<{ i: number; sub: { t: string; k: string; m: string } }>().toExtend<
            SocketRequest
        >();
        expectTypeOf<{ i: number; uns: 1 }>().toExtend<SocketRequest>();
        expectTypeOf<{ p: 1 }>().toExtend<SocketRequest>();
    });
});
