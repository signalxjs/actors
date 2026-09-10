/**
 * The host-to-host call envelope — one JSON header carrying the call
 * metadata the PUBLIC wire deliberately has no fields for. Compat-critical:
 * mixed-version clusters exist during every rolling deploy, so the header
 * name, the `v` discipline, and the remaining-ms deadline semantics are
 * pinned here and versioned. Unknown versions must fail loudly (400), never
 * misbehave.
 *
 * The deadline crosses hosts as REMAINING milliseconds and is re-anchored
 * to the receiver's clock — clock skew never inflates or deflates a call's
 * budget; each hop loses only genuine elapsed time.
 */
import { isValidCallBag, sanitizeWireBag } from '../call-bag-core';
import { timingSafeEquals } from '../timing-safe';
import { isTraceparent } from '../traceparent';
import type { ActorCallContext } from '../types';

export const HOST_CALL_HEADER = 'x-sigx-host-call';
export const HOST_AUTH_HEADER = 'x-sigx-cluster-auth';
// Peers speaking an older vocabulary are already excluded by the
// `/_sigx/host` route prefix, so v1 needs no bump for renames.
export const HOST_PROTO = 1;
/** Defensive forward-loop cap — redirect-not-proxy means hops stay at 1. */
const MAX_HOPS = 8;

interface WireEnvelope {
    v: number;
    callId: string;
    chain: readonly string[];
    remainingMs?: number;
    from: string;
    hops: number;
    /** W3C traceparent. Additive within v1 — decoders ignore unknown keys,
     *  so a peer that predates it interops both ways; no `v` bump. */
    tp?: string;
    /** One-way flag. Additive within v1 like `tp`; a peer that predates it
     *  treats the call as a normal awaited one — delivered exactly the same,
     *  the sender just resolves at turn completion instead of acceptance. */
    ow?: 1;
    /** The request-context bag. Additive within v1 like `tp`; validated and
     *  size-capped at both ends (see `call-context-bag.ts`), and dropped
     *  WHOLE when malformed — a partial identity is worse than none. */
    bag?: Readonly<Record<string, string>>;
    /**
     * The ENCODED principal (rfc-server-v4 §7). Additive within v1 like
     * `tp`: a peer that predates it simply reads no identity, so
     * `ctx.principal` is null there — degrading to anonymous, never to a
     * different principal.
     *
     * A separate slot from `bag` on purpose: identity must not be
     * forgeable through the app-data channel, and the two have different
     * validators. Like the bag it rides OUTSIDE the cluster HMAC, so its
     * trust is the deployment perimeter rather than a proof.
     */
    pr?: string;
}

/** JSON.stringify, with every non-ASCII char escaped: header-value safe. */
function asciiJson(value: unknown): string {
    return JSON.stringify(value).replace(
        /[\u007f-\uffff]/g,
        (c) => `\\u${c.charCodeAt(0).toString(16).padStart(4, '0')}`
    );
}

export function encodeEnvelope(call: ActorCallContext, from: string): string {
    const envelope: WireEnvelope = {
        v: HOST_PROTO,
        callId: call.callId,
        chain: call.callChain,
        ...(call.deadline !== undefined
            ? { remainingMs: Math.max(0, call.deadline - Date.now()) }
            : {}),
        from,
        hops: 1,
        // Shape-checked at the edge so garbage never crosses the wire.
        ...(call.traceparent !== undefined && isTraceparent(call.traceparent)
            ? { tp: call.traceparent }
            : {}),
        // Key set stays byte-identical for normal calls.
        ...(call.oneWay === true ? { ow: 1 as const } : {}),
        // Re-validated even though stamp/.with already did: a useDispatch
        // middleware can put anything on the context, and the peer will drop
        // an invalid bag anyway — better to drop it HERE, where dev mode can
        // say so, than to ship bytes the receiver discards.
        ...(call.bag !== undefined && isValidCallBag(call.bag) && Object.keys(call.bag).length > 0
            ? { bag: call.bag }
            : {}),
        // A string or nothing. The codec that produced it lives on the app,
        // so this end neither parses nor trusts the contents — the receiver
        // decodes it, and a failed decode is anonymous.
        ...(typeof call.principal === 'string' && call.principal.length > 0
            ? { pr: call.principal }
            : {})
    };
    if (__DEV__ && call.bag !== undefined && !isValidCallBag(call.bag)) {
        console.warn(
            `[sigx actors] dropping an invalid context bag from the envelope for call ` +
                `${call.callId} — a middleware put a non-conforming value on ` +
                `ActorCallContext.bag. The call proceeds WITHOUT it; a receiver treats a ` +
                `missing entry as unauthenticated.`
        );
    }
    return asciiJson(envelope);
}

export interface DecodedEnvelope {
    call: Omit<ActorCallContext, 'abortSignal'>;
    from: string;
}

/** Throws a plain Error on a malformed or version-skewed envelope — the
 *  endpoint maps it to a 400. */
export function decodeEnvelope(header: string): DecodedEnvelope {
    let parsed: WireEnvelope;
    try {
        // `JSON.parse`, NOT the pollution-safe `parseWire`, and deliberately.
        //
        // This looks like the one wire-facing parse that skips the reviver,
        // and it was changed to `parseWire` on exactly that reasoning — which
        // was wrong, and the bag tests caught it. `parseWire` STRIPS
        // `__proto__` instead of surfacing it, so a bag arriving as
        // `{"__proto__": "x", "user": "ada"}` reached `isValidCallBag` already
        // sanitized, passed, and was ACCEPTED as `{user: 'ada'}`. The
        // validator's contract is to reject a malformed bag WHOLE — a caller
        // must not be able to smuggle entries past it by attaching a key that
        // the parser will quietly remove.
        //
        // Nothing propagates from the raw parse: `JSON.parse` creates
        // `__proto__` as an own data property rather than through the setter,
        // and every field below is copied into a fresh object literal rather
        // than spread. There is a test asserting `Object.prototype` stays
        // unpolluted. Leave this as `JSON.parse`.
        parsed = JSON.parse(header) as WireEnvelope;
    } catch {
        throw new Error(`[sigx actors] malformed ${HOST_CALL_HEADER} header`);
    }
    if (parsed.v !== HOST_PROTO) {
        throw new Error(
            `[sigx actors] host protocol version skew: got v${parsed.v}, this host speaks ` +
                `v${HOST_PROTO} — are all hosts from compatible deploys?`
        );
    }
    if (
        typeof parsed.callId !== 'string' ||
        !Array.isArray(parsed.chain) ||
        parsed.chain.some((hop) => typeof hop !== 'string') ||
        typeof parsed.from !== 'string' ||
        !Number.isSafeInteger(parsed.hops) ||
        (parsed.remainingMs !== undefined &&
            !(typeof parsed.remainingMs === 'number' && Number.isFinite(parsed.remainingMs)))
    ) {
        // Compat-critical header: malformed fields fail loudly (a NaN
        // deadline or NaN hops would silently break timeout/loop caps).
        throw new Error(`[sigx actors] malformed ${HOST_CALL_HEADER} header`);
    }
    if (parsed.hops > MAX_HOPS) {
        throw new Error(`[sigx actors] host call exceeded ${MAX_HOPS} hops — forwarding loop?`);
    }
    // The bag is the one OBJECT crossing this plain JSON.parse, so it is
    // rebuilt into a fresh null-prototype frozen copy — never aliased — and
    // dropped WHOLE if any entry is malformed or over-cap. Lenient tier: a
    // dropped bag costs its metadata (the receiver treats missing entries as
    // unauthenticated), never the call.
    const bag = sanitizeWireBag(parsed.bag);
    return {
        call: {
            callChain: parsed.chain,
            callId: parsed.callId,
            ...(parsed.remainingMs !== undefined
                ? { deadline: Date.now() + Math.max(0, parsed.remainingMs) }
                : {}),
            // Deliberately NOT part of the loud-failure check above: callId
            // and hops are load-bearing (timeouts, loop caps) so malformed
            // values must 400, but a malformed traceparent may only cost the
            // trace, never the call — silently omit it.
            ...(isTraceparent(parsed.tp) ? { traceparent: parsed.tp } : {}),
            // Same lenient tier: a malformed `ow` degrades the call to a
            // normal awaited delivery — the caller waits longer, nothing is
            // lost — never a 400.
            ...(parsed.ow === 1 ? { oneWay: true as const } : {}),
            ...(bag !== undefined ? { bag } : {}),
            // Lenient tier again: a non-string `pr` is dropped, leaving the
            // call anonymous rather than 400ing it. The decode itself
            // happens later, lazily, in `ctx.principal`.
            ...(typeof parsed.pr === 'string' && parsed.pr.length > 0
                ? { principal: parsed.pr }
                : {})
        },
        from: parsed.from
    };
}

// ---------------------------------------------------------------------------
// Per-request HMAC auth
//
// `x-sigx-cluster-auth: v1.<timestamp>.<hex hmac>` — HMAC-SHA-256 over
// `proto\nsymbol\ncallId\ntimestamp`, keyed by the shared secret. Binding
// the signature to the symbol and callId means a captured header cannot
// authorize a DIFFERENT call; the freshness window bounds how long any
// capture stays usable. Replaying the identical request inside the window
// is out of scope without a nonce store — run mTLS/VPC between hosts for
// transport privacy (documented posture).
//
// The signature covers proto/symbol/callId/timestamp ONLY — the envelope
// body (chain, deadline, and the request-context bag) is NOT
// integrity-protected. A peer that can reach the internal mount can forge
// bag entries without disturbing the signature, so the bag's trust is the
// same perimeter posture as the rest of the envelope: run mTLS/VPC between
// hosts. Extending the HMAC over the bag would be a HOST_PROTO bump and a
// mixed-version deploy problem — deliberately not taken for v1.
//
// Cost: the arithmetic is ~2µs; what a call actually pays depends on WHICH
// HMAC runs it (#440). WebCrypto's `subtle.sign` is asynchronous by
// contract and on Node hops the libuv threadpool — measured at ~27µs per
// call sequentially (~6µs amortized at 64 in flight), with the key cached
// (import is ~2ms, paid once per secret per process). `node:crypto`'s
// `createHmac` does the same arithmetic synchronously in ~2µs. Sign and
// verify both sit on a cross-host call's critical path, so the seam below
// lets a Node deployment swap the implementation without touching the wire.

/** Accept signatures this far from the receiver's clock, either way. A
 *  generous window so HMAC does not reintroduce clock-skew sensitivity. */
const AUTH_WINDOW_MS = 5 * 60_000;

/**
 * How the cluster computes its HMAC-SHA-256 (#440).
 *
 * The seam is the hash alone — the message format, the header grammar and
 * the freshness window stay in this module, so two hosts on different
 * implementations verify each other byte for byte and a fleet can roll
 * from one to the other with no flag day. `hex` may return synchronously;
 * every caller branches on the result rather than awaiting it, so a sync
 * implementation costs no microtask and, more to the point, no threadpool
 * round trip. `webCryptoHmac` is the WinterCG-clean default;
 * `nodeHmac()` on `@sigx/actors/node` is the synchronous one.
 */
export interface HostHmac {
    /** Lowercase hex HMAC-SHA-256 of `message` under `secret`. */
    hex(secret: string, message: string): string | Promise<string>;
}

const encoder = new TextEncoder();
const keyCache = new Map<string, Promise<CryptoKey>>();

function keyFor(secret: string): Promise<CryptoKey> {
    let key = keyCache.get(secret);
    if (!key) {
        key = crypto.subtle.importKey(
            'raw',
            encoder.encode(secret),
            { name: 'HMAC', hash: 'SHA-256' },
            false,
            ['sign']
        );
        keyCache.set(secret, key);
    }
    return key;
}

/** Byte → two lowercase hex digits, built once. */
const HEX = Array.from({ length: 256 }, (_, i) => i.toString(16).padStart(2, '0'));

function toHex(bytes: ArrayBuffer): string {
    const view = new Uint8Array(bytes);
    let out = '';
    for (let i = 0; i < view.length; i++) out += HEX[view[i] as number];
    return out;
}

/** The default `HostHmac`: WebCrypto, asynchronous, runs everywhere. */
export const webCryptoHmac: HostHmac = {
    hex: (secret, message) =>
        keyFor(secret)
            .then((key) => crypto.subtle.sign('HMAC', key, encoder.encode(message)))
            .then(toHex)
};

function authMessage(symbol: string, callId: string, timestamp: number): string {
    return `${HOST_PROTO}\n${symbol}\n${callId}\n${timestamp}`;
}

/**
 * The auth header value for one outbound host call, under `hmac`. A string
 * when the implementation is synchronous, a promise otherwise — branch on
 * it rather than `await`ing, or the sync path pays a microtask for nothing.
 */
export function signAuthWith(
    hmac: HostHmac,
    secret: string,
    symbol: string,
    callId: string
): string | Promise<string> {
    const timestamp = Date.now();
    const hex = hmac.hex(secret, authMessage(symbol, callId, timestamp));
    return typeof hex === 'string'
        ? `v1.${timestamp}.${hex}`
        : hex.then((h) => `v1.${timestamp}.${h}`);
}

/** Produce the auth header value for one outbound host call. */
export function signAuth(
    secret: string,
    symbol: string,
    callId: string,
    hmac: HostHmac = webCryptoHmac
): Promise<string> {
    try {
        return Promise.resolve(signAuthWith(hmac, secret, symbol, callId));
    } catch (error) {
        return Promise.reject(error);
    }
}

/**
 * Verify an inbound auth header against the call it claims to authorize,
 * under `hmac`. Same sync-or-promise contract as `signAuthWith`; every
 * format rejection is synchronous whichever implementation is in use.
 */
export function verifyAuthWith(
    hmac: HostHmac,
    secret: string,
    header: string | null,
    symbol: string,
    callId: string
): boolean | Promise<boolean> {
    if (!header) return false;
    const parts = header.split('.');
    if (parts.length !== 3) return false;
    const [version, timestampRaw, signature] = parts as [string, string, string];
    // Exactly `v1.<decimal ms>.<64 lowercase hex>` — anything looser is a no.
    if (version !== 'v1' || !/^\d{1,15}$/.test(timestampRaw)) return false;
    if (!/^[0-9a-f]{64}$/.test(signature)) return false;
    const timestamp = Number(timestampRaw);
    if (!Number.isSafeInteger(timestamp)) return false;
    if (Math.abs(Date.now() - timestamp) > AUTH_WINDOW_MS) return false;
    const expected = hmac.hex(secret, authMessage(symbol, callId, timestamp));
    return typeof expected === 'string'
        ? timingSafeEquals(signature, expected)
        : expected.then((e) => timingSafeEquals(signature, e));
}

/** Verify an inbound auth header against the call it claims to authorize. */
export function verifyAuth(
    secret: string,
    header: string | null,
    symbol: string,
    callId: string,
    hmac: HostHmac = webCryptoHmac
): Promise<boolean> {
    try {
        return Promise.resolve(verifyAuthWith(hmac, secret, header, symbol, callId));
    } catch (error) {
        return Promise.reject(error);
    }
}
