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
import { timingSafeEquals } from '../timing-safe';
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
        hops: 1
    };
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
    return {
        call: {
            callChain: parsed.chain,
            callId: parsed.callId,
            ...(parsed.remainingMs !== undefined
                ? { deadline: Date.now() + Math.max(0, parsed.remainingMs) }
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
// Cost: ~9µs per sign/verify with the key cached (import is ~2ms, paid
// once per secret per process) vs ≥200µs for even a loopback hop.

/** Accept signatures this far from the receiver's clock, either way. A
 *  generous window so HMAC does not reintroduce clock-skew sensitivity. */
const AUTH_WINDOW_MS = 5 * 60_000;

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

function toHex(bytes: ArrayBuffer): string {
    return Array.from(new Uint8Array(bytes), (b) => b.toString(16).padStart(2, '0')).join('');
}

async function hmacHex(
    secret: string,
    symbol: string,
    callId: string,
    timestamp: number
): Promise<string> {
    const message = `${HOST_PROTO}\n${symbol}\n${callId}\n${timestamp}`;
    return toHex(await crypto.subtle.sign('HMAC', await keyFor(secret), encoder.encode(message)));
}

/** Produce the auth header value for one outbound host call. */
export async function signAuth(secret: string, symbol: string, callId: string): Promise<string> {
    const timestamp = Date.now();
    return `v1.${timestamp}.${await hmacHex(secret, symbol, callId, timestamp)}`;
}

/** Verify an inbound auth header against the call it claims to authorize. */
export async function verifyAuth(
    secret: string,
    header: string | null,
    symbol: string,
    callId: string
): Promise<boolean> {
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
    const expected = await hmacHex(secret, symbol, callId, timestamp);
    return timingSafeEquals(signature, expected);
}
