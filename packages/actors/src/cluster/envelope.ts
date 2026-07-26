/**
 * The silo-to-silo call envelope — one JSON header carrying the call
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
import type { ActorCallContext } from '../types';

export const SILO_CALL_HEADER = 'x-sigx-silo-call';
export const SILO_AUTH_HEADER = 'x-sigx-cluster-auth';
export const SILO_PROTO = 1;
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
        v: SILO_PROTO,
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
        throw new Error(`[sigx actors] malformed ${SILO_CALL_HEADER} header`);
    }
    if (parsed.v !== SILO_PROTO) {
        throw new Error(
            `[sigx actors] silo protocol version skew: got v${parsed.v}, this silo speaks ` +
                `v${SILO_PROTO} — are all silos from compatible deploys?`
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
        throw new Error(`[sigx actors] malformed ${SILO_CALL_HEADER} header`);
    }
    if (parsed.hops > MAX_HOPS) {
        throw new Error(`[sigx actors] silo call exceeded ${MAX_HOPS} hops — forwarding loop?`);
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

/**
 * Constant-time-ish string comparison for the shared secret. Length is
 * checked against the expected secret's own length, so timing reveals
 * nothing an attacker doesn't already know.
 */
export function secretMatches(presented: string | null, expected: string): boolean {
    const a = presented ?? '';
    let mismatch = a.length ^ expected.length;
    for (let i = 0; i < expected.length; i++) {
        mismatch |= (a.charCodeAt(i) || 0) ^ expected.charCodeAt(i);
    }
    return mismatch === 0;
}
