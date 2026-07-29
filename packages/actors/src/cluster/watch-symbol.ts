/**
 * The wire spelling of a WATCH.
 *
 * A watch is an ordinary read dispatched in watch mode, so `Counter#read`
 * alone cannot say which one is meant — the same method is a call in one
 * direction and a subscription in the other, and `streamNames` cannot
 * separate them because a watch is not a stream. The intent has to travel.
 *
 * It travels on the SYMBOL rather than the call envelope because the symbol
 * is what the per-call HMAC actually covers — the signature is over
 * `proto\nsymbol\ncallId\ntimestamp` (see `envelope.ts`), and the envelope
 * is not part of it. On the envelope, anyone who could reach the mount
 * could turn a plain read into a keep-alive-pinning subscription without
 * disturbing the signature; on the symbol they cannot.
 *
 * It also keeps the two wire paths honest about the same thing: on the
 * frame transports `FLAG_STREAM` goes on meaning "the response is a chunk
 * stream", which a watch's response genuinely is, and the symbol carries
 * what kind of call produced it. No new frame type, no protocol bump.
 *
 * Reserving the prefix is safe: `defineActor` refuses a type starting with
 * `$`, and the mount resolves reserved symbols before any definition
 * lookup — exactly the pattern `$sigx:silo#stats` already established.
 */
export const WATCH_SYMBOL_PREFIX = '$watch:';

/** The wire symbol for watching `type.method(...)`. */
export function watchSymbol(type: string, method: string): string {
    return `${WATCH_SYMBOL_PREFIX}${type}#${method}`;
}

/**
 * A watch's leading argument: `{ throttleMs? }`, or null for the defaults.
 *
 * It rides the PAYLOAD rather than the symbol so the receiving mount's
 * per-symbol handler cache stays bounded by types × methods — with the
 * throttle in the symbol a peer could mint an unbounded number of distinct
 * keys. Nothing is given up by putting it there: it is a rate hint, not a
 * privilege, and the arguments it travels beside are already unsigned.
 *
 * REFUSED rather than defaulted when malformed. A non-finite `throttleMs`
 * reaching the watch loop compares false against every bound and disables
 * throttling outright — the same fail-open shape as the `metrics()` caps
 * (#109), where the wrong value looks like healthy behaviour right up until
 * it does not. `status` rides the error so both wire paths answer 400.
 *
 * Lives here rather than in either endpoint because both must agree: a
 * transport that validated this differently would make the same call
 * succeed on TCP and fail on HTTP.
 */
export function parseWatchOptions(
    raw: unknown,
    symbol: string
): { throttleMs?: number } | undefined {
    if (raw === undefined || raw === null) return undefined;
    // `typeof [] === 'object'`, so the array check is not pedantry: without
    // it an array is waved through and read as `{}` — no throttle, no
    // complaint, and nothing to point at when the rate turns out wrong.
    if (typeof raw !== 'object' || Array.isArray(raw)) {
        throw badWatchOption(`silo watch "${symbol}" options must be an object or null`);
    }
    const throttleMs = (raw as { throttleMs?: unknown }).throttleMs;
    if (throttleMs === undefined) return {};
    if (typeof throttleMs !== 'number' || !Number.isFinite(throttleMs) || throttleMs < 0) {
        throw badWatchOption(
            `silo watch "${symbol}" got throttleMs=${String(throttleMs)}; it must be a ` +
                `finite number of milliseconds, 0 or more`
        );
    }
    return { throttleMs };
}

function badWatchOption(message: string): Error {
    return Object.assign(new Error(`[sigx actors] ${message}`), { status: 400 });
}
