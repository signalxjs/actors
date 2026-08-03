/**
 * The request-context bag (#246): a small, string-only key/value bag on
 * `ActorCallContext`, stamped at the server edge (typically by a guard),
 * inherited by `ctx.actor` / `ctx.publish` hops, and carried host-to-host on
 * the envelope. This module owns the DEVELOPER-facing vocabulary — stamp,
 * lift, merge, with descriptive throws — over the string-free validation
 * core in `call-bag-core.ts` (which is all the wire entries pay for). Both
 * live at the package root rather than in any caller because `server/` must
 * not import `cluster/` and both need them.
 *
 * Size-capped and string-only to keep the wire honest: the bag rides an
 * HTTP header (the host-call envelope), where every non-ASCII character
 * inflates threefold under the envelope's `\uXXXX` escaping and proxies
 * commonly cap total header size at 8 KiB. Caps are enforced where a
 * DEVELOPER writes (stamp / `.with({ bag })` — throws, their stack), and
 * re-checked where a PEER writes (envelope decode — the whole bag is
 * silently dropped, never a 400: a malformed-header 400 is a DoS lever, and
 * a partial identity bag is worse than none). Consequence, stated where the
 * caps are: an actor must treat a MISSING entry as unauthenticated.
 */
import {
    bagEntryOk,
    bagTotalsOk,
    freezeBagEntries,
    sanitizeWireBag
} from './call-bag-core';

export {
    CALL_BAG_MAX_ENTRIES,
    CALL_BAG_MAX_KEY_BYTES,
    CALL_BAG_MAX_VALUE_BYTES,
    CALL_BAG_MAX_TOTAL_BYTES,
    EMPTY_CALL_BAG,
    isValidCallBag,
    sanitizeWireBag
} from './call-bag-core';

/** Where a guard's stamp lives inside `rq.locals` until the endpoint lifts
 *  it into the call context. Module-internal on purpose: the shape of the
 *  store is not API, `stampCallBag` is. */
const BAG_LOCALS_KEY = '__sigxCallBag';

function refuse(what: string, key?: string): never {
    throw new Error(
        `[sigx actors] context bag: ${what}${key !== undefined ? ` (key ${JSON.stringify(key)})` : ''} — ` +
            `string-only entries, capped per CALL_BAG_MAX_*.`
    );
}

function checkEntry(key: string, value: unknown): void {
    if (!bagEntryOk(key, value)) refuse('refused an entry', key);
}

function checkTotals(entries: [string, string][]): void {
    if (!bagTotalsOk(entries)) refuse('the merged bag exceeds the entry or byte caps');
}

/**
 * Stamp entries onto the current request, from a guard (or anything else
 * holding the request's `ServerFnContext`). Merges string entries last-wins
 * over entries already stamped on this request; the endpoint lifts the
 * result into `ActorCallContext.bag` after the guard chain, and in-process
 * `actor()` calls lift it the same way. Validation is HERE, at the
 * developer's call site, so a bad stamp throws in their guard with their
 * stack — by the time a bag reaches the wire it is known-good.
 */
export function stampCallBag(
    rq: { locals: Record<string, unknown> },
    entries: Record<string, string>
): void {
    const held = rq.locals[BAG_LOCALS_KEY];
    // A tampered store (a non-object under the reserved key) is replaced,
    // not crashed into; `takeCallBag` would have dropped it anyway.
    const existing = (
        held !== null && typeof held === 'object' && !Array.isArray(held)
            ? held
            : Object.create(null)
    ) as Record<string, string>;
    for (const [key, value] of Object.entries(entries)) {
        checkEntry(key, value);
        existing[key] = value;
    }
    checkTotals(Object.entries(existing));
    rq.locals[BAG_LOCALS_KEY] = existing;
}

/**
 * Lift the stamped bag out of a request's locals — the endpoint half of
 * {@link stampCallBag}. Returns `undefined` when nothing was stamped (or the
 * store was tampered into an invalid shape), otherwise a frozen copy.
 */
export function takeCallBag(
    locals: Record<string, unknown>
): Readonly<Record<string, string>> | undefined {
    return sanitizeWireBag(locals[BAG_LOCALS_KEY]);
}

/**
 * Merge explicit entries (`.with({ bag })`) over an inherited/stamped base,
 * explicit wins. The EXTRAS are developer input and throw on violation; the
 * BASE is whatever the call context carries — a `useDispatch` middleware
 * can have put anything there — so it is sanitized like peer input and
 * dropped WHOLE when invalid, exactly as the envelope would drop it.
 * Propagating a tainted base here would only postpone that drop to encode,
 * where it takes the valid explicit entries down with it. Returns
 * `undefined` only when both sides come up empty.
 */
export function mergeCallBag(
    base: Readonly<Record<string, string>> | undefined,
    extra: Readonly<Record<string, string>> | undefined
): Readonly<Record<string, string>> | undefined {
    const cleanBase = sanitizeWireBag(base);
    if (extra === undefined || Object.keys(extra).length === 0) return cleanBase;
    const merged = Object.create(null) as Record<string, string>;
    if (cleanBase) for (const [key, value] of Object.entries(cleanBase)) merged[key] = value;
    for (const [key, value] of Object.entries(extra)) {
        checkEntry(key, value);
        merged[key] = value;
    }
    const entries = Object.entries(merged);
    checkTotals(entries);
    return freezeBagEntries(entries);
}
