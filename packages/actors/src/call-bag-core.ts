/**
 * The request-context bag's WIRE half (#246): caps, shape validation and
 * the peer-input sanitizer — everything the envelope and the endpoints
 * need, with no developer-facing error strings, so the cluster and frames
 * entries pay for validation and nothing else. The stamp/merge vocabulary
 * (which throws with descriptive messages) lives in `call-context-bag.ts`
 * and is reached only from the root entry. See that file for the whole
 * design story.
 */

/** Max entries per bag. */
export const CALL_BAG_MAX_ENTRIES = 8;
/** Max UTF-8 bytes per key (keys must also be non-empty). */
export const CALL_BAG_MAX_KEY_BYTES = 64;
/** Max UTF-8 bytes per value. */
export const CALL_BAG_MAX_VALUE_BYTES = 256;
/** Max UTF-8 bytes summed over every key and value. */
export const CALL_BAG_MAX_TOTAL_BYTES = 1024;

/** What `ctx.bag` reads outside any turn — one frozen instance, never null. */
export const EMPTY_CALL_BAG: Readonly<Record<string, string>> = Object.freeze(
    Object.create(null) as Record<string, string>
);

const utf8 = new TextEncoder();
const bytes = (value: string): number => utf8.encode(value).length;

/** One entry within shape and caps? The three prototype-grafting keys
 *  (`__proto__`, `constructor`, `prototype`) are refused outright. */
export function bagEntryOk(key: string, value: unknown): boolean {
    return (
        typeof key === 'string' &&
        key.length !== 0 &&
        key !== '__proto__' &&
        key !== 'constructor' &&
        key !== 'prototype' &&
        typeof value === 'string' &&
        bytes(key) <= CALL_BAG_MAX_KEY_BYTES &&
        bytes(value) <= CALL_BAG_MAX_VALUE_BYTES
    );
}

/** Entry count and total bytes within caps? */
export function bagTotalsOk(entries: [string, string][]): boolean {
    let total = 0;
    for (const [key, value] of entries) total += bytes(key) + bytes(value);
    return entries.length <= CALL_BAG_MAX_ENTRIES && total <= CALL_BAG_MAX_TOTAL_BYTES;
}

/** A fresh null-prototype frozen copy of VALIDATED entries — dangerous keys
 *  were rejected above, and the null prototype means even a future
 *  validator bug cannot reach Object.prototype. */
export function freezeBagEntries(entries: [string, string][]): Readonly<Record<string, string>> {
    const bag = Object.create(null) as Record<string, string>;
    for (const [key, value] of entries) bag[key] = value;
    return Object.freeze(bag);
}

/** Full predicate — shape, caps and dangerous keys. Used at envelope encode:
 *  a `useDispatch` middleware may have put anything on the context. */
export function isValidCallBag(value: unknown): value is Readonly<Record<string, string>> {
    if (value === null || typeof value !== 'object' || Array.isArray(value)) return false;
    const entries = Object.entries(value as Record<string, unknown>);
    for (const [key, entry] of entries) {
        if (!bagEntryOk(key, entry)) return false;
    }
    return bagTotalsOk(entries as [string, string][]);
}

/**
 * The peer-input sanitizer: accept a decoded (or locals-held) value ONLY if
 * it is fully valid, and REBUILD it into a fresh null-prototype frozen
 * object — never spread or alias the input, which for the envelope came out
 * of a plain `JSON.parse` and is attacker-influenced. All-or-nothing: a bag
 * with one bad entry is dropped whole (a partial identity is worse than
 * none). Returns `undefined` for absent, empty or invalid input.
 */
export function sanitizeWireBag(value: unknown): Readonly<Record<string, string>> | undefined {
    if (value === undefined || value === null) return undefined;
    if (!isValidCallBag(value)) return undefined;
    const entries = Object.entries(value);
    if (entries.length === 0) return undefined;
    return freezeBagEntries(entries);
}
