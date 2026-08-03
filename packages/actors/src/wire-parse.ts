/**
 * Pollution-safe wire JSON parsing — its own dependency-free module so the
 * frames entry (`./cluster/frames`, budgeted at 4 KB) can use the guarded
 * parse without dragging in `wire-shared`'s codec-seam imports
 * (`@sigx/serialize`). `wire-shared` re-exports everything here.
 */

/** Prototype-pollution keys DROPPED from every parsed payload. */
const DANGEROUS_KEYS = new Set(['__proto__', 'constructor', 'prototype']);
export const reviver = (key: string, value: unknown): unknown =>
    DANGEROUS_KEYS.has(key) ? undefined : value;

/**
 * Pollution-safe `JSON.parse` that keeps V8's fast parser (#218).
 *
 * Passing ANY reviver forces a JS-level walk of every parsed node — measured
 * 5–10.7× slower than a plain parse. So: pre-scan the text, and only take the
 * reviver path when a dangerous key could actually be present. The escape
 * clause is load-bearing — a unicode-escaped `"__proto__"`
 * key contains none of the three literals, so any `\u` at all must force the
 * slow path. Conservative by construction: a false positive costs only the
 * old speed, and a false negative cannot happen because a dangerous key must
 * spell its name either literally or through a `\u` escape.
 */
export function parseWire<T>(text: string): T {
    return (
        text.includes('__proto__') ||
        text.includes('constructor') ||
        text.includes('prototype') ||
        text.includes('\\u')
            ? JSON.parse(text, reviver)
            : JSON.parse(text)
    ) as T;
}

/**
 * `parseWire` for call sites that take the reviver as a parameter (the codec
 * seam). The guarded fast path applies only to the default `reviver`, judged
 * by IDENTITY — a custom codec reviver runs on every node, always, because
 * silently downgrading it to the fast path would drop whatever else it does.
 */
export function parseWireWith<T>(
    text: string,
    r: (key: string, value: unknown) => unknown
): T {
    return r === reviver ? parseWire<T>(text) : (JSON.parse(text, r) as T);
}
