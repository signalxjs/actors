/**
 * Does a data key match an invalidation pattern?
 *
 * Reimplemented rather than imported. Core has this exact function at
 * `@sigx/server/dist/server/key-match.ts`, but it is internal — exported
 * from no subpath — and core's own copy already carries a note that it is
 * duplicated from `@sigx/cache`. Three copies is the sanctioned state of
 * this particular ~12 lines; `key-match.test.ts` pins the semantics against
 * the same table so they cannot drift silently.
 *
 * Two shapes, matching core:
 *
 *  - a **string** pattern is exact equality;
 *  - a **tuple** pattern is a PREFIX match against the entry's canonical
 *    tuple — which is precisely the relation `actorKey()` returns tuples
 *    for. `['@actor','Cart','c1']` addresses every read of that cart;
 *    `['@actor','Cart']` addresses every cart on the page.
 */
import type { ActorKeyArg } from '../actor-key';

/**
 * `entryKey` is a canonical key as `useData` stores it: either a plain
 * string, or the canonical JSON of a tuple (`'["@actor","Cart","c1"]'`).
 *
 * A tuple pattern is constrained to JSON primitives, matching what a
 * canonical key can hold — and what `actorKey()` already enforces on its
 * arguments. The constraint is not cosmetic: `JSON.stringify` THROWS on a
 * `bigint`, and silently collapses `undefined`, a symbol and a function all
 * to `null` — so an unconstrained pattern could quietly match every key
 * whose element there is `null`, refreshing reads it has nothing to do with.
 * A wrong match is worse than a crash, so both are ruled out at compile time.
 */
export function keyMatches(
    entryKey: string,
    pattern: string | readonly ActorKeyArg[]
): boolean {
    if (typeof pattern === 'string') return entryKey === pattern;
    if (pattern.length === 0) return false; // an empty pattern is not "everything"

    const encoded = JSON.stringify(pattern);
    if (entryKey === encoded) return true;

    // Prefix compare on the CANONICAL TEXT, minus the closing bracket. The
    // retained closing quote of the last element is what stops `c1` from
    // matching `c10`: the candidate continues with `0"`, never with `",`.
    const head = encoded.slice(0, -1);
    if (!entryKey.startsWith(head)) return false;
    const next = entryKey.charAt(head.length);
    return next === ',';
}
