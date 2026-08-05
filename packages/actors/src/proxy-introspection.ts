/**
 * Introspection props every actor proxy answers WITHOUT manufacturing a
 * dispatcher. The `get` traps synthesize a live dispatching function for
 * any unknown string prop — the feature that lets methods need no
 * registration — but unguarded it meant `String(ref)`,
 * `JSON.stringify(ref)`, or a library probing `.constructor` silently
 * issued a REAL call (refused server-side, but only after a wasted round
 * trip — or worse, dispatched in-process). The names here are answered
 * locally instead: `toString` reads `[actor Type#key]` so refs interpolate
 * usefully into logs; every other `Object.prototype` name (plus `toJSON`
 * and Node's legacy `inspect`) reads `undefined`, exactly like symbols and
 * `then`.
 *
 * The documented cost: an actor method NAMED like an `Object.prototype`
 * member (`toString`, `valueOf`, `hasOwnProperty`, …) is not reachable
 * through a proxy. Pick another name.
 */
const INTROSPECTION_PROPS: ReadonlySet<string> = new Set([
    ...Object.getOwnPropertyNames(Object.prototype),
    'toJSON',
    'inspect'
]);

/** Is `prop` an introspection name the proxy must answer locally? */
export function isIntrospectionProp(prop: string): boolean {
    return INTROSPECTION_PROPS.has(prop);
}

/**
 * The local answer for an introspection prop: a label function for
 * `toString`, `undefined` for everything else.
 */
export function introspectionMember(prop: string, type: string, key: string): unknown {
    return prop === 'toString' ? () => `[actor ${type}#${key}]` : undefined;
}
