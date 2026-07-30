/**
 * Name lookups into user-supplied tables (`methods:`, `streams:`, `tasks:`).
 *
 * The tables are object literals, so a plain `table[name]` walks up to
 * `Object.prototype` and resolves members nobody declared — dispatch then
 * CALLS them (`A#toString` answered `"[object Undefined]"` with a 200). The
 * checks around dispatch do not have this problem: `streamNames`,
 * `validateReads` and the Vite extractor all work from OWN keys. That
 * asymmetry is the bug — anything on the prototype was invisible to every
 * check and visible to every call — so every lookup has to agree with them.
 *
 * `typeof === 'function'` is not redundant: `__proto__` is an own-ish
 * accessor that yields the prototype OBJECT, which then throws
 * "fn is not a function" as a masked 500 rather than a clean not-found.
 */

/** Resolve `name` to a callable, OWN keys only — otherwise `undefined`. */
export function ownFn<T>(table: object | undefined, name: string): T | undefined {
    if (!table || !Object.hasOwn(table, name)) return undefined;
    const member = (table as Record<string, unknown>)[name];
    return typeof member === 'function' ? (member as T) : undefined;
}

/**
 * Warn (dev only) when a factory returns something whose members live on a
 * prototype — a class instance, typically. Own-key resolution makes those
 * uncallable, and a silent 404 on a method you can see in the source is a
 * bad way to find out. `streamNames` and the Vite extractor already read own
 * keys, so this shape was never fully supported; it just failed quietly in
 * fewer places.
 */
export function warnIfInheritedTable(table: object, what: string, type: string): void {
    const proto = Object.getPrototypeOf(table) as object | null;
    if (proto === Object.prototype || proto === null) return;
    console.warn(
        `[sigx actors] actor "${type}" returned a \`${what}:\` table whose members are ` +
            `INHERITED (its prototype is not Object.prototype). Only the table's own keys ` +
            `are callable, so those members answer "method not found". Return an object ` +
            `literal — e.g. \`{ ...instance }\` is not enough either, since class methods ` +
            `are not enumerable.`
    );
}
