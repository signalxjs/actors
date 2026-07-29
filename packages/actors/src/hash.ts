/**
 * FNV-1a 32-bit — tiny, deterministic, dependency-free.
 *
 * Lives here rather than beside either caller because it has TWO callers
 * with DIFFERENT stability contracts, and neither should be able to change
 * the other's:
 *
 * - `silo/reminder-shards` pins it as STORAGE IDENTITY. An actorId must map
 *   to the same shard forever or reminders silently vanish on upgrade.
 * - `route` uses it for the wire routing token, where a change costs a
 *   one-deploy locality dip and nothing else.
 *
 * The function itself is therefore stable, but "why you may not change it"
 * is stated at each call site, not here.
 */
export function fnv1a(input: string): number {
    let hash = 0x811c9dc5;
    for (let i = 0; i < input.length; i++) {
        hash ^= input.charCodeAt(i);
        hash = Math.imul(hash, 0x01000193);
    }
    return hash >>> 0;
}
