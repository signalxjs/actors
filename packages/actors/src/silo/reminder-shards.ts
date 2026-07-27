/**
 * Reminder-table sharding — shared by the reminder service (which shard a
 * reminder lives in) and the cluster placement (which silo owns a shard).
 *
 * COMPAT-CRITICAL: the shard count and the hash are storage identity. An
 * actorId must map to the same shard forever, or reminders silently vanish
 * between records on upgrade. Never change either; a re-shard needs an
 * explicit migration like the `$all` one.
 */

export const REMINDER_SHARD_COUNT = 16;

/** FNV-1a 32-bit — tiny, deterministic, stable. Pinned forever (see above). */
export function fnv1a(input: string): number {
    let hash = 0x811c9dc5;
    for (let i = 0; i < input.length; i++) {
        hash ^= input.charCodeAt(i);
        hash = Math.imul(hash, 0x01000193);
    }
    return hash >>> 0;
}

/** The storage key of the shard holding `actorId`'s reminders. */
export function reminderShardOf(actorId: string): string {
    return `p${fnv1a(actorId) % REMINDER_SHARD_COUNT}`;
}

/** All shard keys, `p0`..`p15`. */
export function reminderShardKeys(): string[] {
    return Array.from({ length: REMINDER_SHARD_COUNT }, (_v, i) => `p${i}`);
}
