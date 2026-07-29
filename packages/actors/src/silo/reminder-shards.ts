/**
 * Reminder-table sharding — shared by the reminder service (which shard a
 * reminder lives in) and the cluster placement (which silo owns a shard).
 *
 * COMPAT-CRITICAL: the shard count and the hash are storage identity. An
 * actorId must map to the same shard forever, or reminders silently vanish
 * between records on upgrade. Never change either; a re-shard needs an
 * explicit migration like the `$all` one.
 */

import { fnv1a } from '../hash';

export const REMINDER_SHARD_COUNT = 16;

/**
 * FNV-1a 32-bit — tiny, deterministic, stable. Pinned forever (see above).
 *
 * The implementation is shared with the wire routing token (`../route`), but
 * the PIN is not: that caller may re-hash at the cost of a locality dip,
 * this one may never re-hash at all. Re-exported so the pin stays stated
 * where it binds.
 */
export { fnv1a };

/** The storage key of the shard holding `actorId`'s reminders. */
export function reminderShardOf(actorId: string): string {
    return `p${fnv1a(actorId) % REMINDER_SHARD_COUNT}`;
}

/** All shard keys, `p0`..`p15`. */
export function reminderShardKeys(): string[] {
    return Array.from({ length: REMINDER_SHARD_COUNT }, (_v, i) => `p${i}`);
}
