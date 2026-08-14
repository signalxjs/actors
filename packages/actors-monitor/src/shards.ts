/**
 * The reminder-shard claim map, as something a renderer can draw.
 *
 * The three states mean genuinely different things and none of them is a
 * count you would read off a number:
 *
 *   one claimant   healthy
 *   none           NOTHING is ticking that shard — those reminders are not
 *                  firing, and nothing else in the system surfaces it
 *   two or more    views have diverged; safe (the per-shard etag CAS keeps
 *                  delivery at-most-once) but worth knowing
 *
 * Which is why this returns a STATE rather than a tone. A terminal draws it
 * as a glyph in a status grid and a browser as a coloured cell, and both
 * would otherwise have to re-derive "empty is an incident, two is merely a
 * divergence" from a claimant count — the kind of rule that is right in the
 * first renderer and subtly wrong in the second.
 */

/** What a single reminder shard is doing. */
export type ShardState = 'claimed' | 'unclaimed' | 'split';

export interface ShardStatus {
    /** The shard id, e.g. `p7`. */
    label: string;
    state: ShardState;
    /** The hosts claiming it — one when healthy, none or several otherwise. */
    claimants: readonly string[];
}

/**
 * The whole map, worst-case states included, in shard order.
 *
 * Sorted numerically by index rather than lexically, so `p10` does not sit
 * between `p1` and `p2`.
 */
export function shardStates(shards: Record<string, readonly string[]>): ShardStatus[] {
    return Object.keys(shards)
        .sort((a, b) => shardIndex(a) - shardIndex(b))
        .map((label) => {
            const claimants = shards[label] ?? [];
            return {
                label,
                state:
                    claimants.length === 0
                        ? 'unclaimed'
                        : claimants.length === 1
                          ? 'claimed'
                          : 'split',
                claimants
            } satisfies ShardStatus;
        });
}

/** Shards nothing is ticking — the finding worth alerting on. */
export function unclaimedShards(shards: Record<string, readonly string[]>): string[] {
    return shardStates(shards)
        .filter((shard) => shard.state === 'unclaimed')
        .map((shard) => shard.label);
}

/** Shards claimed by more than one host — views have diverged. */
export function splitShards(shards: Record<string, readonly string[]>): string[] {
    return shardStates(shards)
        .filter((shard) => shard.state === 'split')
        .map((shard) => shard.label);
}

/** `p12` → 12; anything unparseable sorts last but stays stable. */
function shardIndex(shard: string): number {
    const digits = /^p(\d+)$/.exec(shard);
    return digits ? Number(digits[1]) : Number.MAX_SAFE_INTEGER;
}
