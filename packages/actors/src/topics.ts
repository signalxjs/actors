/**
 * `topic()` and `publishTopic()` — the isomorphic half of actor pub/sub.
 * Pure declaration plus the ambient-host publish; the fan-out engine lives
 * in `./host/topics` and the root entry never imports the runtime.
 */
import { currentHost } from './seam';
import type { PublishOptions, Topic, TopicPublishReport } from './types';

/**
 * Why a topic NAME may not carry: `#` is the wire symbol separator, NUL the
 * directory separator, and the `$`/`@` prefixes are the runtime's reserved
 * namespaces — the same rules an actor type lives under, because a topic
 * name travels the same wires. Returns the objection, or null when valid.
 */
/** The directory separator byte, spelled so no toolchain can strip it. */
const NUL = String.fromCharCode(0);

export function topicNameObjection(name: unknown): string | null {
    if (typeof name !== 'string' || name.length === 0) {
        return 'needs a non-empty string name';
    }
    if (name.includes('#') || name.includes(NUL)) {
        return 'must not contain "#" or NUL';
    }
    if (name.startsWith('$') || name.startsWith('@')) {
        return 'must not start with "$" or "@" — both are reserved for the runtime';
    }
    return null;
}

function validKey(key: unknown): key is string {
    return typeof key === 'string' && key.length > 0 && !key.includes(NUL);
}

/**
 * Declare a topic. `key` selects which subscriber instance receives a
 * publish (identity-mapped by default); it defaults to `'default'` so a
 * singleton topic is just `topic('config-changed')`.
 *
 * Throws in every build — a bad topic identity is a wire commitment, not a
 * dev-time nicety. Same posture as `defineActor`'s type validation.
 */
export function topic<T = unknown>(name: string, key = 'default'): Topic<T> {
    const objection = topicNameObjection(name);
    if (objection !== null) {
        throw new Error(`[sigx actors] topic name ${JSON.stringify(name)} ${objection}.`);
    }
    if (!validKey(key)) {
        throw new Error(
            `[sigx actors] topic "${name}" needs a non-empty string key without NUL.`
        );
    }
    return { name, key };
}

/**
 * Re-check a value handed to `publish()` — it may not have come from
 * `topic()`. Cheap, and the failure names the call site's mistake instead
 * of letting a malformed name reach the wire.
 */
export function assertTopic(value: unknown): asserts value is Topic {
    const t = value as { name?: unknown; key?: unknown } | null;
    const objection = topicNameObjection(t?.name);
    if (objection !== null) {
        throw new Error(
            `[sigx actors] publish() got a topic whose name ${objection} — build it with topic().`
        );
    }
    if (!validKey(t?.key)) {
        throw new Error(
            '[sigx actors] publish() got a topic with an invalid key — build it with topic().'
        );
    }
}

/**
 * Publish through the ambient host seam — for serverFns, scripts and other
 * server code outside any actor. Inside an actor use `ctx.publish`, which
 * carries the caller's chain so cycles are detected deadlocks.
 *
 * Server-only: in the browser there is no running host and this throws.
 * Publishing is a trusted operation — the public edge has no publish
 * surface.
 */
export function publishTopic<T>(
    topic: Topic<T>,
    payload: T,
    options?: PublishOptions
): Promise<TopicPublishReport> {
    return currentHost().publish(topic, payload, options);
}
