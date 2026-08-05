/**
 * The topics fan-out engine — the runtime half of actor pub/sub.
 *
 * A publish is S ordinary dispatches of the reserved method `$sigx:topic`,
 * one per subscribing TYPE, routed through placement like any other call —
 * so remote subscribers ride the existing transports, HMAC envelope,
 * deadlines and branded errors, and an idle subscriber is activated exactly
 * the way a reminder delivery activates one. No new wire symbol, no storage
 * record: the subscriber set is a pure function of the deploy, derived from
 * the host's definition registry.
 */
import { isActorError } from '../errors';
import type {
    ActorCallContext,
    ActorRef,
    AnyActorDefinition,
    Topic,
    TopicDeliveryFailure,
    TopicEvent,
    TopicPublishReport,
    TopicSubscription,
    TopicSubscriptionHandler
} from '../types';

/** Reserved dispatch method routing to a `subscriptions:` handler. */
export const TOPIC_METHOD = '$sigx:topic';

/** One subscribing type, as the index stores it. */
export interface TopicSubscriberEntry {
    type: string;
    /** Topic key → subscriber key; absent = identity. */
    mapKey?: (topicKey: string) => string;
}

/** Normalize a `subscriptions:` entry to its handler. Returns null for a
 *  shape validation should have refused — callers dev-warn, never throw. */
export function subscriptionHandler(
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    sub: TopicSubscription<any, any> | undefined
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
): TopicSubscriptionHandler<any, any> | null {
    if (typeof sub === 'function') return sub;
    if (sub && typeof sub.handle === 'function') return sub.handle;
    return null;
}

/** The declared subscription for one topic name — own keys only, same rule
 *  as `methodAuthorize`. */
export function subscriptionFor(
    def: AnyActorDefinition,
    topicName: string
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
): TopicSubscription<any, any> | undefined {
    const table = (
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        def.__sigxActor as { subscriptions?: Record<string, TopicSubscription<any, any>> }
    ).subscriptions;
    if (!table || !Object.hasOwn(table, topicName)) return undefined;
    return table[topicName];
}

/**
 * Derive the subscriber index from a host's registry: topic name → the
 * types that declared a subscription to it. Lazy definitions are resolved
 * through the same `definition()` the wire uses; a loader that fails
 * rejects the build — and with it the publish — with the loader's own
 * error, rather than silently publishing to fewer subscribers than the
 * deploy declares.
 */
export async function buildTopicIndex(
    types: Iterable<string>,
    definition: (
        type: string
    ) => AnyActorDefinition | Promise<AnyActorDefinition | null> | null
): Promise<Map<string, TopicSubscriberEntry[]>> {
    const defs = await Promise.all(
        [...types].map(async (type) => await definition(type))
    );
    const index = new Map<string, TopicSubscriberEntry[]>();
    for (const def of defs) {
        if (!def) continue;
        const table = (
            def.__sigxActor as { subscriptions?: Record<string, TopicSubscription<never>> }
        ).subscriptions;
        if (!table) continue;
        for (const [name, sub] of Object.entries(table)) {
            const entry: TopicSubscriberEntry = { type: def.type };
            if (typeof sub === 'object' && sub !== null && typeof sub.key === 'function') {
                entry.mapKey = sub.key;
            }
            const list = index.get(name);
            if (list) list.push(entry);
            else index.set(name, [entry]);
        }
    }
    return index;
}

/**
 * Fan one event out to the index's subscribers for its topic.
 *
 * `Promise.allSettled` posture throughout: a throwing handler, a dead host,
 * a detected deadlock — each is one `failures` entry, never a publisher
 * exception and never another subscriber's problem. Even a throwing `key:`
 * mapper only costs its own delivery.
 */
export async function publishToSubscribers(
    entries: readonly TopicSubscriberEntry[],
    topic: Topic,
    payload: unknown,
    call: ActorCallContext,
    publisher: ActorRef | undefined,
    dispatch: (
        ref: ActorRef,
        method: string,
        args: readonly unknown[],
        call: ActorCallContext
    ) => Promise<unknown>
): Promise<TopicPublishReport> {
    const event: TopicEvent = {
        topic: { name: topic.name, key: topic.key },
        payload,
        at: Date.now(),
        ...(publisher ? { publisher: { type: publisher.type, key: publisher.key } } : {})
    };
    const failures: TopicDeliveryFailure[] = [];
    let delivered = 0;
    await Promise.all(
        entries.map(async (entry) => {
            let key: string | null = null;
            try {
                const mapped = entry.mapKey ? entry.mapKey(topic.key) : topic.key;
                if (typeof mapped !== 'string' || mapped.length === 0) {
                    throw new Error(
                        `the subscription's key() mapped topic key ` +
                            `${JSON.stringify(topic.key)} to ${JSON.stringify(mapped)} — it ` +
                            `must return a non-empty string`
                    );
                }
                key = mapped;
                await dispatch({ type: entry.type, key }, TOPIC_METHOD, [event], call);
                delivered += 1;
            } catch (error) {
                failures.push({
                    type: entry.type,
                    // null = the key() mapper itself failed; there is no
                    // subscriber key to name.
                    key: key ?? '(key() failed)',
                    message: error instanceof Error ? error.message : String(error),
                    ...(isActorError(error) ? { kind: error.kind } : {})
                });
            }
        })
    );
    return { subscribers: entries.length, delivered, failures };
}
