/**
 * `actorKey()` — the canonical data key for an actor read.
 *
 * ISOMORPHIC, and in the root entry on purpose: the browser sees a build-
 * swapped client ref while the server sees the real definition, and both
 * must produce a BYTE-IDENTICAL tuple. SSR seeding, hydration-without-
 * refetch, invalidation and live-subscription identity all rest on that one
 * invariant, so it is pinned by its own test rather than implied by an
 * integration test.
 *
 * A tuple, deliberately — not core's `__sigxKey` string. Stamping
 * `__sigxKey` on the client proxy's members would make
 * `useData(() => [cart.total])` work with no new API at all, but it
 * canonicalizes to a SINGLE string element, and `keyMatches` supports exact
 * strings or tuple PREFIXES. A one-element tuple has no prefix structure, so
 * "invalidate everything about Cart/c1" would degrade to enumerating every
 * method name you happen to know. The tuple gives the hierarchy for free:
 *
 *     ['@actor','Cart','c1']            → every read of that cart
 *     ['@actor','Cart']                 → every cart on the page
 *     ['@actor','Cart','c1','total']    → exactly one read
 */
import type { ActorArgs, ActorReadName, AnyActorDefinition } from './types';

/** Reserved namespace head. `defineActor` refuses a `type` starting with it. */
export const ACTOR_KEY_NS = '@actor';

/** What a canonical key tuple may contain (core's `KeyTuple` element type). */
export type ActorKeyArg = string | number | boolean | null;

/**
 * The compile error a non-primitive key argument produces. A tuple whose
 * first element is the explanation, so TypeScript's "not assignable to
 * parameter of type …" prints the reason rather than a bare `never`.
 */
type NotAKeyArg<T> = readonly [
    error: 'actorKey: this argument becomes part of the data key, so it must be a JSON primitive (string | number | boolean | null). Pass an id instead of',
    received: T
];

/**
 * Constrain a method's parameters to what a data key can hold. Positions
 * that cannot canonicalize become {@link NotAKeyArg}, so the mistake is a
 * type error AT THAT ARGUMENT rather than a dev-time throw.
 *
 * `undefined` is excluded before the check so an OPTIONAL parameter stays
 * usable (omitting it simply shortens the tuple). Passing `undefined`
 * explicitly still throws in dev: `JSON.stringify` turns it into `null`, so
 * it would silently share a cache key with an actual `null` argument.
 */
export type ActorKeyArgs<A extends readonly unknown[]> = {
    [I in keyof A]: Exclude<A[I], undefined> extends ActorKeyArg ? A[I] : NotAKeyArg<A[I]>;
};

/** Anything `actorKey` can read a type name off: a definition or a client ref. */
type ActorTypeSource = AnyActorDefinition | { readonly __sigxActor: string };

/**
 * Read the actor's type from either shape. The client ref carries it as a
 * STRING on `__sigxActor`; the real definition uses that same property for
 * its options object, and exposes `type` directly — so the string test is
 * what discriminates them, not the property's presence.
 */
function typeOf(def: ActorTypeSource): string {
    const brand = (def as { __sigxActor?: unknown }).__sigxActor;
    if (typeof brand === 'string') return brand;
    const type = (def as { type?: unknown }).type;
    if (typeof type === 'string') return type;
    throw new Error(
        '[sigx actors] actorKey() needs an actor definition (or a client ref from the build).'
    );
}

/**
 * Runtime backstop. {@link ActorKeyArgs} rejects non-primitives at compile
 * time, so this only fires where types cannot: JavaScript callers, `any`,
 * and an explicitly-passed `undefined` (which `JSON.stringify` would turn
 * into `null`, silently sharing a key with a real `null` argument).
 */
function assertKeyArgs(type: string, method: string, args: readonly unknown[]): void {
    for (const [i, arg] of args.entries()) {
        const kind = typeof arg;
        if (arg === null || kind === 'string' || kind === 'number' || kind === 'boolean') continue;
        throw new Error(
            `[sigx actors] actorKey(${type}, …, "${method}") argument ${i} is a ${kind}; a data ` +
                `key must be JSON primitives (string | number | boolean | null) so it ` +
                `canonicalizes stably. Pass an id rather than the object it identifies.`
        );
    }
}

/** Every read of one actor — the prefix a mutation invalidates by default. */
export function actorKey<D extends AnyActorDefinition>(
    def: D,
    key: string
): readonly [typeof ACTOR_KEY_NS, string, string];
/** One specific read. */
export function actorKey<D extends AnyActorDefinition, M extends ActorReadName<D>>(
    def: D,
    key: string,
    method: M,
    ...args: ActorKeyArgs<ActorArgs<D, M>>
): readonly [typeof ACTOR_KEY_NS, string, string, M, ...ActorKeyArg[]];
export function actorKey(
    def: ActorTypeSource,
    key: string,
    method?: string,
    ...args: unknown[]
): readonly ActorKeyArg[] {
    const type = typeOf(def);
    if (typeof key !== 'string' || key.length === 0) {
        throw new Error(`[sigx actors] actorKey(${type}) needs a non-empty string actor key.`);
    }
    if (method === undefined) return [ACTOR_KEY_NS, type, key];
    if (__DEV__) assertKeyArgs(type, method, args);
    return [ACTOR_KEY_NS, type, key, method, ...(args as ActorKeyArg[])];
}
