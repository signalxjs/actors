/**
 * `defineActor` — the actor definition API. Pure declaration: no activation
 * machinery lives here (that is `./silo`), so importing a `*.actor.ts`
 * module on the server costs nothing until the silo activates a key.
 */
import type {
    ActorContext,
    ActorDefinition,
    ActorMethodTable,
    ActorOptions,
    ActorReadCache,
    ActorStreamTable
} from './types';
import { warnIfInheritedTable } from './own-member';

/**
 * Inert probe handed to the `streams` factory at definition time so its
 * method NAMES can be enumerated for wire routing. The contract (documented
 * on `ActorOptions.streams`) is that the factory only builds closures;
 * touching ctx during construction is a definition-time error.
 */
function streamProbe(type: string): ActorContext<never> {
    return new Proxy(
        {},
        {
            get(_t, prop) {
                throw new Error(
                    `[sigx actors] the \`streams\` factory of actor "${type}" accessed ` +
                        `ctx.${String(prop)} during construction. Stream factories must only ` +
                        `build closures — read ctx inside the generator body instead ` +
                        `(\`computed\`/\`watch\` setup belongs in the \`methods\` factory).`
                );
            }
        }
    ) as ActorContext<never>;
}

export function defineActor<
    S extends object,
    M extends ActorMethodTable,
    St extends ActorStreamTable = Record<never, never>
>(options: ActorOptions<S, M, St>): ActorDefinition<S, M, St> {
    if (typeof options.type !== 'string' || options.type.length === 0) {
        throw new Error('[sigx actors] defineActor requires a non-empty string `type`.');
    }
    if (options.type.includes('#') || options.type.includes('\u0000')) {
        // '#' is the wire symbol separator, NUL the directory separator.
        throw new Error(
            `[sigx actors] actor type "${options.type}" must not contain "#" or NUL.`
        );
    }
    if (options.type.startsWith('$') || options.type.startsWith('@')) {
        // Reserved namespaces, claimed while nothing has shipped: '$' for
        // the runtime's own mounts on the actor endpoint (`$live#subscribe`),
        // '@' for the data-key head `actorKey()` already emits ('@actor').
        // Taking either later would break whoever got there first.
        throw new Error(
            `[sigx actors] actor type "${options.type}" must not start with "$" or "@" — ` +
                `both are reserved for the runtime's own wire and data-key namespaces.`
        );
    }
    // The contradiction is a definition-time throw in every build (not just
    // __DEV__): shipping an actor that SAYS unguarded but CARRIES guards is a
    // security-posture bug, same rule as core's form-without-input.
    if (options.unguarded === true && options.use && options.use.length > 0) {
        throw new Error(
            `[sigx actors] actor "${options.type}" declares both \`unguarded: true\` and a ` +
                `non-empty \`use\` chain — pick one.`
        );
    }
    if (typeof options.methods !== 'function') {
        throw new Error(`[sigx actors] actor "${options.type}" needs a \`methods\` factory.`);
    }

    let streamNames: readonly string[] = [];
    if (options.streams) {
        const table = options.streams(streamProbe(options.type) as ActorContext<S>);
        // Once here rather than per subscription, where `#streamTable` rebuilds it.
        if (__DEV__) warnIfInheritedTable(table, 'streams', options.type);
        streamNames = Object.freeze(Object.keys(table));
    }

    validateReads(options, streamNames);

    return {
        type: options.type,
        streamNames,
        __sigxActor: options
    };
}

/**
 * Check the `reads:` declaration.
 *
 * Nothing is returned and no names are stored on the definition: unlike
 * `streamNames`, which the dispatch path branches on, the only consumer of a
 * read's declaration needs its VALUE (the endpoint, to compose a header) and
 * reads it from the options directly. A parallel name list would be a second
 * source of truth for no reader.
 *
 * Definition-time throws in EVERY build, not just `__DEV__`: each of these is
 * a header the endpoint would otherwise emit — a promise made to caches
 * everywhere — and getting one wrong is not the kind of thing to discover
 * from a production cache hit.
 *
 * What cannot be checked here is whether a listed name is a real method: the
 * `methods` factory needs a live activation context, so unlike `streams` it
 * cannot be probed. The mapped type on `reads` covers that for typed callers,
 * and a name that matches nothing simply decorates a wrapper that 404s.
 */
function validateReads(
    options: { type: string; reads?: object; use?: readonly unknown[]; methodUse?: object },
    streamNames: readonly string[]
): void {
    if (!options.reads) return;
    for (const [method, cache] of Object.entries(options.reads) as [
        string,
        ActorReadCache | undefined
    ][]) {
        const where = `[sigx actors] actor "${options.type}" read "${method}"`;
        if (!cache) {
            // A present key with no value is the one shape that could ship a
            // client and server that disagree: the build stamps GET-capable
            // names from the KEYS of this object (it cannot evaluate values),
            // while the wrapper is marked cacheable from the VALUE. So the
            // proxy would issue GET and the endpoint would answer 405 — for a
            // method that looks declared in both places. Omit the key instead.
            throw new Error(
                `${where} has no cache declaration. Omit the key, or give it at least ` +
                    `\`{ maxAge: <seconds> }\` — a key with no value makes the build send GET ` +
                    `for a method the endpoint will refuse.`
            );
        }
        if (streamNames.includes(method)) {
            // A stream is a sequence, not a cacheable representation, and the
            // endpoint refuses `__sigxGet` on one anyway (405). Better to say
            // so at definition time than to ship a declaration that silently
            // does nothing.
            throw new Error(`${where} is a \`streams:\` method — a stream cannot be cached.`);
        }
        // `Cache-Control`'s delta-seconds are NON-NEGATIVE INTEGERS (RFC 9111
        // §1.2.2). `max-age=0.5` is not a smaller number to an intermediary,
        // it is a malformed directive — ignored by some, mis-parsed by others,
        // and either way the caching this declaration exists for silently does
        // not happen.
        const seconds = (value: unknown): boolean =>
            typeof value === 'number' && Number.isInteger(value) && value >= 0;
        if (!seconds(cache.maxAge)) {
            throw new Error(`${where} needs a \`maxAge\` of whole, non-negative seconds.`);
        }
        if (cache.staleWhileRevalidate !== undefined && !seconds(cache.staleWhileRevalidate)) {
            throw new Error(
                `${where} needs a \`staleWhileRevalidate\` of whole, non-negative seconds.`
            );
        }
        if (cache.sMaxAge !== undefined && !seconds(cache.sMaxAge)) {
            throw new Error(`${where} needs an \`sMaxAge\` of whole, non-negative seconds.`);
        }
        if (cache.public === true) {
            // `public` puts the response in SHARED caches, where one caller's
            // copy is served to the next. Core's contract for that is
            // args-only — never cookies, auth or headers — and a guard is the
            // one thing here that provably reads the request. There is no way
            // to inspect what it reads, so the safe reading of "this actor has
            // a guard" is "this response is per caller".
            // Own keys only, like the guard pipeline itself: indexing found
            // `Object.prototype.hasOwnProperty` for a read DECLARED as
            // `hasOwnProperty`, whose arity of 1 read as "there is a guard".
            const methodUse = options.methodUse as
                | Record<string, readonly unknown[]>
                | undefined;
            const guarded =
                (options.use?.length ?? 0) > 0 ||
                ((methodUse && Object.hasOwn(methodUse, method)
                    ? methodUse[method]?.length
                    : 0) ?? 0) > 0;
            if (guarded) {
                throw new Error(
                    `${where} declares \`public: true\`, but the actor runs guards on it. A ` +
                        `public read goes in SHARED caches, so its response must depend on its ` +
                        `arguments alone — with a guard in the chain, one caller's copy can be ` +
                        `served to the next. Drop \`public\` (the read is still cached, per ` +
                        `client, with Vary: Cookie) or drop the guard.`
                );
            }
        }
    }
}

/** Brand check: is this value a server-side actor definition? */
export function isActorDefinition(value: unknown): value is ActorDefinition {
    return (
        typeof value === 'object' &&
        value !== null &&
        typeof (value as { type?: unknown }).type === 'string' &&
        typeof (value as { __sigxActor?: unknown }).__sigxActor === 'object'
    );
}
