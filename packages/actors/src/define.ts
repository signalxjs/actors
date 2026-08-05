/**
 * `defineActor` — the actor definition API. Pure declaration: no activation
 * machinery lives here (that is `./host`), so importing a `*.actor.ts`
 * module on the server costs nothing until the host activates a key.
 */
import type {
    ActorContext,
    ActorDefinition,
    ActorMethodTable,
    ActorOptions,
    ActorReadCache,
    ActorStreamTable,
    AnyActorDefinition
} from './types';
import { warnIfInheritedTable } from './own-member';
import { topicNameObjection } from './topics';

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
    // `allowAnonymous` + `authorize` is NOT a contradiction and no longer
    // throws (rfc-server-v4 §1.2 removed the pre-v4 rule with no analog).
    // `unguarded` + `use` was a lie — the chain ran anyway. This pair is
    // coherent: middleware and authentication still run, the identity gate
    // is waived, and the declared policies then decide against a NULLABLE
    // principal. "Anyone may read it, but only the owner may read a private
    // one" is exactly that shape.
    if (typeof options.methods !== 'function') {
        throw new Error(`[sigx actors] actor "${options.type}" needs a \`methods\` factory.`);
    }
    validateMigrateState(options);
    if (options.stateless !== undefined) {
        // The stateless marker is set by `defineWorker`, whose options bag
        // cannot express any of these — but the marker is reachable by a
        // hand-written `defineActor` call, and every one of these options
        // binds behaviour to a single persistent identity that a pooled
        // stateless type does not have. Same rule as `unguarded` + `use`:
        // a contradiction throws in every build.
        const contradiction = (
            [
                'persistence',
                'tasks',
                'subscriptions',
                'onReminder',
                'placement',
                'reentrant',
                'methodReentrancy',
                'migrateState'
            ] as const
        ).find((key) => options[key] !== undefined);
        if (contradiction) {
            throw new Error(
                `[sigx actors] actor "${options.type}" declares \`stateless\` together with ` +
                    `\`${contradiction}\` — a stateless worker has no persistent identity to ` +
                    `bind it to. Use \`defineWorker\`, which cannot express it.`
            );
        }
        // Same check `defineWorker` makes — repeated HERE because the marker
        // is reachable without it, and a zero/fractional cap corrupts the
        // pool arithmetic rather than failing loudly.
        const cap = options.stateless.maxLocal;
        if (cap !== undefined && (!Number.isInteger(cap) || cap < 1)) {
            throw new Error(
                `[sigx actors] worker "${options.type}" needs a positive integer \`maxLocal\`.`
            );
        }
    }

    let streamNames: readonly string[] = [];
    if (options.streams) {
        const table = options.streams(streamProbe(options.type) as ActorContext<S>);
        // Once here rather than per subscription, where `#streamTable` rebuilds it.
        if (__DEV__) warnIfInheritedTable(table, 'streams', options.type);
        streamNames = Object.freeze(Object.keys(table));
    }

    validateReads(options, streamNames);
    validateSubscriptions(options);

    return {
        type: options.type,
        streamNames,
        __sigxActor: options
    };
}

/**
 * Check the `migrateState:` declaration — a definition-time throw in every
 * build, for the same reason the checks above are. A malformed hook fails
 * only on a load that FINDS a record, and a fresh dev run never has one: the
 * failure would first appear in production, on whichever host happens to
 * activate a pre-existing key. Definition time is import time, everywhere.
 */
function validateMigrateState(options: { type: string; migrateState?: unknown }): void {
    const spec = options.migrateState as { migrate?: unknown; persist?: unknown } | undefined;
    if (spec === undefined || typeof spec === 'function') return;
    const where = `[sigx actors] actor "${options.type}" \`migrateState\``;
    // Optional chaining covers null and every non-object: a string's
    // `.migrate` is undefined too, so one check refuses them all.
    if (typeof spec?.migrate !== 'function') {
        throw new Error(`${where} must be a function, or \`{ migrate, persist? }\`.`);
    }
    if (spec.persist !== undefined && spec.persist !== 'lazy' && spec.persist !== 'eager') {
        throw new Error(`${where}: \`persist\` must be 'lazy' (the default) or 'eager'.`);
    }
}

/**
 * Check the `subscriptions:` declaration — definition-time throws in every
 * build, because a topic name is a wire commitment (it rides dispatch
 * symbols cross-host) and a non-callable handler would otherwise surface
 * only when the first event arrives, on whichever host that happens to be.
 */
function validateSubscriptions(options: {
    type: string;
    subscriptions?: Record<string, unknown>;
}): void {
    if (!options.subscriptions) return;
    if (__DEV__) warnIfInheritedTable(options.subscriptions, 'subscriptions', options.type);
    for (const [name, sub] of Object.entries(options.subscriptions)) {
        const where = `[sigx actors] actor "${options.type}" subscription ${JSON.stringify(name)}`;
        const objection = topicNameObjection(name);
        if (objection !== null) {
            throw new Error(`${where}: the topic name ${objection}.`);
        }
        if (typeof sub === 'function') continue;
        const shaped = sub as { key?: unknown; handle?: unknown } | null;
        if (typeof shaped?.handle !== 'function') {
            throw new Error(
                `${where} needs a handler — a function, or \`{ key?, handle }\` with a ` +
                    `\`handle\` function.`
            );
        }
        if (shaped.key !== undefined && typeof shaped.key !== 'function') {
            throw new Error(`${where} has a \`key\` that is not a function.`);
        }
    }
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
    options: {
        type: string;
        reads?: object;
        authorize?: unknown;
        methodAuthorize?: object;
    },
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
            // args-only — never cookies, auth or headers — and an
            // authorization policy is the one thing here that provably
            // decides per CALLER. There is no way to inspect what it reads,
            // so the safe reading of "this actor authorizes" is "this
            // response is per caller".
            //
            // Own keys only, like the pipeline itself: indexing found
            // `Object.prototype.hasOwnProperty` for a read DECLARED as
            // `hasOwnProperty`, whose arity of 1 read as "there is a chain".
            const count = (chain: unknown): number =>
                chain === undefined ? 0 : Array.isArray(chain) ? chain.length : 1;
            const methodAuthorize = options.methodAuthorize as
                | Record<string, unknown>
                | undefined;
            const authorizes =
                count(options.authorize) > 0 ||
                count(
                    methodAuthorize && Object.hasOwn(methodAuthorize, method)
                        ? methodAuthorize[method]
                        : undefined
                ) > 0;
            if (authorizes) {
                throw new Error(
                    `${where} declares \`public: true\`, but the actor authorizes it. A ` +
                        `public read goes in SHARED caches, so its response must depend on its ` +
                        `arguments alone — with a policy deciding per caller, one caller's copy ` +
                        `can be served to the next. Drop \`public\` (the read is still cached, ` +
                        `per client, with Vary: Cookie) or drop the policy.`
                );
            }
        }
    }
}

/**
 * @internal Is this definition a stateless worker pool (built by
 * `defineWorker`)? The runtime's one branch point for pooled dispatch.
 */
export function isStatelessDefinition(def: AnyActorDefinition): boolean {
    return def.__sigxActor.stateless !== undefined;
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
