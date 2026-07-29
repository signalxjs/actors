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
    ActorStreamTable
} from './types';

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
        streamNames = Object.freeze(Object.keys(table));
    }

    return {
        type: options.type,
        streamNames,
        __sigxActor: options
    };
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
