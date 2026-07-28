/**
 * What a command handler actually needs, spelled out.
 *
 * Deliberately NOT `TypedCommandContext<typeof someArgsObject>`. Inferring
 * the context from the `a.*()` builder chain is lovely at the call site and
 * unusable at the module boundary: the inferred type names `ValueArg` and
 * `BooleanArg` from `@sigx/args`, which resolve through pnpm's store, and
 * declaration emit refuses them as non-portable.
 *
 * Stating the parsed shape here costs one interface and buys three things:
 * the handlers stop depending on `@sigx/args` types at all, they are
 * trivially callable from a test with a plain object, and the flags are
 * documented in one place rather than inferred from two builder literals
 * that have to stay in sync.
 */
import type { Logger } from '@sigx/cli/plugin';

/** Every flag `sigx actors` accepts, after parsing. */
export interface ActorsArgs {
    /** The sub-verb: `stats`, `health`, … */
    verb?: string;
    /** Origin of a running silo. Its presence selects HTTP mode. */
    url?: string;
    secret?: string;
    base?: string;
    /** Actor app module for embedded mode. */
    app?: string;
    timeout?: number;
    json?: boolean;
}

/** The subset of `CommandContext` the handlers read. */
export interface ActorsCommandContext {
    cwd: string;
    args: ActorsArgs;
    logger: Logger;
}
