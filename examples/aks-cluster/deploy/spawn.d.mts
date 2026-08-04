/**
 * Types for `spawn.mjs`. Hand-written for the same reason `post-fn.d.mts`
 * is: the module is plain ESM because `testenv.mjs` runs it with no build
 * step, while `benchmarks/src` is typechecked.
 */

/**
 * Rewrite `(command, args)` into a form `execFileSync`/`spawn` can run on
 * this platform, without ever handing the arguments to a shell. Spread the
 * returned `options` into your own.
 */
export function spawnable(
    command: string,
    args?: readonly string[]
): {
    command: string;
    args: string[];
    options: { windowsVerbatimArguments?: boolean };
};
