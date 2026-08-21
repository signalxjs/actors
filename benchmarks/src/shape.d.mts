/**
 * Types for `shape.mjs`. Hand-written for the same reason `spawn.d.mts`
 * is: the module is plain ESM because `testenv.mjs` runs it with no build
 * step, while `benchmarks/src` is typechecked.
 */

/** Marks a mismatch that must ABORT a comparison rather than warn. */
export const INFRA_SHAPE_MISMATCH: string;

/**
 * The fatal, prefixed mismatch line for two `INFRA_SHAPE` strings, or
 * null when they describe the same deployment. Absent and empty are the
 * same claim; anything else is compared verbatim.
 */
export function shapeMismatch(
    a: string | undefined,
    b: string | undefined
): string | null;
