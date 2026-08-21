/**
 * The `INFRA_SHAPE` comparison contract, as one function.
 *
 * Plain `.mjs` (with `shape.d.mts` beside it, the `spawn.mjs` arrangement)
 * because `perf/aks/deploy/testenv.mjs` needs it at RUNTIME: the `ws-load`
 * hand-run guards against a live deployment whose shape differs from the
 * one the operator expects (#224), and a `.ts` import from that script
 * would only load on a Node new enough to strip types — while the
 * identity-validation tests spawn it on every Node in the CI matrix.
 * `env.ts` imports this file, so the contract still has exactly ONE
 * implementation; `benchmarks/__tests__/shape-guard.test.ts` pins that.
 */

/** Marks a mismatch that must ABORT a comparison rather than warn. */
export const INFRA_SHAPE_MISMATCH = 'deployment shape:';

/**
 * Compare two deployment shapes the way `--compare` does. Normalized so
 * that "absent" and "empty" are the same claim — a baseline recorded
 * before the field existed has it undefined, which must read as "no
 * shape", not as a mismatch against an empty one — and verbatim
 * otherwise: a shape is never parsed, only matched. Returns the fatal,
 * prefixed line, or null when the two describe the same deployment.
 *
 * @param {string | undefined} a
 * @param {string | undefined} b
 * @returns {string | null}
 */
export function shapeMismatch(a, b) {
    if ((a ?? '') === (b ?? '')) return null;
    return `${INFRA_SHAPE_MISMATCH} ${a || '(none)'} vs ${b || '(none)'}`;
}
