/**
 * Constant-time string comparison, shared by the two places that compare a
 * secret: the cluster envelope's HMAC signature and the ops endpoint's
 * bearer token.
 *
 * Internal — exported from no entry point. It lives here rather than in
 * either caller because a second copy of a comparison like this is exactly
 * how one of them quietly grows an early `return false`.
 */

/**
 * True iff `a` and `b` are equal, in time that does not depend on WHERE they
 * first differ.
 *
 * The length difference is folded into the accumulator rather than
 * short-circuited, so a mismatched length is not *faster* than a mismatched
 * byte — which is the only reason the loop below is worth having.
 *
 * The loop runs to the LONGER of the two, not to `b.length`. Iterating over
 * one side would make the running time a function of that side's length, and
 * the secret is not always the attacker-supplied one: `ops()` compares a
 * presented token against the configured secret, so bounding by `b` would
 * have timed the secret. Past the end of either string `charCodeAt` is
 * `NaN`, which `|| 0` maps to 0 — a real NUL at that index is still caught,
 * by the length term.
 */
export function timingSafeEquals(a: string, b: string): boolean {
    let mismatch = a.length ^ b.length;
    const length = Math.max(a.length, b.length);
    for (let i = 0; i < length; i++) {
        mismatch |= (a.charCodeAt(i) || 0) ^ (b.charCodeAt(i) || 0);
    }
    return mismatch === 0;
}
