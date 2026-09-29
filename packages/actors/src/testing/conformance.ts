/**
 * The shapes every conformance suite in this repo is built from.
 *
 * No test framework is imported anywhere under `testing/`. A case is a
 * descriptor with a `run()` that throws on failure, so the same cases drive
 * from whatever runner a package uses — and so "what implementing this seam
 * means" is a readable list rather than an archaeology exercise across N
 * packages' test files.
 *
 * `@sigx/actors/testing` is a published entry (#491), so these shapes are
 * public API. `@sigx/actors/cluster/testing` is still wired by
 * tsconfig/vitest aliases only and cannot be imported from outside this
 * workspace.
 */

/** A case that could not run here, and why — reported, never silently passed. */
export interface ConformanceSkip {
    skipped: string;
}

export interface ConformanceCase<Factory> {
    readonly name: string;
    /** One line: what breaks in production when this case fails. */
    readonly why: string;
    run(create: Factory): Promise<void | ConformanceSkip>;
}
