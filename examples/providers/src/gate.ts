/**
 * The env gate every infrastructure-backed demo starts with — the same
 * posture as the provider packages' own test suites, which
 * `describe.skipIf(!PG_URL)` themselves: no variable, no run, exit 0.
 *
 * A skip is a MESSAGE, not a silent exit. It names the variable, says what
 * has to be behind it, and shows one way to get there, so the demo reads
 * as "here is what you need" rather than "nothing happened".
 */
export interface GateSpec {
    /** The demo's name in the message — `pg`, `surreal`. */
    demo: string;
    /** The environment variable the demo needs. */
    env: string;
    /** What must be behind it — `a Postgres >= 13`. */
    needs: string;
    /** One way to have it locally, and the run line that follows. */
    howTo: readonly string[];
}

/** The skip text, exactly as the demo prints it. Pure, so it is testable. */
export function skipMessage(spec: GateSpec): string {
    return [
        `[providers] ${spec.demo} demo SKIPPED: ${spec.env} is not set.`,
        `  It needs ${spec.needs}. One way to get one:`,
        ...spec.howTo.map((line) => `    ${line}`),
        `  (The @sigx/actors-${spec.demo} test suite gates on the same variable; CI runs it in a dedicated job.)`
    ].join('\n');
}

/**
 * The value of `spec.env`, or `null` after printing the skip message.
 * Reads `env` explicitly rather than `process.env` so the test can pass a
 * plain object.
 */
export function gate(spec: GateSpec, env: Record<string, string | undefined>): string | null {
    const value = env[spec.env];
    if (value) return value;
    console.log(skipMessage(spec));
    return null;
}
