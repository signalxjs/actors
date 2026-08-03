/**
 * Where command OUTPUT goes, as distinct from where diagnostics go.
 *
 * `ctx.logger` prefixes every line with `[sigx] `, which is right for
 * "here is what I am doing" and wrong for "here is the answer". It broke
 * `--json` outright — `[sigx] {` is not JSON, so the documented
 * `sigx actors stats --json | jq …` could not work — and it turns a
 * carefully aligned table into one with seven characters of noise in front
 * of every row.
 *
 * So: the payload goes to stdout unprefixed, and diagnostics keep the
 * logger (which writes through `console.warn`/`console.error`, i.e.
 * stderr). That is also what makes `2>/dev/null` do the useful thing.
 */

/** One line of command output. */
export function out(line: string): void {
    process.stdout.write(`${line}\n`);
}

/** A machine-readable payload — the only thing `--json` should emit. */
export function outJson(value: unknown): void {
    process.stdout.write(`${JSON.stringify(value, null, 2)}\n`);
}
