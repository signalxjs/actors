#!/usr/bin/env node
/**
 * Resolve every unqualified `#N` issue reference in the checked paths against
 * this repository, and fail on the ones that cannot be right.
 *
 * The hazard this exists for: the repo was promoted from a private tracker,
 * and the `#N` numbers came with it. Most no longer resolve — but some land on
 * a real, unrelated public issue, so a reader following one arrives somewhere
 * plausible and false. A file mixing genuine and stale references reads as
 * internally consistent, which is what makes it dangerous (#120).
 *
 * Two failure modes are decidable without judgement, and only those are
 * enforced:
 *
 *   1. `#N` above the repository's highest issue/PR number — stale by
 *      construction, whatever it once meant.
 *   2. `#N` that resolves to nothing (404).
 *
 * Whether a *resolvable* number is cited for the right thing is a judgement
 * this cannot make. Keep it that way: a check that guesses would be ignored.
 *
 * Cross-repo references must be written `owner/repo#N` — those are skipped,
 * and writing them that way is the fix when a number belongs elsewhere.
 *
 * Usage:
 *   node scripts/check-issue-refs.mjs               # check the default paths
 *   node scripts/check-issue-refs.mjs --list        # print every ref and skip
 *   node scripts/check-issue-refs.mjs --require-api # fail if the API is unreachable
 *   node scripts/check-issue-refs.mjs <path>...     # check specific paths
 *
 * A file in `EXEMPT_FILES` is skipped even under an explicit directory, but
 * naming the file itself as a `<path>` checks it — an explicit ask wins.
 *
 * Both checks need the repository's issue list, so **neither can run without
 * the GitHub API** — there is no useful offline subset. Without a token or a
 * network this exits 0 with a warning, so a contributor's local run is not
 * blocked by it.
 *
 * CI passes `--require-api`, which turns that into a hard failure. A guard
 * that goes green while checking nothing is the exact failure mode this
 * script exists to prevent, so it must not be able to happen where it counts.
 */
import { execFileSync } from 'node:child_process';
import { readFileSync } from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const REPO = 'signalxjs/actors';

// Pin the root rather than trusting the cwd, as the other scripts here do:
// `git ls-files` returns paths relative to the repo root, so reading them
// from anywhere else silently finds nothing and reports a clean run.
const REPO_ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');

/**
 * Paths under the guard. This list is deliberately narrower than the repo:
 * the CHANGELOGs and `packages/` still carry pre-promotion numbers that need
 * a judgement pass (#122), and a check that starts red is a check people
 * learn to ignore. Widen it as each area is cleaned — that is the point of
 * it being a list.
 */
const DEFAULT_PATHS = [
    'benchmarks',
    'docs',
    'examples',
    'perf',
    'scripts',
    '.github',
    'README.md',
    'AGENTS.md',
    'SECURITY.md',
    'CONTRIBUTING.md'
];

/**
 * Files whose `#N` are placeholders by design, not references. The PR
 * template's "Closes" / "Refs" examples are for the contributor to replace;
 * whether the numbers in them happen to resolve is noise either way.
 */
const EXEMPT_FILES = new Set(['.github/pull_request_template.md']);

const args = process.argv.slice(2);
const list = args.includes('--list');
const requireApi = args.includes('--require-api');
const paths = args.filter((a) => !a.startsWith('--'));
const roots = paths.length > 0 ? paths : DEFAULT_PATHS;

/** Paths named on the command line, in `git ls-files` form, so an exempt
 *  file asked for by name is checked rather than silently dropped. */
const named = new Set(paths.map((p) => path.posix.normalize(p.replace(/\\/g, '/'))));

/** Tracked files under `roots`, minus the binary/lock noise. */
function trackedFiles() {
    const out = execFileSync('git', ['ls-files', '-z', '--', ...roots], {
        cwd: REPO_ROOT,
        encoding: 'utf8',
        maxBuffer: 1e8
    });
    return out
        .split('\0')
        .filter(Boolean)
        .filter((f) => !/pnpm-lock|\.(svg|png|ico|jpg|jpeg|gif|woff2?)$/i.test(f))
        .filter((f) => !EXEMPT_FILES.has(f) || named.has(f));
}

/**
 * Every `#N` that is NOT preceded by `owner/repo`, a word character, `#`, `&`
 * or `-`. Those exclusions keep out `##` headings, HTML entities (`&#39;`),
 * fragment identifiers in URLs, and CSS-ish `-#1`.
 *
 * It deliberately DOES match `(#245)`, `[#12]`, `see #45.` and `,#45` — the
 * parenthesised form is the most common one in this repo, and an earlier
 * version of this check that required whitespace before the `#` found nine
 * references where there were sixty.
 */
const REF = /(?<![0-9A-Za-z_&#-])(?:(?<qualified>[A-Za-z0-9._-]+\/[A-Za-z0-9._-]+))?#(?<num>\d{1,5})\b/g;

function collect() {
    const refs = new Map(); // number -> [{file, line, text}]
    const skipped = [];
    for (const file of trackedFiles()) {
        let text;
        try {
            text = readFileSync(path.join(REPO_ROOT, file), 'utf8');
        } catch {
            continue; // unreadable or binary — nothing to check
        }
        text.split('\n').forEach((line, i) => {
            for (const m of line.matchAll(REF)) {
                const { qualified, num } = m.groups;
                if (qualified) {
                    skipped.push(`${file}:${i + 1}  ${qualified}#${num}`);
                    continue;
                }
                const n = Number(num);
                if (!refs.has(n)) refs.set(n, []);
                refs.get(n).push({ file, line: i + 1, text: line.trim() });
            }
        });
    }
    return { refs, skipped };
}

function gh(endpoint) {
    return JSON.parse(execFileSync('gh', ['api', endpoint], { encoding: 'utf8', maxBuffer: 1e8 }));
}

/** Highest issue OR pull-request number — they share one sequence. */
function highestNumber() {
    const q = `search/issues?q=repo:${REPO}&sort=created&order=desc&per_page=1`;
    const r = gh(q);
    if (!r.items?.length) throw new Error('no items');
    return r.items[0].number;
}

function exists(n) {
    try {
        gh(`repos/${REPO}/issues/${n}`);
        return true;
    } catch {
        return false;
    }
}

const { refs, skipped } = collect();
const numbers = [...refs.keys()].sort((a, b) => a - b);

if (list) {
    for (const n of numbers) {
        for (const loc of refs.get(n)) console.log(`#${n}\t${loc.file}:${loc.line}`);
    }
    for (const s of skipped) console.log(`skip\t${s}`);
}

console.log(`checking ${numbers.length} distinct refs across ${roots.join(', ')}`);
if (numbers.length === 0) process.exit(0);

let max = null;
try {
    max = highestNumber();
} catch (err) {
    if (requireApi) {
        console.error('✗ could not reach the GitHub API, and --require-api is set.');
        console.error(`  ${err instanceof Error ? err.message : String(err)}`);
        console.error('  Nothing was checked. Failing rather than reporting a green guard.');
        process.exit(1);
    }
    console.warn('⚠ could not reach the GitHub API — nothing was checked.');
    console.warn('  (set GH_TOKEN, or run `gh auth login`, for the full check.)');
    process.exit(0);
}

const failures = [];
for (const n of numbers) {
    const where = refs.get(n);
    if (n > max) {
        failures.push({ n, why: `above the repo's highest number (#${max}) — stale by construction`, where });
        continue;
    }
    if (!exists(n)) failures.push({ n, why: 'does not resolve (404)', where });
}

if (failures.length === 0) {
    console.log(`✓ all ${numbers.length} refs resolve (highest in repo: #${max})`);
    process.exit(0);
}

console.error(`\n✗ ${failures.length} bad issue reference(s):\n`);
for (const f of failures) {
    console.error(`  #${f.n} — ${f.why}`);
    for (const loc of f.where) console.error(`      ${loc.file}:${loc.line}  ${loc.text.slice(0, 90)}`);
}
console.error(
    '\nThese are almost certainly pre-promotion private-tracker numbers.\n' +
        'Fix by describing the change in prose and anchoring to the code, or by\n' +
        'pointing at the real public issue. If the number belongs to another repo,\n' +
        'qualify it as owner/repo#N.\n'
);
process.exit(1);
