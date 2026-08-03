/**
 * Set or bump the version of every publishable package — and keep the
 * cross-package peer ranges in step with it.
 *
 * That second half is the whole reason this is a script rather than a loop.
 * Eight packages declare `"peerDependencies": { "@sigx/actors": "^0.1.0" }`.
 * Bumping only `pkg.version` published all eight peering on a range that no
 * longer resolved to `latest`, and nothing downstream caught it: the range is
 * still valid semver, `verify:pack` installs from local tarballs where the
 * `file:` dep satisfies the peer whatever it says, and the breakage only
 * surfaces for a consumer installing from the registry.
 *
 * Edits are textual rather than JSON.parse → JSON.stringify. A round-trip
 * reformats the whole file — it unescapes `—` in every description, so a
 * one-line version bump arrives as a nine-file diff nobody reads.
 *
 *   node scripts/bump-version.js            # patch
 *   node scripts/bump-version.js minor
 *   node scripts/bump-version.js 0.2.0      # exact
 */
import { readFileSync, writeFileSync, readdirSync, statSync } from 'fs';
import { join, dirname } from 'path';
import { fileURLToPath } from 'url';

const __dirname = dirname(fileURLToPath(import.meta.url));
const packagesDir = join(__dirname, '..', 'packages');

const arg = process.argv[2] || 'patch';

// Check if arg is a version number (e.g., "0.2.0") or bump type
const isExactVersion = /^\d+\.\d+\.\d+/.test(arg);
const bumpType = isExactVersion ? null : arg;
const exactVersion = isExactVersion ? arg : null;

function bumpVersion(version, type) {
    const parts = version.split('.').map(Number);
    switch (type) {
        case 'major':
            return `${parts[0] + 1}.0.0`;
        case 'minor':
            return `${parts[0]}.${parts[1] + 1}.0`;
        case 'patch':
        default:
            return `${parts[0]}.${parts[1]}.${parts[2] + 1}`;
    }
}

/** Every package dir with a package.json, publishable or not. */
function readPackages(dir) {
    const found = [];
    for (const entry of readdirSync(dir)) {
        const fullPath = join(dir, entry);
        if (!statSync(fullPath).isDirectory()) continue;
        const pkgPath = join(fullPath, 'package.json');
        let text;
        try {
            text = readFileSync(pkgPath, 'utf-8');
        } catch {
            continue; // no package.json — not a package
        }
        found.push({ pkgPath, text, json: JSON.parse(text) });
    }
    return found;
}

/**
 * The indentation of the root object's own keys, read from the file rather
 * than assumed — it is what distinguishes a top-level key from a nested one.
 */
function topLevelIndent(text) {
    const match = /^\{\r?\n([ \t]+)"/.exec(text);
    if (!match) throw new Error('could not determine the top-level indentation');
    return match[1];
}

/**
 * Replace the root object's `"version"`, and only that one.
 *
 * The indent is matched EXACTLY. A looser `^\s*"version"` matches any depth,
 * so a nested `version` key appearing earlier in the file — inside an
 * `exports` condition, a script, a dependency literally named `version` —
 * would be the one that moved, silently and in the wrong place.
 *
 * The result is then re-parsed and checked, because a regex is the wrong thing
 * to trust alone for an edit whose failure mode is publishing the wrong
 * number.
 */
function setVersion(text, next, name) {
    const indent = topLevelIndent(text);
    const pattern = new RegExp(`(^${indent}"version":\\s*")[^"]+(")`, 'm');
    if (!pattern.test(text)) throw new Error(`${name}: no top-level "version" key to set`);
    const updated = text.replace(pattern, `$1${next}$2`);
    const got = JSON.parse(updated).version;
    if (got !== next) {
        throw new Error(`${name}: version is "${got}" after the edit, expected "${next}"`);
    }
    return updated;
}

/** Repoint a caret range on a sibling; leave `catalog:`/`workspace:` alone. */
function setSiblingRange(text, name, next) {
    const escaped = name.replace(/[/@]/g, '\\$&');
    return text.replace(
        new RegExp(`("${escaped}":\\s*")\\^\\d+\\.\\d+\\.\\d+([^"]*")`, 'g'),
        `$1^${next}$2`
    );
}

const packages = readPackages(packagesDir);

if (exactVersion) {
    console.log(`Setting all packages to version ${exactVersion}...\n`);
} else {
    console.log(`Bumping ${bumpType} version for packages...\n`);
}

// Pass 1 — versions. Collected first so pass 2 knows every new number before
// it rewrites a single range.
const next = new Map();
for (const pkg of packages) {
    if (pkg.json.private) {
        console.log(`Skipping private package: ${pkg.json.name}`);
        continue;
    }
    const from = pkg.json.version;
    const to = exactVersion || bumpVersion(from, bumpType);
    next.set(pkg.json.name, to);
    pkg.text = setVersion(pkg.text, to, pkg.json.name);
    console.log(`${pkg.json.name}: ${from} → ${to}`);
}

// Pass 2 — cross-package ranges, across EVERY package including private ones:
// an example peering on a stale range is a broken example.
for (const pkg of packages) {
    let text = pkg.text;
    for (const [name, version] of next) {
        if (name === pkg.json.name) continue;
        const updated = setSiblingRange(text, name, version);
        if (updated !== text) console.log(`  ${pkg.json.name}: ${name} → ^${version}`);
        text = updated;
    }
    pkg.text = text;
}

for (const pkg of packages) writeFileSync(pkg.pkgPath, pkg.text);

console.log('\nDone!');
