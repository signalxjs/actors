#!/usr/bin/env node

/**
 * SignalX actors - Pre-publish pack smoke test
 *
 * Catches packaging bugs that lint/typecheck/test miss:
 *   - missing files in `files` array
 *   - broken `exports` map
 *   - dist/ produced by stale builds
 *
 * What it does:
 *   1. Build the packages (delegates to `pnpm run build`).
 *   2. `pnpm pack` every publishable package into a temp dir.
 *   3. Spin up a minimal scratch project with file: deps on the tarballs.
 *   4. `npm install` (pulls peer/runtime deps from the npm registry; the
 *      file: dep on @sigx/actors satisfies every sibling's peer on it).
 *   5. `node` import-smoke each package's published entry point.
 *
 * Usage:
 *   node scripts/verify-pack.js
 *
 * No flags. Exits non-zero on any failure.
 */

import { execSync } from 'child_process';
import { mkdirSync, readFileSync, rmSync, writeFileSync, readdirSync } from 'fs';
import { join, dirname } from 'path';
import { fileURLToPath } from 'url';
import { tmpdir } from 'os';

const __dirname = dirname(fileURLToPath(import.meta.url));
const rootDir = join(__dirname, '..');

// Keep in step with scripts/publish.js — the pack smoke exists so nothing in
// that list ships a broken tarball.
const PACKAGES = [
    'packages/actors',
    'packages/actors-monitor',
    'packages/actors-cli',
    'packages/actors-dashboard',
    'packages/actors-cloudflare',
    'packages/actors-k8s',
    'packages/actors-otel',
    'packages/actors-pg',
    'packages/actors-redis',
    'packages/actors-surreal',
    'packages/actors-tcp',
    'packages/actors-ws',
];

// The entry each package is smoke-imported by. One deliberate exception:
// @sigx/actors-otel's ROOT entry imports its OPTIONAL @opentelemetry/api
// peer, which npm does not auto-install — so the OTel-free ./prometheus
// subpath is the one that must import cleanly here, and the root entry is
// asserted separately to fail with exactly that missing peer (below).
const SMOKE_ENTRIES = {
    '@sigx/actors-otel': '@sigx/actors-otel/prometheus',
};

const sandbox = join(tmpdir(), `sigx-actors-verify-pack-${Date.now()}`);
const tarballDir = join(sandbox, 'tarballs');
const appDir = join(sandbox, 'app');

function run(cmd, opts = {}) {
    console.log(`$ ${cmd}${opts.cwd ? `  (in ${opts.cwd})` : ''}`);
    execSync(cmd, { stdio: 'inherit', ...opts });
}

function step(label) {
    console.log(`\n▶  ${label}`);
}

function readJson(path) {
    return JSON.parse(readFileSync(path, 'utf-8'));
}

/**
 * Files npm must find INSIDE the package directory.
 *
 * npm always includes `package.json`, `README`, `LICENSE` and `CHANGELOG` —
 * but only when they sit in the package dir. A `files: ["dist"]` allowlist
 * does not reach up to the repo root, so a root-only LICENSE ships nothing:
 * every tarball would carry `"license": "MIT"` metadata and no license text.
 * That is invisible to lint, typecheck and an import smoke, and only becomes
 * apparent to someone reading the published tarball.
 */
const REQUIRED_IN_TARBALL = [/^LICENSE/i, /^README/i, /^CHANGELOG/i];

function assertTarballContents(pkgFullPath, name) {
    // `npm pack --dry-run --json` lists exactly what would ship, without
    // needing tar to read the archive back — the msys tar on Windows rewrites
    // paths that look like `C:\…`, and Node has no built-in tar reader.
    const listed = JSON.parse(
        execSync('npm pack --dry-run --json', {
            cwd: pkgFullPath,
            encoding: 'utf8',
            stdio: ['ignore', 'pipe', 'ignore']
        })
    );
    // TWO shapes, because the release workflow runs a different npm from
    // every other job — Node 24 + `npm@latest`, since trusted publishing
    // needs >= 11.5.1 — so the shape that passes locally and in CI is not
    // the shape that runs at publish time. That asymmetry cost a release:
    // this read `listed[0].files`, which is right for npm 10 and dies with
    // "Cannot read properties of undefined" on npm 11, INSIDE the release
    // run, after every other gate was green.
    //
    //   npm 10:  [ { name, files: [...], … } ]
    //   npm 11:  { "@scope/name": { name, files: [...], … } }
    // Keyed BY NAME on npm 11, so ask for this package rather than trusting
    // position: a shape with more than one key would otherwise silently
    // assert the wrong package's tarball, which is worse than failing. The
    // sole-value fallback covers a key that is not spelled exactly `name`
    // (a scope alias, say); anything else falls through to the error below,
    // including `null` — `Object.values(null)` throws, and a TypeError here
    // is precisely the unreadable failure this whole change is about.
    const entry = Array.isArray(listed)
        ? listed[0]
        : listed !== null && typeof listed === 'object'
          ? (listed[name] ??
            (Object.keys(listed).length === 1 ? Object.values(listed)[0] : undefined))
          : undefined;
    if (!entry || !Array.isArray(entry.files)) {
        throw new Error(
            `${name}: could not read the file list from \`npm pack --dry-run --json\` ` +
                `(npm ${execSync('npm --version', { encoding: 'utf8' }).trim()}). Got ` +
                `${JSON.stringify(listed).slice(0, 200)}. The output shape changed — ` +
                'teach this function the new one rather than loosening the check, which ' +
                'is what stops a tarball shipping without its LICENSE.'
        );
    }
    const files = entry.files.map((f) => f.path);
    const missing = REQUIRED_IN_TARBALL.filter((re) => !files.some((f) => re.test(f)));
    if (missing.length > 0) {
        throw new Error(
            `${name}: tarball is missing ${missing.map((re) => String(re)).join(', ')}. ` +
                'npm only picks these up from the PACKAGE directory — a root-only file ' +
                'does not travel, and `files` does not reach the repo root.'
        );
    }
}

/** Every `.js` under a directory, recursively. `.map` and `.d.ts` are skipped. */
function distJsFiles(dir) {
    const found = [];
    let entries;
    try {
        entries = readdirSync(dir, { withFileTypes: true });
    } catch {
        return found; // no dist — assertTarballContents already complained
    }
    for (const entry of entries) {
        const full = join(dir, entry.name);
        if (entry.isDirectory()) found.push(...distJsFiles(full));
        else if (entry.name.endsWith('.js')) found.push(full);
    }
    return found;
}

/** `@scope/name/deep` → `@scope/name`; `pkg/deep` → `pkg`. */
function packageOf(specifier) {
    const parts = specifier.split('/');
    return specifier.startsWith('@') ? parts.slice(0, 2).join('/') : parts[0];
}

/**
 * Every package this one IMPORTS at runtime must be one it DECLARES.
 *
 * The gap this closes: the smoke below installs all twelve tarballs into one
 * sandbox, so a package that imports a sibling without declaring it resolves
 * there and fails only for a real consumer installing it alone. That is
 * exactly how `@sigx/actors-dashboard@0.9.0` shipped unloadable (#254) — it
 * imports `digestSnapshot` from `@sigx/actors/host` for the per-host latency
 * row, and named `@sigx/actors` nowhere in its manifest.
 *
 * Static rather than twelve more `npm install`s: it costs seconds, it is
 * deterministic, and "every runtime import is declared" is precisely the
 * invariant that was broken.
 *
 * **An OPTIONAL peer does not count as declared.** npm does not install one,
 * so importing it at runtime crashes a consumer exactly as an undeclared
 * import does — the two are the same bug wearing different manifests. This is
 * what turns the #116 guarantee into something checked rather than claimed:
 * `@sigx/actors` is an optional peer of `@sigx/actors-monitor`, so that
 * package's dist may never import it, and HTTP mode keeps working with no
 * actor runtime present.
 *
 * `OPTIONAL_PEER_IMPORTS` is the deliberate-exception list, in the same shape
 * and spirit as `SMOKE_ENTRIES` above. Each entry states why, because an
 * unexplained one is indistinguishable from the bug this function exists to
 * catch.
 */
/**
 * Optional peers a package is allowed to import, and why.
 *
 * The legitimate pattern is an optional peer reached only from an **opt-in
 * subpath** that exists for that integration: nobody importing `.` pays for
 * it, and anybody importing the subpath has already chosen the dependency.
 * This scan sees a whole `dist` at once, so it cannot verify that
 * confinement — the entry named against each line is the claim, and the
 * reason it is written down.
 *
 * Same shape and spirit as `SMOKE_ENTRIES` above. An unexplained entry here
 * is indistinguishable from the bug this whole function exists to catch, so
 * every one carries its reason.
 */
const OPTIONAL_PEER_IMPORTS = {
    // `./app` is the sigx-app integration and `./vite` is the build plugin.
    // A headless host imports neither, which is the entire point of core
    // staying WinterCG-clean and zero-dependency.
    '@sigx/actors': ['@sigx/runtime-core', '@sigx/vite', 'vite'],
    // `./node` is the Node adapter; `./client` is the WinterCG browser half
    // and must never reach `ws`.
    '@sigx/actors-ws': ['ws'],
    // Intentional AND asserted: the root entry is meant to require the peer,
    // and the smoke below proves it fails with exactly that missing import.
    // `./prometheus` is the OTel-free entry consumers get for free.
    '@sigx/actors-otel': ['@opentelemetry/api'],
    // NOT one of the above — a KNOWN GAP, tracked in #116. The plugin entry
    // is the only entry, and it imports the runtime, so `@sigx/actors` is not
    // really optional here: `sigx actors top --url …` in a project without
    // the runtime installed is the case that fails. Listed rather than
    // silently permitted, so it stays visible. Delete this line when #116
    // lands.
    '@sigx/actors-cli': ['@sigx/actors']
};

function assertImportsDeclared(pkgFullPath, pkgJson) {
    const optional = Object.entries(pkgJson.peerDependenciesMeta ?? {})
        .filter(([, meta]) => meta?.optional)
        .map(([name]) => name);
    const allowed = new Set(OPTIONAL_PEER_IMPORTS[pkgJson.name] ?? []);
    const unusable = new Set(optional.filter((name) => !allowed.has(name)));
    const declared = new Set([
        ...Object.keys(pkgJson.dependencies ?? {}),
        ...Object.keys(pkgJson.peerDependencies ?? {}),
        // A self-import through the package's own name is how a package
        // reaches its own subpath exports; it resolves for consumers.
        pkgJson.name
    ]);
    for (const name of unusable) declared.delete(name);
    // `from "x"`, `import "x"`, `import("x")`, `export … from "x"`.
    //
    // Two guards, both earned. The lookbehind stops `r.from` in minified code
    // being read as an import keyword. And a candidate is only believed if it
    // is a SYNTACTICALLY VALID package specifier — without that, the literal
    // `"collected from"` in the cluster panel matched, because the closing
    // quote of that very string sits exactly where a specifier's opening
    // quote would, and everything up to the next quote was captured as a
    // package name. Filtering on the npm name grammar removes that whole
    // class: a false positive would now need a string that is itself a legal
    // package name sitting immediately after the word `from`.
    const CANDIDATE = /(?<![.\w$])(?:from|import)\s*\(?\s*["']([^"'\n]+)["']/g;
    const PACKAGE_SPECIFIER =
        /^(?:@[a-z0-9-~][a-z0-9-._~]*\/)?[a-z0-9-~][a-z0-9-._~]*(?:\/[a-zA-Z0-9-._~/]*)?$/;
    const offenders = new Map();
    for (const file of distJsFiles(join(pkgFullPath, 'dist'))) {
        const source = readFileSync(file, 'utf-8');
        for (const match of source.matchAll(CANDIDATE)) {
            const specifier = match[1];
            if (specifier.startsWith('node:')) continue;
            if (specifier.startsWith('.')) continue; // relative — not a package
            if (!PACKAGE_SPECIFIER.test(specifier)) continue;
            const name = packageOf(specifier);
            if (!declared.has(name)) offenders.set(name, `${file} → "${specifier}"`);
        }
    }
    if (offenders.size > 0) {
        const lines = [...offenders].map(([name, where]) => {
            const why = unusable.has(name)
                ? ' — declared, but as an OPTIONAL peer, which npm does not install'
                : '';
            return `  ${name}${why}\n      ${where}`;
        });
        throw new Error(
            `${pkgJson.name}: imports packages a consumer will not have:\n${lines.join('\n')}\n` +
                'Add each to "dependencies" or a REQUIRED "peerDependencies" entry — or, if ' +
                'the package is genuinely meant to fail without it, add it to ' +
                'OPTIONAL_PEER_IMPORTS above with the reason. The all-in-one smoke below ' +
                'will NOT catch this: it installs every tarball together, so a missing ' +
                'declaration resolves there and fails only for a consumer installing this ' +
                'package on its own.'
        );
    }
}

function packPackage(pkgPath) {
    const pkgFullPath = join(rootDir, pkgPath);
    const pkgJson = readJson(join(pkgFullPath, 'package.json'));
    assertTarballContents(pkgFullPath, pkgJson.name);
    assertImportsDeclared(pkgFullPath, pkgJson);
    run('pnpm pack --pack-destination ' + JSON.stringify(tarballDir), { cwd: pkgFullPath });
    const safeName = pkgJson.name.replace('@', '').replace('/', '-');
    // Exact filename, not a prefix: with the whole family in one dir,
    // `sigx-actors` as a prefix would happily match `sigx-actors-redis-…`.
    const expected = `${safeName}-${pkgJson.version}.tgz`;
    const match = readdirSync(tarballDir).find((f) => f === expected);
    if (!match) {
        throw new Error(`Could not find tarball ${expected} for ${pkgJson.name} in ${tarballDir}`);
    }
    return { name: pkgJson.name, version: pkgJson.version, tarball: join(tarballDir, match) };
}

function main() {
    step(`Sandbox: ${sandbox}`);
    mkdirSync(tarballDir, { recursive: true });
    mkdirSync(appDir, { recursive: true });

    step('Build package');
    run('pnpm run build', { cwd: rootDir });

    step('Pack publishable packages');
    const packed = PACKAGES.map(packPackage);
    for (const p of packed) {
        console.log(`   📦 ${p.name}@${p.version}  →  ${p.tarball}`);
    }

    step('Create scratch app');
    const deps = Object.fromEntries(
        packed.map((p) => [p.name, `file:${p.tarball.replace(/\\/g, '/')}`])
    );
    const appPkg = {
        name: 'sigx-actors-pack-smoke',
        version: '0.0.0',
        private: true,
        type: 'module',
        scripts: { smoke: 'node smoke.mjs' },
        dependencies: deps,
    };
    writeFileSync(join(appDir, 'package.json'), JSON.stringify(appPkg, null, 2));

    const entries = packed.map((p) => SMOKE_ENTRIES[p.name] ?? p.name);
    writeFileSync(
        join(appDir, 'smoke.mjs'),
        [
            `const entries = ${JSON.stringify(entries)};`,
            'for (const entry of entries) {',
            '    const mod = await import(entry);',
            "    if (!mod || typeof mod !== 'object') throw new Error(entry + ' import returned non-object');",
            '    // `default` does not count: every package here ships named',
            '    // bindings, and a default-only surface would mean a broken build.',
            "    const keys = Object.keys(mod).filter((k) => k !== 'default');",
            "    if (keys.length === 0) throw new Error(entry + ' exports no named bindings (a default alone does not count)');",
            "    console.log('✓', entry, '→', keys.join(', '));",
            '}',
            '',
            '// The one entry that must NOT import here: @sigx/actors-otel\'s root',
            '// needs its OPTIONAL @opentelemetry/api peer, absent by design in this',
            '// scratch app. Failing with exactly that module missing proves the',
            '// exports map resolved and the only gap is the peer the user opted',
            '// out of — any other error is a packaging bug.',
            'try {',
            "    await import('@sigx/actors-otel');",
            "    throw new Error('@sigx/actors-otel root imported WITHOUT @opentelemetry/api — the peer is no longer optional-by-construction');",
            '} catch (err) {',
            "    if (err?.code !== 'ERR_MODULE_NOT_FOUND' || !String(err?.message).includes('@opentelemetry/api')) throw err;",
            "    console.log('✓ @sigx/actors-otel root correctly requires the optional @opentelemetry/api peer');",
            '}',
            '',
        ].join('\n')
    );

    step('Install scratch app (npm — to avoid pnpm workspace hoisting interference)');
    run('npm install --no-audit --no-fund --loglevel=error', { cwd: appDir });

    step('Run import smoke');
    run('npm run smoke --silent', { cwd: appDir });

    step('✅ Pack smoke test passed');
}

try {
    main();
} catch (err) {
    console.error('\n❌ Pack smoke test failed:', err.message);
    console.error(`   Sandbox preserved for inspection: ${sandbox}`);
    process.exitCode = 1;
    process.exit(1);
}

try {
    rmSync(sandbox, { recursive: true, force: true });
} catch {
    // ignore
}
