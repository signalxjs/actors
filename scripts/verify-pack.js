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
    'packages/actors-cli',
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
    const files = listed[0].files.map((f) => f.path);
    const missing = REQUIRED_IN_TARBALL.filter((re) => !files.some((f) => re.test(f)));
    if (missing.length > 0) {
        throw new Error(
            `${name}: tarball is missing ${missing.map((re) => String(re)).join(', ')}. ` +
                'npm only picks these up from the PACKAGE directory — a root-only file ' +
                'does not travel, and `files` does not reach the repo root.'
        );
    }
}

function packPackage(pkgPath) {
    const pkgFullPath = join(rootDir, pkgPath);
    const pkgJson = readJson(join(pkgFullPath, 'package.json'));
    assertTarballContents(pkgFullPath, pkgJson.name);
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
