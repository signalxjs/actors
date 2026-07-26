#!/usr/bin/env node

/**
 * @sigx/store - Pre-publish pack smoke test
 *
 * Catches packaging bugs that lint/typecheck/test miss:
 *   - missing files in `files` array
 *   - broken `exports` map
 *   - dist/ produced by stale builds
 *
 * What it does:
 *   1. Build the package (delegates to `pnpm run build`).
 *   2. `pnpm pack` the package into a temp dir.
 *   3. Spin up a minimal scratch project with a file: dep on the tarball.
 *   4. `npm install` (pulls peer/runtime deps from the npm registry).
 *   5. `node` import-smoke the published entry point.
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

const PACKAGES = ['packages/store'];

const sandbox = join(tmpdir(), `sigx-store-verify-pack-${Date.now()}`);
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

function packPackage(pkgPath) {
    const pkgFullPath = join(rootDir, pkgPath);
    const pkgJson = readJson(join(pkgFullPath, 'package.json'));
    run('pnpm pack --pack-destination ' + JSON.stringify(tarballDir), { cwd: pkgFullPath });
    const tarballs = readdirSync(tarballDir).filter((f) => f.endsWith('.tgz'));
    const safeName = pkgJson.name.replace('@', '').replace('/', '-');
    const match = tarballs.find((f) => f.startsWith(safeName));
    if (!match) {
        throw new Error(`Could not find tarball for ${pkgJson.name} in ${tarballDir}`);
    }
    return { name: pkgJson.name, version: pkgJson.version, tarball: join(tarballDir, match) };
}

function main() {
    step(`Sandbox: ${sandbox}`);
    mkdirSync(tarballDir, { recursive: true });
    mkdirSync(appDir, { recursive: true });

    step('Build package');
    run('pnpm run build', { cwd: rootDir });

    step('Pack publishable package');
    const packed = PACKAGES.map(packPackage);
    for (const p of packed) {
        console.log(`   📦 ${p.name}@${p.version}  →  ${p.tarball}`);
    }

    step('Create scratch app');
    const deps = Object.fromEntries(
        packed.map((p) => [p.name, `file:${p.tarball.replace(/\\/g, '/')}`])
    );
    const appPkg = {
        name: 'sigx-store-pack-smoke',
        version: '0.0.0',
        private: true,
        type: 'module',
        scripts: { smoke: 'node smoke.mjs' },
        dependencies: deps,
    };
    writeFileSync(join(appDir, 'package.json'), JSON.stringify(appPkg, null, 2));

    writeFileSync(
        join(appDir, 'smoke.mjs'),
        [
            "import * as store from '@sigx/store';",
            "if (!store || typeof store !== 'object') throw new Error('import returned non-object');",
            "const keys = Object.keys(store);",
            "if (keys.length === 0) throw new Error('@sigx/store exports no named bindings');",
            "console.log('✓ @sigx/store named exports:', keys.join(', '));",
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
