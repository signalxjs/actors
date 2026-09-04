// @vitest-environment node
/**
 * The `ws-load` shape guard (#224): the hand-run must carry the same
 * `INFRA_SHAPE` the recorded `ws-bench` path builds, name the image it
 * resolved, and — given `expect-shape=<string>` — refuse to run against a
 * deployment whose live shape differs, using the same comparison contract
 * `--compare` has (`benchmarks/src/shape.mjs`).
 *
 * Spawned like `testenv-config.test.ts` (the script dispatches at top
 * level), but with a FAKE `kubectl`/`git` on an otherwise-empty PATH: the
 * shape is read off the live Deployment via jsonpath, and a canned kubectl
 * is what lets the guard be exercised without an estate. helm is absent on
 * purpose — a run that gets PAST the guard dies at helm, which is how the
 * "proceeds" cases tell the guard said yes without running anything.
 */
import { execFileSync } from 'node:child_process';
import { chmodSync, mkdirSync, mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { delimiter, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';

const SCRIPT = fileURLToPath(new URL('../deploy/testenv.mjs', import.meta.url));

/** What the fake cluster reports — the live shape assembled from it. */
const LIVE_SHAPE =
    'ws replicas=3 nodes=3 cpu=1000m sku=Standard_D2ls_v6 image=tag123 knobs=ENABLE_SOCKET=1,SOCKET_MAX_SUBSCRIPTIONS=256';

const IDENTITY = [
    'RG', 'CLUSTER', 'ACR', 'LOCATION', 'CHAT_HOST',
    'DNS_ZONE', 'DNS_RG', 'LOAD_RG', 'LOAD_VM'
];

let fakeBin = '';

/**
 * One Node script per fake tool, plus the wrappers each platform needs to
 * find it on PATH: an executable extensionless shim for POSIX, a `.cmd`
 * for Windows (`spawnable` routes `.cmd` through cmd.exe itself).
 */
function fakeTool(dir: string, name: string, body: string) {
    const impl = join(dir, `fake-${name}.js`);
    writeFileSync(impl, body);
    writeFileSync(
        join(dir, name),
        `#!/bin/sh\nexec "${process.execPath}" "${impl}" "$@"\n`
    );
    chmodSync(join(dir, name), 0o755);
    writeFileSync(
        join(dir, `${name}.cmd`),
        `@echo off\r\n"${process.execPath}" "${impl}" %*\r\n`
    );
}

beforeAll(() => {
    fakeBin = mkdtempSync(join(tmpdir(), 'sigx-fakebin-'));
    mkdirSync(fakeBin, { recursive: true });
    // Answers keyed off the jsonpath (or subcommand) in the argv. The
    // ENABLE_SOCKET filter query must be matched before the env[*] range —
    // both mention `.env[` but they are different questions.
    fakeTool(fakeBin, 'kubectl', `
const args = process.argv.slice(2).join(' ');
const out = (s) => process.stdout.write(s + '\\n');
if (args.includes('get-contexts')) { out('testctx'); process.exit(0); }
if (args.includes('ENABLE_SOCKET')) { out('1'); process.exit(0); }
if (args.includes('.env[*]')) { out('ENABLE_SOCKET=1'); out('SOCKET_MAX_SUBSCRIPTIONS=256'); process.exit(0); }
if (args.includes('.spec.replicas')) { out('3'); process.exit(0); }
if (args.includes('nodeName')) { out('n1'); out('n2'); out('n3'); process.exit(0); }
if (args.includes('containers[0].image')) { out('example.azurecr.io/sigx-actors-test:tag123'); process.exit(0); }
// The cores-per-host and node-SKU fields of the shape (#380): the host
// Deployment's CPU limit, and every node's instance type — n4 is a node
// the hosts do NOT run on, so its SKU must not reach the shape.
if (args.includes('resources.limits.cpu')) { out('1000m'); process.exit(0); }
if (args.includes('instance-type')) { out('n1\tStandard_D2ls_v6'); out('n2\tStandard_D2ls_v6'); out('n3\tStandard_D2ls_v6'); out('n4\tStandard_D8ls_v6'); process.exit(0); }
process.exit(0);
`);
    fakeTool(fakeBin, 'git', `
if (process.argv.includes('rev-parse')) process.stdout.write('fakehead\\n');
process.exit(0);
`);
});

afterAll(() => {
    try {
        rmSync(fakeBin, { recursive: true, force: true, maxRetries: 10, retryDelay: 200 });
    } catch {
        // Windows may hold the dir briefly; a leftover temp dir is fine.
    }
});

function runWsLoad(args: string[]) {
    // PATH carries ONLY the fakes (plus System32 on Windows, which
    // `spawnable`'s `where` lookup and cmd.exe both live in) — so nothing
    // here can reach a real kubectl, helm or az.
    const path = [
        fakeBin,
        ...(process.platform === 'win32'
            ? [join(process.env.SystemRoot ?? 'C:\\Windows', 'System32')]
            : [])
    ].join(delimiter);
    const base: Record<string, string> = {
        PATH: path,
        // `spawnable` resolves a Windows shim via `where`, which matches
        // executable extensions off PATHEXT — strip it and the fake
        // `kubectl.cmd` stops being found. ComSpec/SystemRoot are what
        // cmd.exe itself needs; node is invoked by absolute path.
        ...(process.platform === 'win32' && process.env.PATHEXT
            ? { PATHEXT: process.env.PATHEXT }
            : {}),
        ...(process.env.ComSpec ? { ComSpec: process.env.ComSpec } : {}),
        ...(process.env.SystemRoot ? { SystemRoot: process.env.SystemRoot } : {}),
        RG: 'rg',
        CLUSTER: 'testctx',
        ACR: 'example'
    };
    for (const name of IDENTITY) base[name] ??= '';
    try {
        const stdout = execFileSync(process.execPath, [SCRIPT, 'ws-load', ...args], {
            encoding: 'utf8',
            stdio: ['ignore', 'pipe', 'pipe'],
            env: base
        });
        return { code: 0, stdout, stderr: '' };
    } catch (error) {
        const failed = error as { status: number | null; stdout?: string; stderr?: string };
        return {
            code: failed.status,
            stdout: String(failed.stdout ?? ''),
            stderr: String(failed.stderr ?? '')
        };
    }
}

describe('ws-load shape guard', () => {
    it('prints the live shape (the one ws-bench builds) before running', () => {
        const { stdout } = runWsLoad(['image.tag=tag123', `expect-shape=${LIVE_SHAPE}`]);
        expect(stdout).toContain(`shape: ${LIVE_SHAPE}`);
    });

    it('always names the resolved image, flagging a defaulted tag', () => {
        // No image.tag: the silent gitSha() default must be visible.
        const defaulted = runWsLoad([`expect-shape=${LIVE_SHAPE}`]);
        expect(defaulted.stdout).toContain('image: example.azurecr.io/sigx-actors-test:fakehead');
        expect(defaulted.stdout).toContain('defaulted');
        // An EMPTY tag is unset, not a pin — `repo:` is not an image.
        const empty = runWsLoad(['image.tag=', `expect-shape=${LIVE_SHAPE}`]);
        expect(empty.stdout).toContain('image: example.azurecr.io/sigx-actors-test:fakehead');
        expect(empty.stdout).toContain('defaulted');
        // An explicit tag is not flagged.
        const explicit = runWsLoad(['image.tag=tag123', `expect-shape=${LIVE_SHAPE}`]);
        expect(explicit.stdout).toContain('image: example.azurecr.io/sigx-actors-test:tag123');
        expect(explicit.stdout).not.toContain('defaulted');
    });

    it('refuses to run when expect-shape differs from the live deployment', () => {
        const expected = 'ws replicas=2 nodes=2 image=older';
        const { code, stdout, stderr } = runWsLoad(['image.tag=tag123', `expect-shape=${expected}`]);
        expect(code).toBe(1);
        // The same fatal contract --compare has: prefix, then both shapes.
        expect(stderr).toContain('deployment shape:');
        expect(stderr).toContain(expected);
        expect(stderr).toContain(LIVE_SHAPE);
        // Refused BEFORE the run: the Job was never rendered.
        expect(stdout).not.toContain('rendering');
    });

    it('proceeds past the guard when expect-shape matches', () => {
        const { stdout, stderr } = runWsLoad(['image.tag=tag123', `expect-shape=${LIVE_SHAPE}`]);
        expect(stdout).toContain('expect-shape matches the live deployment');
        expect(stderr).not.toContain('deployment shape:');
        // It went on to attempt the run (and died at the absent helm) —
        // expect-shape must never leak into the chart values.
        expect(stderr).not.toContain('expect-shape');
    });

    it('runs without expect-shape, still printing shape and image', () => {
        const { stdout, stderr } = runWsLoad(['image.tag=tag123']);
        expect(stdout).toContain(`shape: ${LIVE_SHAPE}`);
        expect(stdout).toContain('image: example.azurecr.io/sigx-actors-test:tag123');
        expect(stderr).not.toContain('deployment shape:');
    });

    it('still refuses a token without =', () => {
        const { code, stderr } = runWsLoad(['noequals']);
        expect(code).toBe(1);
        expect(stderr).toContain('key=value');
    });
});
