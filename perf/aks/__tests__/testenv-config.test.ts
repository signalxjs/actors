// @vitest-environment node
/**
 * The structural guard on testenv.mjs's identity handling: every verb must
 * refuse to run without its identity env vars, naming exactly the missing
 * ones. This is what keeps a private estate's names from ever returning as
 * defaults — a default would make these spawns SUCCEED past validation,
 * and the assertions here are on the refusal.
 *
 * Spawned as a child process because testenv.mjs is a script, not a
 * module: it dispatches at top level by design. Validation fires before
 * any az/kubectl call, so nothing here needs (or touches) a cloud.
 */
import { execFileSync } from 'node:child_process';
import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { describe, expect, it } from 'vitest';

const SCRIPT = fileURLToPath(new URL('../deploy/testenv.mjs', import.meta.url));

/** Verb → the env vars its refusal must name (mirrors NEEDS in testenv.mjs). */
const NEEDS: Record<string, string[]> = {
    up: ['RG', 'CLUSTER', 'ACR', 'CHAT_HOST', 'DNS_ZONE', 'DNS_RG'],
    status: ['RG', 'CLUSTER', 'CHAT_HOST', 'LOAD_RG', 'LOAD_VM'],
    test: ['RG', 'CLUSTER', 'CHAT_HOST', 'LOCATION', 'LOAD_RG', 'LOAD_VM'],
    baseline: ['RG', 'CLUSTER', 'CHAT_HOST', 'LOCATION', 'LOAD_RG', 'LOAD_VM'],
    bench: ['RG', 'CLUSTER', 'CHAT_HOST', 'LOCATION', 'LOAD_RG', 'LOAD_VM'],
    load: ['RG', 'CLUSTER', 'CHAT_HOST', 'LOCATION', 'LOAD_RG', 'LOAD_VM'],
    'ws-up': ['RG', 'CLUSTER'],
    'ws-load': ['RG', 'CLUSTER', 'ACR'],
    'ws-bench': ['RG', 'CLUSTER'],
    'wf-load': ['RG', 'CLUSTER', 'ACR'],
    'wf-bench': ['RG', 'CLUSTER'],
    'migrate-check': ['RG', 'CLUSTER', 'ACR', 'CHAT_HOST'],
    down: ['RG', 'CLUSTER', 'CHAT_HOST', 'DNS_ZONE', 'DNS_RG', 'LOAD_RG'],
    'vm-up': ['LOCATION', 'LOAD_RG', 'LOAD_VM']
};

const IDENTITY = [
    'RG', 'CLUSTER', 'ACR', 'LOCATION', 'CHAT_HOST',
    'DNS_ZONE', 'DNS_RG', 'LOAD_RG', 'LOAD_VM'
];

function run(verb: string, env: Record<string, string> = {}) {
    // A minimal environment: every identity var EXPLICITLY empty so a
    // developer's own exported estate cannot leak in and pass validation,
    // and an empty PATH so that if validation ever regressed, the spawned
    // script could not find a real `az`/`kubectl` to aim at anything.
    // (node is invoked by absolute path, so it needs no PATH itself;
    // SystemRoot survives because node needs it on Windows.)
    const base: Record<string, string> = {
        PATH: '',
        ...(process.env.SystemRoot ? { SystemRoot: process.env.SystemRoot } : {})
    };
    for (const name of IDENTITY) base[name] = '';
    try {
        const stdout = execFileSync(process.execPath, [SCRIPT, verb], {
            encoding: 'utf8',
            stdio: ['ignore', 'pipe', 'pipe'],
            env: { ...base, ...env }
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

describe('testenv.mjs identity validation', () => {
    /**
     * The refusal assertions below only mean anything if the script got far
     * enough to validate. It imports across two trees — `post-fn.mjs` from
     * the example it deploys, `spawn.mjs` from `benchmarks/src` — and a
     * module that fails to resolve exits 1 with a stack, which every
     * `code === 1` assertion here would happily accept.
     */
    it('loads: every cross-tree import resolves', () => {
        const { stderr } = run('status');
        expect(stderr).not.toMatch(/ERR_MODULE_NOT_FOUND|Cannot find module/);
    });

    for (const [verb, needs] of Object.entries(NEEDS)) {
        it(`${verb} refuses a bare environment, naming ${needs.join(', ')}`, () => {
            const { code, stderr } = run(verb);
            expect(code).toBe(1);
            expect(stderr).toContain(`${verb} needs ${needs.join(', ')} set in the environment`);
        });
    }

    it('names only what is actually missing', () => {
        const { code, stderr } = run('status', { RG: 'rg', CLUSTER: 'cluster' });
        expect(code).toBe(1);
        expect(stderr).toContain('status needs CHAT_HOST, LOAD_RG, LOAD_VM set');
        expect(stderr).not.toContain('needs RG');
    });

    it('rejects a CHAT_HOST outside DNS_ZONE before any verb runs', () => {
        const { code, stderr } = run('status', {
            CHAT_HOST: 'chat.example.com',
            DNS_ZONE: 'other.example.net'
        });
        expect(code).toBe(1);
        expect(stderr).toContain('must be a name inside DNS_ZONE');
    });

    it('rejects an unknown verb with usage', () => {
        const { code, stdout } = run('nuke');
        expect(code).toBe(1);
        expect(stdout).toContain('usage: node testenv.mjs');
    });

    /**
     * The dispatch workflow lists the verbs as a `choice`, and GitHub
     * refuses anything outside it with a 422 — so a verb added here but not
     * there exists everywhere except the place people actually run it from.
     * That is not hypothetical: `ws-bench` shipped in #185 and could not be
     * dispatched at all until #187, which is this test.
     *
     * The verb list comes from the script's OWN usage line rather than a
     * copy kept here, so the only thing that can drift is the workflow.
     */
    it('offers every verb in the dispatch workflow', () => {
        const { stdout } = run('nuke');
        const usage = /usage: node testenv\.mjs <([^>]+)>/.exec(stdout);
        expect(usage).not.toBeNull();
        const verbs = usage![1]!.split('|');
        expect(verbs.length).toBeGreaterThan(5);

        const workflow = readFileSync(
            fileURLToPath(new URL('../../../.github/workflows/cluster-test.yml', import.meta.url)),
            'utf8'
        );
        // The `options:` block of the `verb` input — the first one in the
        // file, and the only choice input it has.
        const block = /options:\n((?:\s+- \S+\n)+)/.exec(workflow);
        expect(block).not.toBeNull();
        const offered = block![1]!
            .split('\n')
            .map((line) => line.replace(/^\s*-\s*/, '').trim())
            .filter(Boolean);

        expect([...verbs].sort()).toEqual([...offered].sort());
    });
});
