// @vitest-environment node
/**
 * The node-pool `--set` flags testenv.mjs hands helm must render a
 * SCHEDULABLE toleration.
 *
 * This exists because `--set tolerations[0].value=x` on its own does not
 * (#406). Helm merges maps but REPLACES list elements, so that flag alone
 * throws away the key, operator and effect the chart's `values.yaml` gives
 * the same element and leaves `- value: x`. The chart still renders, helm
 * still succeeds, and the API server rejects the Deployment at install with
 * "operator must be Exists when `key` is empty" — so the failure surfaces
 * as a broken `up` against a node pool that has already been created and
 * billed, which is exactly where it was found.
 *
 * The assertion is on the RENDER rather than on the flag text: what matters
 * is that the toleration Kubernetes receives is complete, not how testenv
 * spells it. The flags themselves are read out of testenv.mjs so the test
 * cannot drift from the script by being updated alongside it.
 *
 * Needs `helm` on PATH and skips with the reason otherwise, matching
 * `chart-equivalence.test.ts` — the hosted runners preinstall one.
 */
import { execFileSync } from 'node:child_process';
import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { describe, expect, it } from 'vitest';

const helmVersion = (() => {
    try {
        return execFileSync('helm', ['version', '--short'], { encoding: 'utf8' }).trim();
    } catch {
        return null;
    }
})();

const root = fileURLToPath(new URL('../../..', import.meta.url));
const source = readFileSync(fileURLToPath(new URL('../deploy/testenv.mjs', import.meta.url)), 'utf8');

/** The `--set` flags of `workloadSets`, read from the script itself. */
function workloadSetFlags(workload: string): string[] {
    const body = /const workloadSets = \(workload\) => \[([\s\S]*?)\];/.exec(source)?.[1];
    if (!body) throw new Error('workloadSets not found in testenv.mjs — did it move?');
    return [...body.matchAll(/'--set',\s*[`']([^`']+)[`']/g)].map((m) =>
        m[1].replace('${workload}', workload)
    );
}

/** The `tolerations:` block of a rendered Deployment, as parsed key/values. */
function renderedToleration(chart: string, template: string, flags: string[]): Record<string, string> {
    const out = execFileSync(
        'helm',
        ['template', 'rel', chart, '-n', 'x', '--set', 'image.repository=r', '--set', 'image.tag=t',
            '--set', 'ingress.host=example.test', ...flags.flatMap((f) => ['--set', f]),
            '-s', template],
        { cwd: root, encoding: 'utf8' }
    );
    // Line-based on purpose: a regex for "the indented block after
    // tolerations:" runs straight on into `affinity:` and reports two dozen
    // keys, which is a passing test for the wrong reason.
    const lines = out.split('\n');
    const start = lines.findIndex((l) => /^\s*tolerations:\s*$/.test(l));
    if (start < 0) throw new Error(`no tolerations rendered by ${chart}/${template}`);
    const indent = (l: string) => l.length - l.trimStart().length;
    const listIndent = indent(lines[start + 1] ?? '');
    const item: string[] = [];
    for (const line of lines.slice(start + 1)) {
        if (!line.trim()) break;
        if (indent(line) < listIndent) break;
        // The FIRST element only: a second `- ` at the list indent ends it.
        if (item.length && indent(line) === listIndent && line.trimStart().startsWith('- ')) break;
        item.push(line);
    }
    return Object.fromEntries(
        item.map((l) => /^\s*(?:-\s*)?([a-zA-Z]+):\s*(\S+)\s*$/.exec(l))
            .filter((m): m is RegExpExecArray => m !== null)
            .map((m) => [m[1], m[2]])
    );
}

describe.skipIf(!helmVersion)(`testenv node-pool flags (helm ${helmVersion})`, () => {
    for (const [chart, template] of [
        ['perf/aks/deploy/chart', 'templates/host-deployment.yaml'],
        ['perf/app/deploy/chart', 'templates/host-deployment.yaml']
    ] as const) {
        it(`renders a complete, schedulable toleration for ${chart}`, () => {
            const t = renderedToleration(chart, template, workloadSetFlags('some-pool'));
            // All four, because Kubernetes validates the combination: an
            // empty key demands `operator: Exists`, so a lone `value` is
            // rejected outright rather than merely mis-scheduled.
            expect(t).toMatchObject({
                key: 'workload',
                operator: 'Equal',
                value: 'some-pool',
                effect: 'NoSchedule'
            });
        });
    }

    it('goes red for the flag that shipped — a lone value drops the rest of the element', () => {
        // The negative control, and the actual #406 bug: helm replaces the
        // list element rather than merging into it.
        const t = renderedToleration('perf/aks/deploy/chart', 'templates/host-deployment.yaml', [
            'nodeSelector.workload=some-pool',
            'tolerations[0].value=some-pool'
        ]);
        expect(t).toEqual({ value: 'some-pool' });
        expect(t.key).toBeUndefined();
        expect(t.operator).toBeUndefined();
    });
});
