// @vitest-environment node
/**
 * The render-equivalence guard over the two Helm charts (#59, option 3).
 *
 * `perf/aks/deploy/chart` and `perf/app/deploy/chart` were hand-copied from
 * one another and have been drifting since: every hardening lesson lands in
 * one chart and silently misses the other. Until the shared templates are
 * extracted into a library chart (option 1 of #59), THIS is what keeps the
 * lessons in step. It renders both charts with `helm template` and asserts
 * that the hardening outcomes agree — never the bytes. The charts differ on
 * purpose in a dozen places (the port the probes hit, the env surface, one
 * has an HPA and the other an Ingress), and a byte diff would fail on all
 * of them while saying nothing about whether a lesson went missing.
 *
 * What counts as a lesson here is what each chart's own comments say was
 * learned the hard way: the zero-drop rolling strategy, the always-rendered
 * serviceAccountName (the SSA relinquish trap, #154), a grace period that
 * covers preStop plus the actor drain deadline, the three-probe recipe from
 * the Kubernetes guide, resource requests AND limits, a PodDisruptionBudget
 * that actually selects the host pods, and SOME spread of hosts across
 * nodes — the one place the two charts still legitimately differ in
 * mechanism (podAntiAffinity vs topologySpreadConstraints), so the assertion
 * is on the outcome (hosts spread on the hostname topology), not the field.
 *
 * Needs `helm` on PATH; skips with a message otherwise. CI runs it in
 * `.github/workflows/charts.yml`, which already installs helm for the
 * kind-cluster dry runs — the main matrix has no helm and skips cleanly.
 *
 * The negative controls at the end are how a check earns its place: a case
 * that cannot fail is decoration, so the spread and PDB detectors are each
 * shown to go red against a render with that lesson switched off.
 */
import { execFileSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { describe, expect, it } from 'vitest';
import { parseAllDocuments } from 'yaml';

const REPO = fileURLToPath(new URL('../../../', import.meta.url));

/** True when `helm` resolves on PATH and runs — the only prerequisite. */
const helm = (() => {
    try {
        execFileSync('helm', ['version', '--short'], { stdio: 'ignore' });
        return true;
    } catch {
        return false;
    }
})();
if (!helm) {
    console.warn(
        'chart-equivalence: `helm` is not on PATH — skipping. CI runs this suite in charts.yml.'
    );
}

// Loose on purpose: these are rendered Kubernetes objects and the test reads
// a handful of well-known paths out of them.
type Manifest = Record<string, any>;

interface Chart {
    /** Chart directory, relative to the repo root (forward slashes; helm accepts them everywhere). */
    dir: string;
    namespace: string;
    /** Identity values the chart REQUIRES — the public charts default to none (see charts.yml). */
    identity: string[];
}

/** The two charts, keyed by the name the failure messages use. */
const CHARTS: Record<string, Chart> = {
    'perf/aks': {
        dir: 'perf/aks/deploy/chart',
        namespace: 'sigx-actors-test',
        identity: ['image.repository=registry.example.com/citest', 'image.tag=citest']
    },
    'perf/app': {
        dir: 'perf/app/deploy/chart',
        namespace: 'sigx-chat',
        identity: [
            'image.repository=registry.example.com/citest',
            'image.tag=citest',
            'ingress.host=chat.example.test'
        ]
    }
};

/** `helm template` the chart and return every object it renders. */
function render(chart: Chart, ...set: string[]): Manifest[] {
    const args = ['template', 'release', chart.dir, '-n', chart.namespace];
    for (const kv of [...chart.identity, ...set]) args.push('--set', kv);
    const out = execFileSync('helm', args, { cwd: REPO, encoding: 'utf8' });
    return parseAllDocuments(out)
        .map((doc) => doc.toJS() as Manifest | null)
        .filter((obj): obj is Manifest => !!obj && typeof obj.kind === 'string');
}

/** Every key of `selector` is present in `labels` with the same value. */
const selects = (selector: Record<string, string> | undefined, labels: Record<string, string>) =>
    !!selector &&
    Object.keys(selector).length > 0 &&
    Object.entries(selector).every(([k, v]) => labels[k] === v);

function hostDeployment(manifests: Manifest[]): Manifest {
    const dep = manifests.find(
        (m) =>
            m.kind === 'Deployment' && m.metadata?.labels?.['app.kubernetes.io/component'] === 'host'
    );
    if (!dep) throw new Error('no host Deployment rendered');
    return dep;
}

const podLabels = (dep: Manifest): Record<string, string> => dep.spec.template.metadata.labels;

function container(dep: Manifest, name: string): Manifest {
    const c = dep.spec.template.spec.containers.find((c: Manifest) => c.name === name);
    if (!c) throw new Error(`no '${name}' container on ${dep.metadata.name}`);
    return c;
}

/**
 * How the host pods are spread across nodes, if at all. Either mechanism
 * counts, provided it is on the hostname topology AND its selector matches
 * the host pods themselves — perf/aks also carries a required anti-affinity
 * against the Redis pod, which is a different lesson and must not satisfy
 * this one.
 */
function spreadMechanism(
    dep: Manifest
): 'topologySpreadConstraints' | 'podAntiAffinity' | null {
    const spec = dep.spec.template.spec;
    const labels = podLabels(dep);
    const onHosts = (term: Manifest | undefined) =>
        term?.topologyKey === 'kubernetes.io/hostname' &&
        selects(term.labelSelector?.matchLabels, labels);

    if ((spec.topologySpreadConstraints ?? []).some(onHosts)) return 'topologySpreadConstraints';

    const anti = spec.affinity?.podAntiAffinity ?? {};
    const required: Manifest[] = anti.requiredDuringSchedulingIgnoredDuringExecution ?? [];
    const preferred: Manifest[] = anti.preferredDuringSchedulingIgnoredDuringExecution ?? [];
    if (required.some(onHosts) || preferred.some((p) => onHosts(p.podAffinityTerm))) {
        return 'podAntiAffinity';
    }
    return null;
}

/** The PDB that selects the host pods, or undefined when none does. */
const hostPdb = (manifests: Manifest[], dep: Manifest): Manifest | undefined =>
    manifests.find(
        (m) =>
            m.kind === 'PodDisruptionBudget' && selects(m.spec?.selector?.matchLabels, podLabels(dep))
    );

/** The seconds a `["sleep", "N"]` preStop hook holds the pod for. */
function preStopSleepSeconds(c: Manifest): number {
    const command: string[] = c.lifecycle?.preStop?.exec?.command ?? [];
    expect(command[0], 'preStop hook is a sleep').toBe('sleep');
    return Number(command[1]);
}

/** The shape of a probe with the port name stripped — the charts differ there by design. */
const probeShape = (probe: Manifest | undefined) =>
    probe && {
        path: probe.httpGet?.path,
        failureThreshold: probe.failureThreshold,
        periodSeconds: probe.periodSeconds
    };

describe.skipIf(!helm)('charts: the shared hardening lessons agree', () => {
    const rendered = helm
        ? Object.fromEntries(
              Object.entries(CHARTS).map(([name, chart]) => [name, render(chart)])
          )
        : {};
    const names = Object.keys(CHARTS);
    const host = (name: string) => hostDeployment(rendered[name]);

    it.each(names)('%s: rolls out surge-first, drain-second', (name) => {
        const { strategy } = host(name).spec;
        expect(strategy.type).toBe('RollingUpdate');
        expect(strategy.rollingUpdate.maxUnavailable).toBe(0);
        expect(strategy.rollingUpdate.maxSurge).toBeGreaterThanOrEqual(1);
    });

    it.each(names)('%s: always renders serviceAccountName (SSA relinquish, #154)', (name) => {
        const sa = host(name).spec.template.spec.serviceAccountName;
        expect(typeof sa).toBe('string');
        expect(sa.length).toBeGreaterThan(0);
    });

    it.each(names)(
        '%s: terminationGracePeriodSeconds covers preStop plus the 30s actor drain',
        (name) => {
            const dep = host(name);
            const grace = dep.spec.template.spec.terminationGracePeriodSeconds;
            const preStop = preStopSleepSeconds(container(dep, 'host'));
            expect(preStop).toBeGreaterThan(0);
            expect(grace).toBeGreaterThanOrEqual(preStop + 30);
        }
    );

    it.each(names)('%s: startup, liveness and readiness probes on a real port', (name) => {
        const c = container(host(name), 'host');
        const portNames = new Set(c.ports.map((p: Manifest) => p.name));
        for (const kind of ['startupProbe', 'livenessProbe', 'readinessProbe']) {
            const probe = c[kind];
            expect(probe, kind).toBeDefined();
            expect(probe.httpGet?.path, `${kind} is httpGet`).toBeTruthy();
            expect(portNames.has(probe.httpGet.port), `${kind} port ${probe.httpGet.port}`).toBe(
                true
            );
            expect(probe.periodSeconds, `${kind} periodSeconds`).toBeGreaterThan(0);
        }
        // Startup covers a slow boot (the Redis join), so it must be allowed
        // more than a single miss; readiness is the drain signal, so it has
        // its own endpoint.
        expect(c.startupProbe.failureThreshold).toBeGreaterThan(1);
        expect(c.readinessProbe.httpGet.path).not.toBe(c.livenessProbe.httpGet.path);
    });

    it.each(names)('%s: host and redis carry resource requests and limits', (name) => {
        const containers = [container(host(name), 'host')];
        const redis = rendered[name].find(
            (m: Manifest) =>
                m.kind === 'Deployment' &&
                m.metadata?.labels?.['app.kubernetes.io/component'] === 'redis'
        );
        expect(redis, 'redis Deployment').toBeDefined();
        containers.push(container(redis!, 'redis'));
        for (const c of containers) {
            for (const tier of ['requests', 'limits']) {
                expect(c.resources?.[tier]?.cpu, `${c.name} ${tier}.cpu`).toBeTruthy();
                expect(c.resources?.[tier]?.memory, `${c.name} ${tier}.memory`).toBeTruthy();
            }
        }
    });

    it.each(names)('%s: a PodDisruptionBudget selects the host pods', (name) => {
        const pdb = hostPdb(rendered[name], host(name));
        expect(pdb, 'a PDB whose selector matches the host pod labels').toBeDefined();
        // Permits a voluntary disruption, but not all of them at once.
        expect(pdb!.spec.maxUnavailable).toBeGreaterThanOrEqual(1);
    });

    it.each(names)('%s: hosts are spread across nodes', (name) => {
        expect(spreadMechanism(host(name))).not.toBeNull();
    });

    it('perf/aks: the required self-spread is still a spread', () => {
        const dep = hostDeployment(render(CHARTS['perf/aks'], 'affinity.hostSelfSpread=required'));
        expect(spreadMechanism(dep)).toBe('podAntiAffinity');
    });

    /**
     * The cross-chart half: where the lesson has a NUMBER, the two charts
     * must agree on it. These are the values one chart learned and the
     * other must not have missed — the probe cadence and paths, the drain
     * budget, the disruption budget. The spread MECHANISM is deliberately
     * not compared: #59's library-chart extraction is where the two become
     * one, and until then either is an acceptable spread.
     */
    it('the lessons agree across charts', () => {
        const summary = (name: string) => {
            const dep = host(name);
            const c = container(dep, 'host');
            return {
                rollingUpdate: dep.spec.strategy.rollingUpdate,
                terminationGracePeriodSeconds: dep.spec.template.spec.terminationGracePeriodSeconds,
                preStopSleepSeconds: preStopSleepSeconds(c),
                probes: {
                    startup: probeShape(c.startupProbe),
                    liveness: probeShape(c.livenessProbe),
                    readiness: probeShape(c.readinessProbe)
                },
                pdbMaxUnavailable: hostPdb(rendered[name], dep)?.spec.maxUnavailable
            };
        };
        const [first, ...rest] = names;
        for (const other of rest) {
            expect(summary(other), `${other} vs ${first}`).toEqual(summary(first));
        }
    });

    describe('negative controls: the detectors go red when a lesson is switched off', () => {
        it('spread off renders no spread', () => {
            const dep = hostDeployment(render(CHARTS['perf/app'], 'spread.enabled=false'));
            expect(spreadMechanism(dep)).toBeNull();
        });

        it('pdb off renders no PDB', () => {
            const manifests = render(CHARTS['perf/aks'], 'pdb.enabled=false');
            expect(hostPdb(manifests, hostDeployment(manifests))).toBeUndefined();
        });
    });
});
