/**
 * Running one workflow-engine load Job, as a function (#297) — the twin
 * of `ws-load.mjs`, for the same reason it exists: `testenv.mjs wf-load`
 * and `benchmarks/src/scenarios/workflow.ts` need the SAME orchestration,
 * or a hand-run number and a recorded one describe different runs.
 *
 * Deliberately standalone: its own `kubectl`/`helm` invocations from
 * `spawnable`, nothing imported from `testenv.mjs`.
 *
 * ## What this has to get right
 *
 * **Counts sum, percentiles do not, rates are recomputed.** A run may span
 * several generator pods (`parallelism`); their counts add, their
 * per-template latency percentiles cannot be merged, and their rates were
 * each taken over their own window. So `mergeWfRows` sums the counts, takes
 * latency from ONE pod and says which (`latencyFromPods`), and recomputes
 * rates from summed counts over the longest window.
 *
 * **The mechanism is read from inside the pods.** The engine's counters
 * (`ops.workflow`) are per host, so they are summed across every host pod
 * over `kubectl exec` — a port-forward would land on whichever pod the
 * Service liked. Reported as a before/after delta, and only when BOTH
 * snapshots saw every host, because the error direction of a partial
 * snapshot depends on which end lost a host.
 *
 * **Activations are a gauge.** `peakActivations` is sampled during the run
 * from the metrics section; read afterwards it says nothing about how many
 * sleeping runs the fleet held.
 */
import { execFileSync } from 'node:child_process';
import { mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { spawnable } from '../../../benchmarks/src/spawn.mjs';

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

function sh(cmd, args, { allowFail = false } = {}) {
    const run = spawnable(cmd, args);
    try {
        return execFileSync(run.command, run.args, {
            encoding: 'utf8',
            stdio: ['ignore', 'pipe', 'pipe'],
            ...run.options
        }).trim();
    } catch (error) {
        if (allowFail) return null;
        throw error;
    }
}

function discard(dir) {
    try {
        rmSync(dir, { recursive: true, force: true, maxRetries: 10, retryDelay: 200 });
    } catch {
        // Windows holds the directory briefly after the process exits.
    }
}

/**
 * The cluster counters worth carrying — an allowlist, as `ws-load.mjs`
 * keeps one, so the artifact does not grow a field every time the runtime
 * gains a counter.
 */
const CLUSTER_COUNTERS = [
    'remoteDispatches',
    'routedLocal',
    // The per-request locality pair (#52) — what `remote_dispatch_ratio`
    // in the workflow scenario is derived from.
    'dispatchesLocal',
    'dispatchesRemote',
    'directoryLookups',
    'directoryClaims',
    'claimConflicts',
    'inboundDispatches',
    'retries',
    'unreachableRetries'
];

/**
 * `ops.workflow` summed across every host pod, plus the allowlisted
 * cluster counters under a `cluster/` prefix and the activation gauge.
 *
 * `hostsComplete` is a property of the snapshot: every host pod answered
 * with a workflow section. Anything less makes every sum a lower bound.
 */
export function workflowTotals(kube, namespace) {
    const pods = kube(['-n', namespace, 'get', 'pod', '-l',
        'app.kubernetes.io/component=host',
        '-o', 'jsonpath={.items[*].metadata.name}'], { allowFail: true })
        ?.split(/\s+/).filter(Boolean) ?? [];
    const totals = {};
    let hosts = 0;
    let clusterHosts = 0;
    let activations = 0;
    let activationsHosts = 0;
    /** Queued turns per host — the backlog a new run must not start on. */
    const queued = {};
    for (const pod of pods) {
        const out = kube(['-n', namespace, 'exec', pod, '--', 'node', '-e',
            "fetch('http://127.0.0.1:' + (process.env.PORT || 7311) + '/_sigx/ops', " +
            "{ headers: { authorization: 'Bearer ' + process.env.OPS_SECRET } })" +
            '.then((r) => r.text()).then((t) => console.log(t))'
        ], { allowFail: true });
        if (!out) continue;
        let body;
        try {
            body = JSON.parse(out);
        } catch {
            continue;
        }
        const ops = body?.ops;
        const counters = ops?.cluster?.counters;
        if (counters && !ops.cluster.error) {
            clusterHosts++;
            for (const key of CLUSTER_COUNTERS) {
                const value = counters[key];
                if (typeof value === 'number') {
                    totals[`cluster/${key}`] = (totals[`cluster/${key}`] ?? 0) + value;
                }
            }
        }
        // The live activation count — the top-level `stats` gauge of the
        // ops body (the metrics section carries the same under `gauges`).
        // Absent means "this host could not say", never 0.
        const live = body?.stats?.activations ?? ops?.metrics?.gauges?.activations;
        if (typeof live === 'number') {
            activations += live;
            activationsHosts++;
        }
        const q = body?.stats?.queued ?? ops?.metrics?.gauges?.queued;
        if (typeof q === 'number') queued[pod] = q;
        const section = ops?.workflow;
        if (!section || section.error) continue;
        hosts++;
        for (const [key, value] of Object.entries(section)) {
            if (typeof value === 'number') totals[key] = (totals[key] ?? 0) + value;
        }
    }
    return {
        hosts,
        pods: pods.length,
        totals,
        hostsComplete: pods.length > 0 && hosts === pods.length && clusterHosts === pods.length,
        activations: activationsHosts > 0 ? activations : null,
        queued
    };
}

const sumQueued = (snapshot) => Object.values(snapshot.queued).reduce((a, b) => a + b, 0);

/**
 * A run started on a backlogged fleet measures the backlog. After the first
 * overloaded rung wedged the hosts (#302), every later Job in the same bench
 * run died at its first call and was reported PARTIAL — a label for "a pod
 * died", not for "the cluster was still choking on the previous scenario".
 * So a run waits for the fleet's queued turns to fall under `quietQueued`
 * (bounded by `quietTimeoutMs`) and otherwise refuses with the depth per
 * host in the message.
 */
async function waitForQuietFleet(kube, namespace, { quietQueued, quietTimeoutMs, onLog }) {
    const deadline = Date.now() + quietTimeoutMs;
    let last = workflowTotals(kube, namespace);
    if (sumQueued(last) <= quietQueued) return last;
    onLog(`fleet has ${sumQueued(last)} queued turn(s) — waiting for it to quiet (<= ${quietQueued})`);
    for (;;) {
        await sleep(5000);
        last = workflowTotals(kube, namespace);
        const total = sumQueued(last);
        if (total <= quietQueued) return last;
        if (Date.now() > deadline) {
            throw new Error(
                `[wf-load] fleet backlogged: ${total} queued turn(s) after ${Math.round(quietTimeoutMs / 1000)}s ` +
                    `(${Object.entries(last.queued).map(([p, q]) => `${p}=${q}`).join(', ')}) — ` +
                    'refusing to start a run that would measure the previous one\'s backlog (#302)'
            );
        }
    }
}

const SUMMED = [
    'started', 'startFailures', 'startsDeferred', 'completed', 'failed', 'compensated',
    'cancelled', 'childRuns', 'completedUnreported', 'droppedEvents', 'unknownEvents', 'startFailedButRan',
    'sweepUnpollable', 'signalsSent', 'signalsSkipped', 'signalFailures'
];
/** Engine sums arrive from the aggregator, which every pod reads in full —
 *  so these are taken from ONE pod, not added across pods. */
const FROM_ONE = [
    'transitions', 'timersFired', 'remindersFired', 'wakesFallback', 'wakesLost', 'wakesStale',
    'signalsDelivered', 'signalsBuffered', 'signalsLate', 'signalTimeouts', 'taskAttempts',
    'taskFailures', 'compensations', 'nodeMs', 'wakeLagMs'
];

/**
 * One row per rate rung, summed across the Job's pods. See the header for
 * which fields sum, which come from one pod, and which are recomputed.
 */
export function mergeWfRows(rows, { partial = false } = {}) {
    const byRung = new Map();
    for (const row of rows) {
        const merged = byRung.get(row.rate) ?? {
            rate: row.rate,
            pods: 0,
            arrival: row.arrival,
            mix: row.mix,
            knobs: row.knobs,
            durationMs: 0,
            drainMs: 0,
            stuck: { sleeping: 0, waiting: 0, blocked: 0, running: 0, other: 0, total: 0 },
            byTemplate: {},
            failedByError: {},
            latencyMs: null,
            observedMs: null,
            startMs: null,
            deferredMs: null,
            latencyFromPods: 0,
            errors: { total: 0, byKind: {} }
        };
        merged.pods++;
        for (const key of SUMMED) merged[key] = (merged[key] ?? 0) + (row[key] ?? 0);
        for (const key of FROM_ONE) if (merged[key] === undefined && row[key] !== undefined) merged[key] = row[key];
        for (const key of Object.keys(merged.stuck)) merged.stuck[key] += row.stuck?.[key] ?? 0;
        for (const [reason, count] of Object.entries(row.failedByError ?? {})) {
            merged.failedByError[reason] = (merged.failedByError[reason] ?? 0) + count;
        }
        for (const [template, statuses] of Object.entries(row.byTemplate ?? {})) {
            const t = (merged.byTemplate[template] ??= {});
            for (const [status, count] of Object.entries(statuses)) t[status] = (t[status] ?? 0) + count;
        }
        merged.durationMs = Math.max(merged.durationMs, row.durationMs ?? 0);
        merged.drainMs = Math.max(merged.drainMs, row.drainMs ?? 0);
        if (row.latencyMs && Object.keys(row.latencyMs).length > 0) {
            if (!merged.latencyMs) {
                merged.latencyMs = row.latencyMs;
                merged.observedMs = row.observedMs ?? null;
                merged.startMs = row.startMs ?? null;
                merged.deferredMs = row.deferredMs ?? null;
            }
            merged.latencyFromPods++;
        }
        merged.errors.total += row.errors?.total ?? 0;
        for (const [kind, count] of Object.entries(row.errors?.byKind ?? {})) {
            merged.errors.byKind[kind] = (merged.errors.byKind[kind] ?? 0) + count;
        }
        byRung.set(row.rate, merged);
    }
    return [...byRung.values()]
        .sort((a, b) => a.rate - b.rate)
        .map((row) => {
            const wallS = (row.durationMs + row.drainMs) / 1000;
            return {
                ...row,
                runsStartedPerSec: row.durationMs > 0
                    ? Math.round((row.started / (row.durationMs / 1000)) * 1000) / 1000
                    : 0,
                runsCompletedPerSec: wallS > 0 ? Math.round((row.completed / wallS) * 1000) / 1000 : 0,
                transitionsPerSec: wallS > 0 && typeof row.transitions === 'number'
                    ? Math.round((row.transitions / wallS) * 1000) / 1000
                    : null,
                ...(partial ? { partial: true } : {})
            };
        });
}

/** `helm --set` splits on commas, so a mix or a sweep must arrive escaped. */
const escape = (value) => String(value).replaceAll(',', '\\,');

/**
 * Render, apply, wait (sampling the activation gauge), collect and merge
 * one workflow run.
 *
 * `values` are `loadgen.*` chart values without the prefix: `sweep`,
 * `durationS`, `parallelism`, and the `wf.WF_*` knobs (`wf.WF_START_RATE`
 * or, as a convenience, bare `WF_START_RATE`). The image arrives as
 * `imageRepository`/`imageTag` and nowhere else — an `image.*` key in
 * `values` is refused, for the reason `ws-load.mjs` refuses it.
 */
export async function runWfLoad(options) {
    const {
        context,
        namespace,
        chartDir,
        release = 'sigx',
        imageRepository,
        imageTag,
        workload,
        values = {},
        onLog = () => {},
        sampleIntervalMs = 5000,
        timeoutMs = 3_600_000,
        quietQueued = 50,
        quietTimeoutMs = 600_000
    } = options;

    const smuggled = Object.keys(values).filter((key) => key.startsWith('image.'));
    if (smuggled.length > 0) {
        throw new Error(
            `[wf-load] pass the image as imageRepository/imageTag, not through values ` +
                `(${smuggled.join(', ')}) — two sources for one image is how a run ends up ` +
                'measuring a build nobody named.'
        );
    }

    const kube = (args, opts) => sh('kubectl', ['--context', context, ...args], opts);
    const helm = (args, opts) => sh('helm', ['--kube-context', context, ...args], opts);

    const suffix = `${Date.now()}-${Math.floor(Math.random() * 10000)}`;
    const job = `${release}-loadgen-${suffix}`;
    const sets = [];
    const prefixed = {};
    for (const [key, value] of Object.entries(values)) {
        const full = key.startsWith('loadgen.')
            ? key
            : key.startsWith('WF_')
              ? `loadgen.wf.${key}`
              : `loadgen.${key}`;
        prefixed[full] = value;
    }
    for (const [key, value] of Object.entries({
        'loadgen.enabled': true,
        'loadgen.nameSuffix': suffix,
        'loadgen.mode': 'workflow',
        ...prefixed
    })) {
        sets.push('--set-string', `${key}=${escape(value)}`);
    }

    onLog(`rendering ${job}`);
    const manifest = helm(['template', release, chartDir, '-n', namespace,
        '-s', 'templates/loadgen-job.yaml',
        '--set', `image.repository=${imageRepository}`,
        '--set', `image.tag=${imageTag}`,
        '--set', `nodeSelector.workload=${workload}`,
        ...sets]);

    const before = await waitForQuietFleet(kube, namespace, { quietQueued, quietTimeoutMs, onLog });

    const dir = mkdtempSync(join(tmpdir(), 'sigx-wfload-'));
    try {
        const file = join(dir, 'job.yaml');
        writeFileSync(file, manifest);
        kube(['-n', namespace, 'apply', '-f', file]);
    } finally {
        discard(dir);
    }

    const wanted = Number(kube(['-n', namespace, 'get', 'job', job, '-o',
        'jsonpath={.spec.completions}'], { allowFail: true })) || 1;
    onLog(`waiting for ${job} (${wanted} pod(s))`);

    let peakActivations = null;
    let samples = 0;
    let partial = false;
    const deadline = Date.now() + timeoutMs;
    for (;;) {
        const live = workflowTotals(kube, namespace);
        if (live.hosts > 0) {
            samples++;
            if (typeof live.activations === 'number') {
                peakActivations = Math.max(peakActivations ?? 0, live.activations);
            }
        }
        const state = kube(['-n', namespace, 'get', 'job', job, '-o',
            'jsonpath={.status.succeeded}|{.status.failed}'], { allowFail: true }) ?? '';
        const [succeeded, failed] = state.split('|').map((v) => Number(v) || 0);
        if (succeeded >= wanted || failed > 0) {
            if (failed > 0) {
                onLog(`✗ ${job}: ${failed} pod(s) failed — results are PARTIAL`);
                partial = true;
            }
            break;
        }
        if (Date.now() > deadline) {
            onLog(`✗ ${job} did not complete within the deadline — results are PARTIAL`);
            partial = true;
            break;
        }
        await sleep(sampleIntervalMs);
    }
    const after = workflowTotals(kube, namespace);

    const pods = kube(['-n', namespace, 'get', 'pod', '-l', `job-name=${job}`,
        '-o', 'jsonpath={range .items[*]}{.metadata.name}{"\\n"}{end}'],
        { allowFail: true })?.split('\n').filter(Boolean) ?? [];
    onLog(`collecting logs from ${pods.length} pod(s)`);
    const rows = [];
    for (const pod of pods) {
        const out = kube(['-n', namespace, 'logs', pod, '--tail=-1'], { allowFail: true }) ?? '';
        for (const line of out.split('\n')) {
            const trimmed = line.trim();
            if (!trimmed.startsWith('{')) continue;
            try {
                const row = JSON.parse(trimmed);
                if (row.mode === 'workflow') rows.push(row);
            } catch {
                // a progress line that merely begins with a brace
            }
        }
    }

    // Both ends complete AND the same pods, or no delta at all — the error
    // direction of a partial snapshot depends on which end lost a host, and
    // a pod replaced mid-run (a rollout's surge pod retiring, a restart)
    // takes its counters with it, so the delta comes out short or negative.
    const samePods =
        Object.keys(before.queued).sort().join() === Object.keys(after.queued).sort().join();
    const countersTrustworthy = before.hostsComplete && after.hostsComplete && samePods;
    const delta = {};
    if (countersTrustworthy) {
        for (const [key, value] of Object.entries(after.totals)) {
            delta[key] = value - (before.totals[key] ?? 0);
        }
    }

    return {
        job,
        pods,
        rows,
        merged: mergeWfRows(rows, { partial }),
        hosts: after.hosts,
        peakActivations,
        /** Queued turns per host at the END — a backlog the drain did not clear. */
        queuedAfter: after.queued,
        samples,
        delta,
        countersTrustworthy,
        partial
    };
}
