/**
 * Running one WebSocket connection-scale Job, as a function (#184).
 *
 * This was inside `testenv.mjs`'s `ws-load` verb, which meant the only way
 * to run a ladder was a human reading JSON off a terminal — no baseline, no
 * shape guard, no spread, and no answer to "did that change help?".
 * `benchmarks/src/scenarios/sockets.ts` needs the same orchestration, so it
 * lives here and both callers import it. The precedent is `infra.ts`, which
 * already reaches across trees for `edge-ladder.mjs` and `post-fn.mjs`.
 *
 * Deliberately standalone: it builds its own `kubectl`/`helm` invocations
 * from `spawnable` rather than taking them from `testenv.mjs`, so importing
 * it costs nothing but this file.
 *
 * ## The two numbers this has to get right
 *
 * **`open` is a GAUGE.** It exists only while the connections do, so it is
 * sampled DURING the run and reported as `peakOpen`. Read afterwards it is
 * zero, and the monotonic totals cannot substitute — they count every
 * connection ever opened across every rung, not how many were up at once.
 *
 * **Rows are SUMMED across pods.** So a pod whose logs are missed does not
 * merely lose its own line, it shrinks every total; and a pod that DIED
 * does the same silently. Logs are therefore collected per pod off the
 * `job-name` label (`kubectl logs job/<name>` picks exactly one pod), and a
 * failed pod marks the whole result `partial`.
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

/** Best-effort temp cleanup — a leftover dir beats failing a finished run. */
function discard(dir) {
    try {
        rmSync(dir, { recursive: true, force: true, maxRetries: 10, retryDelay: 200 });
    } catch {
        // Windows holds the directory briefly after the process exits.
    }
}

/**
 * `ops.sockets` AND the cluster watch counters, summed across every host,
 * read from INSIDE the pods.
 *
 * `kubectl exec` rather than a port-forward because the counters are PER
 * HOST and the Service would load-balance a single poll to whichever pod it
 * liked — giving one host's numbers and calling them the fleet's. The
 * bearer secret is already in each pod's environment, so it never has to
 * leave the cluster or land in this process's argv.
 *
 * Two sections, because they answer different questions, and they are
 * gathered DIFFERENTLY.
 *
 * `ops.sockets` is the OUTCOME — connections held, messages delivered, what
 * a client experienced. It is per host, so it is summed across pods.
 *
 * The cluster watch counters are the MECHANISM: `remoteWatches` (cross-host
 * streams a host holds on peers), `coalescedWatches` (attaches that joined
 * an existing one) and `inboundWatches` (what actually crossed). They come
 * from the `cluster` section of `GET {base}` — which is this host's own
 * `placement.report()`, PER HOST, so it is summed across pods exactly like
 * `ops.sockets`.
 *
 * Not from `clusterStats()`: that is a different route (`GET {base}/cluster`)
 * and a different shape (`totals.counters`, already fanned out). Reading a
 * fleet total per pod and summing it would multiply the cluster by the pod
 * count; reading it from one pod would work but pays a full fan-out per
 * poll to learn what the pods are already telling us individually.
 *
 * A pod that does not answer makes the sum a LOWER BOUND, which is why
 * completeness is tracked rather than assumed: a stream count quoted short
 * understates the very thing the run is measuring.
 *
 * The mechanism half was added for #202. Every socket run before it could
 * see a cliff but not what caused one — and #180 was mis-attributed exactly
 * that way: the owner's turn queue was blamed for a shelf that turned out to
 * be the fetch pool (#194), and the stream counts would have said so in one
 * look. They are returned under a `cluster/` prefix so a caller cannot
 * confuse a stream count with a socket count.
 */
export function socketTotals(kube, namespace) {
    const pods = kube(['-n', namespace, 'get', 'pod', '-l',
        'app.kubernetes.io/component=host',
        '-o', 'jsonpath={.items[*].metadata.name}'], { allowFail: true })
        ?.split(/\s+/).filter(Boolean) ?? [];
    const totals = {};
    /** Watch counters, summed per host like the socket ones. */
    const watches = {};
    /** Pods that answered with a cluster section — all of them, or it is short. */
    let clusterHosts = 0;
    /** Pods reporting `tcp` in their transport chain — see the note below. */
    let tcpHosts = 0;
    let hosts = 0;
    for (const pod of pods) {
        const out = kube(['-n', namespace, 'exec', pod, '--', 'node', '-e',
            "fetch('http://127.0.0.1:' + (process.env.PORT || 7311) + '/_sigx/ops', " +
            "{ headers: { authorization: 'Bearer ' + process.env.OPS_SECRET } })" +
            '.then((r) => r.text()).then((t) => console.log(t))'
        ], { allowFail: true });
        if (!out) continue;
        let ops;
        try {
            ops = JSON.parse(out)?.ops;
        } catch {
            continue;
        }
        // The cluster section FIRST, and independently of the socket one —
        // the two are unrelated failures. A pod whose `sockets` section is
        // absent or throwing still has watch counters worth having, and
        // skipping it via the `continue` below would silently cost the run
        // its mechanism numbers.
        // Which transports this host is CONFIGURED with, in chain order.
        // Recorded because a `TRANSPORT=tcp` run that silently measured HTTP
        // is the failure this axis is most exposed to: `tcpTransport` routes
        // to null for a peer advertising no tcp address and the chain falls
        // through to HTTP per link, so a half-rolled-out fleet produces a
        // clean HTTP measurement wearing the tcp label. A host that failed to
        // BIND cannot start at all, so a pod that answers has a listener —
        // the mixed fleet is the case worth catching.
        const transports = ops?.cluster?.transports;
        if (Array.isArray(transports) && transports.includes('tcp')) tcpHosts++;
        const counters = ops?.cluster?.counters;
        if (counters && !ops.cluster.error) {
            clusterHosts++;
            for (const key of WATCH_COUNTERS) {
                const value = counters[key];
                if (typeof value === 'number') {
                    watches[`cluster/${key}`] = (watches[`cluster/${key}`] ?? 0) + value;
                }
            }
        }
        const section = ops?.sockets;
        // A missing section, or an `{ error }` from a throwing provider,
        // must not be added as zeros — that reads as "this host served
        // nothing", which is a different claim.
        if (!section || section.error) continue;
        hosts++;
        for (const [key, value] of Object.entries(section)) {
            if (typeof value === 'number') totals[key] = (totals[key] ?? 0) + value;
        }
    }
    // `watchesComplete` is reported ALONGSIDE the totals rather than as a
    // pseudo-counter inside them: it is a property of the snapshot, and a
    // caller subtracting two snapshots must be able to see it on both.
    // Complete means EVERY pod answered — anything less and the sum is
    // short by however much the silent hosts were holding.
    const watchesComplete = pods.length > 0 && clusterHosts === pods.length;
    return { hosts, totals: { ...totals, ...watches }, watchesComplete, tcpHosts };
}

/**
 * The mechanism counters worth reporting — an allowlist, not everything
 * under `totals.counters`. The section carries dozens of fields (directory
 * ops, locates, retries); pulling them all in would bury the three that
 * describe watch fan-out and quietly grow the recorded artifact every time
 * the runtime gains a counter.
 *
 * `transportFallbacks` rides along (#223): links that reached their peer
 * over a LATER transport in the chain than the preferred one. `tcpHosts`
 * proves the tcp chain is installed; this proves no link actually fell
 * back under load — the stronger claim, and the one `transportGate` below
 * turns into an exit code.
 */
const WATCH_COUNTERS = [
    'remoteWatches',
    'coalescedWatches',
    'inboundWatches',
    'transportFallbacks'
];

/**
 * One knob's value off an `INFRA_SHAPE` string — `TRANSPORT` from
 * `ws … knobs=ENABLE_SOCKET=1,TRANSPORT=tcp`. The shape is otherwise never
 * parsed, only matched (`benchmarks/src/shape.mjs`); this reads a single
 * name out of the knobs block, matched whole so a knob whose value or
 * suffix mentions the name cannot stand in for it.
 *
 * @param {string | undefined} shape
 * @param {string} name
 * @returns {string | undefined}
 */
function shapeKnob(shape, name) {
    const at = (shape ?? '').indexOf(' knobs=');
    if (at < 0) return undefined;
    for (const kv of shape.slice(at + ' knobs='.length).split(',')) {
        const eq = kv.indexOf('=');
        if (eq > 0 && kv.slice(0, eq) === name) return kv.slice(eq + 1);
    }
    return undefined;
}

/**
 * The TCP gate (#223): the runbook's rule that a `TRANSPORT=tcp` measurement
 * on which any link fell through to HTTP is void, as one decision.
 *
 * `tcpTransport` routes to null for a peer advertising no tcp address and
 * the chain falls back PER LINK, so a fleet mid-rollout produces a clean
 * HTTP number wearing the tcp label — and `INFRA_SHAPE` would compare it as
 * tcp. `protocolBreaches` already fails the run for the analogous silent
 * invalidation; this does the same for `cluster/transportFallbacks`.
 *
 * Returns `null` when the run stands: a shape that is not tcp (over HTTP
 * there is nothing to fall back FROM), or a tcp run with zero fallbacks.
 * `valid: false` voids the run. `valid: true` with a message is a tcp run
 * whose count is MISSING — `cluster/*` keys are omitted when a stats
 * fan-out missed a host at either end — which is reported rather than
 * silently passed, but not failed: the absence is a snapshot failure, not
 * evidence of a fallback.
 *
 * @param {string | undefined} shape the live `INFRA_SHAPE`, read AFTER the run
 * @param {Record<string, number>} delta `WsLoadResult.delta`
 * @returns {{ valid: boolean, message: string } | null}
 */
export function transportGate(shape, delta) {
    if (shapeKnob(shape, 'TRANSPORT') !== 'tcp') return null;
    const fallbacks = delta['cluster/transportFallbacks'];
    if (typeof fallbacks !== 'number') {
        return {
            valid: true,
            message:
                'transport fallbacks unknown — a cluster-stats fan-out missed a host, ' +
                'so the tcp gate went unchecked; verify tcpHosts equals hosts by hand'
        };
    }
    if (fallbacks > 0) {
        return {
            valid: false,
            message:
                `${fallbacks} transport fallback${fallbacks === 1 ? '' : 's'} — a link ` +
                'fell through to HTTP on a TRANSPORT=tcp run; the run is not valid'
        };
    }
    return null;
}

/**
 * One row per rung, summed across the Job's pods.
 *
 * COUNTS sum; PERCENTILES DO NOT. Merging p99s across pods produces a
 * number that is the p99 of nothing, so latency is taken from the single
 * publishing pod (the only one carrying probes) and labelled with
 * `latencyFromPods` so a reader can see it does not describe the rest.
 *
 * Rates are not summed either: each pod divided by its own window and the
 * pods do not start together, so the merged rate is recomputed from summed
 * deliveries over the longest window.
 */
export function mergeRows(rows, { partial = false } = {}) {
    const byRung = new Map();
    for (const row of rows) {
        const merged = byRung.get(row.n) ?? {
            n: row.n,
            pods: 0,
            mode: row.mode,
            read: row.read,
            principal: row.principal,
            connected: 0,
            socketsExpected: 0,
            connectFailures: 0,
            deliveries: 0,
            durationMs: 0,
            publishes: 0,
            publishFailures: 0,
            subscriptionErrors: 0,
            drops: 0,
            seqMismatches: 0,
            slowConnections: 0,
            maxBufferedBytes: 0,
            clientRssMb: 0,
            latencyMs: null,
            latencyFromPods: 0
        };
        merged.pods++;
        for (const key of ['connected', 'socketsExpected', 'connectFailures', 'deliveries',
            'publishes', 'publishFailures', 'subscriptionErrors', 'drops', 'seqMismatches',
            'slowConnections']) {
            merged[key] += row[key] ?? 0;
        }
        merged.durationMs = Math.max(merged.durationMs, row.durationMs ?? 0);
        merged.maxBufferedBytes = Math.max(merged.maxBufferedBytes, row.maxBufferedBytes ?? 0);
        merged.clientRssMb = Math.max(merged.clientRssMb, row.clientRssMb ?? 0);
        if (row.latencyMs) {
            merged.latencyMs = row.latencyMs;
            merged.latencyFromPods++;
        }
        // The ladder verdict (#222) survives the merge, or a truncated
        // ladder is indistinguishable from a completed one in the artifact.
        // Stamped only when a pod said so — mirroring `partial` — because a
        // key that is always present reads as a claim about every run.
        if (row.retried) merged.retried = true;
        if (row.ladderStopped !== undefined && merged.ladderStopped === undefined) {
            merged.ladderStopped = row.ladderStopped;
        }
        byRung.set(row.n, merged);
    }
    return [...byRung.values()]
        .sort((a, b) => a.n - b.n)
        .map((row) => ({
            ...row,
            // `n` is PER POD, so the number a reader wants is this one.
            totalConnections: row.connected,
            deliveriesPerSec: row.durationMs > 0
                ? Math.round((row.deliveries / (row.durationMs / 1000)) * 1000) / 1000
                : 0,
            deliveriesPerPublish: row.publishes > 0
                ? Math.round((row.deliveries / row.publishes) * 1000) / 1000
                : 0,
            ...(partial ? { partial: true } : {})
        }));
}

/** `helm --set` splits on commas, so a ladder must arrive escaped. */
const escape = (value) => String(value).replaceAll(',', '\\,');

/**
 * Render, apply, wait (sampling the gauge), collect and merge one run.
 *
 * `values` are `wsLoadgen.*` chart values WITHOUT the prefix. The IMAGE is
 * not among them: it arrives as `imageRepository`/`imageTag` and nowhere
 * else, and an `image.*` key in `values` is refused rather than merged.
 * Reusing the image an earlier `up` built is the normal case — the git SHA
 * moves on every merge while the deployed image does not — so the caller
 * pins the tag, but it pins it in one place.
 */
export async function runWsLoad(options) {
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
        timeoutMs = 3_600_000
    } = options;

    // The image is chosen in EXACTLY ONE place. `values` is forwarded after
    // the explicit `--set image.*`, so an `image.tag` smuggled in there
    // would win — and the run would measure a different build than the one
    // the caller (and the recorded shape) name. Refuse rather than pick a
    // winner: silently measuring the wrong image is the failure mode this
    // whole harness exists to prevent.
    const smuggled = Object.keys(values).filter((key) => key.startsWith('image.'));
    if (smuggled.length > 0) {
        throw new Error(
            `[ws-load] pass the image as imageRepository/imageTag, not through values ` +
                `(${smuggled.join(', ')}) — two sources for one image is how a run ends up ` +
                'measuring a build nobody named.'
        );
    }

    const kube = (args, opts) => sh('kubectl', ['--context', context, ...args], opts);
    const helm = (args, opts) => sh('helm', ['--kube-context', context, ...args], opts);

    const enabled = kube(['-n', namespace, 'get', 'deploy', `${release}-host`, '-o',
        'jsonpath={.spec.template.spec.containers[0].env[?(@.name=="ENABLE_SOCKET")].value}'],
        { allowFail: true });
    if (enabled !== '1') {
        throw new Error('the host has no socket endpoint — run `testenv.mjs ws-up` first');
    }

    const suffix = `${Date.now()}-${Math.floor(Math.random() * 10000)}`;
    const job = `${release}-wsloadgen-${suffix}`;
    const sets = [];
    for (const [key, value] of Object.entries({
        'wsLoadgen.enabled': true,
        'wsLoadgen.nameSuffix': suffix,
        ...Object.fromEntries(Object.entries(values).map(([k, v]) =>
            [k.startsWith('wsLoadgen.') ? k : `wsLoadgen.${k}`, v]))
    })) {
        sets.push('--set-string', `${key}=${escape(value)}`);
    }

    onLog(`rendering ${job}`);
    const manifest = helm(['template', release, chartDir, '-n', namespace,
        '-s', 'templates/wsloadgen-job.yaml',
        '--set', `image.repository=${imageRepository}`,
        '--set', `image.tag=${imageTag}`,
        '--set', `nodeSelector.workload=${workload}`,
        ...sets]);

    // BEFORE the apply. Taken after it, the first rung is already opening
    // and the delta comes out with more closes than opens — which reads as
    // lost connections rather than as a racing baseline.
    const before = socketTotals(kube, namespace);

    const dir = mkdtempSync(join(tmpdir(), 'sigx-wsload-'));
    try {
        const file = join(dir, 'job.yaml');
        writeFileSync(file, manifest);
        kube(['-n', namespace, 'apply', '-f', file]);
    } finally {
        discard(dir);
    }

    // Read the completion count back off the Job rather than trusting the
    // values: `parallelism` may have come from a value, a caller, or the
    // chart default, and the wait has to know when it is done.
    const wanted = Number(kube(['-n', namespace, 'get', 'job', job, '-o',
        'jsonpath={.spec.completions}'], { allowFail: true })) || 1;
    onLog(`waiting for ${job} (${wanted} pod(s))`);

    let peakOpen = 0;
    let peakSubscriptions = 0;
    // The HOST-side send buffer (#252/#208). `null` until some host reports
    // one, and it stays null when no adapter can — a 0 would be the exact
    // claim #208 was filed about ("the hosts never outran the clients"),
    // asserted from an absence of data.
    let peakBufferedBytes = null;
    let samples = 0;
    let partial = false;
    const deadline = Date.now() + timeoutMs;
    for (;;) {
        const live = socketTotals(kube, namespace);
        if (live.hosts > 0) {
            samples++;
            peakOpen = Math.max(peakOpen, live.totals.open ?? 0);
            peakSubscriptions = Math.max(peakSubscriptions, live.totals.subscriptions ?? 0);
            // Absent means no host could answer, which must not become a 0.
            if (typeof live.totals.bufferedBytes === 'number') {
                peakBufferedBytes = Math.max(peakBufferedBytes ?? 0, live.totals.bufferedBytes);
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
    const after = socketTotals(kube, namespace);

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
                rows.push(JSON.parse(trimmed));
            } catch {
                // a progress line that merely begins with a brace
            }
        }
    }

    // A delta is only as trustworthy as BOTH ends of it. `clusterStats` sets
    // `partial` when the polling host could not reach a member, and the two
    // failures point OPPOSITE WAYS: a partial baseline understates `before`
    // and so OVERSTATES the delta, while a partial end snapshot understates
    // it. Neither is a "lower bound" — the error direction depends on which
    // end lost a host, and the magnitude on how many.
    //
    // So the watch counters are reported only when both snapshots saw the
    // whole fleet, and omitted entirely otherwise. A number whose error
    // direction is unknown is worse than no number in a rig whose output
    // gets pasted into BASELINES.md as fact.
    const watchesTrustworthy = before.watchesComplete && after.watchesComplete;
    const delta = {};
    for (const [key, value] of Object.entries(after.totals)) {
        // Gauges describe a moment, not an interval — diffing them is
        // meaningless. `bufferedBytes` (#252) is one of them, and a
        // particularly misleading one to diff: both ends of a healthy run
        // are 0, so the delta is 0 and reads as "no backpressure" even if
        // the middle of the run was drowning. It is reported from the
        // in-run samples as `peakBufferedBytes` instead.
        if (
            key === 'open' ||
            key === 'inFlight' ||
            key === 'subscriptions' ||
            key === 'bufferedBytes'
        ) {
            continue;
        }
        if (key.startsWith('cluster/') && !watchesTrustworthy) continue;
        delta[key] = value - (before.totals[key] ?? 0);
    }

    return {
        job,
        pods,
        rows,
        merged: mergeRows(rows, { partial }),
        hosts: after.hosts,
        peakOpen,
        peakSubscriptions,
        // `null` = no host could report its send buffer for the whole run
        // (an adapter without the `bufferedBytes` seam). NOT zero.
        peakBufferedBytes,
        samples,
        delta,
        // Distinct from `partial` above, which is about the LOAD JOB's pods.
        // False means a cluster-stats fan-out missed a host at one end or
        // the other, so no `cluster/*` key is present in `delta` at all.
        watchesTrustworthy,
        // Hosts configured with the tcp transport at the END of the run. A
        // `TRANSPORT=tcp` run where this is not every host measured a fleet
        // that was still partly on HTTP.
        tcpHosts: after.tcpHosts,
        partial
    };
}
