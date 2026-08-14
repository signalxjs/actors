/**
 * `sigx actors stats` — one snapshot, printed and gone.
 *
 * The non-interactive half of the tool, and the one that has to work where
 * the dashboard cannot: in CI, over ssh, piped into `jq`. It shares the
 * whole data layer with `top`, so if this prints the right numbers the
 * dashboard is rendering the right numbers.
 */
import type { ActorsCommandContext } from './context';
import { out, outJson } from './out';
import { resolveSource } from '../resolve';
import { count, durationMs, percent, uptime } from '@sigx/actors-monitor/format';
import type { MonitorSnapshot } from '@sigx/actors-monitor';

export async function runStats(ctx: ActorsCommandContext): Promise<void> {
    const source = await resolveSource(ctx.cwd, ctx.args);
    try {
        const snapshot = await source.snapshot();
        if (ctx.args.json) {
            // The whole normalized snapshot, not a summary: the point of
            // --json is that something else does the summarising — so it
            // goes to stdout unprefixed, or it is not parseable.
            outJson(snapshot);
            return;
        }
        for (const line of renderStats(snapshot, source.label)) out(line);
    } finally {
        await source.close();
    }
}

/** Pure, so it is testable without a terminal or a running host. */
export function renderStats(snapshot: MonitorSnapshot, label: string): string[] {
    const lines: string[] = [`${label}  ${new Date(snapshot.at).toISOString()}`];

    if (snapshot.partial) {
        // First, and stated as a caveat on the numbers rather than a footnote:
        // every total below is a lower bound, and they still look plausible.
        lines.push(
            '',
            '! PARTIAL — a host did not answer, so every total below is a LOWER BOUND'
        );
    }

    const cluster = snapshot.cluster;
    if (cluster) {
        lines.push(
            '',
            'cluster',
            `  hosts        ${cluster.totals.hosts} (${cluster.view.active} active of ${cluster.view.size}, view #${cluster.view.version})`,
            `  activations  ${count(cluster.totals.activations)}`,
            `  queued       ${count(cluster.totals.queued)}`
        );
        if (cluster.unreachable.length > 0) {
            lines.push(`  unreachable  ${cluster.unreachable.length}`);
            for (const failure of cluster.unreachable) {
                lines.push(`    ${failure.hostId} ${failure.address} — ${failure.reason}`);
            }
        }
        const empty = Object.entries(cluster.reminderShards).filter(([, s]) => s.length === 0);
        const split = Object.entries(cluster.reminderShards).filter(([, s]) => s.length > 1);
        if (empty.length > 0) {
            // Nothing is ticking those shards — reminders on them are simply
            // not firing, which is otherwise completely invisible.
            lines.push(`  ! ${empty.length} reminder shard(s) unclaimed: ${empty.map(([k]) => k).join(' ')}`);
        }
        if (split.length > 0) {
            lines.push(`  ! ${split.length} reminder shard(s) claimed twice — views have diverged`);
        }
    }

    lines.push('', 'hosts');
    for (const host of snapshot.hosts) {
        const marker = host.status === 'active' || host.status === 'unknown' ? ' ' : '!';
        lines.push(
            `  ${marker} ${host.hostId}  ${host.status}  up ${uptime(host.uptimeMs)}  ` +
                `${count(host.stats.activations)} act  ${count(host.stats.queued)} queued` +
                (host.membershipVersion !== null ? `  view #${host.membershipVersion}` : '')
        );
        const { activating, deactivating } = host.stats.transitional;
        if (activating > 0 || deactivating > 0) {
            lines.push(`      in flight: ${activating} activating, ${deactivating} deactivating`);
        }
    }

    // Cluster-wide when the fan-out produced it, the polled host's
    // otherwise — and the heading says which. Printing one under a heading
    // that means the other is the complaint this milestone exists to fix:
    // `calls total 382` directly under `cluster hosts 2` reads as
    // cluster-wide when it was one host's.
    const clusterMetrics = snapshot.cluster?.totals.metrics ?? null;
    const scope = clusterMetrics
        ? `cluster-wide (${clusterMetrics.hosts} of ${cluster?.totals.hosts ?? '?'} hosts reporting)`
        : `host ${cluster?.from ?? snapshot.hosts[0]?.hostId ?? 'local'} ONLY`;
    const metrics = snapshot.metrics;
    if (clusterMetrics) {
        const failed = clusterMetrics.calls.failed;
        lines.push(
            '',
            `calls — ${scope}`,
            `  total        ${count(clusterMetrics.calls.total)} (${count(failed)} failed, ${percent(failed, clusterMetrics.calls.total)})`,
            `  streams      ${count(clusterMetrics.calls.streams)}`
        );
        // One-way failures have no caller to throw to — when any happened,
        // this row is the only place an operator sees them.
        if (clusterMetrics.calls.oneWayFailures > 0) {
            lines.push(`  one-way fail ${count(clusterMetrics.calls.oneWayFailures)}`);
        }
        for (const [label, snap] of [
            ['latency', clusterMetrics.latencyMs],
            ['queue', clusterMetrics.queueMs],
            ['turn', clusterMetrics.turnMs]
        ] as const) {
            if (!snap) continue;
            // Re-derived from the summed buckets, not an average of the
            // hosts' percentiles — which would be a figure describing no
            // call that ever happened.
            lines.push(
                `  ${label.padEnd(12)} p50 ${durationMs(snap.p50Ms)}  p90 ${durationMs(snap.p90Ms)}  p99 ${durationMs(snap.p99Ms)}`
            );
        }
        if (clusterMetrics.hosts < (cluster?.totals.hosts ?? 0)) {
            lines.push(
                `  ! ${clusterMetrics.hosts} of ${cluster?.totals.hosts} hosts reported metrics — the figures above are a LOWER BOUND`
            );
        }
        const kinds = Object.entries(clusterMetrics.errors.byKind);
        if (kinds.length > 0) {
            lines.push(
                '',
                `errors — ${scope}`,
                ...kinds
                    .sort((x, y) => (y[1] ?? 0) - (x[1] ?? 0))
                    .map(([kind, n]) => `  ${kind.padEnd(18)} ${count(n ?? 0)}`)
            );
        }
    } else if (metrics) {
        lines.push(
            '',
            `calls — ${scope}`,
            `  total        ${count(metrics.calls.total)} (${count(metrics.calls.failed)} failed, ${percent(metrics.calls.failed, metrics.calls.total)})`,
            `  streams      ${count(metrics.calls.streams)}`
        );
        if ((metrics.calls.oneWayFailures ?? 0) > 0) {
            lines.push(`  one-way fail ${count(metrics.calls.oneWayFailures ?? 0)}`);
        }
        if (metrics.latencyMs) {
            lines.push(
                `  latency      p50 ${durationMs(metrics.latencyMs.p50Ms)}  p90 ${durationMs(metrics.latencyMs.p90Ms)}  p99 ${durationMs(metrics.latencyMs.p99Ms)}`
            );
        }
        // Side by side, because the comparison is the whole point: a high
        // queue means a hot actor, a high turn means a slow method.
        if (metrics.queueMs && metrics.turnMs) {
            lines.push(
                `  queue        p50 ${durationMs(metrics.queueMs.p50Ms)}  p90 ${durationMs(metrics.queueMs.p90Ms)}  p99 ${durationMs(metrics.queueMs.p99Ms)}`,
                `  turn         p50 ${durationMs(metrics.turnMs.p50Ms)}  p90 ${durationMs(metrics.turnMs.p90Ms)}  p99 ${durationMs(metrics.turnMs.p99Ms)}`
            );
        }
        const kinds = Object.entries(metrics.errors.byKind);
        if (kinds.length > 0) {
            lines.push(
                '',
                `errors — ${scope}`,
                ...kinds
                    .sort((x, y) => (y[1] ?? 0) - (x[1] ?? 0))
                    .map(([kind, n]) => `  ${kind.padEnd(18)} ${count(n ?? 0)}`)
            );
        }
        const slowest = Object.entries(metrics.byMethod)
            .filter(([, m]) => m.turnMs !== null && m.turnMs.count > 0)
            .sort((x, y) => (y[1].turnMs?.p99Ms ?? 0) - (x[1].turnMs?.p99Ms ?? 0))
            .slice(0, 5);
        if (slowest.length > 0) {
            lines.push(
                '',
                `slowest methods (p99 turn) — host ${cluster?.from ?? 'local'}`,
                ...slowest.map(
                    ([name, m]) =>
                        `  ${name.padEnd(28)} ${durationMs(m.turnMs!.p99Ms).padStart(8)}  ${count(m.calls)} calls`
                )
            );
        }
    } else {
        lines.push(
            '',
            'no metrics — add .use(metrics()) to the app to see calls, latency and errors'
        );
    }

    if (snapshot.activations && snapshot.activations.length > 0) {
        lines.push(
            '',
            `hottest actors — host ${cluster?.from ?? 'local'}`,
            ...snapshot.activations
                .slice(0, 10)
                .map(
                    (actor) =>
                        `  ${`${actor.type}/${actor.key}`.padEnd(36)} ${String(actor.queued).padStart(5)} queued  ` +
                        `age ${uptime(actor.ageMs)}  idle ${uptime(actor.idleMs)}`
                )
        );
    }

    return lines;
}
