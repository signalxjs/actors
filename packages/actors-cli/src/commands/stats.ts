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
import { count, durationMs, percent, uptime } from '../model/format';
import type { MonitorSnapshot } from '../source/types';

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

/** Pure, so it is testable without a terminal or a running silo. */
export function renderStats(snapshot: MonitorSnapshot, label: string): string[] {
    const lines: string[] = [`${label}  ${new Date(snapshot.at).toISOString()}`];

    if (snapshot.partial) {
        // First, and stated as a caveat on the numbers rather than a footnote:
        // every total below is a lower bound, and they still look plausible.
        lines.push(
            '',
            '! PARTIAL — a silo did not answer, so every total below is a LOWER BOUND'
        );
    }

    const cluster = snapshot.cluster;
    if (cluster) {
        lines.push(
            '',
            'cluster',
            `  silos        ${cluster.totals.silos} (${cluster.view.active} active of ${cluster.view.size}, view #${cluster.view.version})`,
            `  activations  ${count(cluster.totals.activations)}`,
            `  queued       ${count(cluster.totals.queued)}`
        );
        if (cluster.unreachable.length > 0) {
            lines.push(`  unreachable  ${cluster.unreachable.length}`);
            for (const failure of cluster.unreachable) {
                lines.push(`    ${failure.siloId} ${failure.address} — ${failure.reason}`);
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

    lines.push('', 'silos');
    for (const silo of snapshot.silos) {
        const marker = silo.status === 'active' || silo.status === 'unknown' ? ' ' : '!';
        lines.push(
            `  ${marker} ${silo.siloId}  ${silo.status}  up ${uptime(silo.uptimeMs)}  ` +
                `${count(silo.stats.activations)} act  ${count(silo.stats.queued)} queued` +
                (silo.membershipVersion !== null ? `  view #${silo.membershipVersion}` : '')
        );
        const { activating, deactivating } = silo.stats.transitional;
        if (activating > 0 || deactivating > 0) {
            lines.push(`      in flight: ${activating} activating, ${deactivating} deactivating`);
        }
    }

    const metrics = snapshot.metrics;
    if (metrics) {
        lines.push(
            '',
            'calls',
            `  total        ${count(metrics.calls.total)} (${count(metrics.calls.failed)} failed, ${percent(metrics.calls.failed, metrics.calls.total)})`,
            `  streams      ${count(metrics.calls.streams)}`
        );
        if (metrics.latencyMs) {
            lines.push(
                `  latency      p50 ${durationMs(metrics.latencyMs.p50Ms)}  p90 ${durationMs(metrics.latencyMs.p90Ms)}  p99 ${durationMs(metrics.latencyMs.p99Ms)}`
            );
        }
        // Side by side, because the comparison is the whole point: a high
        // queue means a hot grain, a high turn means a slow method.
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
                'errors',
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
                'slowest methods (p99 turn)',
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
            'hottest grains',
            ...snapshot.activations
                .slice(0, 10)
                .map(
                    (grain) =>
                        `  ${`${grain.type}/${grain.key}`.padEnd(36)} ${String(grain.queued).padStart(5)} queued  ` +
                        `age ${uptime(grain.ageMs)}  idle ${uptime(grain.idleMs)}`
                )
        );
    }

    return lines;
}
