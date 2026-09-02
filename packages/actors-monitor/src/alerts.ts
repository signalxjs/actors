/**
 * Everything currently wrong, worst first — and what the numbers are ABOUT.
 *
 * Both halves are here for the same reason. A dashboard's job is not to show
 * everything, it is to make the thing that is wrong impossible to miss; and
 * the failure that made this material worth writing was never a wrong number,
 * it was a right number under no label at all, sitting directly beneath one of
 * a different scope. Neither decision should be taken twice, once per
 * renderer, and quietly differently.
 *
 * The tone is a severity, not a colour: `@sigx/terminal` theme tokens and CSS
 * custom properties are different vocabularies, and this module belongs to
 * neither.
 */
import { count } from './format';
import { splitShards, unclaimedShards } from './shards';
import type { DashboardView } from './state';
import type { HostView, MonitorSnapshot } from './types';

/** How bad. `danger` is "something is broken now"; `warn` is "read this". */
export type AlertTone = 'danger' | 'warn';

export interface Alert {
    text: string;
    tone: AlertTone;
}

/**
 * The alert banner, worst first.
 *
 * Callers have to BUDGET for these as well as draw them — a terminal counts
 * the lines against its pane, a browser lets them push the table down — so
 * this returns the list rather than rendering it.
 */
export function alertLines(view: DashboardView): Alert[] {
    const { snapshot, error, paused } = view;
    const lines: Alert[] = [];

    if (error) {
        // Two different situations, and the wrong caption for either is the
        // same class of error this module exists to prevent. With a snapshot,
        // the numbers below are real but STALE and must not read as current.
        // Without one — a first poll that never landed — there is nothing
        // below at all, and "showing the last good snapshot" would be
        // describing a screen that does not exist (#256).
        lines.push({
            text: snapshot
                ? `poll failed — showing the last good snapshot: ${error}`
                : `poll failed — nothing has been read yet: ${error}`,
            tone: 'danger'
        });
    }
    if (paused) lines.push({ text: 'PAUSED — press p to resume', tone: 'warn' });
    if (snapshot?.partial) {
        lines.push({
            text: 'PARTIAL — a host did not answer, so every total is a LOWER BOUND',
            tone: 'warn'
        });
    }
    const shards = snapshot?.cluster?.reminderShards;
    if (shards) {
        const empty = unclaimedShards(shards);
        const split = splitShards(shards);
        if (empty.length > 0) {
            lines.push({
                text: `${empty.length} reminder shard(s) UNCLAIMED (${empty.join(' ')}) — nothing is ticking them`,
                tone: 'danger'
            });
        }
        if (split.length > 0) {
            lines.push({
                text: `${split.length} reminder shard(s) claimed twice — membership views have diverged`,
                tone: 'warn'
            });
        }
    }
    const fenced = snapshot?.hosts.filter((s) => s.status === 'fenced') ?? [];
    if (fenced.length > 0) {
        // The one a load balancer cannot see: a fenced host still publishes
        // `active` while refusing every activation.
        lines.push({
            text: `${fenced.length} host(s) FENCED — refusing activations while still published as active`,
            tone: 'danger'
        });
    }
    const auth = totalCounter(snapshot?.hosts ?? [], 'authFailures');
    if (auth > 0) {
        lines.push({
            text: `${count(auth)} cluster auth failure(s) — a secret rotation has not reached every host`,
            tone: 'danger'
        });
    }
    return lines;
}

/**
 * What the numbers on a panel are ABOUT — cluster-wide, or just this host.
 *
 * Stated on every panel that shows any. `cluster · 3 host(s) / 1 node(s)`:
 * the host count is `snapshot.hosts.length`, which is the same figure as
 * the wire's `cluster.totals.hosts` (the collector sets one from the other,
 * and both sources map `report.hosts` one-to-one) — counted from the list
 * here so the hosts and the nodes they span come from ONE set of rows. A
 * source that filtered `hosts` would have to filter the total too.
 */
export function scopeOf(snapshot: MonitorSnapshot): string {
    const cluster = snapshot.cluster;
    if (!cluster) return 'this host';
    return `cluster · ${hostSpread(snapshot.hosts)}`;
}

/**
 * How many distinct machines the hosts run on, from `meta.node` — or null
 * when no host reports one.
 *
 * Null rather than 0, and null rather than "one per host": a fleet outside
 * Kubernetes, or on a chart that predates the field, has said nothing about
 * where it runs, and a dashboard that guessed either way would be claiming a
 * layout nobody measured. Hosts that report no node do not count as a node.
 */
export function nodeCount(hosts: readonly HostView[]): number | null {
    const nodes = new Set<string>();
    for (const host of hosts) {
        const node = host.meta?.node;
        if (node) nodes.add(node);
    }
    return nodes.size > 0 ? nodes.size : null;
}

/**
 * `3 host(s) / 1 node(s)` — the packing summary, or just `3 host(s)` when
 * no host reports a node.
 *
 * The whole reason `meta.node` reaches a dashboard (#51): three replicas
 * on one node capped a real cluster's public endpoint while three other
 * nodes idled, and every replica readout said `3/3`. This is the one-glance
 * version of that finding, and it is derived HERE so the terminal and the
 * browser cannot count nodes differently.
 */
export function hostSpread(hosts: readonly HostView[]): string {
    const nodes = nodeCount(hosts);
    const base = `${hosts.length} host(s)`;
    return nodes === null ? base : `${base} / ${nodes} node(s)`;
}

/**
 * A short label per node the hosts run on — full `meta.node` → the part
 * that tells it from the others. Empty when no host reports a node.
 *
 * Node names are long and differ only in their tail (AKS mints
 * `aks-<pool>-<8 digits>-vmss00000N`, GKE `gke-<cluster>-<pool>-<hash>-<id>`),
 * and both renderers truncate a table cell from the RIGHT — so two hosts on
 * two different nodes read `aks-sigxacto…` twice: a spread fleet posing as
 * a packed one, the inverse of the finding the column exists for (#51), on
 * the one screen with no `nodes` figure beside it. The label drops the
 * prefix every reported node shares, cut at a separator (`-`, `.`, `_`) so
 * what remains is a whole segment, and marks the cut with a leading `…`:
 * `…vmss000000` / `…vmss000001`. Where the names diverge early — two pools
 * — the distinguishing segment leads, and survives right-truncation for
 * the same reason.
 *
 * A lone node shares nothing with anyone and keeps its full name: the same
 * name repeated down the column IS the packed-fleet finding. So does a set
 * whose shared prefix has no separator (`node1` / `node2`), rather than
 * labelling them `…1` / `…2`. The full name stays on the drill-down and in
 * the tooltip. Derived here, beside `nodeCount`, so the terminal and the
 * browser cannot abbreviate the same fleet differently.
 */
export function nodeLabels(hosts: readonly HostView[]): ReadonlyMap<string, string> {
    const nodes = new Set<string>();
    for (const host of hosts) {
        const node = host.meta?.node;
        if (node) nodes.add(node);
    }
    const names = [...nodes];
    const cut = sharedPrefixCut(names);
    const labels = new Map<string, string>();
    for (const name of names) labels.set(name, cut > 0 ? `…${name.slice(cut)}` : name);
    return labels;
}

/**
 * Length of the longest separator-terminated prefix every name shares, or
 * 0 when there is nothing worth cutting: fewer than two names, no separator
 * inside the shared run, or a name that IS the prefix (which would label as
 * a bare `…`).
 */
function sharedPrefixCut(names: readonly string[]): number {
    if (names.length < 2) return 0;
    let common = names[0]!;
    for (const name of names) {
        let i = 0;
        while (i < common.length && i < name.length && common[i] === name[i]) i++;
        common = common.slice(0, i);
    }
    const cut = Math.max(common.lastIndexOf('-'), common.lastIndexOf('.'), common.lastIndexOf('_')) + 1;
    return cut > 0 && names.every((name) => name.length > cut) ? cut : 0;
}

/** The host whose own numbers these are — the one being polled. */
export function polledLabel(view: DashboardView): string {
    const from = view.snapshot?.cluster?.from;
    return from ? `host ${from}` : 'this host';
}

/**
 * Why cluster-wide totals might be a lower bound, or null when they are not.
 *
 * A host with no `metrics()`, or one mid-rolling-deploy on an older build,
 * contributes nothing — and totals covering two thirds of the fleet look
 * exactly like totals covering all of it.
 */
export function coverageNote(snapshot: MonitorSnapshot): string | null {
    const totals = snapshot.cluster?.totals;
    if (!totals) return null;
    const metrics = totals.metrics;
    if (!metrics) {
        return snapshot.metrics
            ? 'calls and latency below are THIS host only — no host reported cluster metrics'
            : null;
    }
    if (metrics.hosts >= totals.hosts) return null;
    return `metrics from ${metrics.hosts} of ${totals.hosts} hosts — every figure below is a LOWER BOUND`;
}

/**
 * Severity for a host's membership status.
 *
 * `null` is "nothing to say" — `active` is the healthy case, and `unknown`
 * is a single-node host with no membership to report one, not a problem.
 * `fenced` and `leaving` must never look like `active`.
 */
export function hostTone(status: string): AlertTone | 'dim' | null {
    if (status === 'active' || status === 'unknown') return null;
    if (status === 'fenced') return 'danger';
    if (status === 'leaving') return 'warn';
    return 'dim';
}

function totalCounter(hosts: readonly HostView[], key: 'authFailures'): number {
    let total = 0;
    for (const host of hosts) total += host.counters?.[key] ?? 0;
    return total;
}
