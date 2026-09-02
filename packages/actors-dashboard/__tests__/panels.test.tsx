/** @jsxImportSource @sigx/runtime-core */
/**
 * The panels, actually RENDERED.
 *
 * Against the SAME fixture the terminal suite uses
 * (`packages/actors-monitor/__tests__/fixture.ts`), which is the point: the
 * claim this layering makes is that a terminal and a browser over one data
 * layer cannot disagree about what the cluster is doing, and two fixtures
 * could not detect it if they did.
 *
 * Assertions are on the TEXT, not on the styling. Every colour here is a CSS
 * custom property resolved at paint time; pinning one would fail the first
 * time somebody retunes the palette without changing a single fact on screen.
 *
 * Each case below is a thing that fails SILENTLY when it is wrong — a
 * plausible number, never an exception. That is the whole reason they are
 * tests rather than a screenshot.
 */
import { describe, expect, it } from 'vitest';
import { DashboardState, type MonitorSnapshot } from '@sigx/actors-monitor';
import {
    ActorsPanel,
    ClusterPanel,
    HealthPanel,
    HostPanel,
    HostsPanel,
    OverviewPanel
} from '@sigx/actors-dashboard';
import {
    demoSnapshot,
    demoState,
    host,
    inertSource
} from '../../actors-monitor/__tests__/fixture';
import { mount } from './render';

/** `demoSnapshot` with fields replaced. */
const snapshotWith = (over: Partial<MonitorSnapshot>): MonitorSnapshot => ({
    ...demoSnapshot,
    ...over
});

/** A state whose first poll has not landed. */
const pending = (): DashboardState => new DashboardState({ source: inertSource });

describe('before the first snapshot', () => {
    const ALL = [OverviewPanel, HostsPanel, ActorsPanel, ClusterPanel, HealthPanel];

    it('says it is connecting rather than rendering an empty frame', () => {
        // An empty panel and a healthy cluster of zero hosts look identical.
        for (const Panel of ALL) {
            const view = mount(<Panel state={pending()} />);
            expect(view.text()).toBe('connecting…');
            view.unmount();
        }
    });

    it('shows WHY when the first poll has already failed', () => {
        // The bug this replaces (#256): every panel returned "connecting…"
        // BEFORE rendering the alert banner, and a failed first poll is
        // exactly the case with no snapshot — so the commonest failure of
        // all, "the source cannot be reached", put its reason nowhere on the
        // page. A banner that goes quiet when it matters is worse than none,
        // because the silence reads as "still working on it".
        for (const Panel of ALL) {
            const state = pending();
            state.view.error = 'connect ECONNREFUSED 127.0.0.1:5392';
            const view = mount(<Panel state={state} />);
            const text = view.text();
            expect(text).not.toBe('connecting…');
            // The banner is present, and carries the underlying error.
            expect(view.all('.sxad-alert').length).toBeGreaterThan(0);
            expect(text).toContain('connect ECONNREFUSED 127.0.0.1:5392');
            // And it says plainly that nothing has arrived, so an empty page
            // is not mistaken for an empty cluster.
            expect(text).toContain('no data yet');
            // Naming the source removes the "which thing is unreachable"
            // question a portal with several tenants would otherwise ask.
            expect(text).toContain('http://127.0.0.1:5391');
            view.unmount();
        }
    });

    it('does not blame reachability for a failure that is not one', () => {
        // `httpSource` fails for reasons that have nothing to do with
        // reaching the host: a 401 means it answered perfectly and rejected
        // the secret. Saying "could not reach" there sends you to check the
        // network over a wrong bearer token — the same wasted ten minutes
        // that endpoint's own error text exists to prevent.
        const state = pending();
        state.view.error =
            'http://127.0.0.1:5391 rejected the ops secret (401) — check --secret / SIGX_OPS_SECRET';
        const view = mount(<OverviewPanel state={state} />);
        const text = view.text();
        expect(text).not.toContain('could not reach');
        expect(text).toContain('the first poll of');
        // The banner still carries the actionable part.
        expect(text).toContain('rejected the ops secret (401)');
        view.unmount();
    });

    it('puts the banner ABOVE the message, not after it', () => {
        // Ordering is the whole fix; asserting only presence would pass on a
        // version that buried the reason under the placeholder.
        const state = pending();
        state.view.error = 'boom';
        const view = mount(<OverviewPanel state={state} />);
        const nodes = [...view.el.querySelectorAll('.sxad-alert, .sxad-note')];
        expect(nodes[0]?.className).toContain('sxad-alert');
        view.unmount();
    });
});

describe('OverviewPanel', () => {
    it('says WHICH scope its numbers are, before showing any of them', () => {
        const view = mount(<OverviewPanel state={demoState()} />);
        // The #121 failure was a right number under no label at all.
        expect(view.text()).toContain('cluster · 2 host(s)');
        view.unmount();
    });

    it('labels a rate as per-second and a gauge as a count', () => {
        const view = mount(<OverviewPanel state={demoState()} />);
        const text = view.text();
        expect(text).toContain('calls/s');
        expect(text).toContain('failures/s');
        // Queue depth and activation count are how many there are RIGHT NOW.
        // "33/s" under either claims a throughput nobody measured. (The label
        // and its value are separate elements, so there is no space between
        // them in the collapsed text.)
        expect(text).toMatch(/queued\s*\d/);
        expect(text).not.toMatch(/queued\s*[\d.]+k?\/s/);
        expect(text).not.toMatch(/activations\s*[\d.]+k?\/s/);
        view.unmount();
    });

    it('renders no metrics as an em dash, never as zero', () => {
        // "No metrics plugin" is not "no traffic", and a panel of zeroes
        // asserts the second.
        const state = new DashboardState({ source: inertSource });
        state.view.snapshot = snapshotWith({
            metrics: null,
            cluster: {
                ...demoSnapshot.cluster!,
                totals: { ...demoSnapshot.cluster!.totals, metrics: null }
            }
        });
        const view = mount(<OverviewPanel state={state} />);
        const text = view.text();
        expect(text).toContain('no metrics — add .use(metrics())');
        expect(text).toContain('—');
        expect(text).not.toMatch(/calls\/s 0\/s/);
        view.unmount();
    });

    it('draws an empty histogram as no reading, not as three zeroes', () => {
        const view = mount(<OverviewPanel state={demoState(emptyHistograms())} />);
        // A row of zeroed bars asserts "we measured, and it was fast".
        expect(view.text()).toContain('no samples');
        view.unmount();
    });

    it('calls partial metrics coverage a LOWER BOUND', () => {
        const thin = snapshotWith({
            cluster: {
                ...demoSnapshot.cluster!,
                totals: {
                    ...demoSnapshot.cluster!.totals,
                    metrics: { ...demoSnapshot.cluster!.totals.metrics!, hosts: 1 }
                }
            }
        });
        const view = mount(<OverviewPanel state={demoState(thin)} />);
        expect(view.text()).toContain('metrics from 1 of 2 hosts');
        expect(view.text()).toContain('LOWER BOUND');
        view.unmount();
    });
});

describe('the sparkline', () => {
    /** One `<path>` per unbroken run of readings. */
    const paths = (state: DashboardState): number =>
        mount(<OverviewPanel state={state} />).all('.sxad-spark path').length;

    it('BREAKS on a gap rather than joining across it', () => {
        // The rule the whole data layer exists for. A counter going backwards
        // is a reset() or a restart, so the interval has no rate — and joining
        // the line across it draws a slope that reads as real change. This
        // fails as a SPIKE, never as an error, which is why it is pinned.
        const state = pending();
        state.view.snapshot = demoSnapshot;
        [10, 12, null, 14, 16].forEach((value) => state.calls.push(value));
        expect(paths(state)).toBe(2);
    });

    it('draws one unbroken path when nothing reset', () => {
        const state = pending();
        state.view.snapshot = demoSnapshot;
        [10, 12, 14, 16].forEach((value) => state.calls.push(value));
        expect(paths(state)).toBe(1);
    });

    it('still draws a line for a flat-zero series', () => {
        // "We measured, and it was zero" is a real reading. Drawing nothing
        // would make an idle cluster indistinguishable from an unreachable
        // one.
        const state = pending();
        state.view.snapshot = demoSnapshot;
        [0, 0, 0].forEach((value) => state.calls.push(value));
        expect(paths(state)).toBe(1);
    });

    it('draws nothing at all when every sample is a gap', () => {
        const state = pending();
        state.view.snapshot = demoSnapshot;
        [null, null].forEach((value) => state.calls.push(value));
        expect(paths(state)).toBe(0);
    });
});

describe('the alert banner', () => {
    it('is absent entirely when nothing is wrong', () => {
        const healthy = snapshotWith({
            cluster: { ...demoSnapshot.cluster!, reminderShards: { p0: ['s.2sme5hx2'] } }
        });
        const view = mount(<HostsPanel state={demoState(healthy)} />);
        expect(view.all('.sxad-alert')).toHaveLength(0);
        view.unmount();
    });

    it('keeps the last good snapshot on screen after a failed poll, and says so', () => {
        // Blanking the panel destroys exactly the context you need to
        // understand the hiccup.
        const state = demoState();
        state.view.error = 'connect ECONNREFUSED 127.0.0.1:5391';
        const view = mount(<HostsPanel state={state} />);
        expect(view.text()).toContain('poll failed');
        expect(view.text()).toContain('last good snapshot');
        // The hosts are still there.
        expect(view.text()).toContain('s.2sme5hx2');
        view.unmount();
    });

    it('calls a partial fan-out a LOWER BOUND', () => {
        const view = mount(<HostsPanel state={demoState(snapshotWith({ partial: true }))} />);
        expect(view.text()).toContain('PARTIAL');
        expect(view.text()).toContain('LOWER BOUND');
        view.unmount();
    });

    it('names an unclaimed reminder shard — nothing else surfaces it', () => {
        const view = mount(<ClusterPanel state={demoState()} />);
        expect(view.text()).toContain('UNCLAIMED');
        expect(view.text()).toContain('p3');
        view.unmount();
    });

    it('flags a fenced host, which a load balancer cannot see', () => {
        const fenced = snapshotWith({ hosts: [host({ status: 'fenced' })] });
        const view = mount(<HostsPanel state={demoState(fenced)} />);
        expect(view.text()).toContain('FENCED');
        expect(view.text()).toContain('still published as active');
        view.unmount();
    });
});

describe('HostsPanel', () => {
    it('shows EVERY host readiness, not just the polled one', () => {
        // A fleet with one bad peer used to read as healthy because only one
        // host's health was visible anywhere.
        const view = mount(<HostsPanel state={demoState()} />);
        const ready = view.all('tbody tr').map((row) => row.children[2]?.textContent);
        expect(ready).toEqual(['yes', 'NO']);
        view.unmount();
    });

    it('distinguishes FATAL from merely not ready', () => {
        // FATAL means REPLACE this host; draining will not help. Reading it
        // as draining is how a zombie pod sits there forever.
        const fatal = snapshotWith({
            hosts: [host({ health: { ready: false, fatal: true, checks: {} } })]
        });
        const view = mount(<HostsPanel state={demoState(fatal)} />);
        expect(view.text()).toContain('FATAL');
        view.unmount();
    });

    it('reports an unreachable host as a finding, not as a missing row', () => {
        const broken = snapshotWith({
            cluster: {
                ...demoSnapshot.cluster!,
                unreachable: [
                    {
                        hostId: 's.gone',
                        address: 'http://127.0.0.1:5393',
                        reason: 'unreachable',
                        message: 'connect ECONNREFUSED'
                    }
                ]
            }
        });
        const view = mount(<HostsPanel state={demoState(broken)} />);
        expect(view.text()).toContain('unreachable');
        expect(view.text()).toContain('s.gone');
        view.unmount();
    });

    it('opens the drill-down through focus(), so the detail poll is requested', () => {
        // Not local view state: an open drill-down changes what is ASKED FOR
        // on the next poll, because the host has to walk its activation table.
        const state = demoState();
        const view = mount(<HostsPanel state={state} />);
        (view.all('tbody tr')[0] as HTMLElement).click();
        expect(state.view.focus).toBe('s.2sme5hx2');
        view.unmount();
    });

    it('makes the drill-down a real BUTTON, not a clickable row', () => {
        // A click handler on a `<tr>` is invisible to the keyboard and
        // announced as a plain row, so the drill-down would simply not exist
        // for anyone not using a mouse — with no visible symptom at all.
        const state = demoState();
        const view = mount(<HostsPanel state={state} />);
        const button = view.one('tbody tr .sxad-rowbtn') as HTMLButtonElement;
        expect(button).not.toBeNull();
        expect(button.tagName).toBe('BUTTON');
        // Named for what it opens, not "open" repeated once per row.
        expect(button.getAttribute('aria-label')).toBe('open host s.2sme5hx2');

        button.click();
        expect(state.view.focus).toBe('s.2sme5hx2');
        view.unmount();
    });

    it('puts exactly one button per row, in the identifying column', () => {
        const view = mount(<HostsPanel state={demoState()} />);
        const rows = view.all('tbody tr');
        expect(view.all('tbody .sxad-rowbtn')).toHaveLength(rows.length);
        // First cell: the host id. Anywhere else and the accessible name is
        // a status word or a number.
        expect(rows[0]!.children[0]!.querySelector('.sxad-rowbtn')).not.toBeNull();
        view.unmount();
    });

    it('does not fire the pick twice when the button inside a row is clicked', () => {
        // The row keeps a click handler as a mouse convenience, so the
        // button's own click must not bubble into it as well.
        const state = demoState();
        let picks = 0;
        const original = state.focus.bind(state);
        state.focus = (hostId: string | null) => {
            picks++;
            original(hostId);
        };
        const view = mount(<HostsPanel state={state} />);
        (view.one('tbody tr .sxad-rowbtn') as HTMLElement).click();
        expect(picks).toBe(1);
        view.unmount();
    });

    it('leaves a table with no drill-down free of buttons', () => {
        // The Actors table is not pickable; giving every row a button there
        // would put N unreachable-looking controls in the tab order.
        const view = mount(<ActorsPanel state={demoState()} />);
        expect(view.all('tbody .sxad-rowbtn')).toHaveLength(0);
        view.unmount();
    });
});

describe('ActorsPanel', () => {
    it('says the list is one host’s, not the fleet’s', () => {
        const view = mount(<ActorsPanel state={demoState()} />);
        expect(view.text()).toContain('actors on host s.2sme5hx2');
        view.unmount();
    });

    it('distinguishes "no activation list" from "no activations"', () => {
        // ops({ activations: 0 }) is a real setting — the list carries actor
        // KEYS, which can be personal data. An empty table would claim the
        // host is idle.
        const none = snapshotWith({ activations: null });
        const view = mount(<ActorsPanel state={demoState(none)} />);
        expect(view.text()).toContain('no activation list — the source reports none');
        view.unmount();

        const empty = mount(<ActorsPanel state={demoState(snapshotWith({ activations: [] }))} />);
        expect(empty.text()).toContain('no live activations');
        empty.unmount();
    });

    it('shows the actor key in full via a title, however long it is', () => {
        const view = mount(<ActorsPanel state={demoState()} />);
        const keys = view.all('td.sxad-key').map((cell) => cell.getAttribute('title'));
        expect(keys).toContain('cart');
        view.unmount();
    });
});

describe('HostPanel', () => {
    it('states that nothing on it is a cluster total', () => {
        const view = mount(<HostPanel state={demoState()} hostId="s.2sme5hx2" />);
        expect(view.text()).toContain('nothing here is a cluster total');
        view.unmount();
    });

    it('says a host has LEFT the view rather than rendering an empty panel', () => {
        const view = mount(<HostPanel state={demoState()} hostId="s.departed" />);
        expect(view.text()).toContain('s.departed is no longer in the membership view');
        view.unmount();
    });

    it('waits for the detail poll rather than claiming no actors', () => {
        // The panel renders before the detail poll it triggered has landed.
        const view = mount(<HostPanel state={demoState()} hostId="s.2sme5hx2" />);
        expect(view.text()).toContain('waiting for a detail poll…');
        view.unmount();
    });

    it('renders a host that reported no metrics as silent, not as idle', () => {
        const view = mount(<HostPanel state={demoState()} hostId="s.ikfugf49" />);
        expect(view.text()).toContain('no metrics from this host');
        view.unmount();
    });

    it('goes back to the list', () => {
        const state = demoState();
        state.focus('s.2sme5hx2');
        const view = mount(<HostPanel state={state} hostId="s.2sme5hx2" />);
        (view.one('.sxad-back') as HTMLElement).click();
        expect(state.view.focus).toBeNull();
        view.unmount();
    });
});

describe('ClusterPanel', () => {
    it('says single-node rather than drawing an empty cluster', () => {
        const view = mount(<ClusterPanel state={demoState(snapshotWith({ cluster: null }))} />);
        expect(view.text()).toContain('single-node — no cluster to report on');
        view.unmount();
    });

    it('keeps remote and inbound dispatches apart — the gap IS the signal', () => {
        const view = mount(<ClusterPanel state={demoState()} />);
        const text = view.text();
        expect(text).toContain('remoteDispatches');
        expect(text).toContain('inboundDispatches');
        // The per-request fraction (#52), from the dispatches pair.
        expect(text).toContain('locality');
        expect(text).toContain('67% local');
        view.unmount();
    });

    it('legends the shard grid, so a coloured pill means something', () => {
        const view = mount(<ClusterPanel state={demoState()} />);
        expect(view.text()).toContain('UNCLAIMED (nothing is ticking them)');
        // 16 shards in the fixture, one of them unclaimed.
        expect(view.all('.sxad-shard')).toHaveLength(16);
        expect(view.all('.sxad-shard.sxad-danger')).toHaveLength(1);
        view.unmount();
    });

    it('orders shards numerically, so p10 does not sit between p1 and p2', () => {
        const view = mount(<ClusterPanel state={demoState()} />);
        const labels = view.all('.sxad-shard').map((pill) => pill.textContent);
        expect(labels.slice(0, 4)).toEqual(['p0', 'p1', 'p2', 'p3']);
        expect(labels[10]).toBe('p10');
        view.unmount();
    });
});

describe('HealthPanel', () => {
    it('tells you to REPLACE a fatal host, not to drain it', () => {
        const fatal = snapshotWith({
            health: { ...demoSnapshot.health!, fatal: true, ready: false }
        });
        const view = mount(<HealthPanel state={demoState(fatal)} />);
        expect(view.text()).toContain('replace it, draining will not help');
        view.unmount();
    });

    it('tells you to DRAIN a host that is alive but out of rotation', () => {
        const draining = snapshotWith({
            health: { ...demoSnapshot.health!, ready: false }
        });
        const view = mount(<HealthPanel state={demoState(draining)} />);
        expect(view.text()).toContain('drain it, do not restart it');
        view.unmount();
    });

    it('says nothing prescriptive about a healthy host', () => {
        const view = mount(<HealthPanel state={demoState()} />);
        expect(view.text()).not.toContain('drain it');
        expect(view.text()).not.toContain('replace it');
        view.unmount();
    });

    it('names etag conflicts as discarded work, not as a storage volume', () => {
        const view = mount(<HealthPanel state={demoState()} />);
        expect(view.text()).toContain('etag conflicts — each one discarded an activation');
        view.unmount();
    });
});

/** The fixture with every histogram emptied — an uninstrumented fleet. */
function emptyHistograms(): MonitorSnapshot {
    const blank = { count: 0, minMs: 0, maxMs: 0, meanMs: 0, p50Ms: 0, p90Ms: 0, p99Ms: 0 };
    return snapshotWith({
        metrics: {
            ...demoSnapshot.metrics!,
            latencyMs: blank,
            queueMs: blank,
            turnMs: blank
        },
        cluster: {
            ...demoSnapshot.cluster!,
            totals: {
                ...demoSnapshot.cluster!.totals,
                metrics: {
                    ...demoSnapshot.cluster!.totals.metrics!,
                    latencyMs: blank,
                    queueMs: blank,
                    turnMs: blank
                }
            }
        }
    });
}

describe('the node each host runs on (#51)', () => {
    // Three replicas packed onto one node read as `3/3` in every replica
    // readout; finding out otherwise meant joining `kubectl top pods`
    // against `kubectl get pods -o wide` by hand. The chart now publishes
    // `spec.nodeName` as `meta.node`, and these are the seats it renders in.
    const packed = snapshotWith({
        hosts: [
            host({ meta: { node: 'node-1' } }),
            host({ hostId: 's.b', meta: { node: 'node-1' } }),
            host({ hostId: 's.c', meta: { node: 'node-1' } })
        ]
    });

    it('gives the host table a node column', () => {
        const view = mount(<HostsPanel state={demoState(packed)} />);
        expect(view.all('th').map((th) => th.textContent)).toContain('node');
        const cells = view.all('td').map((td) => td.textContent);
        // The same name down the column IS the finding.
        expect(cells.filter((c) => c === 'node-1')).toHaveLength(3);
        view.unmount();
    });

    it('draws a dash, not a blank, for a host that reports no node', () => {
        const view = mount(<HostsPanel state={demoState()} />);
        const header = view.all('th').findIndex((th) => th.textContent === 'node');
        expect(header).toBeGreaterThan(-1);
        const firstRow = view.all('tbody tr')[0]!;
        expect(firstRow.querySelectorAll('td')[header]!.textContent).toBe('—');
        view.unmount();
    });

    it('keeps two nodes tellable apart in a 28ch cell', () => {
        // Real node names are long and differ only in their tail (AKS:
        // `aks-<pool>-<8 digits>-vmss00000N`), and the key cell ellipsises
        // from the RIGHT — so the raw name would read `aks-sigxactors-…`
        // in both rows: a spread fleet posing as a packed one. The label
        // is the monitor's, the same one the terminal shows; the full name
        // stays in the tooltip.
        const aks = (n: number) => `aks-sigxactors-12345678-vmss00000${n}`;
        const spread = snapshotWith({
            hosts: [host({ meta: { node: aks(0) } }), host({ hostId: 's.b', meta: { node: aks(1) } })]
        });
        const view = mount(<HostsPanel state={demoState(spread)} />);
        const header = view.all('th').findIndex((th) => th.textContent === 'node');
        const cells = view.all('tbody tr').map((tr) => tr.querySelectorAll('td')[header]!);
        expect(cells.map((td) => td.textContent)).toEqual(['…vmss000000', '…vmss000001']);
        expect(cells.map((td) => td.getAttribute('title'))).toEqual([aks(0), aks(1)]);
        view.unmount();
    });

    it('says how many nodes the hosts span in the overview', () => {
        const view = mount(<OverviewPanel state={demoState(packed)} />);
        const text = view.text();
        // Both in the scope heading and as its own row under `hosts`.
        expect(text).toContain('cluster · 3 host(s) / 1 node(s)');
        expect(text).toMatch(/nodes\s*1/);
        view.unmount();
    });

    it('leaves the nodes row out when no host reports one', () => {
        // Absent, not `nodes 0` and not `nodes 1`: a fleet outside
        // Kubernetes has not said where it runs, and either guess is a
        // claim nobody measured.
        const view = mount(<OverviewPanel state={demoState()} />);
        expect(view.text()).toContain('cluster · 2 host(s)');
        expect(view.text()).not.toMatch(/nodes\s*\d/);
        view.unmount();
    });

    it('names the node in the host drill-down', () => {
        const view = mount(<HostPanel state={demoState(packed)} hostId="s.b" />);
        expect(view.text()).toMatch(/node\s*node-1/);
        view.unmount();
    });
});

describe('socket sessions (#166)', () => {
    const sockets = {
        connectionsOpened: 7,
        connectionsClosed: 4,
        connectionsRefused: 2,
        callsStarted: 40,
        callsFailed: 3,
        subscriptionsOpened: 9,
        subscriptionsClosed: 5,
        protocolBreaches: 1,
        lifetimeCloses: 2,
        deliveries: 1200,
        deliveryBytes: 48_000,
        throttleQuantized: 0,
        open: 3,
        inFlight: 1,
        subscriptions: 4,
        bufferedBytes: null,
        lifetimeMs: null
    };
    const reporting = (): MonitorSnapshot =>
        snapshotWith({ hosts: [host({ sockets }), demoSnapshot.hosts[1]!] });

    it('shows the sockets column only when some host reported one', () => {
        // A column of `—` across a fleet that never said would read as "no
        // sockets anywhere".
        const silent = mount(<HostsPanel state={demoState()} />);
        expect(silent.text()).not.toContain('sockets');
        silent.unmount();

        const view = mount(<HostsPanel state={demoState(reporting())} />);
        const headers = view.all('thead th').map((th) => th.textContent);
        const column = headers.indexOf('sockets');
        expect(column).toBeGreaterThan(-1);
        const cells = view.all('tbody tr').map((row) => row.children[column]?.textContent);
        // The polled host's count; a peer that said nothing draws a gap.
        expect(cells).toEqual(['3', '—']);
        view.unmount();
    });

    it('lists the sessions, deliveries and evictions in the drill-down', () => {
        const state = demoState(reporting());
        const view = mount(<HostPanel state={state} hostId="s.2sme5hx2" />);
        const text = view.text();
        // `text()` collapses runs of whitespace, hence the `\s+`.
        expect(text).toContain('socket sessions');
        expect(text).toMatch(/3 open\s+1 in flight\s+4 subs/);
        expect(text).toMatch(/1\.2k frames\s+~48 kB/);
        expect(text).toMatch(/7 opened\s+4 closed\s+2 refused/);
        expect(text).toMatch(/2 lifetime\s+1 protocol breach/);
        // Unknown buffered bytes is a gap, never `0 B` (#208); no completed
        // lifetime is "no samples", never three zeroes.
        expect(text).toMatch(/buffered\s*—/);
        expect(text).toMatch(/lifetime\s*no samples/);
        view.unmount();
    });

    it('draws no socket block for a host that reported none', () => {
        const view = mount(<HostPanel state={demoState()} hostId="s.2sme5hx2" />);
        expect(view.text()).not.toContain('socket sessions');
        view.unmount();
    });
});
