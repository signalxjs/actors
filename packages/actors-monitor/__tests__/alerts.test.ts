/**
 * What is wrong, and what the numbers are about.
 *
 * Both used to be private functions inside the terminal screens, which meant
 * a second renderer would have re-decided them — and every one of these is a
 * decision that fails SILENTLY when it is re-decided differently. A partial
 * fan-out under-reports totals that still look plausible; a fenced host
 * publishes itself as active; a cluster figure printed under a per-host label
 * is right about the wrong thing.
 */
import { describe, expect, it } from 'vitest';
import {
    alertLines,
    coverageNote,
    DashboardState,
    hostTone,
    polledLabel,
    scopeOf,
    type MonitorSnapshot
} from '@sigx/actors-monitor';
import { demoSnapshot, demoState, host, inertSource } from './fixture';

/** A view with no snapshot at all — what the first second looks like. */
const emptyView = new DashboardState({ source: inertSource }).view;

/** `demoSnapshot`, with the given fields replaced. */
const snapshotWith = (over: Partial<MonitorSnapshot>): MonitorSnapshot => ({
    ...demoSnapshot,
    ...over
});

const texts = (snapshot: MonitorSnapshot | null, over: Record<string, unknown> = {}): string[] => {
    const state = snapshot ? demoState(snapshot) : new DashboardState({ source: inertSource });
    Object.assign(state.view, over);
    return alertLines(state.view).map((alert) => alert.text);
};

describe('alertLines', () => {
    it('says nothing when nothing is wrong', () => {
        // The fixture has an unclaimed shard on purpose, so the healthy case
        // has to be built rather than assumed.
        const healthy = snapshotWith({
            cluster: { ...demoSnapshot.cluster!, reminderShards: { p0: ['s.2sme5hx2'] } }
        });
        expect(texts(healthy)).toEqual([]);
    });

    it('has nothing to say before the first poll', () => {
        expect(alertLines(emptyView)).toEqual([]);
    });

    it('does not claim a last good snapshot when there has never been one', () => {
        // With no snapshot the panel below the banner is EMPTY, so
        // "showing the last good snapshot" describes a screen that does not
        // exist. Same class of error as any other caption that outruns its
        // data (#256).
        const state = new DashboardState({ source: inertSource });
        state.view.error = 'connect ECONNREFUSED';
        const [first] = alertLines(state.view);
        expect(first!.text).toContain('nothing has been read yet');
        expect(first!.text).not.toContain('last good snapshot');
        expect(first!.text).toContain('connect ECONNREFUSED');
    });

    it('says the numbers on screen are the LAST GOOD ones after a failed poll', () => {
        // The alternative — blanking the panel — destroys exactly the
        // context you need to understand the hiccup.
        const [first] = texts(demoSnapshot, { error: 'connect ECONNREFUSED' });
        expect(first).toContain('poll failed');
        expect(first).toContain('last good snapshot');
        expect(first).toContain('connect ECONNREFUSED');
    });

    it('ranks a failed poll above everything else', () => {
        const lines = alertLines(
            Object.assign(demoState(demoSnapshot).view, { error: 'boom', paused: true })
        );
        expect(lines[0]!.text).toContain('poll failed');
        expect(lines[0]!.tone).toBe('danger');
    });

    it('calls a partial fan-out a LOWER BOUND rather than smoothing it over', () => {
        // A dashboard that silently under-reports during a partial outage is
        // worse than one that says so: the numbers still look plausible.
        const line = texts(snapshotWith({ partial: true })).find((t) => t.includes('PARTIAL'));
        expect(line).toContain('LOWER BOUND');
    });

    it('names the unclaimed reminder shards, because nothing else surfaces them', () => {
        const line = texts(demoSnapshot).find((t) => t.includes('UNCLAIMED'));
        expect(line).toContain('p3');
        expect(line).toContain('nothing is ticking them');
    });

    it('reports a doubly-claimed shard as a divergence, not as an incident', () => {
        const split = snapshotWith({
            cluster: {
                ...demoSnapshot.cluster!,
                reminderShards: { p0: ['a', 'b'], p1: ['a'] }
            }
        });
        const line = alertLines(demoState(split).view).find((a) => a.text.includes('claimed twice'));
        expect(line?.tone).toBe('warn');
        // The unclaimed line is a `danger`, and this must not be lumped in
        // with it: a split shard is safe (the per-shard etag CAS keeps
        // delivery at-most-once), just worth knowing.
        expect(line?.text).toContain('views have diverged');
    });

    it('flags a fenced host, which nothing else can see', () => {
        const fenced = snapshotWith({ hosts: [host({ status: 'fenced' })] });
        const line = texts(fenced).find((t) => t.includes('FENCED'));
        expect(line).toContain('still published as active');
    });

    it('flags cluster auth failures as a half-finished secret rotation', () => {
        const counters = { ...demoSnapshot.cluster!.totals.counters, authFailures: 4 };
        const bad = snapshotWith({ hosts: [host({ counters })] });
        const line = texts(bad).find((t) => t.includes('auth failure'));
        expect(line).toContain('secret rotation has not reached every host');
    });

    it('sums auth failures across hosts', () => {
        const counters = (n: number) => ({
            ...demoSnapshot.cluster!.totals.counters,
            authFailures: n
        });
        const bad = snapshotWith({
            hosts: [host({ counters: counters(1200) }), host({ counters: counters(300) })]
        });
        // 1.5k, not "1.2k" from the first host alone.
        expect(texts(bad).find((t) => t.includes('auth failure'))).toContain('1.5k');
    });
});

describe('scopeOf', () => {
    it('says how many hosts a cluster total covers', () => {
        expect(scopeOf(demoSnapshot)).toBe('cluster · 2 host(s)');
    });

    it('says "this host" when there is no cluster, rather than implying one', () => {
        expect(scopeOf(snapshotWith({ cluster: null }))).toBe('this host');
    });
});

describe('polledLabel', () => {
    it('names the host the fan-out was collected from', () => {
        expect(polledLabel(demoState().view)).toBe('host s.2sme5hx2');
    });

    it('falls back to "this host" for a single-node deployment', () => {
        expect(polledLabel(demoState(snapshotWith({ cluster: null })).view)).toBe('this host');
    });
});

describe('coverageNote', () => {
    it('is silent when every host reported metrics', () => {
        expect(coverageNote(demoSnapshot)).toBeNull();
    });

    it('calls partial coverage a LOWER BOUND', () => {
        const thin = snapshotWith({
            cluster: {
                ...demoSnapshot.cluster!,
                totals: {
                    ...demoSnapshot.cluster!.totals,
                    metrics: { ...demoSnapshot.cluster!.totals.metrics!, hosts: 1 }
                }
            }
        });
        expect(coverageNote(thin)).toContain('metrics from 1 of 2 hosts');
        expect(coverageNote(thin)).toContain('LOWER BOUND');
    });

    it('warns when the figures shown are one host’s but the panel is a cluster', () => {
        const noClusterMetrics = snapshotWith({
            cluster: {
                ...demoSnapshot.cluster!,
                totals: { ...demoSnapshot.cluster!.totals, metrics: null }
            }
        });
        expect(coverageNote(noClusterMetrics)).toContain('THIS host only');
    });

    it('says nothing when there are no metrics anywhere', () => {
        // "No metrics plugin" is not a coverage problem, and captioning an
        // empty panel with a lower-bound warning implies numbers exist.
        const none = snapshotWith({
            metrics: null,
            cluster: {
                ...demoSnapshot.cluster!,
                totals: { ...demoSnapshot.cluster!.totals, metrics: null }
            }
        });
        expect(coverageNote(none)).toBeNull();
    });

    it('says nothing for a single-node host', () => {
        expect(coverageNote(snapshotWith({ cluster: null }))).toBeNull();
    });
});

describe('hostTone', () => {
    it('has nothing to say about a healthy host', () => {
        expect(hostTone('active')).toBeNull();
        // A single-node host has no membership to report a status from.
        // That is not a problem, and colouring it as one is noise.
        expect(hostTone('unknown')).toBeNull();
    });

    it('never lets fenced or leaving look like active', () => {
        expect(hostTone('fenced')).toBe('danger');
        expect(hostTone('leaving')).toBe('warn');
    });

    it('dims anything it does not recognise', () => {
        expect(hostTone('joining')).toBe('dim');
    });
});
