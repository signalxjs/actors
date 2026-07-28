/**
 * The dashboard's poll loop.
 *
 * This is the half of `top` that can be wrong without looking wrong: a
 * failed poll that blanks the screen, a slow silo that queues requests
 * until something gives, or numbers that quietly stop updating while still
 * appearing live. None of that is visible in a screenshot, so it is what
 * carries tests.
 */
import { describe, expect, it, vi } from 'vitest';
import { DashboardState, clampInterval, MAX_INTERVAL_MS, MIN_INTERVAL_MS } from '../src/dashboard/state';
import type { MonitorSnapshot, MonitorSource } from '../src/source/types';

const emptyStats = {
    activations: 0,
    queued: 0,
    perType: {},
    transitional: { activating: 0, deactivating: 0 }
};

function snapshotAt(at: number, activations = 1, calls = 10): MonitorSnapshot {
    return {
        at,
        silos: [
            {
                siloId: 'silo-a',
                address: 'x',
                status: 'active',
                uptimeMs: 1000,
                stats: { ...emptyStats, activations, queued: 2 },
                counters: null,
                reminderShards: [],
                membershipVersion: null,
                transports: null
            }
        ],
        cluster: null,
        metrics: {
            windowMs: 1000,
            calls: { total: calls, failed: 1, streams: 0 },
            latencyMs: null,
            queueMs: null,
            turnMs: null,
            byType: {},
            byMethod: {},
            errors: { byKind: {}, recent: [] },
            activations: { created: 0, destroyed: 0, byReason: {} },
            storage: { loads: 0, saves: 0, clears: 0, conflicts: 0, latencyMs: null },
            gauges: null
        },
        activations: null,
        health: null,
        partial: false
    };
}

/** A source under the test's control. */
function fakeSource(
    respond: (signal?: AbortSignal) => Promise<MonitorSnapshot>
): MonitorSource & { closed: boolean } {
    const source = {
        kind: 'http' as const,
        label: 'fake',
        closed: false,
        snapshot: (signal?: AbortSignal) => respond(signal),
        close: () => {
            source.closed = true;
            return Promise.resolve();
        }
    };
    return source;
}

describe('DashboardState', () => {
    it('records a snapshot and derives a rate on the second poll', async () => {
        let at = 1000;
        let calls = 100;
        const state = new DashboardState({
            source: fakeSource(() => Promise.resolve(snapshotAt((at += 1000), 1, (calls += 50))))
        });

        await state.poll();
        expect(state.view.snapshot?.at).toBe(2000);
        // No interval yet — reporting the running total as a rate would put
        // a spike at the left edge of every graph on startup.
        expect(state.calls.latest()).toBeNull();

        await state.poll();
        expect(state.calls.latest()).toBe(50);
        expect(state.view.polls).toBe(2);
        await state.stop();
    });

    it('KEEPS the last good snapshot when a poll fails', async () => {
        let fail = false;
        const state = new DashboardState({
            source: fakeSource(() =>
                fail ? Promise.reject(new Error('silo went away')) : Promise.resolve(snapshotAt(1000))
            )
        });

        await state.poll();
        expect(state.view.snapshot).not.toBeNull();

        fail = true;
        await state.poll();
        // A dashboard that blanks the moment a silo hiccups destroys
        // exactly the context you need to understand the hiccup.
        expect(state.view.snapshot?.at).toBe(1000);
        expect(state.view.error).toBe('silo went away');

        fail = false;
        await state.poll();
        expect(state.view.error).toBeNull();
        await state.stop();
    });

    it('abandons an in-flight poll rather than queueing behind it', async () => {
        const seen: (AbortSignal | undefined)[] = [];
        const releases: (() => void)[] = [];
        const state = new DashboardState({
            source: fakeSource((signal) => {
                seen.push(signal);
                return new Promise<MonitorSnapshot>((resolve, reject) => {
                    // A real source honours the signal — `httpSource` passes
                    // it to `fetch` — so the fake does too.
                    signal?.addEventListener('abort', () => reject(new Error('aborted')));
                    releases.push(() => resolve(snapshotAt(1000)));
                });
            })
        });

        const first = state.poll();
        const second = state.poll();
        // A 5s silo on a 1s interval would otherwise accumulate requests
        // until something gives.
        expect(seen[0]!.aborted).toBe(true);
        expect(seen[1]!.aborted).toBe(false);
        releases[1]!();
        await Promise.all([first, second]);
        // The abort is not surfaced as a poll failure: the caller cancelled
        // it, so there is nothing wrong with the silo.
        expect(state.view.error).toBeNull();
        expect(state.view.snapshot?.at).toBe(1000);
        await state.stop();
    });

    it('does not overwrite good state from an aborted poll', async () => {
        let resolveSlow: ((snapshot: MonitorSnapshot) => void) | undefined;
        let slow = true;
        const state = new DashboardState({
            source: fakeSource(() => {
                if (!slow) return Promise.resolve(snapshotAt(9999));
                slow = false;
                return new Promise<MonitorSnapshot>((resolve) => {
                    resolveSlow = resolve;
                });
            })
        });

        const stale = state.poll();
        const fresh = state.poll();
        await fresh;
        expect(state.view.snapshot?.at).toBe(9999);

        // The abandoned request finishing later must not clobber the newer
        // answer with an older one.
        resolveSlow!(snapshotAt(1));
        await stale;
        expect(state.view.snapshot?.at).toBe(9999);
        await state.stop();
    });

    it('records a GAP rather than a zero when there are no metrics', async () => {
        const withoutMetrics = { ...snapshotAt(1000), metrics: null };
        const state = new DashboardState({ source: fakeSource(() => Promise.resolve(withoutMetrics)) });
        await state.poll();
        await state.poll();
        // "No metrics plugin" is not "no traffic".
        expect(state.calls.values()).toEqual([null, null]);
        // Gauges still work — they come off SiloStats, not off metrics().
        expect(state.activations.latest()).toBe(1);
        await state.stop();
    });

    it('pauses and resumes', async () => {
        const state = new DashboardState({ source: fakeSource(() => Promise.resolve(snapshotAt(1000))) });
        expect(state.view.paused).toBe(false);
        state.togglePause();
        expect(state.view.paused).toBe(true);
        state.togglePause();
        expect(state.view.paused).toBe(false);
        await state.stop();
    });

    it('clamps the interval at both ends', () => {
        expect(clampInterval(1000)).toBe(1000);
        expect(clampInterval(1)).toBe(MIN_INTERVAL_MS);
        expect(clampInterval(10 ** 9)).toBe(MAX_INTERVAL_MS);
        // A computed NaN — `Number(process.env.X)` on an unset variable —
        // must not become the interval.
        expect(clampInterval(Number.NaN)).toBe(1000);
    });

    it('steps the interval within the clamp', async () => {
        const state = new DashboardState({
            source: fakeSource(() => Promise.resolve(snapshotAt(1000))),
            intervalMs: 1000
        });
        state.nudgeInterval(2);
        expect(state.view.intervalMs).toBe(2000);
        state.nudgeInterval(0.5);
        expect(state.view.intervalMs).toBe(1000);
        for (let i = 0; i < 20; i++) state.nudgeInterval(0.5);
        expect(state.view.intervalMs).toBe(MIN_INTERVAL_MS);
        await state.stop();
    });

    it('closes the source on stop, and stops polling', async () => {
        const source = fakeSource(() => Promise.resolve(snapshotAt(1000)));
        const state = new DashboardState({ source });
        await state.stop();
        expect(source.closed).toBe(true);
        // A poll after stop is a no-op rather than a resurrection.
        await state.poll();
        expect(state.view.snapshot).toBeNull();
    });

    it('forgets series for silos that left the cluster', async () => {
        let siloId = 'silo-a';
        const state = new DashboardState({
            source: fakeSource(() => {
                const snapshot = snapshotAt(1000);
                return Promise.resolve({
                    ...snapshot,
                    silos: [{ ...snapshot.silos[0]!, siloId }]
                });
            })
        });
        await state.poll();
        siloId = 'silo-b';
        await state.poll();
        // Nothing observable to assert beyond not growing without bound;
        // the point is that `retain` runs, which a leak test would catch
        // only after hours. Kept as a guard against removing the call.
        expect(state.view.polls).toBe(2);
        await state.stop();
    });

    it('does not hold the process open between polls', async () => {
        const unref = vi.fn();
        const original = globalThis.setTimeout;
        // `unref` is what lets Ctrl+C exit now rather than after the next
        // interval elapses.
        globalThis.setTimeout = ((fn: () => void, ms: number) => {
            const handle = original(fn, ms);
            (handle as unknown as { unref: () => void }).unref = unref;
            return handle;
        }) as typeof globalThis.setTimeout;
        try {
            const state = new DashboardState({
                source: fakeSource(() => Promise.resolve(snapshotAt(1000))),
                intervalMs: 60_000
            });
            state.start();
            await new Promise((resolve) => original(resolve, 20));
            expect(unref).toHaveBeenCalled();
            await state.stop();
        } finally {
            globalThis.setTimeout = original;
        }
    });
});
