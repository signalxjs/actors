import { describe, expect, it } from 'vitest';
import { heartbeatClock } from '@sigx/actors/cluster';

/**
 * Both clocks are injected, so every case below is exact — no timers, no
 * sleeping, and the two can be moved INDEPENDENTLY, which is the only way to
 * express a suspended VM (wall advances, monotonic does not) or an NTP step
 * (wall jumps, monotonic does not).
 */
function harness(ttlMs = 1_000) {
    let mono = 10_000;
    let wall = 1_700_000_000_000;
    let suspects = 0;
    const clock = heartbeatClock({
        ttlMs,
        onSuspect: () => suspects++,
        monotonicNow: () => mono,
        wallNow: () => wall
    });
    return {
        clock,
        suspects: () => suspects,
        /** Time passing normally: both clocks advance together. */
        advance: (ms: number) => {
            mono += ms;
            wall += ms;
        },
        /** A suspend: the world moved on, CLOCK_MONOTONIC did not. */
        suspend: (ms: number) => {
            wall += ms;
        },
        /** An NTP step, forwards or backwards, with no real time passing. */
        stepWall: (ms: number) => {
            wall += ms;
        },
        stepMono: (ms: number) => {
            mono += ms;
        }
    };
}

describe('heartbeatClock', () => {
    it('a punctual beat never suspects', () => {
        const h = harness(1_000);
        h.clock.arm();
        for (let i = 0; i < 10; i++) {
            h.advance(300);
            h.clock.beat();
            h.clock.confirmed();
        }
        expect(h.suspects()).toBe(0);
    });

    it('a beat that resumes past the TTL suspects — even though the write then succeeds', () => {
        // The #45 scenario: nothing failed. The loop simply stopped running
        // for longer than the record lives, so peers evicted our claims
        // while we were away and the write we are about to make will be a
        // perfectly successful one.
        const h = harness(1_000);
        h.clock.arm();
        h.advance(1_500);
        h.clock.beat();
        expect(h.suspects()).toBe(1);
    });

    it('fires once per lapse, not once per beat, and a confirmed write re-arms it', () => {
        const h = harness(1_000);
        h.clock.arm();
        h.advance(1_500);
        h.clock.beat();
        h.clock.beat();
        h.clock.beat();
        expect(h.suspects()).toBe(1); // latched

        h.clock.confirmed(); // recovery un-latches
        h.advance(1_500);
        h.clock.beat();
        expect(h.suspects()).toBe(2);
    });

    it('a write that STARTED in time but completed past the TTL suspects too', () => {
        // The stall on the store's side rather than ours: the beat was
        // punctual, the write succeeded, but the round trip outlasted the
        // whole TTL. We cannot know when it actually landed — anywhere
        // between send and ack — so the record may have expired mid-flight
        // with peers evicting our claims.
        const h = harness(1_000);
        h.clock.arm();
        h.advance(300);
        h.clock.beat(); // punctual: nothing to suspect yet
        expect(h.suspects()).toBe(0);

        h.advance(2_000); // …a very slow round trip
        h.clock.confirmed();
        expect(h.suspects()).toBe(1);
    });

    it('a write that completes inside the window is silent', () => {
        const h = harness(1_000);
        h.clock.arm();
        h.advance(300);
        h.clock.beat();
        h.advance(300); // a normal round trip
        h.clock.confirmed();
        expect(h.suspects()).toBe(0);
    });

    it('arming stamps the window — a slow join does not fence on the first beat', () => {
        // join() writes, THEN sadds, bumps the version and runs a full O(N)
        // refresh before arming the beat. On a large or slow store that
        // chain can outlast the TTL by itself; stamping only at the write
        // would fence every host at startup, terminally.
        const h = harness(1_000);
        h.clock.confirmed(); // the join write
        h.advance(5_000); // …then a very slow rest-of-join
        h.clock.arm(); // arming the interval re-opens the window
        h.advance(300);
        h.clock.beat();
        expect(h.suspects()).toBe(0);
    });

    it('detects a suspend the monotonic clock slept through', () => {
        // CLOCK_MONOTONIC does not advance while a VM/container is paused,
        // and setTimeout rides it — so the beat looks punctual locally while
        // the store, on another machine, expired the key minutes ago.
        const h = harness(1_000);
        h.clock.arm();
        h.suspend(300_000);
        h.clock.beat();
        expect(h.suspects()).toBe(1);
    });

    it('detects a stall the wall clock was stepped backwards through', () => {
        // The mirror case: a real monotonic lapse must still fire even if
        // NTP corrects the wall clock backwards over the same window.
        const h = harness(1_000);
        h.clock.arm();
        h.stepMono(5_000);
        h.stepWall(-60_000);
        h.clock.beat();
        expect(h.suspects()).toBe(1);
    });

    it('a backwards wall step alone never suspects', () => {
        const h = harness(1_000);
        h.clock.arm();
        h.stepWall(-60_000);
        h.clock.beat();
        expect(h.suspects()).toBe(0);
    });

    it('a failed write suspects only once the window has actually lapsed', () => {
        // The pre-existing semantic, preserved: a write can fail for a
        // moment (a failover, a dropped connection) without the record
        // having expired, and that must not fence a healthy host.
        const h = harness(1_000);
        h.clock.arm();
        h.advance(300);
        h.clock.failed();
        expect(h.suspects()).toBe(0);

        h.advance(900); // now past the TTL with nothing confirmed since
        h.clock.failed();
        expect(h.suspects()).toBe(1);
    });

    it('lost() fires immediately — proof does not wait for the clocks', () => {
        // A provider that has been TOLD its record is gone (a 404 on the
        // Kubernetes Lease) must not wait out a TTL it can no longer prove
        // anything with. Timing would never catch it: the next write
        // succeeds promptly and looks perfectly healthy.
        const h = harness(1_000);
        h.clock.arm();
        h.advance(1); // nowhere near the TTL
        h.clock.lost();
        expect(h.suspects()).toBe(1);
    });

    it('lost() is latched, and a confirmed write clears it', () => {
        const h = harness(1_000);
        h.clock.arm();
        h.clock.lost();
        h.clock.lost();
        h.clock.lost();
        expect(h.suspects()).toBe(1);

        h.clock.confirmed();
        h.clock.lost();
        expect(h.suspects()).toBe(2);
    });

    it('never suspects before it is armed — including lost()', () => {
        // Pre-arm this host holds no claims, so there is nothing for a peer
        // to have evicted: fencing would buy a refused host and a pod
        // restart for nothing. Proof-based or not, the trigger stays silent.
        const h = harness(1_000);
        h.advance(999_999);
        h.clock.beat();
        h.clock.failed();
        h.clock.lost();
        expect(h.suspects()).toBe(0);
    });
});
