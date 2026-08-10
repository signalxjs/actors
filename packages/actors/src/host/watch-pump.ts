/**
 * The watch read pump — what keeps P watch loops from being P queue slots.
 *
 * Every watch loop re-invokes its read as a turn (`watch.ts`), and a read
 * that consults `ctx.principal` means one loop per distinct identity
 * (#121). Before this module each loop enqueued its own turn: a publish to
 * an actor with P identities put O(P) read turns on the single serial
 * queue, and anything behind them — an external call, or a NEW
 * subscriber's seeding read — waited for all of them. On a real cluster
 * that FIFO is the establishment collapse of #180: seeds starve behind
 * re-reads, dialling clients time out and retry, and the retries are more
 * establishment work.
 *
 * The pump batches instead: watch reads register here, and the pump holds
 * AT MOST ONE turn on the queue — a drain that runs pending reads
 * back-to-back, up to `sliceSize` per turn, **seeds first**. Three
 * consequences, each load-bearing:
 *
 *  - The whole watch population contributes O(1) queue slots however large
 *    P grows; external calls wait for at most one slice, not for P reads.
 *  - A new subscriber's seed jumps ahead of every pending re-read — the
 *    acute half of #180 (at 1 publish/s the cluster served 216 identities
 *    fine; every failure was a dial).
 *  - Work arriving mid-drain goes to a TRAILING drain enqueued at the
 *    tail, never run reentrantly inside the current one — so a publisher
 *    interleaves between slices, and a throttle-0 self-dirtying read
 *    cannot livelock the queue any harder than it already did.
 *
 * Each read still runs with full per-turn isolation — the runner the
 * activation hands in is its own `#turn`, which swaps the call context,
 * fires the observer, and closes the change boundary per read. Only the
 * `Turns` chain sees one entry per slice. Failure is per job: one read
 * rejecting reaches only its own loop, and the drain turn itself never
 * rejects (the `Turns` tail must never carry a rejection — the same
 * posture as the write-behind system turn).
 *
 * Kept out of `activation.ts` for the same reason `watch.ts` is: it is
 * self-contained, handed exactly two capabilities (enqueue one serial
 * turn; ask whether the queue is closed) and knows nothing else about an
 * activation.
 */
import { HostShutdownError } from '../errors';

interface WatchReadJob {
    run: () => Promise<unknown>;
    resolve: (value: unknown) => void;
    reject: (error: unknown) => void;
}

export interface WatchReadPump {
    /**
     * Run one watch read on the batched lane; settles with that read's own
     * outcome. `seed` marks a loop's FIRST read — establishment — which
     * drains ahead of every pending re-read.
     */
    schedule(run: () => Promise<unknown>, seed: boolean): Promise<unknown>;
}

export interface WatchReadPumpDeps {
    /** Enqueue ONE serial turn (the activation's `turns.run`). */
    enqueueTurn: (body: () => Promise<void>) => Promise<unknown>;
    /** `turns.closed` — pending jobs reject once the queue refuses turns. */
    closed: () => boolean;
    /** Max reads per drain turn; the remainder re-enqueues. Default 32. */
    sliceSize?: number;
}

const DEFAULT_SLICE = 32;

export function createWatchReadPump(deps: WatchReadPumpDeps): WatchReadPump {
    const sliceSize = deps.sliceSize ?? DEFAULT_SLICE;
    const seeds: WatchReadJob[] = [];
    const reads: WatchReadJob[] = [];
    /** A drain turn is enqueued or running — single flight. */
    let queued = false;

    const rejectAll = (error: unknown): void => {
        for (const job of seeds.splice(0)) job.reject(error);
        for (const job of reads.splice(0)) job.reject(error);
    };

    const drain = async (): Promise<void> => {
        // The slice is taken UP FRONT: seeds first, unconditionally —
        // establishment must never wait behind steady-state re-reads
        // (#180) — then re-reads to the cap. Work scheduled by a job
        // mid-drain stays in the arrays for the TRAILING drain, so a read
        // that re-requests itself (throttle 0 on a self-dirtying method)
        // costs one turn per round, with the queue's other work
        // interleaving between rounds — never a drain that feeds itself.
        const slice = seeds.splice(0, sliceSize);
        if (slice.length < sliceSize) slice.push(...reads.splice(0, sliceSize - slice.length));
        for (let at = 0; at < slice.length; at++) {
            if (deps.closed()) {
                // The queue died mid-drain; the rest of the slice and
                // everything still pending get exactly what an
                // already-queued read got before the pump existed.
                const error = new HostShutdownError();
                for (const job of slice.slice(at)) job.reject(error);
                rejectAll(error);
                break;
            }
            const job = slice[at]!;
            try {
                job.resolve(await job.run());
            } catch (error) {
                // Per-job isolation: one failing read reaches its own loop
                // only, and the drain turn itself never rejects.
                job.reject(error);
            }
        }
        // Slice remainder and mid-drain arrivals: a TRAILING drain at the
        // tail of the queue, so whatever landed meanwhile (a publisher, an
        // external call) runs between slices — bounded delay instead of
        // O(pending).
        if (seeds.length > 0 || reads.length > 0) enqueue();
        else queued = false;
    };

    const enqueue = (): void => {
        queued = true;
        deps.enqueueTurn(drain).catch((error) => {
            // The queue refused the turn (closed): everything pending gets
            // the enqueue rejection, exactly as a direct `enqueue` got it.
            queued = false;
            rejectAll(error);
        });
    };

    return {
        schedule(run, seed) {
            return new Promise<unknown>((resolve, reject) => {
                (seed ? seeds : reads).push({ run, resolve, reject });
                if (!queued) enqueue();
            });
        }
    };
}
