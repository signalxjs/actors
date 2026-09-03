/**
 * Reminders against the real alarm API and the real `blockConcurrencyWhile`.
 *
 * These are the assertions the fakes could only ever *assume*. Two claims in
 * particular have been load-bearing since the package shipped and had never
 * met the platform: that the platform clears an alarm before invoking
 * `alarm()` (which is the entire reason `rearm(consumed)` exists), and that
 * holding the concurrency gate across delivery deadlocks the object (#140).
 */
import {
    env,
    evictDurableObject,
    runInDurableObject,
    SELF
} from 'cloudflare:test';
import { describe, expect, it } from 'vitest';
import { encodeSymbolPath } from '../../../actors/src/wire-url';
import type { Env } from './fixture-worker';

declare module 'cloudflare:test' {
    interface ProvidedEnv extends Env {}
}

const SEP = '\u0000';

function stubFor(key: string) {
    const name = `Counter${SEP}${key}`;
    return env.ACTORS.get(env.ACTORS.idFromName(name));
}

async function invoke(symbol: string, args: readonly unknown[]): Promise<unknown> {
    const res = await SELF.fetch(
        `https://edge.test/_sigx/actor/${encodeSymbolPath(symbol)}`,
        {
            method: 'POST',
            headers: { 'content-type': 'application/json' },
            body: JSON.stringify({ args })
        }
    );
    const body = (await res.json()) as { data?: unknown; error?: { message?: string } };
    if (!res.ok || body.error) throw new Error(body.error?.message ?? `HTTP ${res.status}`);
    return body.data;
}

/** Poll until `read()` reports `want`, or give up. Real alarms are driven by
 *  the platform, not by us, so the only honest wait is a bounded one. */
async function until(
    symbol: string,
    key: string,
    want: number,
    budgetMs = 4_000
): Promise<number> {
    const deadline = Date.now() + budgetMs;
    let last = -1;
    while (Date.now() < deadline) {
        last = (await invoke(symbol, [key])) as number;
        if (last === want) return last;
        await scheduler.wait(25);
    }
    return last;
}

describe('reminders on the real alarm API', () => {
    it('fires with nothing driving it but the platform', async () => {
        // No `runDurableObjectAlarm` here on purpose. The alarm is scheduled
        // by the runtime and delivered by workerd; nothing in the test loop
        // pushes it. That is the whole contract the DO reminders exist for,
        // and it has never once been exercised.
        await expect(invoke('Counter#armIn', ['r1', 0])).resolves.toBe(true);
        await expect(until('Counter#woke', 'r1', 1)).resolves.toBe(1);
    });

    it('leaves no alarm armed once a one-shot has fired', async () => {
        await invoke('Counter#armIn', ['r2', 0]);
        await expect(until('Counter#woke', 'r2', 1)).resolves.toBe(1);
        await runInDurableObject(stubFor('r2'), async (_instance, state) => {
            expect(await state.storage.getAlarm()).toBeNull();
        });
    });

    it('lets a handler reschedule itself from inside delivery', async () => {
        // Rescheduling from `onReminder` is the documented pattern, and it
        // takes the concurrency gate from inside delivery.
        //
        // #140 restructured `onAlarm()` claiming the real gate would deadlock
        // here. It does NOT — `gate.test.ts` measures that directly. The
        // three-phase split is still right, because holding a Durable
        // Object's gate across an arbitrary user callback blocks every other
        // event on that object for as long as the handler runs; but the
        // justification is latency, not deadlock.
        await expect(invoke('Counter#armRescheduling', ['r3'])).resolves.toBe(true);
        // Fires once, reschedules itself from inside the handler, fires again.
        await expect(until('Counter#woke', 'r3', 2)).resolves.toBe(2);
    });

    it('re-arms a reminder whose delivery failed, one tick out on the real alarm', async () => {
        // The alarm deleted the one-shot BEFORE delivering it, and the
        // handler threw. Before #326 that was the end of the wake; now the
        // entry is re-armed `reminderTickMs` (500ms in the fixture) out and
        // the platform fires it again.
        await expect(invoke('Counter#armFlaky', ['r6'])).resolves.toBe(true);
        await expect(until('Counter#woke', 'r6', 1, 6_000)).resolves.toBe(1);
        // A one-shot that finally fired leaves nothing armed.
        await runInDurableObject(stubFor('r6'), async (_instance, state) => {
            expect(await state.storage.getAlarm()).toBeNull();
        });
    }, 10_000);

    it('recovers the owner from storage after a real eviction', async () => {
        // The most important claim in reminders.ts: the owner ref is
        // persisted alongside the entries precisely because an evicted object
        // is woken by its alarm BEFORE anything is activated, so an
        // in-memory-only owner would be null exactly when a reminder is due.
        await invoke('Counter#armIn', ['r4', 1_500]);
        // Actually destroy the isolate — not a second instance over the same
        // Map, which is all a fake can do.
        await evictDurableObject(stubFor('r4'));
        await expect(until('Counter#woke', 'r4', 1, 8_000)).resolves.toBe(1);
    }, 15_000);

    it('keeps state across a real eviction', async () => {
        await invoke('Counter#increment', ['r5', 7]);
        await evictDurableObject(stubFor('r5'));
        await expect(invoke('Counter#read', ['r5'])).resolves.toBe(7);
    });
});
