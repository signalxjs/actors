/**
 * The shared `ActorReminders` conformance suite (#385), run against the
 * Durable Object alarm provider over fakes — no Workers runtime.
 *
 * The alarm provider has no tick loop: the platform fires `onAlarm()` at
 * the earliest due time. The harness says so (`tickDriven: false`), and the
 * suite reports the tick-driven cases as skips rather than passes; the
 * `ReminderApi` cases run in full — except the cross-actor one, because a
 * Durable Object hosts exactly one actor (`singleActor: true`). The alarm's own claim, re-arm and
 * "meanwhile" rules are pinned by hand in `durable-objects.test.ts`, where
 * the alarm can be fired and the clock moved.
 */
import { describe, expect, it } from 'vitest';
import {
    remindersConformance,
    type RemindersConformanceFactory,
    type RemindersConformanceHarness
} from '@sigx/actors/testing';
import { durableObjectReminders, type DurableAlarms, type DurableStorage } from '@sigx/actors-cloudflare';

function fakeStorage(): DurableStorage {
    const map = new Map<string, unknown>();
    return {
        get: async <T,>(key: string) => (map.has(key) ? (structuredClone(map.get(key)) as T) : undefined),
        put: async <T,>(key: string, value: T) => void map.set(key, structuredClone(value)),
        delete: async (key: string) => map.delete(key)
    };
}

function fakeAlarms(): DurableAlarms {
    let at: number | null = null;
    return {
        getAlarm: async () => at,
        setAlarm: async (t) => void (at = t),
        deleteAlarm: async () => void (at = null)
    };
}

const createDurableObjectReminders: RemindersConformanceFactory =
    async (): Promise<RemindersConformanceHarness> => {
        // One object's storage per harness: a "restart" is a new provider
        // over the same storage, as an evicted-and-woken object would be.
        const storage = fakeStorage();
        const alarms = fakeAlarms();
        return {
            reminders: () => durableObjectReminders({ storage, alarms }),
            stop: async () => {},
            tickDriven: false,
            singleActor: true
        };
    };

describe('durableObjectReminders conformance', () => {
    for (const c of remindersConformance) {
        it(c.name, async () => {
            const outcome = await c.run(createDurableObjectReminders);
            // Either passed, or a reported skip — never a silent pass.
            if (outcome !== undefined) {
                expect(outcome.skipped).toMatch(/platform alarm|one actor/);
            }
        });
    }

    it('reports the tick-driven cases as skips, and runs the rest', async () => {
        const outcomes = await Promise.all(remindersConformance.map((c) => c.run(createDurableObjectReminders)));
        const skipped = outcomes.filter((o) => o !== undefined).length;
        expect(skipped).toBeGreaterThan(0);
        expect(outcomes.length - skipped).toBeGreaterThanOrEqual(4);
    });
});
