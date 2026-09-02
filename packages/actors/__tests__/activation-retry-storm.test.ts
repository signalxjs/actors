import { describe, expect, it, vi } from 'vitest';
import {
    ActorWrongHostError,
    defineActor,
    isActorError,
    type ActorDispatcher,
    type ActorPlacement,
    type PlacementBindings
} from '@sigx/actors';
import { createHost, memoryStorage } from '@sigx/actors/host';
import {
    ACTIVATION_RETRY_STORM_THRESHOLD,
    ACTIVATION_RETRY_STORM_WINDOW_MS
} from '../src/host/local-host';

/**
 * The `__DEV__` retry-storm warning (#54). A poisoned actor — one whose
 * `onActivate` (or state load, or `migrateState`) throws — fails every
 * caller and is forgotten, so the next caller activates it again: a hot
 * loop throttled only by the callers. Dev builds say so once per streak.
 */

const quiet = { sweepIntervalMs: 60_000, reminderTickMs: 60_000, callTimeoutMs: 0 };
const N = ACTIVATION_RETRY_STORM_THRESHOLD;

/** Only the storm warnings — the host has other dev warnings of its own. */
const storms = (warn: { mock: { calls: unknown[][] } }): string[] =>
    warn.mock.calls.flat().map(String).filter((m) => /activated-and-failed/.test(m));

function poisoned(type: string, gate: { poison: boolean }) {
    return defineActor({
        type,
        allowAnonymous: true,
        state: () => ({}),
        onActivate() {
            if (gate.poison) throw new Error('poisoned');
        },
        methods: () => ({
            async ping() {
                return 'pong';
            }
        })
    });
}

describe('activation retry-storm warning', () => {
    it(`warns once when an actor activates-and-fails ${N} times in a row`, async () => {
        const warn = vi.spyOn(console, 'warn').mockImplementation(() => {});
        try {
            const gate = { poison: true };
            const def = poisoned('Poison', gate);
            const host = createHost({ actors: [def], storage: memoryStorage(), defaults: quiet });
            const client = host.actor(def, 'k');
            for (let i = 0; i < N - 1; i++) await expect(client.ping()).rejects.toThrow();
            expect(storms(warn)).toEqual([]);
            await expect(client.ping()).rejects.toThrow();
            expect(storms(warn)).toHaveLength(1);
            expect(storms(warn)[0]).toMatch(
                new RegExp(`Poison/k activated-and-failed ${N} times in \\d+ms`)
            );
            // Past the threshold the streak keeps counting but does not re-warn.
            await expect(client.ping()).rejects.toThrow();
            expect(storms(warn)).toHaveLength(1);
        } finally {
            warn.mockRestore();
        }
    });

    it('a successful activation resets the streak', async () => {
        const warn = vi.spyOn(console, 'warn').mockImplementation(() => {});
        try {
            const gate = { poison: true };
            const def = poisoned('Poison2', gate);
            const host = createHost({ actors: [def], storage: memoryStorage(), defaults: quiet });
            const client = host.actor(def, 'k');
            for (let i = 0; i < N - 1; i++) await expect(client.ping()).rejects.toThrow();
            gate.poison = false;
            await expect(client.ping()).resolves.toBe('pong');
            await host.deactivateType('Poison2');
            gate.poison = true;
            for (let i = 0; i < N - 1; i++) await expect(client.ping()).rejects.toThrow();
            expect(storms(warn)).toEqual([]);
            await expect(client.ping()).rejects.toThrow();
            expect(storms(warn)).toHaveLength(1);
        } finally {
            warn.mockRestore();
        }
    });

    it('a streak older than the window restarts instead of warning', async () => {
        const warn = vi.spyOn(console, 'warn').mockImplementation(() => {});
        const realNow = Date.now;
        let skew = 0;
        const now = vi.spyOn(Date, 'now').mockImplementation(() => realNow() + skew);
        try {
            const gate = { poison: true };
            const def = poisoned('Poison4', gate);
            const host = createHost({ actors: [def], storage: memoryStorage(), defaults: quiet });
            const client = host.actor(def, 'k');
            for (let i = 0; i < N - 1; i++) await expect(client.ping()).rejects.toThrow();
            // The third failure lands after the window: a flaky actor, not
            // a storm — it starts a fresh streak instead of completing one.
            skew = ACTIVATION_RETRY_STORM_WINDOW_MS + 1;
            await expect(client.ping()).rejects.toThrow();
            expect(storms(warn)).toEqual([]);
            for (let i = 0; i < N - 1; i++) await expect(client.ping()).rejects.toThrow();
            expect(storms(warn)).toHaveLength(1);
            // ...and the reported span is the NEW streak's, not the day's.
            expect(storms(warn)[0]).toMatch(/in \d{1,4}ms/);
        } finally {
            now.mockRestore();
            warn.mockRestore();
        }
    });

    it('beforeActivate refusals are placement, not a poisoned actor — they do not count', async () => {
        const warn = vi.spyOn(console, 'warn').mockImplementation(() => {});
        try {
            let local: ActorDispatcher | undefined;
            const bindings: PlacementBindings = {
                beforeActivate(ref) {
                    throw new ActorWrongHostError(`${ref.type}/${ref.key}`, { hostId: 's.other' });
                }
            };
            const placement: ActorPlacement = {
                dispatcherFor: () => local!,
                bind(dispatcher) {
                    local = dispatcher;
                    return bindings;
                }
            };
            const def = poisoned('Poison5', { poison: false });
            const host = createHost({ actors: [def], storage: memoryStorage(), placement, defaults: quiet });
            const client = host.actor(def, 'k');
            for (let i = 0; i < N + 1; i++) {
                await expect(client.ping()).rejects.toSatisfy(
                    (e: unknown) => isActorError(e) && e.kind === 'wrong-host'
                );
            }
            expect(storms(warn)).toEqual([]);
        } finally {
            warn.mockRestore();
        }
    });

    it('streaks are per actor, not per type', async () => {
        const warn = vi.spyOn(console, 'warn').mockImplementation(() => {});
        try {
            const gate = { poison: true };
            const def = poisoned('Poison3', gate);
            const host = createHost({ actors: [def], storage: memoryStorage(), defaults: quiet });
            for (let i = 0; i < N - 1; i++) {
                await expect(host.actor(def, 'a').ping()).rejects.toThrow();
                await expect(host.actor(def, 'b').ping()).rejects.toThrow();
            }
            expect(storms(warn)).toEqual([]);
        } finally {
            warn.mockRestore();
        }
    });
});
