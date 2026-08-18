/**
 * The connection this package OWNS — the `url` form of every provider
 * factory. Not env-gated: the SDK is mocked, because what is asserted here
 * is the posture the options carry, not anything a server would answer.
 *
 * Both options are load-bearing and both are the opposite of the SDK's
 * default, which is exactly why they are pinned: a "simplification" back to
 * the defaults is silent until a lost CAS race raises instead of reporting a
 * conflict, or until a 31-second outage kills the process's socket for good.
 */
import { describe, expect, it, vi } from 'vitest';

const connect = vi.fn(async (_url: string, _options?: unknown): Promise<void> => undefined);

vi.mock('surrealdb', () => ({
    Surreal: class {
        connect = connect;
        query = async (): Promise<unknown[]> => [];
    },
    RecordId: class {},
    Table: class {}
}));

const { surrealHandle } = await import('../src/connection');

describe('the connection this package owns', () => {
    it('reconnects without limit, and retries conflicts', async () => {
        const handle = surrealHandle({ url: 'ws://db.test', namespace: 'ns', database: 'db' });
        // Lazy by construction — nothing connects until the first query.
        expect(connect).not.toHaveBeenCalled();
        await handle.query('INFO FOR DB');

        expect(connect).toHaveBeenCalledTimes(1);
        const options = connect.mock.calls[0]![1] as {
            reconnect: { enabled: boolean; attempts: number };
            retry: { enabled: boolean; retryable: (error: unknown) => boolean };
        };
        // `-1` is the SDK's spelling of "unlimited". Five (its default) means
        // ~31 s of backoff and then a socket that is dead for the life of the
        // process, while the membership heartbeat keeps beating into it
        // silently until the host's TTL lapses (#272).
        expect(options.reconnect).toEqual({ enabled: true, attempts: -1 });
        expect(options.retry.enabled).toBe(true);
        expect(options.retry.retryable(new Error('Transaction conflict: Write conflict'))).toBe(
            true
        );
    });

    it('opens ONE connection for every provider sharing the handle', async () => {
        connect.mockClear();
        const handle = surrealHandle({ url: 'ws://db.test' });
        await Promise.all([handle.query('a'), handle.query('b'), handle.query('c')]);
        expect(connect).toHaveBeenCalledTimes(1);
    });
});
