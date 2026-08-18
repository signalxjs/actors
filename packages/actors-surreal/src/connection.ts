/**
 * Connection plumbing shared by every provider in this package: option
 * resolution, the table-name prefix, and the lazily-connected handle.
 *
 * Its own module rather than living in `index.ts`, so `storage.ts` and
 * `reminders.ts` can use it without an import cycle back through the entry.
 */
import { Surreal } from 'surrealdb';

/**
 * The query surface the providers call. Structural rather than the `Surreal`
 * class, so an app can share its own connection (or a test its own fake)
 * without this package owning the lifecycle.
 */
export interface SurrealQueryable {
    query<R extends unknown[] = unknown[]>(
        query: string,
        bindings?: Record<string, unknown>
    ): PromiseLike<R>;
}

/** One managed live subscription, as this package uses it. */
export interface LiveHandle {
    subscribe(listener: (...args: unknown[]) => void): unknown;
    kill(): PromiseLike<unknown>;
}

/** The optional push surface — present on `Surreal`, absent on a fake. */
export interface SurrealLive {
    live?(what: unknown): PromiseLike<LiveHandle>;
}

export interface SurrealConnectionOptions {
    /**
     * An existing, already-connected `Surreal` (shared with the app).
     *
     * **It must be connected with UNLIMITED reconnect** —
     * `reconnect: { enabled: true, attempts: -1 }`. The SDK's default is
     * five attempts, roughly 31 s of backoff, after which the socket is
     * dead for the life of the process: `membership`'s heartbeat then beats
     * into a broken connection forever, silently, and the host fences on a
     * TTL it can never renew (#272). A membership heartbeat is a
     * process-lifetime obligation, so its transport may not have a
     * finite one.
     *
     * ```ts
     * await db.connect(url, {
     *     reconnect: { enabled: true, attempts: -1 },
     *     retry: { enabled: true, attempts: 5, retryable: surrealRetryable }
     * });
     * ```
     *
     * The `url` form below does this for you — this note is for the shared
     * connection, whose lifecycle this package does not own.
     */
    db?: SurrealQueryable;
    /** Or an endpoint — the package connects and owns its own `Surreal`.
     *  `ws://`/`wss://` is strongly preferred: the HTTP engine
     *  re-authenticates on every request and cannot serve live queries. */
    url?: string;
    /** Namespace for the self-constructed connection. */
    namespace?: string;
    /** Database for the self-constructed connection. */
    database?: string;
    /** Root/record credentials for the self-constructed connection. */
    auth?: { username: string; password: string };
    /** Table-name prefix. Default `sigx_`. */
    prefix?: string;
}

/**
 * Is this error SurrealDB refusing a write because another transaction
 * touched the same key?
 *
 * **Retry is not an optimisation here, it is part of the contract.** With no
 * lock primitive in SurrealQL, `claim()` and the create arm of `save()` are
 * correct *because* two racers collide at commit and the loser re-runs to
 * observe the winner. Without a retry the loser sees a raw conflict error
 * instead of the winning entry.
 *
 * The SDK's own `isRetryableConflict` matches only the structured
 * `TransactionConflict` detail (wire code -32009), which in practice does not
 * arrive: a bare statement surfaces `Transaction conflict: Write conflict,
 * retry the transaction. This transaction can be retried` through the
 * `NotExecuted` path instead. So this matches the message, exactly as
 * SurrealDB's own tests do. It deliberately does NOT match the
 * `BEGIN…COMMIT` rewrite (`The query was not executed due to a failed
 * transaction`), which is ambiguous — a `THROW` inside a caller's own
 * transaction produces it too. The one place this package needs that broader
 * rule is its own reminder claim, which handles it locally.
 *
 * Pass it when you build the connection yourself:
 *
 * ```ts
 * await db.connect(url, {
 *     retry: { enabled: true, attempts: 5, retryable: surrealRetryable }
 * });
 * ```
 */
export function surrealRetryable(error: unknown): boolean {
    const message = String((error as { message?: unknown } | null)?.message ?? error);
    return /Transaction conflict:|This transaction can be retried/.test(message);
}

/**
 * A table prefix is an IDENTIFIER — it cannot ride a bound parameter, so it
 * is validated instead: the character class every generated statement can
 * safely interpolate.
 */
export function checkPrefix(prefix: string): string {
    if (!/^[a-z_][a-z0-9_]{0,48}$/.test(prefix)) {
        throw new Error(
            `[sigx actors-surreal] prefix ${JSON.stringify(prefix)} must match ` +
                `[a-z_][a-z0-9_]* (it is interpolated as a SurrealQL identifier).`
        );
    }
    return prefix;
}

/** The five table names one prefix produces. */
export interface SurrealTables {
    state: string;
    dir: string;
    host: string;
    mver: string;
    reminder: string;
}

export function tablesFor(prefix = 'sigx_'): SurrealTables {
    const p = checkPrefix(prefix);
    return {
        state: `${p}state`,
        dir: `${p}dir`,
        host: `${p}host`,
        mver: `${p}mver`,
        reminder: `${p}reminder`
    };
}

/**
 * Resolve `db` | `url` into a lazily-connected handle.
 *
 * The connection is established on first use rather than in the factory, so
 * the provider constructors stay synchronous like their pg/redis siblings.
 * One `Surreal` per process is the right shape — the WebSocket multiplexes
 * concurrent RPCs by id, and there is no connection pool to build.
 */
function connector(options: SurrealConnectionOptions): () => Promise<SurrealQueryable> {
    if (options.db) {
        const db = options.db;
        return () => Promise.resolve(db);
    }
    const url = options.url;
    if (url === undefined) {
        throw new Error('[sigx actors-surreal] pass either `db` (a connected Surreal) or `url`.');
    }
    let pending: Promise<SurrealQueryable> | null = null;
    return () => {
        pending ??= (async () => {
            const db = new Surreal();
            await db.connect(url, {
                namespace: options.namespace,
                database: options.database,
                authentication: options.auth,
                // UNLIMITED, against the SDK's default of five attempts
                // (~31 s of backoff) after which the socket is dead for the
                // life of the process. That default is sized for a request
                // that can fail; this connection carries a MEMBERSHIP
                // HEARTBEAT, whose obligation lasts as long as the host. Its
                // exhaustion is silent — the beat keeps firing into a broken
                // socket — so the host fences on a TTL it can no longer
                // renew and never comes back (#272).
                reconnect: { enabled: true, attempts: -1 },
                // See `surrealRetryable`: the CAS statements rely on a loser
                // re-running to observe the winner, and the SDK ships retry
                // DISABLED by default.
                retry: { enabled: true, attempts: 5, retryable: surrealRetryable }
            });
            return db as unknown as SurrealQueryable;
        })();
        return pending;
    };
}

/**
 * One lazily-connected handle, shareable by several providers — so a `url`
 * produces ONE socket for the whole host rather than one per provider.
 */
export function surrealHandle(
    options: SurrealConnectionOptions
): SurrealQueryable & SurrealLive {
    const connect = connector(options);
    return {
        query<R extends unknown[] = unknown[]>(
            query: string,
            bindings?: Record<string, unknown>
        ): PromiseLike<R> {
            return connect().then((db) => db.query<R>(query, bindings));
        },
        // Forwarded, not dropped: without it `surrealCluster({ url })` would
        // silently lose membership push and run poll-only. The connection is
        // lazy, so support cannot be probed up front — an engine with no live
        // queries rejects here and the caller falls back to polling.
        async live(what: unknown): Promise<LiveHandle> {
            const db = (await connect()) as SurrealQueryable & SurrealLive;
            if (typeof db.live !== 'function') {
                throw new Error('[sigx actors-surreal] connection has no live queries.');
            }
            return db.live(what);
        }
    };
}
