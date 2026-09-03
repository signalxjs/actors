/**
 * The `ActorStorage` conformance suite: what "this is a storage the host can
 * run on" means, as runnable cases (#65).
 *
 * It exists because five adapters (`memoryStorage`, `fileStorage`, pg, redis,
 * SurrealDB — and Durable Objects makes six) had each re-pinned the same
 * seam in their own test file: the load-miss shape, the etag CAS, the conflict
 * brand, the clear rules, NUL-bearing keys, non-object state. Five copies of
 * one contract drift — one had the resurrection case, another the scalar
 * case, none had all of them — and every new adapter started the archaeology
 * over.
 *
 * **Assert the OUTCOME, never the mechanism.** Postgres decides a CAS by a
 * row count, Redis inside a Lua script, SurrealDB by a commit-time write
 * check, `memoryStorage` by a synchronous compare. Nothing here may care:
 * every case is phrased as what a caller sees through the seam's methods,
 * and an etag is an opaque non-empty string that is equal or is not.
 *
 * What is deliberately NOT here:
 *
 *  - **Concurrent writers.** Whether two racing `save`s on one etag produce
 *    exactly one winner is a property of the BACKEND's atomicity, and each
 *    provider pins it with the mechanism it relies on (a Lua script, a
 *    commit-time conflict plus retry, the platform's per-object
 *    serialisation for a Durable Object). Those tests stay with the provider.
 *  - **The host's corrupt-state handling** (`migrateState` throwing leaves
 *    the record byte-identical) — that is host behaviour, in `runtime.test.ts`.
 *    Its precondition IS here: a refused write never alters a record.
 *  - **The save-side ownership rule.** `save` takes the tree at the call and
 *    the CALLER must not touch it afterwards; that is an obligation on the
 *    host, not an observable of the store. The load side is asserted.
 */
import { isStorageConflict } from '../errors';
import type { ActorStorage } from '../types';
import type { ConformanceCase, ConformanceSkip } from './conformance';

// ---------------------------------------------------------------------------
// The harness a provider supplies

export interface StorageConformanceHarness {
    /**
     * OPTIONAL: this package's schema bootstrap (`ensurePgSchema`, …), run
     * ONCE before the storage is touched — so the harness a provider wrote
     * for `bootstrapConformance` serves here unchanged. A harness that
     * bootstraps in its factory simply omits it.
     */
    bootstrap?(): Promise<void>;
    /**
     * A storage over a namespace that is EMPTY when the harness is created —
     * a fresh schema, namespace, key prefix, directory or Map. Every case
     * takes a fresh harness, writes under its own keys, and never cleans up
     * after itself: `stop()` is the cleanup.
     */
    storage(): ActorStorage;
    /** Drop the namespace and close every connection the factory opened. */
    stop(): Promise<void>;
    /**
     * Declare that this storage IMPLEMENTS `saveText`. The text cases then
     * FAIL when it is missing, instead of reporting a skip.
     *
     * `saveText` is optional on the seam, so its absence is normally a
     * legitimate, reported skip (`memoryStorage` wants the tree). But absence
     * is also exactly what a DECORATOR produces when it returns a fixed
     * three-method literal (see the decorator rule on `ActorStorage`) — the
     * host silently falls back to the two-walk save path and nothing says so.
     * A harness over a text-capable adapter, or over a decorator of one,
     * sets this so that drop is a red case rather than a green skip.
     */
    saveText?: boolean;
    /**
     * Declare that this storage IMPLEMENTS `appendText` (#312). The append
     * cases then FAIL when it is missing, instead of reporting a skip.
     *
     * Same reasoning as `saveText`: the seam is optional, so absence is a
     * legitimate, reported skip (`fileStorage` and `durableObjectStorage`
     * decline on purpose) — but absence is also what a decorator produces
     * when it forgets to forward the member, and the host then silently
     * falls back to a full save per append. A harness over an
     * append-capable adapter, or over a decorator of one, sets this.
     */
    appendText?: boolean;
}

export type StorageConformanceFactory = () => Promise<StorageConformanceHarness>;

// ---------------------------------------------------------------------------
// Assertions — deliberately framework-free

function assert(condition: unknown, message: string): asserts condition {
    if (!condition) throw new Error(`[storage conformance] ${message}`);
}

/** Structural equality over JSON-shaped values, key order ignored. */
function sameJson(a: unknown, b: unknown): boolean {
    if (Object.is(a, b)) return true;
    if (typeof a !== 'object' || typeof b !== 'object' || a === null || b === null) return false;
    if (Array.isArray(a) !== Array.isArray(b)) return false;
    if (Array.isArray(a)) {
        return (
            a.length === (b as unknown[]).length &&
            a.every((item, i) => sameJson(item, (b as unknown[])[i]))
        );
    }
    const keysA = Object.keys(a).sort();
    const keysB = Object.keys(b).sort();
    return (
        keysA.length === keysB.length &&
        keysA.every(
            (k, i) =>
                k === keysB[i] &&
                sameJson((a as Record<string, unknown>)[k], (b as Record<string, unknown>)[k])
        )
    );
}

function show(value: unknown): string {
    try {
        return JSON.stringify(value) ?? String(value);
    } catch {
        return String(value);
    }
}

function assertEtag(etag: unknown, what: string): asserts etag is string {
    assert(
        typeof etag === 'string' && etag.length > 0,
        `${what} must resolve to a non-empty string etag, got ${show(etag)}`
    );
}

/** The record under `key` must exist and hold exactly `state`/`etag`. */
async function assertRecord(
    storage: ActorStorage,
    type: string,
    key: string,
    state: unknown,
    etag: string,
    what: string
): Promise<void> {
    const record = await storage.load(type, key);
    assert(record !== null && record !== undefined, `${what}: load(${type}, ${show(key)}) returned ${show(record)}`);
    assert(
        sameJson(record.state, state),
        `${what}: the record's state is ${show(record.state)}, expected ${show(state)}`
    );
    assert(
        record.etag === etag,
        `${what}: the record's etag is ${show(record.etag)}, expected ${show(etag)}`
    );
}

async function assertAbsent(
    storage: ActorStorage,
    type: string,
    key: string,
    what: string
): Promise<void> {
    const record = await storage.load(type, key);
    assert(record === null, `${what}: load(${type}, ${show(key)}) returned ${show(record)}, expected null`);
}

/**
 * The record under `key` must hold exactly `state`/`etag` AND a `log` that is
 * exactly `entries`, oldest first. An append-capable storage reports the log
 * on every load — an empty array when nothing was appended — because the
 * host distinguishes "nothing to replay" from "this store never says".
 */
async function assertRecordWithLog(
    storage: ActorStorage,
    type: string,
    key: string,
    state: unknown,
    etag: string,
    entries: readonly unknown[],
    what: string
): Promise<void> {
    await assertRecord(storage, type, key, state, etag, what);
    const record = (await storage.load(type, key))!;
    assert(
        Array.isArray(record.log),
        `${what}: an append-capable storage must return log as an array on every load, got ${show(record.log)}`
    );
    assert(
        sameJson(record.log, entries),
        `${what}: the record's log is ${show(record.log)}, expected ${show(entries)}`
    );
}

/**
 * `op` must reject with the storage-conflict BRAND — the runtime classifies
 * by the brand, not the class, because a provider may live in a different
 * module graph. A resolve, or a rejection without the brand, both fail.
 */
async function assertConflict(op: () => Promise<unknown>, what: string): Promise<void> {
    let outcome: { resolved: true; value: unknown } | { resolved: false; error: unknown };
    try {
        outcome = { resolved: true, value: await op() };
    } catch (error) {
        outcome = { resolved: false, error };
    }
    if (outcome.resolved) {
        assert(false, `${what} must reject, but resolved with ${show(outcome.value)}`);
    }
    assert(
        isStorageConflict(outcome.error),
        `${what} must reject with the ActorStorageConflict brand ` +
            `(__sigxActorStorageConflict: true); the host cannot classify ${String(outcome.error)}`
    );
}

/**
 * `op` must resolve. A raw rejection out of the storage is re-thrown as a
 * conformance failure that names the step, so a provider author reads
 * "save with the etag saveText returned rejected" rather than an
 * `ActorStorageConflict` from three calls earlier.
 */
async function mustResolve<T>(op: () => Promise<T>, what: string): Promise<T> {
    try {
        return await op();
    } catch (error) {
        assert(false, `${what} must resolve, but rejected: ${String(error)}`);
    }
}

/**
 * Run `body` against a fresh, bootstrapped harness and always tear it down.
 * A `stop()` that rejects fails a case that would otherwise have passed — a
 * green suite leaking schemas, namespaces or connections across runs is the
 * flake nobody can reproduce. When the case itself failed, its error wins.
 */
async function withHarness<T>(
    create: StorageConformanceFactory,
    body: (storage: ActorStorage, harness: StorageConformanceHarness) => Promise<T>
): Promise<T> {
    const harness = await create();
    let result: T;
    try {
        await harness.bootstrap?.();
        result = await body(harness.storage(), harness);
    } catch (error) {
        await harness.stop().catch(() => {});
        throw error;
    }
    await harness.stop();
    return result;
}

const T = 'StorageConformance';

/** The storage's `saveText`, a reported skip, or — when declared — a failure. */
function textPath(
    storage: ActorStorage,
    harness: StorageConformanceHarness
): NonNullable<ActorStorage['saveText']> | ConformanceSkip {
    if (typeof storage.saveText === 'function') return storage.saveText.bind(storage);
    // The host gates on truthiness and then CALLS it: a present-but-not-callable
    // saveText crashes the first durable save, so it is a failure, never a skip.
    assert(
        storage.saveText == null,
        `saveText is present but not a function (${show(storage.saveText)}); the host would call it`
    );
    assert(
        !harness.saveText,
        'the harness declares saveText, but the storage has none. A decorator that returns ' +
            'a fixed three-method literal drops it silently — forward every member the inner ' +
            'storage has (see the decorator rule on ActorStorage)'
    );
    return { skipped: 'this storage has no saveText — it wants the tree, which is a legitimate answer' };
}

/** The storage's `appendText`, a reported skip, or — when declared — a failure. */
function appendPath(
    storage: ActorStorage,
    harness: StorageConformanceHarness
): NonNullable<ActorStorage['appendText']> | ConformanceSkip {
    if (typeof storage.appendText === 'function') return storage.appendText.bind(storage);
    // Same rule as saveText: the host gates on truthiness and then calls it.
    assert(
        storage.appendText == null,
        `appendText is present but not a function (${show(storage.appendText)}); the host would call it`
    );
    assert(
        !harness.appendText,
        'the harness declares appendText, but the storage has none. A decorator that returns ' +
            'a fixed literal drops it silently — forward every member the inner storage has ' +
            '(see the decorator rule on ActorStorage)'
    );
    return { skipped: 'this storage has no appendText — every write is a full save, which is a legitimate answer' };
}

// ---------------------------------------------------------------------------
// The cases

const missIsNull: ConformanceCase<StorageConformanceFactory> = {
    name: 'load of a record that was never saved is null',
    why: 'null is how the host knows to seed state() — undefined or an empty record becomes a phantom activation or a crash',
    run: (create) =>
        withHarness(create, async (s) => {
            const record = await s.load(T, 'never-saved');
            assert(record === null, `expected exactly null, got ${show(record)}`);
        })
};

const createRoundTrips: ConformanceCase<StorageConformanceFactory> = {
    name: 'save with expectedEtag null creates, and load returns that state under the minted etag',
    why: 'the first save of every actor is this call; a state that comes back different is silent data loss',
    run: (create) =>
        withHarness(create, async (s) => {
            const state = { n: 1, deep: { list: [1, 2, null], flag: false }, tag: 'x', empty: {} };
            const etag = await s.save(T, 'k', state, null);
            assertEtag(etag, 'save');
            await assertRecord(s, T, 'k', state, etag, 'after the first save');
        })
};

const etagChain: ConformanceCase<StorageConformanceFactory> = {
    name: 'every save mints a fresh etag and the previous one goes stale',
    why: 'the etag IS the optimistic-concurrency token — a reused one lets two activations overwrite each other unnoticed',
    run: (create) =>
        withHarness(create, async (s) => {
            const first = await s.save(T, 'k', { n: 1 }, null);
            const second = await mustResolve(
                () => s.save(T, 'k', { n: 2 }, first),
                'save with the etag the first save returned'
            );
            assertEtag(second, 'the second save');
            assert(second !== first, `the second save returned the same etag ${show(first)}`);
            await assertConflict(() => s.save(T, 'k', { n: 3 }, first), 'save with the previous etag');
            await assertRecord(s, T, 'k', { n: 2 }, second, 'after the stale save was refused');
        })
};

const createOverExistingConflicts: ConformanceCase<StorageConformanceFactory> = {
    name: 'a create over an existing record conflicts',
    why: 'two hosts activating the same actor both start from null — the loser must learn it lost, not overwrite',
    run: (create) =>
        withHarness(create, async (s) => {
            const etag = await s.save(T, 'k', { n: 1 }, null);
            await assertConflict(() => s.save(T, 'k', { n: 2 }, null), 'a second save with null');
            await assertRecord(s, T, 'k', { n: 1 }, etag, 'after the second create was refused');
        })
};

const updateOfMissingConflicts: ConformanceCase<StorageConformanceFactory> = {
    name: 'an update of a record that does not exist conflicts and creates nothing',
    why: 'a writer holding an etag for a record that is gone is stale by definition — an upsert here resurrects deleted state',
    run: (create) =>
        withHarness(create, async (s) => {
            await assertConflict(
                () => s.save(T, 'k', { n: 1 }, 'not-an-etag'),
                'save with a non-null etag against a missing record'
            );
            await assertAbsent(s, T, 'k', 'after the refused update');
        })
};

const clearIsCompareAndDelete: ConformanceCase<StorageConformanceFactory> = {
    name: 'clear is compare-and-delete; null expected asserts absence',
    why: 'a clear that ignores the etag lets a stale activation wipe state a newer one just wrote',
    run: (create) =>
        withHarness(create, async (s) => {
            const etag = await s.save(T, 'k', { n: 1 }, null);
            await assertConflict(() => s.clear(T, 'k', 'stale'), 'clear with a stale etag');
            // Present + expected null = conflict: the record exists.
            await assertConflict(() => s.clear(T, 'k', null), 'clear with null over an existing record');
            await assertRecord(s, T, 'k', { n: 1 }, etag, 'after two refused clears');
            await mustResolve(() => s.clear(T, 'k', etag), 'clear with the current etag');
            await assertAbsent(s, T, 'k', 'after clear with the current etag');
            // Missing + expected null = success, a no-op.
            await mustResolve(() => s.clear(T, 'k', null), 'clear with null of a missing record');
            // Missing + a concrete etag = conflict: that version no longer exists.
            await assertConflict(() => s.clear(T, 'k', etag), 'clear of a missing record with an etag');
            await assertConflict(() => s.clear(T, 'never', 'stale'), 'clear of a never-saved record with an etag');
        })
};

const noResurrection: ConformanceCase<StorageConformanceFactory> = {
    name: 'a stale writer cannot resurrect a cleared record',
    why: 'deactivate-and-clear followed by a late save from the old activation would bring a deleted actor back from the dead',
    run: (create) =>
        withHarness(create, async (s) => {
            const etag = await s.save(T, 'k', { n: 1 }, null);
            await s.clear(T, 'k', etag);
            await assertConflict(() => s.save(T, 'k', { n: 2 }, etag), 'save with the cleared record\'s etag');
            await assertAbsent(s, T, 'k', 'after the refused resurrection');
            // The key is reusable, from null, as any new actor would.
            const again = await mustResolve(
                () => s.save(T, 'k', { n: 3 }, null),
                'save with null after clear'
            );
            await assertRecord(s, T, 'k', { n: 3 }, again, 'after re-creating from null');
        })
};

const refusedWriteIsNoop: ConformanceCase<StorageConformanceFactory> = {
    name: 'a refused write leaves the record exactly as it was — same state, same etag',
    why: 'the host promises corrupt or contested state is loud and never silently changed; that promise rests on a conflict writing NOTHING',
    run: (create) =>
        withHarness(create, async (s) => {
            const state = { items: ['a'], nested: { n: 1 } };
            const etag = await s.save(T, 'k', state, null);
            await assertConflict(() => s.save(T, 'k', { items: [] }, null), 'create over existing');
            await assertConflict(() => s.save(T, 'k', { items: [] }, 'stale'), 'stale update');
            await assertConflict(() => s.clear(T, 'k', 'stale'), 'stale clear');
            await assertConflict(() => s.clear(T, 'k', null), 'null clear over existing');
            if (typeof s.saveText === 'function') {
                await assertConflict(() => s.saveText!(T, 'k', '{"items":[]}', 'stale'), 'stale saveText');
                await assertConflict(() => s.saveText!(T, 'k', '{"items":[]}', null), 'saveText create over existing');
            }
            if (typeof s.appendText === 'function') {
                await assertConflict(() => s.appendText!(T, 'k', '{"step":1}', 'stale'), 'stale appendText');
                await assertRecordWithLog(s, T, 'k', state, etag, [], 'after the refused append');
            }
            await assertRecord(s, T, 'k', state, etag, 'after every refused write');
        })
};

const loadedRecordIsCallersToMutate: ConformanceCase<StorageConformanceFactory> = {
    name: 'a loaded record is the caller\'s to mutate — the store never sees the change',
    why: 'the host revives state in place; a store handing out its own tree by reference is corrupted by the first turn (#25)',
    run: (create) =>
        withHarness(create, async (s) => {
            const state = { rows: [{ n: 1 }], tags: ['a'] };
            const etag = await s.save(T, 'k', state, null);
            const first = await s.load(T, 'k');
            assert(first !== null, 'the saved record did not load');
            const tree = first.state as { rows: { n: number }[]; tags: string[] };
            tree.rows[0]!.n = 99;
            tree.tags.push('mutated');
            (first as { etag: string }).etag = 'mutated';
            await assertRecord(s, T, 'k', state, etag, 'after mutating a loaded record');
            // And two loads are two records: mutating one never shows in the other.
            const a = await s.load(T, 'k');
            const b = await s.load(T, 'k');
            (a!.state as { rows: { n: number }[] }).rows[0]!.n = 7;
            assert(
                sameJson(b!.state, state),
                `two loads share a tree: the second saw ${show(b!.state)}`
            );
        })
};

const nonObjectState: ConformanceCase<StorageConformanceFactory> = {
    name: 'state is whatever the codec produced — arrays, scalars and null round-trip and stay distinct from absent',
    why: 'a store that assumes a JSON object at the top level turns an array into a document and null into "no record"',
    run: (create) =>
        withHarness(create, async (s) => {
            const shapes: [string, unknown][] = [
                ['array', [1, 'two', { three: 3 }, [4], null]],
                ['null', null],
                ['zero', 0],
                ['false', false],
                ['empty-string', ''],
                ['string', 'plain text'],
                ['empty-object', {}],
                ['empty-array', []]
            ];
            for (const [key, state] of shapes) {
                const etag = await s.save(T, key, state, null);
                assertEtag(etag, `save of the ${key} shape`);
                await assertRecord(s, T, key, state, etag, `the ${key} shape`);
            }
            await assertAbsent(s, T, 'absent', 'a key nothing was saved under');
        })
};

const keysAreOpaqueAndDistinct: ConformanceCase<StorageConformanceFactory> = {
    name: 'type and key together name a record; keys are opaque, NUL and separators included',
    why: 'the runtime stores a task ledger under `type<NUL>key` and reminder shards keyed the same way — an escaping layer that collides or trims loses them',
    run: (create) =>
        withHarness(create, async (s) => {
            const NUL = String.fromCharCode(0);
            const ledgerKey = `Cart${NUL}user-42`;
            const shardState = {
                [`Room${NUL}general`]: { cleanup: { nextDue: 123, period: 60_000 } },
                note: `a user string may hold ${NUL} too`,
                path: 'C:\\backslashes\\stay\\distinct'
            };
            const keys: [string, unknown][] = [
                [ledgerKey, shardState],
                ['Cart\\0user-42', { looksEscaped: true }],
                ['k', { b: 2 }],
                ['k ', { c: 3 }],
                [' k', { d: 4 }],
                ['a/b', { slash: true }],
                ['a:b', { colon: true }],
                ['a b', { space: true }],
                ['ünïcödé ☃', { unicode: true }]
            ];
            const etags = new Map<string, string>();
            for (const [key, state] of keys) {
                try {
                    etags.set(key, await s.save(T, key, state, null));
                } catch (error) {
                    // A CONFLICT here means two DISTINCT keys landed on one
                    // record — an escaping or trimming layer that is not
                    // injective. Any other rejection is the backend refusing
                    // the key itself; say which, so the failure is debuggable.
                    assert(
                        false,
                        isStorageConflict(error)
                            ? `save under the fresh key ${show(key)} conflicted — it collided with an earlier key`
                            : `save under the fresh key ${show(key)} rejected: ${String(error)}`
                    );
                }
            }
            for (const [key, state] of keys) {
                await assertRecord(s, T, key, state, etags.get(key)!, `the key ${show(key)}`);
            }
            // Same key, another type: a different record entirely.
            const other = await s.save(`${T}Other`, 'k', { other: true }, null);
            await assertRecord(s, `${T}Other`, 'k', { other: true }, other, 'the other type');
            await assertRecord(s, T, 'k', { b: 2 }, etags.get('k')!, 'the first type, after the other was written');
            await s.clear(`${T}Other`, 'k', other);
            await assertRecord(s, T, 'k', { b: 2 }, etags.get('k')!, 'the first type, after the other was cleared');
            // Clearing one key clears only that key.
            await s.clear(T, ledgerKey, etags.get(ledgerKey)!);
            await assertAbsent(s, T, ledgerKey, 'the cleared NUL key');
            await assertRecord(s, T, 'Cart\\0user-42', { looksEscaped: true }, etags.get('Cart\\0user-42')!, 'the escaped-looking key');
        })
};

const saveTextEquivalence: ConformanceCase<StorageConformanceFactory> = {
    name: 'saveText(json) is save(JSON.parse(json)): the same record either way',
    why: 'the host picks a path per boundary (#238) — a record written by one host must read identically on another whichever path wrote it',
    run: (create) =>
        withHarness(create, async (s, h) => {
            const saveText = textPath(s, h);
            if (typeof saveText !== 'function') return saveText;
            const state = { n: 1, deep: { list: [1, 2, null] }, tag: 'x', nul: `a${String.fromCharCode(0)}b` };
            const viaText = await saveText(T, 'text', JSON.stringify(state), null);
            assertEtag(viaText, 'saveText');
            const viaTree = await s.save(T, 'tree', state, null);
            assertEtag(viaTree, 'save');
            await assertRecord(s, T, 'text', state, viaText, 'the record saveText wrote');
            const text = await s.load(T, 'text');
            const tree = await s.load(T, 'tree');
            assert(
                sameJson(text!.state, tree!.state),
                `saveText and save disagree: ${show(text!.state)} vs ${show(tree!.state)}`
            );
            await assertRecord(s, T, 'tree', state, viaTree, 'the record save wrote');
        })
};

const saveTextHonoursCas: ConformanceCase<StorageConformanceFactory> = {
    name: 'saveText honours the same CAS and throws the same brand',
    why: 'a text path with looser concurrency than the tree path is a data race the host cannot see',
    run: (create) =>
        withHarness(create, async (s, h) => {
            const saveText = textPath(s, h);
            if (typeof saveText !== 'function') return saveText;
            const etag = await saveText(T, 'k', '{"n":1}', null);
            assertEtag(etag, 'saveText');
            await assertConflict(() => saveText(T, 'k', '{"n":2}', 'stale'), 'saveText with a stale etag');
            await assertConflict(() => saveText(T, 'k', '{"n":2}', null), 'saveText create over existing');
            await assertConflict(
                () => saveText(T, 'missing', '{"n":2}', 'stale'),
                'saveText update of a missing record'
            );
            await assertAbsent(s, T, 'missing', 'after the refused saveText update');
            await assertRecord(s, T, 'k', { n: 1 }, etag, 'after the refused saveTexts');
            const next = await mustResolve(
                () => saveText(T, 'k', '{"n":2}', etag),
                'saveText with the current etag'
            );
            assertEtag(next, 'saveText with the current etag');
            assert(next !== etag, 'saveText returned the etag it was given');
            await assertRecord(s, T, 'k', { n: 2 }, next, 'after saveText with the current etag');
        })
};

const saveTextInterleaves: ConformanceCase<StorageConformanceFactory> = {
    name: 'save and saveText interleave on one record\'s etag chain, and clear closes it',
    why: 'one activation alternates paths between boundaries — an etag minted by one path must be honoured by the other',
    run: (create) =>
        withHarness(create, async (s, h) => {
            const saveText = textPath(s, h);
            if (typeof saveText !== 'function') return saveText;
            const e1 = await s.save(T, 'k', { n: 1 }, null);
            const e2 = await mustResolve(
                () => saveText(T, 'k', '{"n":2}', e1),
                'saveText with the etag save returned'
            );
            assertEtag(e2, 'saveText after save');
            await assertConflict(() => s.save(T, 'k', { n: 9 }, e1), 'save with the etag saveText replaced');
            const e3 = await mustResolve(
                () => s.save(T, 'k', { n: 3 }, e2),
                'save with the etag saveText returned'
            );
            assertEtag(e3, 'save after saveText');
            await assertConflict(() => saveText(T, 'k', '{"n":9}', e2), 'saveText with the etag save replaced');
            await assertRecord(s, T, 'k', { n: 3 }, e3, 'after the interleaved chain');
            await mustResolve(() => s.clear(T, 'k', e3), "clear with the chain's last etag");
            await assertAbsent(s, T, 'k', 'after clear with the chain\'s last etag');
        })
};

// ---------------------------------------------------------------------------
// The optional append path (#312): one entry onto the record's log, CAS on
// the record's etag; a full save is the compaction that truncates it.

const appendRequiresARecord: ConformanceCase<StorageConformanceFactory> = {
    name: 'appendText requires a record: a missing one conflicts and nothing is created',
    why: 'there is nothing to append to — an append that creates would give a cleared actor a log with no snapshot to fold it into',
    run: (create) =>
        withHarness(create, async (s, h) => {
            const appendText = appendPath(s, h);
            if (typeof appendText !== 'function') return appendText;
            await assertConflict(
                () => appendText(T, 'k', '{"step":1}', 'not-an-etag'),
                'appendText on a record that was never saved'
            );
            await assertAbsent(s, T, 'k', 'after the refused append');
            // A cleared record is a missing record, even to the writer that
            // held its last etag.
            const etag = await s.save(T, 'gone', { n: 1 }, null);
            await s.clear(T, 'gone', etag);
            await assertConflict(
                () => appendText(T, 'gone', '{"step":1}', etag),
                "appendText with a cleared record's etag"
            );
            await assertAbsent(s, T, 'gone', 'after the refused append to a cleared record');
        })
};

const appendHonoursCas: ConformanceCase<StorageConformanceFactory> = {
    name: 'appendText honours the CAS and throws the same brand: a stale etag appends nothing',
    why: 'an append that skips the etag lets a stale activation interleave entries into a log a newer one is folding',
    run: (create) =>
        withHarness(create, async (s, h) => {
            const appendText = appendPath(s, h);
            if (typeof appendText !== 'function') return appendText;
            const etag = await s.save(T, 'k', { n: 1 }, null);
            await assertConflict(() => appendText(T, 'k', '{"step":1}', 'stale'), 'appendText with a stale etag');
            await assertRecordWithLog(s, T, 'k', { n: 1 }, etag, [], 'after the refused append');
            const next = await mustResolve(
                () => appendText(T, 'k', '{"step":1}', etag),
                'appendText with the current etag'
            );
            assertEtag(next, 'appendText with the current etag');
            await assertConflict(() => appendText(T, 'k', '{"step":2}', etag), 'appendText with the etag the append replaced');
            await assertRecordWithLog(s, T, 'k', { n: 1 }, next, [{ step: 1 }], 'after the second, stale append was refused');
        })
};

const appendReturnsANewEtag: ConformanceCase<StorageConformanceFactory> = {
    name: 'appendText mints a fresh etag and load shows the unchanged state plus the one entry',
    why: 'the etag IS the write token — an append that returned the etag it was given would let a stale full save land on top of the entry',
    run: (create) =>
        withHarness(create, async (s, h) => {
            const appendText = appendPath(s, h);
            if (typeof appendText !== 'function') return appendText;
            const state = { n: 1, deep: { list: [1, 2, null] } };
            const etag = await s.save(T, 'k', state, null);
            await assertRecordWithLog(s, T, 'k', state, etag, [], 'a freshly created record');
            const entry = { step: 1, note: `nul ${String.fromCharCode(0)} stays`, rows: [{ a: 1 }] };
            const next = await mustResolve(
                () => appendText(T, 'k', JSON.stringify(entry), etag),
                'appendText with the current etag'
            );
            assertEtag(next, 'appendText');
            assert(next !== etag, 'appendText returned the etag it was given');
            await assertRecordWithLog(s, T, 'k', state, next, [entry], 'after one append');
            // The log is the caller's to mutate, like the state (#25): the
            // host revives entries in place before folding them.
            const loaded = (await s.load(T, 'k'))!;
            (loaded.log![0] as { step: number }).step = 99;
            loaded.log!.push('mutated');
            loaded.log = [];
            await assertRecordWithLog(s, T, 'k', state, next, [entry], 'after mutating a loaded log');
        })
};

const appendsAreOrdered: ConformanceCase<StorageConformanceFactory> = {
    name: 'appended entries load in append order, oldest first, whatever their JSON shape — and only on their own record',
    why: 'the host folds the log through a reducer — an entry out of order, an object where a scalar was appended, or a neighbour\'s entry, folds to the wrong state',
    run: (create) =>
        withHarness(create, async (s, h) => {
            const appendText = appendPath(s, h);
            if (typeof appendText !== 'function') return appendText;
            let etag = await s.save(T, 'k', { n: 0 }, null);
            // Neighbours whose keys are what a store might DERIVE a log
            // key from — a suffix, a separator, a NUL — each with one
            // entry of its own. Keys are opaque; a log that lives under
            // `${key}:l` is another actor's record.
            const NUL = String.fromCharCode(0);
            const neighbours = ['k:l', 'k:log', `k${NUL}l`, 'k/l', 'k_log', 'kl'];
            const neighbourEtags = new Map<string, string>();
            for (const key of neighbours) {
                const created = await s.save(T, key, { neighbour: key }, null);
                neighbourEtags.set(key, await appendText(T, key, JSON.stringify({ own: key }), created));
            }
            const entries: unknown[] = [
                { step: 1 },
                [2, 'two'],
                'three',
                4,
                null,
                false,
                { step: 7, nested: { list: [] } },
                {}
            ];
            const etags = new Set<string>([etag]);
            for (const [i, entry] of entries.entries()) {
                etag = await mustResolve(
                    () => appendText(T, 'k', JSON.stringify(entry), etag),
                    `append #${i + 1} with the etag the previous write returned`
                );
                assertEtag(etag, `append #${i + 1}`);
                assert(!etags.has(etag), `append #${i + 1} reused an earlier etag ${show(etag)}`);
                etags.add(etag);
            }
            await assertRecordWithLog(s, T, 'k', { n: 0 }, etag, entries, 'after eight appends');
            for (const key of neighbours) {
                await assertRecordWithLog(
                    s,
                    T,
                    key,
                    { neighbour: key },
                    neighbourEtags.get(key)!,
                    [{ own: key }],
                    `the neighbour ${show(key)}, after eight appends to "k"`
                );
            }
            // And the same type under another type: a different record.
            await assertAbsent(s, `${T}Other`, 'k', 'the key "k" under another type');
        })
};

const fullSaveTruncatesTheLog: ConformanceCase<StorageConformanceFactory> = {
    name: 'a full save truncates the appended log, clear removes it, and a re-created record starts empty',
    why: 'a full save IS the compaction — a log that survives it is replayed onto a state that already contains it',
    run: (create) =>
        withHarness(create, async (s, h) => {
            const appendText = appendPath(s, h);
            if (typeof appendText !== 'function') return appendText;
            let etag = await s.save(T, 'k', { n: 0 }, null);
            etag = await appendText(T, 'k', '{"step":1}', etag);
            etag = await appendText(T, 'k', '{"step":2}', etag);
            await assertRecordWithLog(s, T, 'k', { n: 0 }, etag, [{ step: 1 }, { step: 2 }], 'before the compaction');
            // via save
            etag = await mustResolve(() => s.save(T, 'k', { n: 2 }, etag), 'save with the etag the append returned');
            await assertRecordWithLog(s, T, 'k', { n: 2 }, etag, [], 'after the full save');
            etag = await appendText(T, 'k', '{"step":3}', etag);
            await assertRecordWithLog(s, T, 'k', { n: 2 }, etag, [{ step: 3 }], 'after appending onto the compacted record');
            // via saveText, where the storage has it
            if (typeof s.saveText === 'function') {
                etag = await mustResolve(() => s.saveText!(T, 'k', '{"n":3}', etag), 'saveText with the etag the append returned');
                await assertRecordWithLog(s, T, 'k', { n: 3 }, etag, [], 'after the full saveText');
                etag = await appendText(T, 'k', '{"step":4}', etag);
            }
            // via clear + re-create
            await mustResolve(() => s.clear(T, 'k', etag), 'clear with the etag the append returned');
            await assertAbsent(s, T, 'k', 'after clear');
            const again = await mustResolve(() => s.save(T, 'k', { n: 9 }, null), 'save with null after clear');
            await assertRecordWithLog(s, T, 'k', { n: 9 }, again, [], 'after re-creating the record');
        })
};

const appendChainsWithSave: ConformanceCase<StorageConformanceFactory> = {
    name: 'appendText shares the etag chain: the etag it returns is what the next save or clear must present',
    why: 'an append with its own chain would let a full save from before the append win over it — the log entry and the snapshot would disagree about what happened',
    run: (create) =>
        withHarness(create, async (s, h) => {
            const appendText = appendPath(s, h);
            if (typeof appendText !== 'function') return appendText;
            const e1 = await s.save(T, 'k', { n: 1 }, null);
            const e2 = await mustResolve(() => appendText(T, 'k', '{"step":1}', e1), 'appendText with the etag save returned');
            await assertConflict(() => s.save(T, 'k', { n: 9 }, e1), 'save with the etag the append replaced');
            await assertConflict(() => s.clear(T, 'k', e1), 'clear with the etag the append replaced');
            if (typeof s.saveText === 'function') {
                await assertConflict(() => s.saveText!(T, 'k', '{"n":9}', e1), 'saveText with the etag the append replaced');
            }
            await assertRecordWithLog(s, T, 'k', { n: 1 }, e2, [{ step: 1 }], 'after the stale writes were refused');
            const e3 = await mustResolve(() => s.save(T, 'k', { n: 3 }, e2), 'save with the etag the append returned');
            assertEtag(e3, 'save after append');
            await assertConflict(() => appendText(T, 'k', '{"step":2}', e2), 'appendText with the etag save replaced');
            const e4 = await mustResolve(() => appendText(T, 'k', '{"step":2}', e3), 'appendText with the etag save returned');
            await assertConflict(() => s.clear(T, 'k', e3), 'clear with the etag the append replaced');
            await assertRecordWithLog(s, T, 'k', { n: 3 }, e4, [{ step: 2 }], 'after the interleaved chain');
            await mustResolve(() => s.clear(T, 'k', e4), "clear with the chain's last etag");
            await assertAbsent(s, T, 'k', "after clear with the chain's last etag");
        })
};

/**
 * The suite. Cheapest and most fundamental first, so a broken harness fails
 * on "does a miss load as null" rather than inside the etag chain; the
 * optional text path and then the optional append path last, where their
 * skips are easy to read.
 */
export const storageConformance: readonly ConformanceCase<StorageConformanceFactory>[] = [
    missIsNull,
    createRoundTrips,
    etagChain,
    createOverExistingConflicts,
    updateOfMissingConflicts,
    clearIsCompareAndDelete,
    noResurrection,
    refusedWriteIsNoop,
    loadedRecordIsCallersToMutate,
    nonObjectState,
    keysAreOpaqueAndDistinct,
    saveTextEquivalence,
    saveTextHonoursCas,
    saveTextInterleaves,
    appendRequiresARecord,
    appendHonoursCas,
    appendReturnsANewEtag,
    appendsAreOrdered,
    fullSaveTruncatesTheLog,
    appendChainsWithSave
];
