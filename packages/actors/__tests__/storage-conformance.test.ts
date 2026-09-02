/**
 * The shared `ActorStorage` conformance suite (#65) against everything this
 * package ships or wraps:
 *
 *  - `memoryStorage` — the incumbent, whose behaviour IS the contract. That
 *    it passes is what proves the suite describes the seam rather than a
 *    newcomer's habits.
 *  - `fileStorage` — the dev store, on a temp directory.
 *  - an in-memory TEXT adapter — the in-repo stand-in for pg/redis/surreal,
 *    which all store the serialized form, so the `saveText` cases run here
 *    and not only in CI's provider jobs.
 *  - the `metrics()` storage decorator over that adapter, with `saveText`
 *    DECLARED — the decorator rule as a red-or-green case: a wrapper that
 *    dropped the optional member would fail, not skip.
 *
 * And the second rule of conformance suites: a case that cannot fail is
 * decoration. The last block runs the suite against deliberately broken
 * adapters and asserts the matching case goes red.
 */
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { describe, expect, it } from 'vitest';
import { ActorStorageConflict, defineActor } from '@sigx/actors';
import {
    defineActorApp,
    memoryStorage,
    metrics,
    type ActorPlugin,
    type ActorStorage
} from '@sigx/actors/host';
import { fileStorage } from '@sigx/actors/node';
import {
    storageConformance,
    type StorageConformanceFactory
} from '@sigx/actors/testing';

const quiet = { sweepIntervalMs: 60_000, reminderTickMs: 60_000, callTimeoutMs: 0 };

/**
 * `memoryStorage` semantics over a STRING: `save` delegates to `saveText`
 * exactly as the real text-storing adapters do (#238).
 */
function textStorage(): ActorStorage {
    const records = new Map<string, { json: string; etag: string }>();
    const id = (type: string, key: string): string => `${type}\0${key}`;
    let counter = 0;
    const put = async (
        type: string,
        key: string,
        json: string,
        expectedEtag: string | null
    ): Promise<string> => {
        const existing = records.get(id(type, key));
        if ((existing?.etag ?? null) !== expectedEtag) throw new ActorStorageConflict(type, key);
        const etag = String(++counter);
        records.set(id(type, key), { json, etag });
        return etag;
    };
    return {
        async load(type, key) {
            const record = records.get(id(type, key));
            return record ? { state: JSON.parse(record.json) as unknown, etag: record.etag } : null;
        },
        save: (type, key, state, expectedEtag) =>
            put(type, key, JSON.stringify(state), expectedEtag),
        saveText: put,
        async clear(type, key, expectedEtag) {
            const existing = records.get(id(type, key));
            if (!existing && expectedEtag === null) return;
            if ((existing?.etag ?? null) !== expectedEtag) throw new ActorStorageConflict(type, key);
            records.delete(id(type, key));
        }
    };
}

const createMemory: StorageConformanceFactory = async () => ({
    storage: () => memoryStorage(),
    stop: async () => {}
});

const createFile: StorageConformanceFactory = async () => {
    const dir = await mkdtemp(join(tmpdir(), 'sigx-storage-conformance-'));
    return {
        storage: () => fileStorage({ dir }),
        stop: () => rm(dir, { recursive: true, force: true })
    };
};

const createText: StorageConformanceFactory = async () => ({
    storage: () => textStorage(),
    stop: async () => {},
    saveText: true
});

const Noop = defineActor({
    type: 'StorageConformanceNoop',
    allowAnonymous: true,
    state: () => ({}),
    methods: () => ({})
});

/**
 * The storage the HOST sees after `metrics()` has decorated a text-capable
 * one. Decorators apply in registration order and each receives the previous
 * result, so a capture plugin registered after `metrics()` is handed the
 * metrics-wrapped storage — the object the host actually calls.
 */
const createMetricsDecorated: StorageConformanceFactory = async () => {
    let decorated: ActorStorage | null = null;
    const capture: ActorPlugin = {
        name: 'capture-storage',
        setup: (registry) =>
            registry.decorateStorage((inner) => {
                decorated = inner;
                return inner;
            })
    };
    const app = defineActorApp({ actors: [Noop], storage: textStorage(), defaults: quiet })
        .use(metrics())
        .use(capture);
    await app.start();
    return {
        storage: () => {
            if (!decorated) throw new Error('the storage decorators never ran');
            return decorated;
        },
        stop: () => app.stop(),
        saveText: true
    };
};

function runSuite(title: string, create: StorageConformanceFactory, expectSkips: boolean): void {
    describe(title, () => {
        for (const conformanceCase of storageConformance) {
            it(`${conformanceCase.name} — ${conformanceCase.why}`, async (ctx) => {
                const outcome = await conformanceCase.run(create);
                if (outcome && 'skipped' in outcome) {
                    // Only the optional text path may skip, and only where the
                    // storage has no saveText. Anything else skipping is a
                    // harness bug hiding behind a reported outcome.
                    expect(expectSkips, `${conformanceCase.name} skipped: ${outcome.skipped}`).toBe(true);
                    expect(conformanceCase.name).toMatch(/saveText/);
                    ctx.skip(outcome.skipped);
                }
                expect(outcome).toBeUndefined();
            });
        }
    });
}

runSuite('storageConformance × memoryStorage (the incumbent)', createMemory, true);
runSuite('storageConformance × fileStorage', createFile, true);
runSuite('storageConformance × an in-memory text adapter', createText, false);
runSuite('storageConformance × metrics() decorating a text adapter', createMetricsDecorated, false);

/**
 * Sabotage table: each entry breaks ONE thing about a correct adapter, and
 * names the case that must catch it. The suite's failure text is the
 * assertion — every entry must reject with a `[storage conformance]` error,
 * and the named case must be the one that did.
 */
describe('a case that cannot fail is decoration — the suite goes red against a broken adapter', () => {
    const wrap = (over: (inner: ActorStorage) => ActorStorage, saveText = false) =>
        (async () => ({
            storage: () => over(memoryStorage()),
            stop: async () => {},
            saveText
        })) satisfies StorageConformanceFactory;

    const withoutBrand = (type: string, key: string) =>
        new Error(`unbranded conflict on ${type}/${key}`);

    const broken: [string, string, StorageConformanceFactory][] = [
        [
            'a miss loads as undefined',
            'load of a record that was never saved is null',
            wrap((inner) => ({
                ...inner,
                load: async (type, key) => (await inner.load(type, key)) ?? (undefined as never)
            }))
        ],
        [
            'save ignores the expected etag',
            'every save mints a fresh etag and the previous one goes stale',
            wrap((inner) => ({
                ...inner,
                save: async (type, key, state, _expected) => {
                    const current = await inner.load(type, key);
                    return inner.save(type, key, state, current?.etag ?? null);
                }
            }))
        ],
        [
            'a conflict is thrown without the brand',
            'a create over an existing record conflicts',
            wrap((inner) => ({
                ...inner,
                save: (type, key, state, expected) =>
                    inner.save(type, key, state, expected).catch(() => {
                        throw withoutBrand(type, key);
                    })
            }))
        ],
        [
            'save is an upsert',
            'an update of a record that does not exist conflicts and creates nothing',
            wrap((inner) => ({
                ...inner,
                save: async (type, key, state, expected) =>
                    (await inner.load(type, key)) === null
                        ? inner.save(type, key, state, null)
                        : inner.save(type, key, state, expected)
            }))
        ],
        [
            'clear ignores the expected etag',
            'clear is compare-and-delete; null expected asserts absence',
            wrap((inner) => ({
                ...inner,
                clear: async (type, key) => {
                    const current = await inner.load(type, key);
                    await inner.clear(type, key, current?.etag ?? null);
                }
            }))
        ],
        [
            'a refused save still writes',
            'a refused write leaves the record exactly as it was — same state, same etag',
            wrap((inner) => ({
                ...inner,
                save: async (type, key, state, expected) => {
                    try {
                        return await inner.save(type, key, state, expected);
                    } catch (error) {
                        const current = await inner.load(type, key);
                        if (current) await inner.save(type, key, state, current.etag);
                        throw error;
                    }
                }
            }))
        ],
        [
            'load hands out the stored tree by reference',
            "a loaded record is the caller's to mutate — the store never sees the change",
            wrap(() => {
                const records = new Map<string, { state: unknown; etag: string }>();
                let n = 0;
                return {
                    load: async (type, key) => records.get(`${type}/${key}`) ?? null,
                    save: async (type, key, state, expected) => {
                        const cur = records.get(`${type}/${key}`);
                        if ((cur?.etag ?? null) !== expected) throw new ActorStorageConflict(type, key);
                        const etag = String(++n);
                        records.set(`${type}/${key}`, { state, etag });
                        return etag;
                    },
                    clear: async (type, key, expected) => {
                        const cur = records.get(`${type}/${key}`);
                        if (!cur && expected === null) return;
                        if ((cur?.etag ?? null) !== expected) throw new ActorStorageConflict(type, key);
                        records.delete(`${type}/${key}`);
                    }
                };
            })
        ],
        [
            'null state reads back as a miss',
            'state is whatever the codec produced — arrays, scalars and null round-trip and stay distinct from absent',
            wrap((inner) => ({
                ...inner,
                load: async (type, key) => {
                    const record = await inner.load(type, key);
                    return record && record.state === null ? null : record;
                }
            }))
        ],
        [
            'keys are trimmed',
            'type and key together name a record; keys are opaque, NUL and separators included',
            wrap((inner) => ({
                load: (type, key) => inner.load(type, key.trim()),
                save: (type, key, state, expected) => inner.save(type, key.trim(), state, expected),
                clear: (type, key, expected) => inner.clear(type, key.trim(), expected)
            }))
        ],
        [
            'NUL in a key is replaced',
            'type and key together name a record; keys are opaque, NUL and separators included',
            wrap((inner) => {
                const esc = (key: string) => key.replace(/\0/g, '\\0');
                return {
                    load: (type, key) => inner.load(type, esc(key)),
                    save: (type, key, state, expected) => inner.save(type, esc(key), state, expected),
                    clear: (type, key, expected) => inner.clear(type, esc(key), expected)
                };
            })
        ],
        [
            'a decorator returns a fixed three-method literal over a text-capable storage',
            'saveText(json) is save(JSON.parse(json)): the same record either way',
            wrap(() => {
                const inner = textStorage();
                return {
                    load: (type, key) => inner.load(type, key),
                    save: (type, key, state, expected) => inner.save(type, key, state, expected),
                    clear: (type, key, expected) => inner.clear(type, key, expected)
                };
            }, true)
        ],
        [
            'saveText is present but not callable',
            'saveText(json) is save(JSON.parse(json)): the same record either way',
            // The host gates on truthiness and then calls it — this adapter
            // crashes the first durable save, and must not pass on a skip.
            wrap((inner) => ({
                ...inner,
                saveText: true as unknown as ActorStorage['saveText']
            }))
        ],
        [
            'saveText stores the JSON text as a string state',
            'saveText(json) is save(JSON.parse(json)): the same record either way',
            wrap((inner) => ({
                ...inner,
                saveText: (type, key, json, expected) => inner.save(type, key, json, expected)
            }))
        ],
        [
            'saveText skips the CAS',
            'saveText honours the same CAS and throws the same brand',
            wrap((inner) => ({
                ...inner,
                saveText: async (type, key, json, _expected) => {
                    const current = await inner.load(type, key);
                    return inner.save(type, key, JSON.parse(json), current?.etag ?? null);
                }
            }))
        ],
        [
            'saveText keeps its own etag chain',
            "save and saveText interleave on one record's etag chain, and clear closes it",
            wrap((inner) => ({
                ...inner,
                // Writes through, but the etag it hands back is not the one
                // the record now carries — the tree path will never honour it.
                saveText: async (type, key, json, expected) => {
                    await inner.save(type, key, JSON.parse(json), expected);
                    return `text-${Math.random()}`;
                }
            }))
        ]
    ];

    for (const [sabotage, caseName, create] of broken) {
        it(`${sabotage} → "${caseName}" goes red`, async () => {
            const conformanceCase = storageConformance.find((c) => c.name === caseName);
            expect(conformanceCase, `no case named ${caseName}`).toBeDefined();
            await expect(conformanceCase!.run(create)).rejects.toThrow(/\[storage conformance\]/);
        });
    }

    it('the incumbent passes every case the sabotage table names', () => {
        // Pins the table to the suite: a renamed case would otherwise make
        // its sabotage entry look for nothing and pass vacuously.
        const names = new Set(storageConformance.map((c) => c.name));
        for (const [, caseName] of broken) expect(names.has(caseName), caseName).toBe(true);
    });
});

describe('teardown: a failing stop() is not swallowed behind a green case', () => {
    const passing = storageConformance.find((c) => c.name === 'load of a record that was never saved is null')!;

    it('a case that passed rejects with the stop() error', async () => {
        const create: StorageConformanceFactory = async () => ({
            storage: () => memoryStorage(),
            stop: async () => {
                throw new Error('namespace cleanup failed');
            }
        });
        await expect(passing.run(create)).rejects.toThrow('namespace cleanup failed');
    });

    it("a case that failed keeps its own error when stop() fails too", async () => {
        const create: StorageConformanceFactory = async () => ({
            storage: () => ({
                ...memoryStorage(),
                load: async () => undefined as unknown as null
            }),
            stop: async () => {
                throw new Error('namespace cleanup failed');
            }
        });
        await expect(passing.run(create)).rejects.toThrow(/\[storage conformance\]/);
    });
});
