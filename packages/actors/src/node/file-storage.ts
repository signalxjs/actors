/**
 * File-based dev storage — one JSON file per actor, human-inspectable
 * (`cat` an actor's state while debugging). Atomic via temp-file + rename.
 *
 * Deliberately files, not SQLite: zero dependencies, no Node-version or
 * native-module constraints, and the concurrency SQLite buys is what a
 * single silo doesn't need (saves are serialized per actor by the mailbox;
 * an in-process chain serializes same-file access here). Production storage
 * belongs in real providers implementing the same three methods.
 *
 * UNDER VITE: if `dir` is inside the project root, add it to
 * `server.watch.ignored` (both examples in this repo do). A save is a temp
 * file plus a rename, and Vite's HMR path reads a file it has just seen
 * appear — it loses that race, surfacing as an `ENOENT … .tmp` error
 * overlay. Actor state is not source: it should not reload the page either.
 */
import { mkdir, readFile, rename, rm, writeFile } from 'node:fs/promises';
import { dirname, join } from 'node:path';
import { ActorStorageConflict } from '../errors';
import type { ActorStorage } from '../types';

interface FileRecord {
    etag: string;
    state: unknown;
}

export function fileStorage(options: { dir: string }): ActorStorage {
    const chains = new Map<string, Promise<unknown>>();

    const fileFor = (type: string, key: string) =>
        join(options.dir, encodeURIComponent(type), `${encodeURIComponent(key)}.json`);

    /** Serialize operations per file (the process-local mutex). */
    function withFile<T>(file: string, op: () => Promise<T>): Promise<T> {
        const prev = chains.get(file) ?? Promise.resolve();
        const run = prev.then(op, op);
        chains.set(
            file,
            run.catch(() => {})
        );
        return run;
    }

    async function read(file: string): Promise<FileRecord | null> {
        try {
            return JSON.parse(await readFile(file, 'utf8')) as FileRecord;
        } catch (error) {
            if ((error as { code?: string }).code === 'ENOENT') return null;
            throw error;
        }
    }

    return {
        load(type, key) {
            const file = fileFor(type, key);
            return withFile(file, async () => {
                const record = await read(file);
                return record ? { state: record.state, etag: record.etag } : null;
            });
        },
        save(type, key, state, expectedEtag) {
            const file = fileFor(type, key);
            return withFile(file, async () => {
                const existing = await read(file);
                if ((existing?.etag ?? null) !== expectedEtag) {
                    throw new ActorStorageConflict(type, key);
                }
                const etag = String((Number(existing?.etag) || 0) + 1);
                await mkdir(dirname(file), { recursive: true });
                const tmp = `${file}.${process.pid}.tmp`;
                await writeFile(tmp, JSON.stringify({ etag, state } satisfies FileRecord, null, 2));
                await rename(tmp, file);
                return etag;
            });
        },
        clear(type, key, expectedEtag) {
            const file = fileFor(type, key);
            return withFile(file, async () => {
                const existing = await read(file);
                if (!existing && expectedEtag === null) return;
                if ((existing?.etag ?? null) !== expectedEtag) {
                    throw new ActorStorageConflict(type, key);
                }
                await rm(file, { force: true });
            });
        }
    };
}
