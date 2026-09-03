/**
 * `surrealStorage` — the `ActorStorage` seam over SurrealDB, with
 * optimistic concurrency on a client-minted etag.
 *
 * One record per actor at `{prefix}state:[type, key]`. A COMPOSITE record id,
 * not a secondary index: the array id is v3's partitioning tool, so every
 * load and save is a primary-index hit with no `WHERE` scan anywhere, and
 * `[type, …]` stays range-scannable for operators without costing a write.
 *
 * Each operation is ONE statement, therefore one implicit transaction and
 * one round trip, and each write answers with a bare boolean — the
 * verdict, not the row. `array::len(…) > 0` collapses the result
 * server-side so a rejected CAS costs nothing on the wire.
 *
 * The append seam (#312) is the `l` array field on the same record:
 * `UPDATE $id SET e = $e, l += $l WHERE e = $x` pushes one entry under the
 * same CAS (`+=` on an array field appends; on a record that predates the
 * field, where `l` is NONE, it creates the one-element array), the CAS
 * save sets `l = []` in the statement that writes the snapshot, and
 * `CREATE` starts it empty. Entries are JSON STRINGS like the state, for
 * the same reasons as below.
 *
 * **`UPDATE … WHERE`, deliberately not `UPSERT … WHERE`.** `UPSERT`'s create
 * arm carries no condition check, so a writer holding a stale etag whose
 * record had since been deleted would RESURRECT it. `UPDATE` never creates,
 * so "record missing" and "etag mismatch" both land as an empty result —
 * and both are conflicts under this seam's contract, which is why that path
 * needs no `record::exists` disambiguation.
 *
 * State is stored as a JSON STRING, deliberately not as a native document.
 * Actor state is whatever the codec produced: it may be a top-level array or
 * scalar, may contain NUL, and distinguishes `null` from absent. Round
 * -tripping that through SurrealDB's value model would risk `none`/`null`
 * conflation, datetime and record-id reinterpretation, and v3's collapsing
 * of differently-typed numeric ids into one key. The serialized form has
 * none of those hazards and costs one `JSON.stringify` rather than a
 * per-field CBOR walk on the hot path.
 *
 * No NUL escaping layer: `type`/`key` ride a record-id array, and SurrealDB
 * carries a NUL in a string verbatim and injectively. (Postgres `text`
 * cannot, which is the only reason `@sigx/actors-pg` has `pgText`.)
 */
import { ActorStorageConflict, type ActorStorage, type ActorStorageRecord } from '@sigx/actors';
import { RecordId } from 'surrealdb';
import { surrealHandle, tablesFor, type SurrealConnectionOptions } from './connection';

export type SurrealStorageOptions = SurrealConnectionOptions;

const LOAD = `SELECT e, s, l FROM ONLY $id`;
/** Create-if-absent. Two racers both observe `!exists` and both CREATE; the
 *  commit-time write–write check aborts one, which retries, sees the record
 *  and reports the conflict truthfully (see `surrealRetryable`). */
const CREATE =
    `RETURN IF record::exists($id) { false } ` +
    `ELSE { CREATE $id CONTENT { e: $e, s: $s, l: [] } RETURN NONE; true }`;
const CAS = `RETURN array::len((UPDATE $id SET e = $e, s = $s, l = [] WHERE e = $x RETURN VALUE e)) > 0`;
const APPEND = `RETURN array::len((UPDATE $id SET e = $e, l += $l WHERE e = $x RETURN VALUE e)) > 0`;
const CAD = `RETURN array::len((DELETE $id WHERE e = $x RETURN BEFORE)) > 0`;
const EXISTS = `RETURN record::exists($id)`;

export function surrealStorage(options: SurrealStorageOptions): ActorStorage {
    const db = surrealHandle(options);
    const t = tablesFor(options.prefix);
    const rid = (type: string, key: string): RecordId => new RecordId(t.state, [type, key]);

    // The one CAS body, reached with JSON either way — from the host's own
    // single-walk emitter via `saveText`, or from a tree `save` stringifies
    // itself (#238). State is stored as a JSON STRING here by design (see
    // the header), so `saveText` hands the column exactly what it wanted,
    // one walk earlier. A local function rather than `this.saveText`, so the
    // two halves stay wired together even if the storage is destructured.
    async function put(
        type: string,
        key: string,
        s: string,
        expectedEtag: string | null
    ): Promise<string> {
        const etag = globalThis.crypto.randomUUID();
        const id = rid(type, key);
        const [written] =
            expectedEtag === null
                ? await db.query<[boolean]>(CREATE, { id, e: etag, s })
                : await db.query<[boolean]>(CAS, { id, e: etag, s, x: expectedEtag });
        if (written !== true) throw new ActorStorageConflict(type, key);
        return etag;
    }

    return {
        async load(type, key): Promise<ActorStorageRecord | null> {
            const [row] = await db.query<[{ e: string; s: string; l?: string[] | null } | null]>(LOAD, {
                id: rid(type, key)
            });
            if (!row) return null;
            return {
                state: JSON.parse(row.s) as unknown,
                etag: row.e,
                // `l` is NONE on a record written before the field existed.
                log: (row.l ?? []).map((entry) => JSON.parse(entry) as unknown)
            };
        },

        save: (type, key, state, expectedEtag) =>
            put(type, key, JSON.stringify(state), expectedEtag),
        saveText: put,
        async appendText(type, key, l, expectedEtag): Promise<string> {
            const etag = globalThis.crypto.randomUUID();
            const [written] = await db.query<[boolean]>(APPEND, { id: rid(type, key), e: etag, l, x: expectedEtag });
            if (written !== true) throw new ActorStorageConflict(type, key);
            return etag;
        },

        async clear(type, key, expectedEtag): Promise<void> {
            const id = rid(type, key);
            if (expectedEtag === null) {
                // "Assert absence": clearing with no etag is only valid when
                // nothing is stored. A record here means someone wrote after
                // the caller last read.
                const [exists] = await db.query<[boolean]>(EXISTS, { id });
                if (exists === true) throw new ActorStorageConflict(type, key);
                return;
            }
            const [removed] = await db.query<[boolean]>(CAD, { id, x: expectedEtag });
            if (removed !== true) throw new ActorStorageConflict(type, key);
        }
    };
}
