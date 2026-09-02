/**
 * `@sigx/actors-surreal` — storage, membership and the directory all riding
 * one SurrealDB connection, on the same three-host cluster.
 *
 *     SURREAL_URL=ws://127.0.0.1:8000 \
 *       pnpm --filter providers-example surreal      # after `pnpm build`
 *
 * Env-gated the way the package's own tests are: no `SURREAL_URL`, no run.
 * The seam swap is the same three lines as Postgres — `surrealStorage`,
 * `surrealCluster`, `ensureSurrealSchema` — plus the one that is NOT
 * optional on a connection you build yourself: `surrealRetryable`.
 * SurrealDB has no `SELECT … FOR UPDATE`, no `SKIP LOCKED` and no advisory
 * lock, so a commit-time write–write conflict is the only mutual exclusion
 * there is, and the loser has to re-run to observe the winner. The SDK
 * ships retry DISABLED, with a predicate that never matches. Pass `url`
 * instead of `db` and the package installs all of this for you; this demo
 * builds the connection by hand to show what that means.
 */
import { RecordId, Surreal } from 'surrealdb';
import {
    ensureSurrealSchema,
    surrealCluster,
    surrealRetryable,
    surrealStorage
} from '@sigx/actors-surreal';
import { gate } from './src/gate.ts';
import { startCluster } from './src/cluster.ts';

const url = gate(
    {
        demo: 'surreal',
        env: 'SURREAL_URL',
        needs: 'a SurrealDB >= 3.0 (3.2.4 or newer recommended) on a ws:// endpoint',
        howTo: [
            'docker run --rm -p 8000:8000 surrealdb/surrealdb:latest start --user root --pass root memory',
            'SURREAL_URL=ws://127.0.0.1:8000 pnpm --filter providers-example surreal'
        ]
    },
    process.env
);
if (!url) process.exit(0);

const namespace = process.env.SURREAL_NS ?? 'sigx_demo';
const database = process.env.SURREAL_DB ?? 'demo';
const db = new Surreal();
await db.connect(url, {
    namespace,
    database,
    authentication: {
        username: process.env.SURREAL_USER ?? 'root',
        password: process.env.SURREAL_PASS ?? 'root'
    },
    // Unlimited reconnect: the SDK's default gives up after five attempts,
    // after which a membership heartbeat beats into a dead socket forever.
    reconnect: { enabled: true, attempts: -1 },
    // Retry is part of the contract — see the header.
    retry: { enabled: true, attempts: 5, retryable: surrealRetryable }
});

console.log(`\n=== 0. Schema — the DDL step is mandatory in SurrealDB 3 ===`);
// `connect()` SELECTS a namespace/database; it does not create them. That
// is a deployment decision (and needs root), so the package leaves it to
// you — this demo does it because it runs as root against a throwaway.
await db.query(
    `DEFINE NAMESPACE IF NOT EXISTS ${namespace}; USE NS ${namespace}; ` +
        `DEFINE DATABASE IF NOT EXISTS ${database};`
);
await ensureSurrealSchema(db);
console.log(`ensureSurrealSchema(db) defined the sigx_* tables in ${namespace}/${database}`);
console.log(
    '(reading an undefined table ERRORS in v3, so this cannot be skipped; concurrent boots converge by retry)'
);

const demo = await startCluster({
    label: 'SurrealDB',
    storage: surrealStorage({ db }),
    // Short cadences so the walk is brisk. Expiry is judged on the DATABASE
    // clock (`time::now()`), never a host's.
    providers: () => surrealCluster({ db, heartbeatMs: 1000, ttlMs: 3000, pollMs: 1000 })
});

await demo.spread();
const { owner } = await demo.singleActivation();
await demo.crossHost(owner);
const { survivor } = await demo.failover(owner);

demo.step(`The records — what the walk left in ${namespace}/${database}`);
// A COMPOSITE record id, `sigx_state:[type, key]`: the primary index, so no
// load or save ever scans. The state is a JSON STRING (`s`), so top-level
// arrays, scalars, null and NUL round-trip exactly.
const [cart] = await db.query('SELECT * FROM ONLY $id', {
    id: new RecordId('sigx_state', ['Counter', demo.key('cart')])
});
console.log(`state record for '${demo.key('cart')}':`, cart);
const [[{ n }]] = await db.query(
    'SELECT count() AS n FROM sigx_state WHERE string::ends_with(id[1], $run) GROUP ALL',
    { run: `-${demo.run}` }
);
console.log(`${n} actor records from this run`);
const [hosts] = await db.query('SELECT id, x > time::now() AS live FROM sigx_host ORDER BY id');
console.log(
    `host records: ${hosts.map((r) => `${r.id.id}${r.live ? '' : ' (expired)'}`).join(', ')}`
);
console.log(`(${demo.hostId(owner)} left gracefully, so its record is gone; a crash would sit there until x)`);

await demo.report(survivor);
await demo.stop();
await db.close();
console.log('\nSURREAL DEMO COMPLETE — storage, membership and directory on one SurrealDB connection.');
