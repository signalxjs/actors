/**
 * `@sigx/actors-pg` — storage, membership and the directory all riding one
 * Postgres pool, on the same three-host cluster.
 *
 *     PG_URL=postgres://postgres:postgres@localhost:5432/postgres \
 *       pnpm --filter providers-example pg           # after `pnpm build`
 *
 * Env-gated the way the package's own tests are: no `PG_URL`, no run. The
 * seam swap is three lines — `pgStorage` for the store, `pgCluster` for
 * membership + directory — and one that people forget: `ensurePgSchema`.
 * DDL is explicit here. The providers never issue it, so a production role
 * needs DML grants only, and every replica may run the bootstrap at boot.
 */
import pg from 'pg';
import { ensurePgSchema, pgCluster, pgSchemaSql, pgStorage } from '@sigx/actors-pg';
import { gate } from './src/gate.ts';
import { startCluster } from './src/cluster.ts';

const url = gate(
    {
        demo: 'pg',
        env: 'PG_URL',
        needs: 'a Postgres >= 13',
        howTo: [
            'docker run --rm -p 5432:5432 -e POSTGRES_PASSWORD=postgres postgres:16',
            'PG_URL=postgres://postgres:postgres@localhost:5432/postgres pnpm --filter providers-example pg'
        ]
    },
    process.env
);
if (!url) process.exit(0);

const schema = process.env.PG_SCHEMA ?? 'sigx_demo';
const pool = new pg.Pool({ connectionString: url });

console.log(`\n=== 0. Schema — DDL is explicit; the providers never issue it ===`);
const statements = pgSchemaSql(schema).split(';').filter((s) => s.trim()).length;
await ensurePgSchema(pool, { schema });
console.log(
    `ensurePgSchema(pool, { schema: '${schema}' }) ran ${statements} idempotent statements ` +
        `under pg_advisory_xact_lock`
);
console.log('(safe from every replica at boot, concurrently; re-running is a no-op)');

const demo = await startCluster({
    label: 'Postgres',
    storage: pgStorage({ pool, schema }),
    // Short cadences so the walk is brisk. Expiry is judged on the DATABASE
    // clock (`now()`), never a host's, so skew cannot fake a death.
    providers: () => pgCluster({ pool, schema, heartbeatMs: 1000, ttlMs: 3000, pollMs: 1000 })
});

await demo.spread();
const { owner } = await demo.singleActivation();
await demo.crossHost(owner);
const { survivor } = await demo.failover(owner);

demo.step(`The tables — what the walk left in ${schema}`);
const cart = await pool.query(
    `SELECT type, key, etag, state FROM ${schema}.state WHERE type = $1 AND key = $2`,
    ['Counter', demo.key('cart')]
);
console.log(`state row for '${demo.key('cart')}':`, cart.rows[0]);
const rows = await pool.query(`SELECT count(*)::int AS n FROM ${schema}.state WHERE key LIKE $1`, [
    `%-${demo.run}`
]);
console.log(`${rows.rows[0].n} actor rows from this run (state is JSON in a text column, one row per actor)`);
const hosts = await pool.query(
    `SELECT host_id, expires_at > now() AS live FROM ${schema}.hosts ORDER BY host_id`
);
console.log(
    `hosts table: ${hosts.rows.map((r) => `${r.host_id}${r.live ? '' : ' (expired)'}`).join(', ')}`
);
console.log(`(${demo.hostId(owner)} left gracefully, so its row is gone; a crash would sit there until expires_at)`);

await demo.report(survivor);
await demo.stop();
await pool.end();
console.log('\nPG DEMO COMPLETE — storage, membership and directory on one Postgres pool.');
