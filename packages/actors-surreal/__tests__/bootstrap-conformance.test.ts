/**
 * The shared bootstrap conformance suite, run against a REAL SurrealDB.
 *
 * This is the case #76 came from, and it can only be proved on a real server:
 * the conflict is raised by the commit-time write–write check, which no fake
 * reproduces honestly. The deterministic mechanics live in `schema.test.ts`.
 *
 * Each racer opens its OWN socket with the SDK's DEFAULT retry posture
 * (disabled) and skips the namespace DDL — see `ConnectOptions` in
 * `helpers.ts` for why both matter.
 */
import { describe, expect, it } from 'vitest';
import type {
    BootstrapConformanceFactory,
    BootstrapConformanceHarness
} from '@sigx/actors/testing';
import { bootstrapConformance, BOOTSTRAP_RACERS } from '@sigx/actors/testing';
import { ensureSurrealSchema, surrealStorage } from '@sigx/actors-surreal';
import { connect, dropNamespace, testNamespace, type TestDb } from './helpers';

const SURREAL_URL = process.env.SURREAL_URL;

const createSurrealBootstrap: BootstrapConformanceFactory =
    async (): Promise<BootstrapConformanceHarness> => {
        const namespace = testNamespace();
        // The admin connection creates the throwaway namespace and reads back
        // through storage; it never bootstraps, so it cannot serialise the
        // racers by sharing their socket.
        const admin = await connect(namespace);
        // Every racer's socket is up BEFORE the race, so `bootstrap()` issues
        // the DDL and nothing else. Measured on 3.2.4: connecting inside
        // `bootstrap()` staggers eight racers enough to produce zero
        // conflicts, where eight pre-opened ones produce six.
        const ready: TestDb[] = await Promise.all(
            Array.from({ length: BOOTSTRAP_RACERS }, () =>
                // `define: false` — the namespace DDL is the SAME race
                // (measured: 1/8), and a collision there would fail the case
                // for a reason that is not the thing under test.
                // `retry: false` — the SDK's default, and #76's environment.
                connect(namespace, 'test', { define: false, retry: false })
            )
        );
        let next = 0;
        return {
            async bootstrap() {
                const db = ready[next++];
                if (!db) throw new Error('bootstrap conformance: ran out of pre-opened racers');
                await ensureSurrealSchema(db);
            },
            storage: () => surrealStorage({ db: admin }),
            async stop() {
                await Promise.allSettled(ready.map((db) => db.close()));
                await dropNamespace(admin, namespace);
            }
        };
    };

describe.skipIf(!SURREAL_URL)('bootstrap conformance: ensureSurrealSchema()', () => {
    for (const testCase of bootstrapConformance) {
        it(testCase.name, async (ctx) => {
            const outcome = await testCase.run(createSurrealBootstrap);
            if (outcome && 'skipped' in outcome) {
                ctx.skip(outcome.skipped);
            }
            expect(outcome).toBeUndefined();
        });
    }
});

describe.runIf(!SURREAL_URL)('bootstrap conformance (surreal)', () => {
    it('skips the SurrealDB suite when SURREAL_URL is not set', () => {
        expect(SURREAL_URL).toBeUndefined();
    });
});
