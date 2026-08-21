/**
 * The INFRA_SHAPE comparison contract lives in ONE place — `src/shape.mjs`,
 * plain ESM so `perf/aks/deploy/testenv.mjs` can import it at runtime on
 * any Node in the CI matrix — and `env.ts`'s `envMismatch` must route
 * through it. The conformance case at the bottom is what keeps the two
 * from drifting apart: a second copy of "how do two shapes compare" is
 * exactly how a hand-run and a recorded run would start disagreeing about
 * whether a comparison is valid.
 */
import { describe, expect, it } from 'vitest';
import { INFRA_SHAPE_MISMATCH, shapeMismatch } from '../src/shape.mjs';
import { captureEnv, envMismatch } from '../src/env.ts';

describe('shapeMismatch', () => {
    it('returns null for identical shapes', () => {
        expect(shapeMismatch('ws replicas=3 nodes=3 image=abc', 'ws replicas=3 nodes=3 image=abc'))
            .toBeNull();
    });

    it('normalizes absent and empty to the same claim', () => {
        // A baseline recorded before the field existed has it undefined,
        // which must read as "no shape", not as a mismatch against ''.
        expect(shapeMismatch(undefined, '')).toBeNull();
        expect(shapeMismatch('', undefined)).toBeNull();
        expect(shapeMismatch(undefined, undefined)).toBeNull();
    });

    it('reports a mismatch with the fatal prefix and both shapes verbatim', () => {
        const line = shapeMismatch('replicas=3 image=old', 'replicas=3 image=new');
        expect(line).toBe(`${INFRA_SHAPE_MISMATCH} replicas=3 image=old vs replicas=3 image=new`);
    });

    it('names an absent side as (none)', () => {
        expect(shapeMismatch('', 'replicas=3')).toBe(`${INFRA_SHAPE_MISMATCH} (none) vs replicas=3`);
        expect(shapeMismatch('replicas=3', undefined)).toBe(
            `${INFRA_SHAPE_MISMATCH} replicas=3 vs (none)`
        );
    });

    it('is the same contract envMismatch enforces', () => {
        const base = captureEnv();
        const a = { ...base, infraShape: 'replicas=3 nodes=3 image=old' };
        const b = { ...base, infraShape: 'replicas=3 nodes=3 image=new' };
        // Environments identical except for the shape: the one differing
        // line must be exactly what shapeMismatch says.
        expect(envMismatch(a, b)).toEqual([shapeMismatch(a.infraShape, b.infraShape)]);
        expect(envMismatch(a, { ...a })).toEqual([]);
    });
});
