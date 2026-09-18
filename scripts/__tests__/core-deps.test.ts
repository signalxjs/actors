import { describe, expect, it } from 'vitest';
import { alignSpec } from '../lib/core-deps.mjs';

// `sync:core` must pin ONE minor (the single-copy guarantee), but a patch
// floor inside that minor is deliberate: it names the release an import first
// exists on. Lowering it is how #293 proposed dropping `deepTrack` (#459).
describe('alignSpec', () => {
    it('keeps a patch floor that is already inside the target minor', () => {
        expect(alignSpec('^0.15.3', '^0.15.0')).toBe('^0.15.3');
        expect(alignSpec('^0.15.6', '^0.15.0')).toBe('^0.15.6');
        expect(alignSpec('^0.15.0', '^0.15.0')).toBe('^0.15.0');
    });

    it('rewrites an entry on another minor to the target', () => {
        expect(alignSpec('^0.14.9', '^0.15.0')).toBe('^0.15.0');
        expect(alignSpec('^0.16.2', '^0.15.0')).toBe('^0.15.0');
        expect(alignSpec('^1.15.2', '^0.15.0')).toBe('^0.15.0');
    });

    it('rewrites anything that is not a plain caret range', () => {
        expect(alignSpec('>=0.14.0 <0.16.0', '^0.15.0')).toBe('^0.15.0');
        expect(alignSpec('0.15.3', '^0.15.0')).toBe('^0.15.0');
        expect(alignSpec('~0.15.3', '^0.15.0')).toBe('^0.15.0');
        expect(alignSpec('^0.15.3-beta.1', '^0.15.0')).toBe('^0.15.0');
    });
});
