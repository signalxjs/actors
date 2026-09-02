import { describe, expect, it, vi } from 'vitest';
import { gate, skipMessage, type GateSpec } from '../src/gate.ts';

const spec: GateSpec = {
    demo: 'pg',
    env: 'PG_URL',
    needs: 'a Postgres >= 13',
    howTo: ['docker run --rm -p 5432:5432 postgres:16', 'PG_URL=postgres://… pnpm --filter providers-example pg']
};

describe('gate', () => {
    it('returns the value and prints nothing when the variable is set', () => {
        const log = vi.spyOn(console, 'log').mockImplementation(() => {});
        try {
            expect(gate(spec, { PG_URL: 'postgres://x' })).toBe('postgres://x');
            expect(log).not.toHaveBeenCalled();
        } finally {
            log.mockRestore();
        }
    });

    it('prints the skip message and returns null when it is unset or empty', () => {
        const log = vi.spyOn(console, 'log').mockImplementation(() => {});
        try {
            expect(gate(spec, {})).toBeNull();
            expect(gate(spec, { PG_URL: '' })).toBeNull();
            expect(log).toHaveBeenCalledTimes(2);
            expect(log).toHaveBeenCalledWith(skipMessage(spec));
        } finally {
            log.mockRestore();
        }
    });

    /**
     * A skip is a message, not a silent exit: the text has to tell the
     * reader which variable, what must be behind it, and how to get there.
     * Each how-to line is quoted verbatim so the README's copy of the
     * message and the demo's cannot drift apart on a paraphrase.
     */
    it('names the variable, the requirement and every how-to line', () => {
        const text = skipMessage(spec);
        const lines = text.split('\n');
        expect(lines[0]).toBe('[providers] pg demo SKIPPED: PG_URL is not set.');
        expect(lines[1]).toContain('a Postgres >= 13');
        for (const line of spec.howTo) expect(text).toContain(line);
        expect(text).toContain('@sigx/actors-pg test suite');
    });
});
