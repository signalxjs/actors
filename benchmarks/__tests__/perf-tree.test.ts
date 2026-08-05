// @vitest-environment node
/**
 * The Tier-3 scenarios reach outside `benchmarks/` for exactly one thing:
 * `perf/aks/deploy/edge-ladder.mjs`, which is read off disk, base64'd and
 * executed on a same-region VM.
 *
 * That edge has no compiler behind it. The path is a runtime `new URL(…)`,
 * the scenarios are gated on `BENCH_INFRA` plus four env vars, and the whole
 * tier is skipped without them — so a directory move can break it and every
 * local run, every CI run and `pnpm test` stay green. The next place it
 * would surface is a paid cluster, in the middle of a session someone is
 * paying for by the minute.
 *
 * Hence this file: assert the edge in the one suite that always runs.
 */
import { existsSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { describe, expect, it } from 'vitest';
import { spawnable } from '../src/spawn.mjs';

describe('the Tier-3 edge into perf/', () => {
    it('can still find the edge ladder it ships to the load VM', () => {
        // The same expression `infra.ts` resolves LADDER_PATH with — keep the
        // two in step, or this asserts nothing.
        const ladder = fileURLToPath(
            new URL('../../perf/aks/deploy/edge-ladder.mjs', import.meta.url)
        );
        expect(existsSync(ladder)).toBe(true);
    });

    it('has spawnable available locally rather than across trees', () => {
        // It used to live in the perf tree and be imported from here, which
        // is what made moving that tree break `pnpm bench` outright.
        // Shape differs by platform — POSIX passes the argv through, while
        // Windows resolves the `.cmd` shim and escapes each argument into a
        // single `cmd.exe /d /s /c "…"` string. Assert only what both must
        // carry: the command is reachable and no argument was dropped.
        const { command, args } = spawnable('az', ['group', 'list']);
        expect(command).toMatch(/az|cmd/i);
        const line = [command, ...args].join(' ');
        expect(line).toContain('group');
        expect(line).toContain('list');
    });
});
