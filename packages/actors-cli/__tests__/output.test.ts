/**
 * Where command output goes.
 *
 * This file exists because of a bug that every other test missed: the
 * commands wrote their payload through `ctx.logger`, which prefixes each
 * line with `[sigx] `. The unit tests called `renderStats()` directly and
 * the end-to-end checks called `plugin.commands.actors.run()` with their
 * own logger, so nothing ever observed what the real binary prints — and
 * `--json` emitted `[sigx] {`, which is not JSON. The documented
 * `sigx actors stats --json | jq …` could not work.
 *
 * So these assert on STDOUT, the thing a pipe actually receives.
 */
import { afterEach, describe, expect, it, vi } from 'vitest';
import { runStats } from '../src/commands/stats';
import { runHealth } from '../src/commands/health';
import type { ActorsCommandContext } from '../src/commands/context';
import type { MonitorSnapshot, MonitorSource } from '../src/source/types';

/**
 * Built inside `vi.hoisted` because the `vi.mock` factory below closes over
 * `source`, and `vi.mock` is hoisted above the imports — a plain
 * module-level `const` would be in its temporal dead zone if the factory
 * ever ran before module evaluation finished. It happens not to today, but
 * that is a property of when the mocked module is first imported, not
 * something this file should depend on.
 */
const { source } = vi.hoisted(() => {
    const snapshot: MonitorSnapshot = {
        at: 1_700_000_000_000,
        hosts: [
            {
                hostId: 'host-a',
                address: 'http://a:3000',
                status: 'active',
                uptimeMs: 65_000,
                stats: {
                    activations: 2,
                    queued: 0,
                    perType: { Cart: 2 },
                    transitional: { activating: 0, deactivating: 0 }
                },
                counters: null,
                reminderShards: [],
                membershipVersion: null,
                transports: null,
                metrics: null,
                health: null,
                activations: null
            }
        ],
        cluster: null,
        metrics: null,
        activations: null,
        health: { live: true, ready: true, fatal: false, uptimeMs: 65_000, checks: {}, host: null },
        partial: false
    };
    const source: MonitorSource = {
        kind: 'http',
        label: 'http://a:3000',
        snapshot: () => Promise.resolve(snapshot),
        close: () => Promise.resolve()
    };
    return { source };
});

/** Capture stdout, and a logger that records rather than prints. */
function capture() {
    const stdout: string[] = [];
    const logged: string[] = [];
    vi.spyOn(process.stdout, 'write').mockImplementation((chunk: unknown) => {
        stdout.push(String(chunk));
        return true;
    });
    const ctx = {
        cwd: process.cwd(),
        args: { url: 'http://a:3000' },
        logger: {
            log: (m: string) => logged.push(m),
            warn: (m: string) => logged.push(m),
            error: (m: string) => logged.push(m)
        }
    } satisfies ActorsCommandContext;
    return { stdout, logged, ctx, text: () => stdout.join('') };
}

/** `resolveSource` would open a real one; this bypasses it. */
vi.mock('../src/resolve', () => ({
    resolveSource: () => Promise.resolve(source),
    resolveAppModule: () => 'test://app'
}));

afterEach(() => {
    vi.restoreAllMocks();
    process.exitCode = 0;
});

describe('command output', () => {
    it('emits --json that actually parses', async () => {
        const c = capture();
        await runStats({ ...c.ctx, args: { ...c.ctx.args, json: true } });
        // The whole point of --json. `[sigx] {` is not JSON.
        const parsed = JSON.parse(c.text()) as MonitorSnapshot;
        expect(parsed.hosts).toHaveLength(1);
        expect(parsed.partial).toBe(false);
    });

    it('writes the table to stdout with no prefix', async () => {
        const c = capture();
        await runStats(c.ctx);
        const text = c.text();
        expect(text).toContain('host-a');
        // Seven characters of noise in front of every row would also break
        // the column alignment the table works to produce.
        expect(text).not.toContain('[sigx]');
        // The first line is the source label, not a prefix.
        expect(text.split('\n')[0]).toBe('http://a:3000  2023-11-14T22:13:20.000Z');
    });

    it('keeps the payload out of the logger', async () => {
        const c = capture();
        await runStats(c.ctx);
        // Diagnostics go through the logger (and so to stderr); the answer
        // does not. That is what makes `2>/dev/null` do the useful thing.
        expect(c.logged).toEqual([]);
    });

    it('emits health as parseable JSON too', async () => {
        const c = capture();
        await runHealth({ ...c.ctx, args: { ...c.ctx.args, json: true } });
        const parsed = JSON.parse(c.text()) as { ready: boolean };
        expect(parsed.ready).toBe(true);
        expect(process.exitCode).toBe(0);
    });

    it('writes the human form of health to stdout, and still sets the exit code', async () => {
        const c = capture();
        await runHealth(c.ctx);
        expect(c.text()).toContain('live / ready');
        expect(c.text()).not.toContain('[sigx]');
        expect(process.exitCode).toBe(0);
    });
});
