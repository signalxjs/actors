/**
 * `sigx actors top` — the dashboard.
 *
 * Hosted by `runShell` from `@sigx/cli/shell`, which exists for exactly
 * this: a long-running plugin command that owns the screen. Using it rather
 * than mounting our own app is what makes the tabs, the status line, the
 * slash-command palette and the teardown consistent with every other sigx
 * tool, and what lets other plugins' contributions merge in alongside ours.
 */
import { runShell, type ShellHandle } from '@sigx/cli/shell';
import { signal } from '@sigx/terminal';
import type { ActorsCommandContext } from './context';
import { out } from './out';
import { resolveSource } from '../resolve';
import { DashboardState } from '../dashboard/state';
import {
    ClusterScreen,
    GrainsScreen,
    HealthScreen,
    OverviewScreen,
    SilosScreen
} from '../dashboard/screens';
import { moveCursor } from '../tui/table';
import { count, uptime } from '../model/format';

export async function runTop(ctx: ActorsCommandContext): Promise<void> {
    const source = await resolveSource(ctx.cwd, ctx.args);
    const state = new DashboardState({
        source,
        intervalMs: ctx.args.interval
    });

    // Cursors are per-table and live here rather than in DashboardState:
    // they are view state, and a poll must never move somebody's selection.
    const siloCursor = signal({ index: 0 });
    const grainCursor = signal({ index: 0 });

    /**
     * Move BOTH cursors, rather than only the visible tab's.
     *
     * `ShellHandle` exposes `switchTab` but not which tab is showing, so
     * there is no way to ask. Moving both is not a compromise: each cursor
     * is clamped to its own table's length, only one table is on screen, and
     * a selection that survives tab-switching is what you would want anyway.
     * The alternative — tracking the active tab ourselves — would be a
     * second source of truth that the shell's own `1`–`9` keys silently
     * desynchronise.
     */
    const move = (delta: number): void => {
        const snapshot = state.view.snapshot;
        const silos = snapshot?.silos.length ?? 0;
        const grains = snapshot?.activations?.length ?? 0;
        siloCursor.index = moveCursor(siloCursor.index, delta, silos);
        grainCursor.index = moveCursor(grainCursor.index, delta, grains);
    };



    const handle = await runShell({
        mode: 'fullscreen',
        title: `actors — ${source.label}`,
        version: ctx.cliVersion,
        // Merged in from every other discovered plugin, so `sigx actors top`
        // in an app that also has, say, a dev-server plugin shows both.
        plugins: ctx.plugins,
        tabs: [
            { id: 'overview', label: 'Overview', render: () => OverviewScreen({ state }) },
            {
                id: 'silos',
                label: 'Silos',
                render: () => SilosScreen({ state, cursor: siloCursor.index })
            },
            {
                id: 'grains',
                label: 'Grains',
                render: () => GrainsScreen({ state, cursor: grainCursor.index })
            },
            { id: 'cluster', label: 'Cluster', render: () => ClusterScreen({ state }) },
            { id: 'health', label: 'Health', render: () => HealthScreen({ state }) }
        ],
        status: () => {
            const view = state.view;
            const stale = view.lastOk === 0 ? null : Date.now() - view.lastOk;
            return [
                { label: 'src', value: source.kind },
                { label: 'every', value: `${view.intervalMs}ms` },
                ...(view.paused ? [{ label: 'state', value: 'PAUSED', tone: 'warn' }] : []),
                ...(view.error ? [{ label: 'poll', value: 'FAILING', tone: 'danger' }] : []),
                ...(stale !== null && stale > view.intervalMs * 3
                    ? // Stale data that still looks live is the failure mode
                      // this whole status line exists to prevent.
                      [{ label: 'age', value: uptime(stale), tone: 'warn' }]
                    : []),
                ...(state.view.snapshot?.partial ? [{ label: 'totals', value: 'PARTIAL', tone: 'warn' }] : [])
            ];
        },
        shortcuts: [
            { key: 'p', label: 'pause', run: () => state.togglePause() },
            { key: 'r', label: 'refresh', run: () => void state.poll() },
            { key: '+', label: 'slower', run: () => state.nudgeInterval(2) },
            { key: '-', label: 'faster', run: () => state.nudgeInterval(0.5) },
            { key: 'j', label: 'down', run: () => move(1) },
            { key: 'k', label: 'up', run: () => move(-1) }
        ],
        commands: [
            {
                name: '/reset',
                description: 'clear the sparkline history',
                run: (sh) => {
                    state.calls.clear();
                    state.failures.clear();
                    state.queued.clear();
                    state.activations.clear();
                    sh.say('history cleared');
                }
            },
            {
                name: '/interval',
                description: 'show the current poll interval',
                run: (sh) => sh.say(`polling every ${state.view.intervalMs}ms`)
            }
        ],
        onReady(sh: ShellHandle) {

            state.start();
            sh.say(`watching ${source.label} (${source.kind})`);
        },
        async onExit() {
            await state.stop();
        }
    });



    // Non-TTY (piped, CI): the shell renders one frame at teardown, so an
    // interactive dashboard would exit before it had anything to show.
    // Poll once, print that, and leave — the same reason `stats` exists.
    if (!handle.isInteractive) {
        await state.poll();
        const view = state.view;
        const snapshot = view.snapshot;
        if (view.error) ctx.logger.error(view.error);
        else if (snapshot) {
            out(
                `${source.label}  ${snapshot.silos.length} silo(s), ` +
                    `${count(snapshot.silos.reduce((n, s) => n + s.stats.activations, 0))} activations`
            );
        }
        // Non-zero when we could not read the silo. Exiting 0 after logging
        // an error would make a piped or CI invocation treat a connection
        // failure as success — the one thing an exit code is FOR. A missing
        // snapshot counts too: no error and no data still means we learned
        // nothing.
        handle.exit(view.error || !snapshot ? 2 : 0);
    }
}
