/**
 * `sigx actors top` — the dashboard.
 *
 * Hosted by `runShell` from `@sigx/cli/shell`, which exists for exactly
 * this: a long-running plugin command that owns the screen. Using it rather
 * than mounting our own app is what makes the tabs, the status line, the
 * slash-command palette and the teardown consistent with every other sigx
 * tool, and what lets other plugins' contributions merge in alongside ours.
 */
import { runShell, type ShellHandle, type ShellPane } from '@sigx/cli/shell';
import { createModel, moveCursor, signal, type Model } from '@sigx/terminal';
import type { ActorsCommandContext } from './context';
import { out } from './out';
import { resolveSource } from '../resolve';
import { DashboardState } from '../dashboard/state';
import {
    ClusterScreen,
    GrainsScreen,
    HealthScreen,
    OverviewScreen,
    SiloScreen,
    SilosScreen
} from '../dashboard/screens';
import { count, uptime } from '../model/format';

/** The tabs that own a cursor. */
const SILOS_TAB = 'silos';
const GRAINS_TAB = 'grains';

export async function runTop(ctx: ActorsCommandContext): Promise<void> {
    const source = await resolveSource(ctx.cwd, ctx.args);
    const state = new DashboardState({
        source,
        intervalMs: ctx.args.interval
    });

    // Cursors are per-table and live here rather than in DashboardState:
    // they are view state, and a poll must never move somebody's selection.
    //
    // Each is handed to its `DataTable` as a MODEL rather than as a plain
    // index. The table reads it to mark the row and to window around it,
    // while the shell's own shortcuts stay the thing that MOVES it — so `r`
    // keeps meaning refresh rather than the table's reverse-sort, and we do
    // not depend on focus routing inside a shell we do not own.
    const cursors: Record<string, { index: number }> = {
        [SILOS_TAB]: signal({ index: 0 }),
        [GRAINS_TAB]: signal({ index: 0 })
    };
    const modelFor = (tab: string): Model<number> =>
        createModel<number>([cursors[tab]!, 'index'], (value) => {
            cursors[tab]!.index = value;
        });
    const siloCursor = modelFor(SILOS_TAB);
    const grainCursor = modelFor(GRAINS_TAB);

    /** Rows the tab's table currently holds — the clamp for its cursor. */
    const lengthOf = (tab: string): number => {
        const snapshot = state.view.snapshot;
        if (tab === GRAINS_TAB) return snapshot?.activations?.length ?? 0;
        return snapshot?.silos.length ?? 0;
    };

    /**
     * Move the VISIBLE tab's cursor.
     *
     * This used to move EVERY cursor at once, because `ShellHandle` could be
     * told which tab to switch to but could not say which one was showing,
     * and a plugin-side copy desynchronised silently the moment somebody
     * pressed the shell's own `1`–`9`. `@sigx/cli` 0.9 answers it directly
     * (signalxjs/cli#88), so the workaround is gone.
     */
    const move = (shell: ShellHandle, delta: number): void => {
        const tab = shell.activeTab;
        const cursor = cursors[tab];
        if (!cursor) return;
        cursor.index = moveCursor(cursor.index, delta, lengthOf(tab));
    };

    const handle = await runShell({
        mode: 'fullscreen',
        title: `actors — ${source.label}`,
        version: ctx.cliVersion,
        // Merged in from every other discovered plugin, so `sigx actors top`
        // in an app that also has, say, a dev-server plugin shows both.
        plugins: ctx.plugins,
        tabs: [
            // Every screen is handed the pane it has to fill — the terminal
            // minus the shell's own chrome. Ignoring it means guessing, and
            // guessing means either clipping or leaving most of the screen
            // empty; this dashboard did the latter until `@sigx/cli` 0.9.
            {
                id: 'overview',
                label: 'Overview',
                render: (pane: ShellPane) => OverviewScreen({ state, pane })
            },
            {
                id: SILOS_TAB,
                label: 'Silos',
                // One tab, two views: the list, and the selected silo in
                // full. Swapped here rather than through `pushView` because
                // the drill-down is a different rendering of the SAME
                // selection — `esc` returns to the list with the cursor
                // where it was, and the tab strip does not grow a sixth
                // entry that is only sometimes meaningful.
                render: (pane: ShellPane) =>
                    state.view.focus
                        ? SiloScreen({ state, pane, siloId: state.view.focus })
                        : SilosScreen({ state, pane, cursor: siloCursor })
            },
            {
                id: GRAINS_TAB,
                label: 'Grains',
                render: (pane: ShellPane) => GrainsScreen({ state, pane, cursor: grainCursor })
            },
            {
                id: 'cluster',
                label: 'Cluster',
                render: (pane: ShellPane) => ClusterScreen({ state, pane })
            },
            {
                id: 'health',
                label: 'Health',
                render: (pane: ShellPane) => HealthScreen({ state, pane })
            }
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
            { key: 'j', label: 'down', run: (sh) => move(sh, 1) },
            { key: 'k', label: 'up', run: (sh) => move(sh, -1) },
            {
                // The cursor now leads somewhere. Selecting a silo used to
                // move a highlight and change nothing else, because the
                // fan-out carried no per-silo detail to open.
                //
                // `'\r'` rather than `'enter'`: the shell matches a
                // shortcut against the RAW key the terminal delivered, so a
                // friendly name would simply never fire. Esc is not
                // available at all — the shell takes it first, to close its
                // palette or pop its own view stack — hence `h` for back,
                // which also keeps the vim shape of `j`/`k`.
                key: '\r',
                label: 'open silo',
                run: (sh) => {
                    if (sh.activeTab !== SILOS_TAB || state.view.focus) return;
                    const silo = state.view.snapshot?.silos[cursors[SILOS_TAB]!.index];
                    if (silo) state.focus(silo.siloId);
                }
            },
            {
                key: 'h',
                label: 'back',
                run: () => state.focus(null)
            }
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
