/** @jsxImportSource @sigx/runtime-core */
/**
 * Actors — the live activations on the host being POLLED.
 *
 * The heading says so rather than sitting unlabelled under a screen of
 * cluster totals: this list comes from one host, not from the fan-out, and
 * mistaking it for the fleet's is the #121 failure in miniature.
 */
import { component } from '@sigx/runtime-core';
import { alertLines, polledLabel } from '@sigx/actors-monitor';
import { uptime } from '@sigx/actors-monitor/format';
import type { ActivationInfo } from '@sigx/actors/host';
import { Alerts, DataTable, Section, type Column } from '../parts/primitives';
import { panelState, slowestMethodLines, type PanelProps } from './shared';

export const activationColumns: readonly Column<ActivationInfo>[] = [
    { key: 'type', header: 'type', value: (a) => a.type },
    // Actor keys are user data and open-ended, so this is the column that
    // matters most and the one that gets the monospace + ellipsis treatment:
    // one UUID key must not push every number off the panel.
    { key: 'key', header: 'key', value: (a) => a.key, key_: true },
    { key: 'queue', header: 'queue', value: (a) => String(a.queued), numeric: true },
    { key: 'age', header: 'age', value: (a) => uptime(a.ageMs), numeric: true },
    { key: 'idle', header: 'idle', value: (a) => uptime(a.idleMs), numeric: true },
    { key: 'tasks', header: 'tasks', value: (a) => (a.tasks > 0 ? String(a.tasks) : ''), numeric: true },
    { key: 'kept', header: 'kept', value: (a) => (a.keptAlive ? 'yes' : '') }
];

/** A queue is the signal worth colouring: it means turns are backing up. */
export const activationTone = (actor: ActivationInfo): 'warn' | null =>
    actor.queued > 0 ? 'warn' : null;

export const ActorsPanel = component<PanelProps>((ctx) => () => {
    const state = panelState(ctx.props);
    const snapshot = state.view.snapshot;
    if (!snapshot) return <p class="sxad-empty">connecting…</p>;

    return (
        <div>
            <Alerts alerts={alertLines(state.view)} />
            <p class="sxad-scope">actors on {polledLabel(state.view)}</p>
            {snapshot.activations ? (
                <DataTable
                    columns={activationColumns}
                    rows={snapshot.activations}
                    tone={activationTone}
                    emptyText="no live activations"
                />
            ) : (
                // `ops({ activations: 0 })` is a real and reasonable setting —
                // the list carries actor KEYS, which can be personal data. An
                // empty table would claim the host has no actors.
                <p class="sxad-empty">no activation list — the source reports none</p>
            )}
            <Section title="slowest methods (p99 turn)" lines={slowestMethodLines(snapshot)} />
        </div>
    );
});
