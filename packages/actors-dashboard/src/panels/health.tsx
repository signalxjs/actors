/** @jsxImportSource @sigx/runtime-core */
/**
 * Health — the POLLED host's readiness, and what is failing.
 *
 * Scoped to one host on purpose, and labelled as such. Every host's readiness
 * is on the Hosts tab and in its drill-down; this panel being the ONLY place
 * readiness appeared is how a fleet with a fenced peer used to read as
 * healthy.
 *
 * The verdict line under the gauges is the part an operator acts on, and the
 * two states it distinguishes call for opposite responses: `FATAL` means
 * replace this host, draining will not help; alive-but-not-ready means drain
 * it, do not restart it.
 */
import { component } from '@sigx/runtime-core';
import { alertLines, polledLabel } from '@sigx/actors-monitor';
import { count, uptime } from '@sigx/actors-monitor/format';
import { Alerts, DetailList, Section, type DetailRow } from '../parts/primitives';
import {
    checkLines,
    errorKindLines,
    panelState,
    recentFailureLines,
    type PanelProps
} from './shared';

export const HealthPanel = component<PanelProps>((ctx) => () => {
    const state = panelState(ctx.props);
    const snapshot = state.view.snapshot;
    if (!snapshot) return <p class="sxad-empty">connecting…</p>;

    const health = snapshot.health;
    const metrics = snapshot.metrics;

    const rows: DetailRow[] = health
        ? [
              // `fatal` is not "very not ready": it says this host identity is
              // unrecoverable, so liveness fails and the pod is meant to be
              // REPLACED. Without it, a fenced host reads as merely draining
              // and sits there forever.
              {
                  label: 'live',
                  value: health.fatal ? 'FATAL' : health.live ? 'yes' : 'no',
                  tone: health.fatal || !health.live ? 'danger' : 'ok'
              },
              { label: 'ready', value: health.ready ? 'yes' : 'NO', tone: health.ready ? 'ok' : 'warn' },
              { label: 'uptime', value: uptime(health.uptimeMs) }
          ]
        : [];

    const verdict = health?.fatal
        ? 'FATAL — this host cannot recover; replace it, draining will not help'
        : health && health.live && !health.ready
          ? 'ALIVE but out of rotation — drain it, do not restart it'
          : null;

    const checks = checkLines(health);
    const kinds = errorKindLines(metrics);

    return (
        <div>
            <Alerts alerts={alertLines(state.view)} />
            <p class="sxad-scope">{polledLabel(state.view)}</p>
            {health ? (
                <DetailList rows={rows} />
            ) : (
                <p class="sxad-empty">no health status — export an ops() or health() handle</p>
            )}
            {verdict ? (
                <p class={`sxad-alert ${health?.fatal ? 'sxad-danger' : 'sxad-warn'}`}>{verdict}</p>
            ) : null}

            <Section
                title="checks"
                lines={checks.length > 0 ? checks : ['no readiness checks contributed']}
            />
            {metrics ? (
                <Section title="errors by kind" lines={kinds.length > 0 ? kinds : ['none']} />
            ) : null}
            <Section
                title="recent failures"
                lines={recentFailureLines(metrics?.errors.recent ?? [], 6)}
                tone="danger"
            />
            {metrics ? (
                <Section
                    title="storage"
                    lines={[
                        `${count(metrics.storage.loads)} loads  ${count(metrics.storage.saves)} saves  ${count(metrics.storage.clears)} clears`,
                        // Each conflict discarded an activation's work — the
                        // one storage number that is a finding rather than a
                        // volume.
                        `${count(metrics.storage.conflicts)} etag conflicts — each one discarded an activation`
                    ]}
                />
            ) : null}
        </div>
    );
});
