/**
 * The dashboard's stylesheet, and the one-time injection of it.
 *
 * Self-contained on purpose. This is a component a team drops into an admin
 * portal they already own, and the alternative — ship class names, document
 * them, and let every consumer write the CSS — means the first render is
 * unreadable and the tenth consumer's version has drifted. So it arrives
 * styled.
 *
 * Every colour and metric is a custom property under `--sigx-actors-`, which
 * is how it stays THEIRS: a portal overrides tokens on any ancestor and the
 * whole dashboard follows, without overriding a single rule. `styles: false`
 * opts out entirely and `actorsDashboardCss` is exported for anyone who would
 * rather ship the sheet themselves.
 *
 * Two rules the palette is built around, and neither is decoration:
 *
 *   - **`danger` and `warn` must never be confusable.** They mean different
 *     things everywhere in this dashboard — an unclaimed reminder shard is an
 *     incident, a doubly-claimed one is a divergence — and a palette that
 *     renders them as two similar oranges throws that away.
 *   - **A gap is not a zero.** `--sigx-actors-gap` is the colour of "no
 *     reading", and it is deliberately not the same as the series colour at
 *     low opacity: a reset must look like missing data, not like quiet
 *     traffic.
 */

/** The marker attribute on the injected `<style>`, so injection is idempotent. */
const STYLE_MARKER = 'data-sigx-actors-dashboard';

/**
 * The stylesheet, as a string.
 *
 * Exported so a consumer with a CSP that forbids injected styles, or a build
 * that extracts CSS, can ship it themselves and pass `styles={false}`.
 */
export const actorsDashboardCss = `
.sxad {
  /* Palette — light. Overridable from any ancestor. */
  --sigx-actors-bg: #ffffff;
  --sigx-actors-panel: #f6f7f9;
  --sigx-actors-border: #d8dce2;
  --sigx-actors-text: #1a1d21;
  --sigx-actors-dim: #6b7280;
  --sigx-actors-accent: #2563eb;
  --sigx-actors-ok: #15803d;
  --sigx-actors-warn: #b45309;
  --sigx-actors-danger: #b91c1c;
  --sigx-actors-gap: #c3c8d0;

  /* Metrics. */
  --sigx-actors-radius: 6px;
  --sigx-actors-gap-sm: 6px;
  --sigx-actors-gap-md: 12px;
  --sigx-actors-gap-lg: 20px;
  --sigx-actors-font: ui-sans-serif, system-ui, -apple-system, "Segoe UI", sans-serif;
  --sigx-actors-mono: ui-monospace, SFMono-Regular, "SF Mono", Menlo, Consolas, monospace;

  background: var(--sigx-actors-bg);
  color: var(--sigx-actors-text);
  font-family: var(--sigx-actors-font);
  font-size: 14px;
  line-height: 1.45;
  container-type: inline-size;
}

@media (prefers-color-scheme: dark) {
  .sxad:not([data-sxad-theme="light"]) {
    --sigx-actors-bg: #101215;
    --sigx-actors-panel: #191c21;
    --sigx-actors-border: #2c313a;
    --sigx-actors-text: #e6e8eb;
    --sigx-actors-dim: #9aa1ad;
    --sigx-actors-accent: #60a5fa;
    --sigx-actors-ok: #4ade80;
    --sigx-actors-warn: #fbbf24;
    --sigx-actors-danger: #f87171;
    --sigx-actors-gap: #3b4048;
  }
}

.sxad[data-sxad-theme="dark"] {
  --sigx-actors-bg: #101215;
  --sigx-actors-panel: #191c21;
  --sigx-actors-border: #2c313a;
  --sigx-actors-text: #e6e8eb;
  --sigx-actors-dim: #9aa1ad;
  --sigx-actors-accent: #60a5fa;
  --sigx-actors-ok: #4ade80;
  --sigx-actors-warn: #fbbf24;
  --sigx-actors-danger: #f87171;
  --sigx-actors-gap: #3b4048;
}

.sxad *, .sxad *::before, .sxad *::after { box-sizing: border-box; }

/* Chrome ------------------------------------------------------------ */

.sxad-bar {
  display: flex;
  flex-wrap: wrap;
  align-items: center;
  gap: var(--sigx-actors-gap-sm);
  padding-bottom: var(--sigx-actors-gap-sm);
  border-bottom: 1px solid var(--sigx-actors-border);
}
.sxad-tabs { display: flex; flex-wrap: wrap; gap: 2px; margin-right: auto; }
.sxad-tab {
  appearance: none;
  border: 1px solid transparent;
  border-radius: var(--sigx-actors-radius);
  background: transparent;
  color: var(--sigx-actors-dim);
  font: inherit;
  padding: 4px 10px;
  cursor: pointer;
}
.sxad-tab:hover { color: var(--sigx-actors-text); }
.sxad-tab[aria-selected="true"] {
  color: var(--sigx-actors-text);
  background: var(--sigx-actors-panel);
  border-color: var(--sigx-actors-border);
}
.sxad-tab:focus-visible { outline: 2px solid var(--sigx-actors-accent); outline-offset: 1px; }

.sxad-status { display: flex; flex-wrap: wrap; gap: var(--sigx-actors-gap-md); align-items: center; }
.sxad-status > span { color: var(--sigx-actors-dim); font-size: 12px; white-space: nowrap; }
.sxad-status b { color: var(--sigx-actors-text); font-weight: 600; }
.sxad-status .sxad-warn b { color: var(--sigx-actors-warn); }
.sxad-status .sxad-danger b { color: var(--sigx-actors-danger); }

.sxad-body { padding-top: var(--sigx-actors-gap-md); }

/* Alerts ------------------------------------------------------------ */

.sxad-alerts { display: flex; flex-direction: column; gap: 4px; margin-bottom: var(--sigx-actors-gap-md); }
.sxad-alert {
  border: 1px solid currentColor;
  border-left-width: 4px;
  border-radius: var(--sigx-actors-radius);
  padding: 6px 10px;
  font-weight: 500;
}
.sxad-alert.sxad-danger { color: var(--sigx-actors-danger); }
.sxad-alert.sxad-warn { color: var(--sigx-actors-warn); }
.sxad-alert span { color: var(--sigx-actors-text); font-weight: 400; }

/* Panels and sections ----------------------------------------------- */

.sxad-scope { color: var(--sigx-actors-dim); font-size: 12px; text-transform: uppercase; letter-spacing: .04em; }
.sxad-note { color: var(--sigx-actors-warn); font-size: 12px; margin: 4px 0 0; }
.sxad-empty { color: var(--sigx-actors-dim); font-style: italic; }
.sxad-section { margin-top: var(--sigx-actors-gap-lg); }
.sxad-section > h3 {
  margin: 0 0 var(--sigx-actors-gap-sm);
  font-size: 12px;
  font-weight: 600;
  text-transform: uppercase;
  letter-spacing: .04em;
  color: var(--sigx-actors-dim);
}
.sxad-cols { display: grid; grid-template-columns: repeat(auto-fit, minmax(240px, 1fr)); gap: var(--sigx-actors-gap-lg); }

/* Definition rows ---------------------------------------------------- */

.sxad-list { display: grid; grid-template-columns: max-content 1fr; gap: 2px var(--sigx-actors-gap-md); margin: var(--sigx-actors-gap-sm) 0 0; }
.sxad-list dt { color: var(--sigx-actors-dim); }
.sxad-list dd { margin: 0; font-variant-numeric: tabular-nums; }
.sxad-list dd.sxad-warn { color: var(--sigx-actors-warn); }
.sxad-list dd.sxad-danger { color: var(--sigx-actors-danger); }
.sxad-list dd.sxad-ok { color: var(--sigx-actors-ok); }
.sxad-list dd.sxad-dim { color: var(--sigx-actors-dim); }

/* Tables -------------------------------------------------------------- */

.sxad-tablewrap { overflow-x: auto; margin-top: var(--sigx-actors-gap-sm); }
.sxad-table { width: 100%; border-collapse: collapse; font-variant-numeric: tabular-nums; }
.sxad-table th, .sxad-table td {
  text-align: left;
  padding: 4px 10px 4px 0;
  border-bottom: 1px solid var(--sigx-actors-border);
  white-space: nowrap;
}
.sxad-table th { color: var(--sigx-actors-dim); font-size: 12px; font-weight: 600; text-transform: uppercase; letter-spacing: .04em; }
.sxad-table td.sxad-num, .sxad-table th.sxad-num { text-align: right; }
.sxad-table td.sxad-key { font-family: var(--sigx-actors-mono); max-width: 28ch; overflow: hidden; text-overflow: ellipsis; }
.sxad-table tr.sxad-warn td { color: var(--sigx-actors-warn); }
.sxad-table tr.sxad-danger td { color: var(--sigx-actors-danger); }
.sxad-table tr.sxad-dim td { color: var(--sigx-actors-dim); }
.sxad-table tr[data-sxad-click] { cursor: pointer; }
.sxad-table tr[data-sxad-click]:hover td { background: var(--sigx-actors-panel); }

/* Series --------------------------------------------------------------- */

.sxad-series { display: grid; grid-template-columns: max-content 1fr max-content; gap: var(--sigx-actors-gap-sm); align-items: center; margin-top: 4px; }
.sxad-series > .sxad-label { color: var(--sigx-actors-dim); font-size: 12px; }
.sxad-series > .sxad-value { font-variant-numeric: tabular-nums; text-align: right; min-width: 8ch; }
.sxad-spark { display: block; width: 100%; height: 28px; overflow: visible; }
.sxad-spark path { fill: none; stroke-width: 1.5; vector-effect: non-scaling-stroke; }

/* Bars ----------------------------------------------------------------- */

.sxad-bars { display: grid; grid-template-columns: max-content 1fr max-content; gap: 2px var(--sigx-actors-gap-sm); align-items: center; }
.sxad-bars > .sxad-label { color: var(--sigx-actors-dim); font-size: 12px; }
.sxad-bars > .sxad-value { font-variant-numeric: tabular-nums; font-size: 12px; text-align: right; }
.sxad-track { background: var(--sigx-actors-panel); border-radius: 2px; height: 8px; overflow: hidden; }
.sxad-fill { height: 100%; background: currentColor; }
.sxad-track.sxad-noread { background: repeating-linear-gradient(90deg, var(--sigx-actors-gap) 0 2px, transparent 2px 5px); }

/* Shard grid ------------------------------------------------------------ */

.sxad-shards { display: flex; flex-wrap: wrap; gap: 4px; margin-top: var(--sigx-actors-gap-sm); }
.sxad-shard {
  display: inline-flex;
  align-items: center;
  gap: 4px;
  border: 1px solid var(--sigx-actors-border);
  border-radius: var(--sigx-actors-radius);
  padding: 2px 6px;
  font-size: 12px;
  font-family: var(--sigx-actors-mono);
}
.sxad-shard::before { content: ""; width: 8px; height: 8px; border-radius: 50%; background: currentColor; }
.sxad-shard.sxad-ok { color: var(--sigx-actors-ok); }
.sxad-shard.sxad-warn { color: var(--sigx-actors-warn); }
.sxad-shard.sxad-danger { color: var(--sigx-actors-danger); }
.sxad-legend { color: var(--sigx-actors-dim); font-size: 12px; margin-top: 4px; }

/* Log lines -------------------------------------------------------------- */

.sxad-log { margin: 0; padding: 0; list-style: none; font-family: var(--sigx-actors-mono); font-size: 12px; }
.sxad-log li { padding: 1px 0; white-space: pre-wrap; word-break: break-word; }
.sxad-log li.sxad-danger { color: var(--sigx-actors-danger); }

.sxad-back {
  appearance: none;
  background: transparent;
  border: 1px solid var(--sigx-actors-border);
  border-radius: var(--sigx-actors-radius);
  color: var(--sigx-actors-text);
  font: inherit;
  padding: 2px 8px;
  cursor: pointer;
}
`;

/**
 * Put the stylesheet in the document, at most once.
 *
 * Called from the component rather than at module scope, deliberately: this
 * package's entry is imported in bare Node by `scripts/verify-pack.js`, and
 * on a server during SSR, and neither has a `document`. A top-level DOM touch
 * would turn both into a crash.
 */
export function injectStyles(doc: Document | undefined = globalThis.document): void {
    if (!doc?.head) return;
    if (doc.querySelector(`style[${STYLE_MARKER}]`)) return;
    const style = doc.createElement('style');
    style.setAttribute(STYLE_MARKER, '');
    style.textContent = actorsDashboardCss;
    doc.head.appendChild(style);
}
