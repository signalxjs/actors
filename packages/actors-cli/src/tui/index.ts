/**
 * What is left of the terminal layer once upstream grew the components.
 *
 * This directory used to hold a sparkline, a table layout, cell-accurate
 * width measurement and percentile bars, written pure precisely so that
 * upstreaming them would be cheap. That happened: signalxjs/terminal#103
 * was accepted, and `@sigx/terminal` 0.11 ships all of it generically —
 * `DataTable`, `Sparkline`, `Trend`, `Meter`, `BarChart`, `DetailList` and
 * `StatusGrid` over `displayWidth` / `fitCell` / `layoutTable` /
 * `scrollWindow` / `commonScale` / `statusGrid`, with our decisions kept as
 * defaults (zero-anchored scale, reserved lowest block, `null` as `·`,
 * shrink-from-the-right, marked truncation, identity-tiebroken sort,
 * clamped cursor, shared scale as an explicit input).
 *
 * So the local copies are DELETED rather than wrapped — a second
 * implementation of a sparkline is how two panels start disagreeing about
 * what a flat line means. `cellWidth` is gone too, as its own comment
 * promised it would be when `fitCell`/`padCell` landed.
 *
 * What remains is the MAPPING between two vocabularies. The actor-shaped
 * judgements themselves — an empty histogram is "no reading" and not three
 * zeroes; a reminder shard with no claimant is an incident while one with two
 * is merely a divergence — are `@sigx/actors-monitor`'s now (#239), so the web
 * dashboard reaches them too. What is left here turns those into
 * `@sigx/terminal` types, plus `Line`, which still has to exist because
 * `<text>` is a span.
 */
export { ALERT_COLOR, histogramScale, percentileItems, shardCells } from './bars';
export { wrapText } from './wrap';
