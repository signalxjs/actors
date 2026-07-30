/**
 * Terminal building blocks the dashboard needs and `@sigx/terminal` 0.9
 * does not have.
 *
 * Upstream 0.9 ships `ProgressBar`, `Spinner`, `Card`, `Tabs`, `StatusBar`,
 * `KeyHints`, `LogView`, `Badge` and `Divider` — nothing time-series, and a
 * `Table` that is `columns: string[]` / `rows: string[][]`: static, no
 * scrolling, no sorting, no selection.
 *
 * **These are intended to move to `@sigx/terminal-ui`.** They are written
 * to make that cheap:
 *
 *   - the layout and rendering are PURE functions over plain data, so they
 *     carry their own tests, and nothing here has a RUNTIME dependency on
 *     `@sigx/actors` — `bars.ts` type-imports `HistogramSnapshot` and that
 *     is the whole of it, so upstreaming means restating one interface of
 *     seven numbers rather than untangling a dependency;
 *   - the JSX layer over them is deliberately thin, so porting is mostly
 *     re-styling rather than re-implementing;
 *   - colour goes through theme TOKENS (`accent`, `success`, `warn`,
 *     `danger`, `dim`), never raw hex, so they inherit whatever theme the
 *     host shell is running.
 *
 * ## Why there is no JSX in this directory yet
 *
 * `@sigx/runtime-terminal` declares its intrinsics — `box`, `text`, `br`,
 * `row` — by augmenting the GLOBAL `JSX.IntrinsicElements`. So does `sigx`,
 * with the DOM/SVG set. This repo's root `tsconfig.json` has both packages
 * in one program, and `text` exists in both: terminal's takes `color`,
 * SVG's is `SVGAttributes<SVGTextElement>`, and the DOM declaration wins.
 * A per-file `@jsxImportSource @sigx/terminal` pragma does not help, because
 * the collision is in the global namespace rather than in the factory.
 *
 * The fix is a separate tsconfig project for the terminal sources, excluded
 * from the root program — which belongs with the work that first needs to
 * RENDER something, not here. Keeping this layer pure until then is not a
 * workaround: it is the half that carries the tests and the half that
 * upstreams, and it stays honest either way.
 */
export {
    sparkline,
    peakOf,
    meter,
    trend,
    type SparklineOptions
} from './sparkline';
export {
    layoutTable,
    fit,
    cellWidth,
    scrollOffset,
    moveCursor,
    sortRows,
    type Column,
    type LayoutOptions,
    type LaidOutTable
} from './table';
export {
    histogramRow,
    commonScale,
    shardGrid,
    unclaimedShards,
    splitShards,
    type HistogramRow,
    type PercentileCell,
    type ShardCell,
    type ShardTone
} from './bars';
