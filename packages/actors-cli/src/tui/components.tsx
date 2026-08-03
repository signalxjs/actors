/** @jsxImportSource @sigx/terminal */
/**
 * The one component this package still owns.
 *
 * Everything else that used to live here — the sparkline, the meter, the
 * percentile bars, the table, the shard grid, the key/value list, the
 * trend arrow — is `@sigx/terminal` 0.11's now, so the screens compose
 * upstream components directly instead of going through a local wrapper.
 *
 * `Line` survives because the trap that produced #121 has not gone away:
 * `Text` is a SPAN — terminal-zero calls it "deliberately INLINE, unlike
 * every other component" — so consecutive `Text` siblings share a line, and
 * a `Text` following a block element joins THAT block's line. (`Heading` is
 * itself a block; it is only the inline text after it that merges.) Every
 * standalone line of prose therefore needs a block wrapper, and having it
 * named at the call site is what makes the intent visible rather than
 * implied by a bare `<box>`.
 */
import { Text } from '@sigx/terminal';

/** One standalone line of text. */
export function Line(props: { color?: string; bold?: boolean; children?: unknown }) {
    return (
        <box>
            <Text color={props.color} bold={props.bold}>
                {props.children as never}
            </Text>
        </box>
    );
}
