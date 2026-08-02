/**
 * Fitting prose to a pane.
 *
 * Tables get `fitCell`, which truncates — right for a cell, wrong for a
 * sentence. An alert reads "3 reminder shard(s) UNCLAIMED (p3 p7 p9) —
 * nothing is ticking them", and cutting it at the pane edge throws away the
 * half that says what to do about it. So banners WRAP and cells truncate.
 *
 * Measured in display cells rather than code units, for the same reason
 * every column is: a wide glyph is two cells, and a line that "fits" by
 * `.length` still runs off the right edge.
 */
import { displayWidth, truncateToWidth } from '@sigx/terminal';

/**
 * Break `text` into lines of at most `width` display cells, at spaces where
 * it can and mid-word only when a single word is itself too long.
 *
 * `indent` is prefixed to every line after the first, so a wrapped alert
 * still reads as one item rather than as two.
 */
export function wrapText(text: string, width: number, indent = ''): string[] {
    if (width <= 0) return [];
    const lines: string[] = [];
    let current = '';
    const prefix = (): string => (lines.length === 0 ? '' : indent);
    const room = (): number => width - displayWidth(prefix());

    for (const word of text.split(/\s+/).filter(Boolean)) {
        const candidate = current ? `${current} ${word}` : word;
        if (displayWidth(candidate) <= room()) {
            current = candidate;
            continue;
        }
        if (current) {
            lines.push(prefix() + current);
            current = '';
        }
        // A single word longer than the pane — an actor key, a URL — has to
        // be cut, but it is cut visibly.
        let rest = word;
        while (displayWidth(rest) > room()) {
            const head = truncateToWidth(rest, room());
            lines.push(prefix() + head);
            rest = rest.slice(head.length);
            if (!head) break;
        }
        current = rest;
    }
    if (current) lines.push(prefix() + current);
    return lines;
}
