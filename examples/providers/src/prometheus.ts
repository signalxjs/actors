/**
 * A reader for the Prometheus text exposition format — the minimum needed
 * to take a scrape from `prometheusOps()` and answer "what is
 * `sigx_actors_calls_total{type="Counter"}` on this host?" so the demo can
 * ASSERT on the scrape rather than merely print it.
 *
 * It reads samples, not metadata: `# HELP` and `# TYPE` lines are skipped.
 * Label values are unescaped the way the format defines (`\\`, `\"`,
 * `\n`), which is exactly the set `renderPrometheus` escapes.
 */
export interface Sample {
    name: string;
    labels: Record<string, string>;
    value: number;
}

const SAMPLE = /^([A-Za-z_:][A-Za-z0-9_:]*)(?:\{(.*)\})?\s+(\S+)(?:\s+\d+)?$/;
const LABEL = /([A-Za-z_][A-Za-z0-9_]*)="((?:[^"\\]|\\.)*)"/g;

function unescapeLabel(value: string): string {
    return value.replace(/\\(.)/g, (_m, c: string) => (c === 'n' ? '\n' : c));
}

/** Every sample line in a scrape, in order. Malformed lines are skipped. */
export function parseExposition(text: string): Sample[] {
    const samples: Sample[] = [];
    for (const raw of text.split('\n')) {
        const line = raw.trim();
        if (line === '' || line.startsWith('#')) continue;
        const match = SAMPLE.exec(line);
        if (!match) continue;
        const [, name, labelText, valueText] = match as unknown as [string, string, string | undefined, string];
        const labels: Record<string, string> = {};
        if (labelText) {
            for (const m of labelText.matchAll(LABEL)) {
                labels[m[1] as string] = unescapeLabel(m[2] as string);
            }
        }
        const value = valueText === '+Inf' ? Infinity : valueText === '-Inf' ? -Infinity : Number(valueText);
        // A non-numeric value is a malformed line, not a NaN sample: NaN
        // would slip past every `!== null` check and fail `toBe` silently.
        if (Number.isNaN(value)) continue;
        samples.push({ name, labels, value });
    }
    return samples;
}

/**
 * The value of the one sample matching `name` and every label in `labels`
 * (labels not named are not constrained), or `null` when there is none.
 */
export function sampleValue(
    samples: readonly Sample[],
    name: string,
    labels: Record<string, string> = {}
): number | null {
    const hit = samples.find(
        (s) => s.name === name && Object.entries(labels).every(([k, v]) => s.labels[k] === v)
    );
    return hit ? hit.value : null;
}
