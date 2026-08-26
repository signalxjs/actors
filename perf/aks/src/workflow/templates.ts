/**
 * The workflows the load generator runs — shaped like the ones a real
 * engine spends its life on, each one leaning on a different runtime axis:
 *
 *   order     validate → parallel[reserve-stock, charge] → delay(ship) →
 *             notify → end. Worker-pool concurrency inside one turn, then
 *             the timer/reminder threshold, then a save per node.
 *   approval  submit → wait 'approve' (timeout → escalate) → finalize.
 *             Buffered signals, the signal-vs-timeout race, a run that
 *             deactivates while it waits.
 *   etl       plan → fanout N (child runs OR pool tasks) → aggregate.
 *             Cross-host sub-workflows and the idempotent durable join, or
 *             pool saturation — one knob apart.
 *   saga      book-flight → book-hotel(retry) → book-car, each with a
 *             compensation; a failure walks the compensations backwards.
 *
 * Every number is a knob so a scenario can move ONE of them. The knobs are
 * the loadgen's (`WF_*` env on the Job); the definitions are `put` into
 * `WorkflowDefinition` actors once per load run under `seedVersion`.
 */
import type { WorkflowDef } from './types.ts';

export interface TemplateKnobs {
    taskMs: number;
    delayMs: number;
    fanoutWidth: number;
    fanoutMode: 'children' | 'tasks';
    failureRate: number;
    signalTimeoutMs: number;
    retryMax: number;
    retryBackoffMs: number;
    version: number;
}

export const DEFAULT_KNOBS: TemplateKnobs = {
    taskMs: 20,
    delayMs: 5_000,
    fanoutWidth: 8,
    fanoutMode: 'children',
    failureRate: 0.1,
    signalTimeoutMs: 30_000,
    retryMax: 3,
    retryBackoffMs: 500,
    version: 1
};

export const TEMPLATE_NAMES = ['order', 'approval', 'etl', 'saga'] as const;
export type TemplateName = (typeof TEMPLATE_NAMES)[number];

export function order(k: TemplateKnobs): WorkflowDef {
    return {
        name: 'order',
        version: k.version,
        start: 'validate',
        nodes: {
            validate: { type: 'task', worker: 'compute', ms: k.taskMs, next: 'reserve-and-charge' },
            'reserve-and-charge': {
                type: 'parallel',
                branches: [['reserve-stock'], ['charge']],
                next: 'ship-delay'
            },
            'reserve-stock': { type: 'task', worker: 'io', ms: k.taskMs, next: 'ship-delay' },
            charge: {
                type: 'task',
                worker: 'io',
                ms: k.taskMs,
                failureRate: k.failureRate,
                retry: { maxAttempts: k.retryMax, backoffMs: k.retryBackoffMs },
                next: 'ship-delay'
            },
            'ship-delay': { type: 'delay', ms: k.delayMs, next: 'notify' },
            notify: { type: 'task', worker: 'io', ms: k.taskMs, next: 'end' },
            end: { type: 'end' }
        }
    };
}

export function approval(k: TemplateKnobs): WorkflowDef {
    return {
        name: 'approval',
        version: k.version,
        start: 'submit',
        nodes: {
            submit: { type: 'task', worker: 'compute', ms: k.taskMs, next: 'await-approve' },
            'await-approve': {
                type: 'wait',
                signal: 'approve',
                timeoutMs: k.signalTimeoutMs,
                onTimeout: 'escalate',
                next: 'finalize'
            },
            finalize: { type: 'task', worker: 'io', ms: k.taskMs, next: 'end' },
            escalate: { type: 'task', worker: 'io', ms: k.taskMs, next: 'end' },
            end: { type: 'end' }
        }
    };
}

export function etl(k: TemplateKnobs): WorkflowDef {
    return {
        name: 'etl',
        version: k.version,
        start: 'plan',
        nodes: {
            plan: { type: 'task', worker: 'compute', ms: k.taskMs, next: 'extract' },
            extract: {
                type: 'fanout',
                width: k.fanoutWidth,
                mode: k.fanoutMode,
                child: { workflow: 'etl-chunk', version: k.version },
                task: { worker: 'compute', ms: k.taskMs },
                next: 'aggregate'
            },
            aggregate: {
                type: 'task',
                worker: 'compute',
                // Proportional to what it folds, floored so a width of 1
                // is still a task.
                ms: Math.max(k.taskMs, Math.round((k.taskMs * k.fanoutWidth) / 4)),
                next: 'end'
            },
            end: { type: 'end' }
        }
    };
}

/** The unit `etl` fans out to in `children` mode — its own definition,
 *  so a child run reads a definition like any other. */
export function etlChunk(k: TemplateKnobs): WorkflowDef {
    return {
        name: 'etl-chunk',
        version: k.version,
        start: 'transform',
        nodes: {
            transform: { type: 'task', worker: 'compute', ms: k.taskMs, next: 'end' },
            end: { type: 'end' }
        }
    };
}

export function saga(k: TemplateKnobs): WorkflowDef {
    return {
        name: 'saga',
        version: k.version,
        start: 'book-flight',
        onFailure: 'compensate',
        nodes: {
            'book-flight': {
                type: 'task',
                worker: 'io',
                ms: k.taskMs,
                compensate: 'cancel-flight',
                next: 'book-hotel'
            },
            'book-hotel': {
                type: 'task',
                worker: 'io',
                ms: k.taskMs,
                failureRate: k.failureRate,
                retry: { maxAttempts: k.retryMax, backoffMs: k.retryBackoffMs },
                compensate: 'cancel-hotel',
                next: 'book-car'
            },
            'book-car': {
                type: 'task',
                worker: 'io',
                ms: k.taskMs,
                // No retry: the node whose failure actually triggers the
                // compensation walk at the configured rate.
                failureRate: k.failureRate,
                next: 'end'
            },
            'cancel-hotel': { type: 'task', worker: 'io', ms: k.taskMs, next: 'end' },
            'cancel-flight': { type: 'task', worker: 'io', ms: k.taskMs, next: 'end' },
            end: { type: 'end' }
        }
    };
}

/**
 * The definition version a knob bag seeds under: a hash of every knob but
 * `version` itself. `WorkflowDefinition.put` is idempotent BY VERSION, so
 * two runs seeding different knobs under the same version share the first
 * one's definition — which is how a recorded run once measured 2 s delays
 * while its Job said 90 s. Same knobs → same version → one definition;
 * any knob moved → a version of its own.
 */
export function seedVersionFor(k: Omit<TemplateKnobs, 'version'> & { version?: number }): number {
    const { version: _ignored, ...rest } = k;
    void _ignored;
    const text = JSON.stringify(rest, Object.keys(rest).sort());
    let h = 0x811c9dc5;
    for (let i = 0; i < text.length; i++) {
        h ^= text.charCodeAt(i);
        h = Math.imul(h, 0x01000193);
    }
    // Positive, and above any hand-picked small number.
    return 1_000_000 + ((h >>> 0) % 1_000_000_000);
}

/** Every definition a load run seeds, `etl-chunk` included. */
export function allDefinitions(k: TemplateKnobs): WorkflowDef[] {
    return [order(k), approval(k), etl(k), etlChunk(k), saga(k)];
}

/**
 * `"order:50,approval:20,etl:20,saga:10"` → weights. Unknown names and
 * non-positive weights are errors: a typo that silently ran nothing but
 * `order` would still print a plausible result.
 */
export function templateWeights(mix: string): Record<TemplateName, number> {
    const weights: Record<TemplateName, number> = { order: 0, approval: 0, etl: 0, saga: 0 };
    for (const part of mix.split(',')) {
        const trimmed = part.trim();
        if (trimmed === '') continue;
        const [name, raw] = trimmed.split(':');
        const weight = Number(raw ?? '1');
        if (!(TEMPLATE_NAMES as readonly string[]).includes(name ?? '')) {
            throw new Error(`[workflow] unknown template '${name}' in mix '${mix}'`);
        }
        if (!Number.isFinite(weight) || weight <= 0) {
            throw new Error(`[workflow] bad weight for '${name}' in mix '${mix}'`);
        }
        weights[name as TemplateName] = weight;
    }
    if (Object.values(weights).every((w) => w === 0)) {
        throw new Error(`[workflow] empty mix '${mix}'`);
    }
    return weights;
}

/** Pick a template from weights with a caller-supplied uniform sample. */
export function pickTemplate(weights: Record<TemplateName, number>, u: number): TemplateName {
    const total = Object.values(weights).reduce((a, b) => a + b, 0);
    let acc = 0;
    for (const name of TEMPLATE_NAMES) {
        acc += weights[name];
        if (u * total < acc) return name;
    }
    return TEMPLATE_NAMES[TEMPLATE_NAMES.length - 1];
}
