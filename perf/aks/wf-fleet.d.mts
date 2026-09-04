/**
 * Types for `wf-fleet.mjs` — plain JS because it runs the host and the
 * generator with no build step; declared here so
 * `benchmarks/src/scenarios/wf-local.ts` (which IS typechecked) sees a
 * shape and not `any`. The same arrangement as `deploy/wf-load.d.mts`.
 */
import type { WfLoadRow } from './deploy/wf-load.d.mts';

export interface CpuSummary {
    /** Peak `%cpu` sample of one process, 100 = a full core; null when unsampled. */
    peak: number | null;
    avg: number | null;
    samples: number;
}

/** One rate rung on one fleet size. */
export interface WfFleetRung {
    rate: number;
    row: WfLoadRow;
    /** `ops.workflow` + `cluster/*` deltas; empty unless `countersTrustworthy`. */
    delta: Record<string, number>;
    countersTrustworthy: boolean;
    /** Sampled during the rung; null when no host reported a gauge. */
    peakActivations: number | null;
    /** Queued turns per host after the generator exited. */
    queuedAfter: Record<string, number>;
    /** The BUSIEST host per sample, summarised — one host at a core is the signal. */
    hostCpu: CpuSummary;
    generatorCpu: CpuSummary;
    proxyRequests: number;
    samples: number;
}

export interface WfFleetResult {
    hosts: number;
    namespace: string;
    rungs: WfFleetRung[];
    wallMs: number;
}

export interface WfFleetOptions {
    hosts: number;
    rate?: number;
    sweep?: number[];
    /** Seconds of arrivals per rung. Default 30. */
    durationS?: number;
    /** `WF_*`, `FETCH_CONNECTIONS`, `TRANSPORT` — passed to hosts and generator. */
    env?: Record<string, string>;
    redisUrl?: string;
    basePort?: number;
    sampleIntervalMs?: number;
    onLog?: (line: string) => void;
}

export interface FleetArgs {
    hosts?: number;
    rate?: number;
    sweep?: number[];
    durationS?: number;
    basePort?: number;
    redisUrl?: string;
    env: Record<string, string>;
    json: boolean;
}

export function runWfFleet(options: WfFleetOptions): Promise<WfFleetResult>;
export function parseFleetArgs(words: readonly string[]): FleetArgs;
export function cpuSummary(samples: readonly (number | null)[]): CpuSummary;
/** The integer base port, or a throw when the http or tcp range is not valid for `hosts`. */
export function validateBasePort(raw: unknown, hosts: number): number;
