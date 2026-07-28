import { dispatchScenarios } from './dispatch.ts';
import { lifecycleScenarios } from './lifecycle.ts';
import { memoryScenarios } from './memory.ts';
import { stateScenarios } from './state.ts';
import { wireScenarios } from './wire.ts';
import type { Scenario } from '../types.ts';

/**
 * Order matters for reading the output: the dispatch ladder first, since
 * every later number is interpreted as a delta from it.
 */
export const ALL_SCENARIOS: Scenario[] = [
    ...dispatchScenarios,
    ...stateScenarios,
    ...wireScenarios,
    ...lifecycleScenarios,
    ...memoryScenarios
];

export function selectScenarios(filters: readonly string[]): Scenario[] {
    if (filters.length === 0) return ALL_SCENARIOS;
    return ALL_SCENARIOS.filter((s) => filters.some((f) => s.name.includes(f)));
}
