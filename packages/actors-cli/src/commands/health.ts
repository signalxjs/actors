/**
 * `sigx actors health` — probe liveness and readiness, and EXIT ACCORDINGLY.
 *
 * The exit code is the feature. This is the command you put in a deploy
 * gate or a smoke test, where nothing reads stdout and the only thing that
 * matters is whether the process succeeded.
 *
 *   0  ready
 *   1  reachable but NOT ready — drain it, do not restart it
 *   2  could not reach it at all
 *
 * 1 and 2 are deliberately distinct. A silo that answers "not ready" is
 * alive and handing off its activations; a silo that answers nothing is a
 * different incident with a different fix, and collapsing them is how a
 * rolling deploy turns into an outage.
 */
import type { ActorsCommandContext } from './context';
import { resolveSource } from '../resolve';
import { uptime } from '../model/format';

export const EXIT_READY = 0;
export const EXIT_NOT_READY = 1;
export const EXIT_UNREACHABLE = 2;

export async function runHealth(ctx: ActorsCommandContext): Promise<void> {
    let source;
    try {
        source = await resolveSource(ctx.cwd, ctx.args);
    } catch (error) {
        ctx.logger.error(`unreachable: ${(error as Error)?.message ?? String(error)}`);
        process.exitCode = EXIT_UNREACHABLE;
        return;
    }

    try {
        const snapshot = await source.snapshot();
        const health = snapshot.health;
        if (!health) {
            // The silo answered, so it is alive; it simply reports no
            // readiness aggregate. Not ready is the safe reading, but say
            // WHY rather than implying a failing check.
            ctx.logger.warn('reachable, but the silo reports no health status');
            process.exitCode = EXIT_NOT_READY;
            return;
        }
        if (ctx.args.json) {
            ctx.logger.log(JSON.stringify(health, null, 2));
        } else {
            ctx.logger.log(
                `${source.label}  ${health.live ? 'live' : 'not live'} / ` +
                    `${health.ready ? 'ready' : 'NOT READY'}  up ${uptime(health.uptimeMs)}`
            );
            for (const [name, check] of Object.entries(health.checks)) {
                const detail = check.detail ? ` — ${check.detail}` : '';
                ctx.logger.log(`  ${check.ready ? 'ok  ' : 'FAIL'} ${name}${detail}`);
            }
            if (health.live && !health.ready) {
                // The distinction the whole command exists for.
                ctx.logger.log('');
                ctx.logger.log('  This silo is ALIVE but out of rotation — drain it, do not restart it.');
            }
        }
        process.exitCode = health.ready ? EXIT_READY : EXIT_NOT_READY;
    } catch (error) {
        ctx.logger.error(`unreachable: ${(error as Error)?.message ?? String(error)}`);
        process.exitCode = EXIT_UNREACHABLE;
    } finally {
        await source.close();
    }
}
