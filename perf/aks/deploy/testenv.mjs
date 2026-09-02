#!/usr/bin/env node
/**
 * The Azure test environment, as one command per verb.
 *
 *   node testenv.mjs up            # node pool + both releases + DNS
 *   node testenv.mjs status        # what exists, what it costs
 *   node testenv.mjs test [args]   # assertions, then perf, then one verdict
 *   node testenv.mjs baseline      # record the perf baseline for THIS shape
 *   node testenv.mjs bench [args]  # the raw Tier-3 run (args to the harness)
 *   node testenv.mjs load [args]   # run the edge ladder from a same-region VM
 *   node testenv.mjs ws-up [vals]  # mount the socket endpoint on the hosts
 *   node testenv.mjs ws-load [vals]# the WebSocket connection-scale Job
 *   node testenv.mjs ws-bench [a]  # the RECORDED socket run (baseline/compare)
 *   node testenv.mjs migrate-check # the two-deploy migrateState assertion
 *   node testenv.mjs down          # delete everything this script created
 *
 * Why a script and not a README section: every measurement in scenario (l)
 * of the RUNBOOK depends on a specific shape (hosts spread one-per-node, a
 * load client in the same region, an ingress that does not buffer), and a
 * shape assembled by hand is a shape nobody can reproduce or bill
 * correctly. `down` is as important as `up` — this pool costs money idle.
 *
 * Shape is defaulted; identity is not. The pool, sizes and namespaces below
 * carry generic defaults, but every name that identifies an Azure estate
 * (RG, CLUSTER, ACR, …) must come from the environment — this file is
 * public, so it names nobody's infrastructure. Locally, export them once;
 * in CI they arrive as Actions secrets, which GitHub then masks in every
 * log line. A verb fails fast naming exactly what it is missing. `up` is
 * idempotent, so re-running it converges rather than duplicating. Requires
 * az (logged in), kubectl and helm on PATH.
 */
import { execFileSync, spawn } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';
import { existsSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { createHmac } from 'node:crypto';
import { postMessageFnId } from '../../app/deploy/post-fn.mjs';
import { spawnable } from '../../../benchmarks/src/spawn.mjs';
import { shapeMismatch } from '../../../benchmarks/src/shape.mjs';
import { runWsLoad, transportGate } from './ws-load.mjs';
import { runWfLoad } from './wf-load.mjs';

const here = dirname(fileURLToPath(import.meta.url));
const repoRoot = join(here, '..', '..', '..');

const cfg = {
    subscription: process.env.SUBSCRIPTION ?? '',          // '' = az default
    // Identity — no defaults, see the header. Validated per verb below.
    rg: process.env.RG ?? '',
    cluster: process.env.CLUSTER ?? '',
    acr: process.env.ACR ?? '',
    location: process.env.LOCATION ?? '',
    chatHost: process.env.CHAT_HOST ?? '',
    dnsZone: process.env.DNS_ZONE ?? '',
    dnsRg: process.env.DNS_RG ?? '',
    // The load client: its own resource group so `down` is one delete.
    loadRg: process.env.LOAD_RG ?? '',
    loadVm: process.env.LOAD_VM ?? '',
    // Shape — generic, so defaults are fine.
    pool: process.env.POOL ?? 'sigxactors',
    poolSize: process.env.POOL_SIZE ?? 'Standard_D2ls_v6',
    poolCount: process.env.POOL_COUNT ?? '4',
    poolMax: process.env.POOL_MAX ?? '8',
    // The taint/label pair every chart in here selects on.
    workload: process.env.WORKLOAD ?? 'sigx-actors-test',
    actorsNs: process.env.ACTORS_NS ?? 'sigx-actors-test',
    chatNs: process.env.CHAT_NS ?? 'sigx-chat',
    loadVmSize: process.env.LOAD_VM_SIZE ?? 'Standard_D4s_v5',
    ingressNs: process.env.INGRESS_NS ?? 'ingress-nginx',
    // Mirrors the chart's ports.internal — where health and ops live.
    ports: { internal: Number(process.env.INTERNAL_PORT ?? 7311) }
};

// Suffix-anchored, and fatal if it does not match: a loose replace() on a
// host that merely CONTAINS the zone would compute the wrong record-set
// name, and ensureDns() would then happily rewrite somebody else's record.
// Only judged once both are present — requiredness itself is the per-verb
// check's job, and half an identity should fail as "missing", not as
// "mismatched".
const dnsSuffix = `.${cfg.dnsZone}`;
if (cfg.chatHost && cfg.dnsZone && !cfg.chatHost.endsWith(dnsSuffix)) {
    console.error(`CHAT_HOST (${cfg.chatHost}) must be a name inside DNS_ZONE (${cfg.dnsZone})`);
    process.exit(1);
}
const dnsName = cfg.dnsZone && cfg.chatHost.endsWith(dnsSuffix)
    ? cfg.chatHost.slice(0, -dnsSuffix.length)
    : '';

// ---------------------------------------------------------------------------

const log = (...a) => console.log(...a);
const step = (s) => log(`\n▸ ${s}`);

/**
 * Best-effort temp cleanup. Retried because Windows holds a directory open
 * for a moment after a process that touched it exits, and NEVER fatal: a
 * leftover temp dir is a far better outcome than failing a build that
 * already succeeded, which is exactly what an unguarded `rmSync` did.
 */
function discard(dir) {
    try {
        rmSync(dir, { recursive: true, force: true, maxRetries: 10, retryDelay: 200 });
    } catch (error) {
        log(`  (left ${dir} behind: ${error.code ?? error.message})`);
    }
}

function sh(cmd, args, { quiet = false, allowFail = false, cwd = repoRoot } = {}) {
    // `spawnable` is what lets this reach `az` and `pnpm` on Windows, where
    // both are `.cmd` shims that `execFileSync` refuses outright. It is a
    // rewrite of the command, never a shell: arguments still arrive at the
    // callee exactly as written here.
    const run = spawnable(cmd, args);
    try {
        const out = execFileSync(run.command, run.args, {
            encoding: 'utf8',
            stdio: quiet ? ['ignore', 'pipe', 'pipe'] : ['ignore', 'pipe', 'inherit'],
            cwd,
            ...run.options
        });
        return out.trim();
    } catch (error) {
        if (allowFail) return null;
        log(`✗ ${cmd} ${args.slice(0, 4).join(' ')} …`);
        throw error;
    }
}

const az = (args, opts) => sh('az', [...args, ...(cfg.subscription ? ['--subscription', cfg.subscription] : [])], opts);
// Always pin the context: a stray `az aks get-credentials` elsewhere can
// repoint current-context, and a teardown aimed at the wrong cluster is
// not a mistake you get to undo.
const kube = (args, opts) => sh('kubectl', ['--context', cfg.cluster, ...args], opts);
const helm = (args, opts) => sh('helm', ['--kube-context', cfg.cluster, ...args], opts);

const gitSha = () => sh('git', ['rev-parse', '--short', 'HEAD'], { quiet: true });

// ---------------------------------------------------------------------------

function ensureCredentials() {
    const has = sh('kubectl', ['config', 'get-contexts', '-o', 'name'], { quiet: true, allowFail: true });
    if (!has?.split('\n').includes(cfg.cluster)) {
        step(`fetching credentials for ${cfg.cluster}`);
        az(['aks', 'get-credentials', '-g', cfg.rg, '-n', cfg.cluster, '--overwrite-existing']);
        // An AAD-enabled cluster hands back an exec-auth kubeconfig that
        // wants an interactive device-code login — dead on a CI runner.
        // kubelogin flips it to ride the ambient az session (in CI, the
        // OIDC login that just happened). Best-effort on purpose: no
        // kubelogin on PATH or a local-accounts kubeconfig both mean
        // there is nothing to convert.
        sh('kubelogin', ['convert-kubeconfig', '-l', 'azurecli'], { quiet: true, allowFail: true });
    }
}

/**
 * In Actions, GitHub masks the VALUES of secrets wherever they appear in a
 * log — which covers every name this script is given. Values it DERIVES
 * (the ingress IP, for one) are not secrets, so they are registered for
 * masking explicitly the moment they are first known. A no-op outside CI.
 */
function maskInCi(value) {
    if (process.env.GITHUB_ACTIONS && value) log(`::add-mask::${value}`);
    return value;
}

function ensurePool() {
    const pools = az(['aks', 'nodepool', 'list', '-g', cfg.rg, '--cluster-name', cfg.cluster,
        '--query', '[].name', '-o', 'tsv'], { quiet: true }) ?? '';
    if (pools.split(/\s+/).includes(cfg.pool)) {
        log(`  pool ${cfg.pool} exists`);
        return;
    }
    step(`creating node pool ${cfg.pool} (${cfg.poolCount}× ${cfg.poolSize}, autoscale 0-${cfg.poolMax})`);
    az(['aks', 'nodepool', 'add', '-g', cfg.rg, '--cluster-name', cfg.cluster,
        '--name', cfg.pool, '--mode', 'User',
        '--node-vm-size', cfg.poolSize, '--node-count', cfg.poolCount,
        '--enable-cluster-autoscaler', '--min-count', '0', '--max-count', cfg.poolMax,
        '--labels', `workload=${cfg.workload}`,
        '--node-taints', `workload=${cfg.workload}:NoSchedule`]);
}

/**
 * Build in ACR from a clean export of the COMMIT, not from the checkout.
 *
 * `.dockerignore` already says what belongs in the upload — "dependencies
 * are installed and dists are rebuilt INSIDE the container" — and
 * `git archive` enforces that instead of hoping the ignore list stays ahead
 * of every artifact a local build drops. Two things follow, both wanted:
 *
 * - The image matches its tag. The tag IS the git sha, so building a dirty
 *   tree under it produces an image nobody can reproduce from that sha.
 *   Uncommitted work is now excluded, and warned about, rather than
 *   silently shipped.
 * - It works at all. Uploading the checkout as context fails against ACR
 *   with `failed to download context` — reproducibly, from either checkout,
 *   while the same tree exported clean builds fine. The difference is the
 *   ~50k ignored files under `node_modules` that the CLI still walks.
 *
 * `--no-logs` because the CLI cannot stream a build log containing
 * non-ASCII on a Windows console: colorama writes through cp1252 and dies
 * with UnicodeEncodeError partway through OUR build output, failing a build
 * that actually succeeded. Logs are fetched explicitly when it fails, which
 * is when anyone wants them.
 */
function buildImage(name, dockerfile, tag) {
    step(`building ${name}:${tag} in ${cfg.acr}`);
    const dirty = sh('git', ['status', '--porcelain'], { quiet: true, allowFail: true });
    if (dirty) {
        log(`  ! ${dirty.split('\n').length} uncommitted change(s) — NOT in this image (tag is ${tag})`);
    }
    const dir = mkdtempSync(join(tmpdir(), 'sigx-ctx-'));
    try {
        // `git archive` cannot write a directory tree, so: tar out, tar in.
        const tar = spawnable('git', ['archive', '--format=tar', '-o', join(dir, 'ctx.tar'), 'HEAD']);
        execFileSync(tar.command, tar.args, { cwd: repoRoot, stdio: 'ignore', ...tar.options });
        // RELATIVE filename with cwd, never an absolute one: the `tar` first
        // on PATH may be msys GNU tar, which reads `C:\…` as `host:path` and
        // tries to resolve a machine called `C`. No drive letter, no problem.
        const untar = spawnable('tar', ['-xf', 'ctx.tar']);
        execFileSync(untar.command, untar.args, { cwd: dir, stdio: 'ignore', ...untar.options });
        rmSync(join(dir, 'ctx.tar'), { force: true });
        // The context is the POSITIONAL argument, not `.` with cwd set to
        // it: the CLI leaves a background telemetry process behind that
        // inherits its cwd, and on Windows that holds the directory open
        // long enough to make cleanup fail after a successful build.
        const out = sh('az', ['acr', 'build', '--registry', cfg.acr, '--image', `${name}:${tag}`,
            '--platform', 'linux/amd64', '--file', dockerfile, dir, '--no-logs',
            ...(cfg.subscription ? ['--subscription', cfg.subscription] : [])],
            { quiet: true, allowFail: true });
        if (out === null) {
            log(`✗ ${name}:${tag} failed to build — its logs:`);
            const run = az(['acr', 'task', 'list-runs', '-r', cfg.acr, '--top', '1',
                '--query', '[0].runId', '-o', 'tsv'], { quiet: true, allowFail: true });
            if (run) az(['acr', 'task', 'logs', '-r', cfg.acr, '--run-id', run], { allowFail: true });
            throw new Error(`az acr build failed for ${name}:${tag}`);
        }
        log(`  built ${name}:${tag}`);
    } finally {
        discard(dir);
    }
}

/**
 * Helm's `major.minor`, once per run.
 *
 * Two flags this script wants are recent, and on an older helm each is a
 * hard parse error rather than a fallback — so they get checked rather
 * than assumed. Reported once, naming the version, because "invalid
 * argument \"server\" for \"--dry-run\"" is not a message anyone connects
 * to their helm being from 2022.
 */
let helmVersion = null;
function helmAtLeast(major, minor) {
    if (helmVersion === null) {
        const short = sh('helm', ['version', '--short'], { quiet: true, allowFail: true }) ?? '';
        const [, gotMajor, gotMinor] = /v(\d+)\.(\d+)\./.exec(short) ?? [];
        helmVersion = { major: Number(gotMajor) || 0, minor: Number(gotMinor) || 0, short: short.trim() };
        if (helmVersion.major < 3 || (helmVersion.major === 3 && helmVersion.minor < 14)) {
            log(`  ! helm ${helmVersion.short || '(unknown)'} is old — needs >= 3.13 for a`);
            log('    server-side preflight and >= 3.14 for --reset-then-reuse-values.');
            log('    Falling back; upgrade helm to get the stronger checks back.');
        }
    }
    return (
        helmVersion.major > major || (helmVersion.major === major && helmVersion.minor >= minor)
    );
}

function release(name, chart, ns, extra) {
    const exists = helm(['list', '-n', ns, '-q'], { quiet: true, allowFail: true })?.split('\n').includes(name);
    kube(['create', 'namespace', ns], { quiet: true, allowFail: true });
    // Preflight against the REAL API server: admission webhooks reject
    // things `helm lint` and `helm template` render happily (an nginx
    // Ingress path containing `$` under pathType Prefix, for one). On an
    // older helm this degrades to a client-side render, which still catches
    // a broken template — just not a webhook.
    const dryFlag = helmAtLeast(3, 13) ? ['--dry-run=server'] : ['--dry-run'];
    const dry = helm(['upgrade', '--install', name, chart, '-n', ns, ...dryFlag, ...extra],
        { quiet: true, allowFail: true });
    if (dry === null) {
        log(`✗ ${name}: the chart was rejected — running it again for the error:`);
        helm(['upgrade', '--install', name, chart, '-n', ns, ...dryFlag, ...extra]);
    }
    step(`${exists ? 'upgrading' : 'installing'} ${name} in ${ns}`);
    helm([exists ? 'upgrade' : 'install', name, chart, '-n', ns,
        // reset-then-reuse (not --reuse-values): replaying old values breaks
        // the moment a chart gains a key, and every value we care about is
        // passed explicitly below anyway. Before helm 3.14 that flag does
        // not exist, so fall back to plain `--reset-values` — which is the
        // same idea minus the "keep what the operator set by hand" half,
        // and is if anything the more reproducible of the two.
        ...(exists ? [helmAtLeast(3, 14) ? '--reset-then-reuse-values' : '--reset-values'] : []),
        ...extra]);
}

function ensureDns(ip) {
    const existing = az(['network', 'dns', 'record-set', 'a', 'show', '-g', cfg.dnsRg,
        '-z', cfg.dnsZone, '-n', dnsName, '--query', 'ARecords[0].ipv4Address', '-o', 'tsv'],
        { quiet: true, allowFail: true });
    if (existing === ip) return log(`  dns ${cfg.chatHost} → ${ip} already set`);
    if (existing) {
        az(['network', 'dns', 'record-set', 'a', 'delete', '-g', cfg.dnsRg,
            '-z', cfg.dnsZone, '-n', dnsName, '-y'], { quiet: true, allowFail: true });
    }
    step(`pointing ${cfg.chatHost} at ${ip}`);
    az(['network', 'dns', 'record-set', 'a', 'add-record', '-g', cfg.dnsRg,
        '-z', cfg.dnsZone, '-n', dnsName, '-a', ip, '--ttl', '300'], { quiet: true });
}

const ingressIp = () =>
    maskInCi(kube(['-n', cfg.ingressNs, 'get', 'svc', 'ingress-nginx-controller',
        '-o', 'jsonpath={.status.loadBalancer.ingress[0].ip}'], { quiet: true }));

/**
 * The chart hashes the actor path on `$sigx_hash`, a `map` only the
 * controller ConfigMap's `http-snippet` can define (RUNBOOK §0 (b)). This
 * script does not own that ConfigMap, so it cannot install the map — but a
 * controller WITHOUT it makes the released chart worse than round-robin (the
 * variable is nil, every actor call hashes "" onto one pod), so the omission
 * is refused here, before the image build, rather than found by `test`.
 */
function ensureEdgeHashMap() {
    const snippet = kube(['-n', cfg.ingressNs, 'get', 'configmap', 'ingress-nginx-controller',
        '-o', 'jsonpath={.data.http-snippet}'], { quiet: true, allowFail: true });
    if (snippet?.includes('$sigx_hash')) return log(`  ${cfg.ingressNs}: http-snippet defines $sigx_hash`);
    log(`✗ ${cfg.ingressNs}/ingress-nginx-controller has no \`$sigx_hash\` map in its http-snippet — ` +
        `the chart's upstream-hash-by would pin the WHOLE actor path to one pod. Apply RUNBOOK §0 (b) first.`);
    process.exit(1);
}

// ---------------------------------------------------------------------------

async function up() {
    const tag = gitSha();
    ensurePool();
    ensureEdgeHashMap();
    buildImage('sigx-actors-test', 'perf/aks/Dockerfile', tag);
    buildImage('sigx-chat', 'perf/app/Dockerfile', tag);

    release('sigx', 'perf/aks/deploy/chart', cfg.actorsNs, [
        '--set', `image.repository=${cfg.acr}.azurecr.io/sigx-actors-test`,
        '--set', `image.tag=${tag}`,
        '--set', `nodeSelector.workload=${cfg.workload}`
    ]);
    release('chat', 'perf/app/deploy/chart', cfg.chatNs, [
        '--set', `image.repository=${cfg.acr}.azurecr.io/sigx-chat`,
        '--set', `image.tag=${tag}`,
        '--set', `ingress.host=${cfg.chatHost}`,
        '--set', `nodeSelector.workload=${cfg.workload}`
    ]);

    step('waiting for rollouts');
    kube(['-n', cfg.actorsNs, 'rollout', 'status', 'deploy/sigx-host', '--timeout=420s']);
    kube(['-n', cfg.chatNs, 'rollout', 'status', 'deploy/chat-host', '--timeout=420s']);
    ensureDns(ingressIp());
    await status();
    log(`\n✓ up. https://${cfg.chatHost} — tear down with: node ${'testenv.mjs'} down`);
}

async function status() {
    step('cluster');
    const nodes = kube(['get', 'nodes', '-l', `workload=${cfg.workload}`, '--no-headers'],
        { quiet: true, allowFail: true }) ?? '';
    log(`  pool nodes: ${nodes ? nodes.split('\n').length : 0}`);
    for (const [ns, dep] of [[cfg.actorsNs, 'sigx-host'], [cfg.chatNs, 'chat-host']]) {
        const ready = kube(['-n', ns, 'get', 'deploy', dep, '-o',
            'jsonpath={.status.readyReplicas}/{.spec.replicas}'], { quiet: true, allowFail: true });
        log(`  ${ns}/${dep}: ${ready ?? '(absent)'}`);
        // Spread is the difference between N replicas and N× capacity.
        const spread = kube(['-n', ns, 'get', 'pods', '-l', 'app.kubernetes.io/component=host',
            '-o', 'jsonpath={range .items[*]}{.spec.nodeName}{"\\n"}{end}'], { quiet: true, allowFail: true });
        if (spread) {
            const distinct = new Set(spread.split('\n').filter(Boolean)).size;
            log(`     across ${distinct} node(s)${distinct < spread.split('\n').filter(Boolean).length ? ' — sharing nodes, so replicas add less than they look like' : ''}`);
        }
    }
    const vm = az(['vm', 'show', '-g', cfg.loadRg, '-n', cfg.loadVm, '--query', 'name', '-o', 'tsv'],
        { quiet: true, allowFail: true });
    log(`  load vm: ${vm ? `${cfg.loadVm} (${cfg.loadVmSize}) — running, billable` : '(absent)'}`);
    const code = sh('curl', ['-s', '-o', '/dev/null', '-w', '%{http_code}', '--max-time', '8',
        `https://${cfg.chatHost}/`], { quiet: true, allowFail: true });
    log(`  https://${cfg.chatHost}: ${code ?? 'unreachable'}`);
}

async function loadVmUp() {
    const exists = az(['vm', 'show', '-g', cfg.loadRg, '-n', cfg.loadVm, '--query', 'name', '-o', 'tsv'],
        { quiet: true, allowFail: true });
    if (exists) return log(`  load vm ${cfg.loadVm} exists`);
    step(`creating load vm ${cfg.loadVm} in ${cfg.location} (same region as the cluster — a laptop measures its own latency, not the endpoint)`);
    az(['group', 'create', '-n', cfg.loadRg, '-l', cfg.location], { quiet: true });
    az(['vm', 'create', '-g', cfg.loadRg, '-n', cfg.loadVm, '--image', 'Ubuntu2404',
        '--size', cfg.loadVmSize, '--location', cfg.location, '--generate-ssh-keys',
        '--public-ip-sku', 'Standard', '--nsg-rule', 'NONE', '--os-disk-size-gb', '32']);
    step('installing node on the vm');
    runOnVm(`
V=$(curl -fsSL https://nodejs.org/dist/index.json | grep -o '"version":"v22[^"]*"' | head -1 | cut -d'"' -f4)
curl -fsSL "https://nodejs.org/dist/$V/node-$V-linux-x64.tar.xz" -o /tmp/n.txz
mkdir -p /opt/n && tar xJf /tmp/n.txz -C /opt/n --strip-components=1
/opt/n/bin/node --version`);
}

/**
 * The script goes to `az` as `@file`, not inline.
 *
 * Inline was fine until it wasn't: a Windows command line cannot carry a
 * raw newline, so a multi-line `--scripts` argument is unrepresentable
 * there — and this one also base64s a whole ladder into it, which is a lot
 * of command line on any platform. A file sidesteps both, and `az` has
 * taken `@path` for any argument value all along.
 */
function runOnVm(script) {
    const dir = mkdtempSync(join(tmpdir(), 'sigx-vm-'));
    const file = join(dir, 'script.sh');
    try {
        writeFileSync(file, script);
        return az(['vm', 'run-command', 'invoke', '-g', cfg.loadRg, '-n', cfg.loadVm,
            '--command-id', 'RunShellScript', '--scripts', `@${file}`,
            '--query', 'value[0].message', '-o', 'tsv'], { quiet: true });
    } finally {
        discard(dir);
    }
}

/**
 * Run the edge ladder from the VM. Extra args become env for the ladder,
 * e.g. `load LADDER=64,128,256 MIX=0.2 DURATION_MS=30000`.
 */
async function load(args) {
    await loadVmUp();
    const cookie = mintCookie();
    const b64 = Buffer.from(readFileSync(join(here, 'edge-ladder.mjs'))).toString('base64');
    // POST_FN comes from the BUILD — the id is hashed and moves, and a
    // stale one 404s every write while looking like extra throughput.
    const env = ['ROOMS=64', 'WORKERS=4', 'DURATION_MS=20000', 'LADDER=32,64,128,256,512',
        `POST_FN=${postMessageFnId()}`,
        ...args].map((kv) => `export ${kv}`).join('\n');
    step('running the ladder from the vm');
    const out = runOnVm(`
set -e
echo '${b64}' | base64 -d > /tmp/ladder.mjs
ulimit -n 65535
export COOKIE='${cookie}'
export TARGET_URL='https://${cfg.chatHost}'
${env}
/opt/n/bin/node /tmp/ladder.mjs`);
    log(out);
}

// ---------------------------------------------------------------------------
// The WebSocket scale test (#172)


/**
 * Enable (and configure) the socket endpoint on the running actors release.
 *
 * Separate from `ws-load` on purpose: turning the socket on is a ROLLOUT,
 * and a rollout in the middle of a load verb would silently re-measure a
 * different deployment than the one the previous rung ran against. Extra
 * args are chart values, e.g.
 *   ws-up socket.env.SOCKET_MAX_SUBSCRIPTIONS=1024 socket.sessions=true
 */
async function wsUp(args) {
    step('enabling the socket endpoint on the actors release');
    const sets = ['socket.enabled=true', ...args].flatMap((kv) => ['--set', kv]);
    helm(['upgrade', '--install', 'sigx', join(here, 'chart'), '-n', cfg.actorsNs,
        '--reset-then-reuse-values', '--wait', '--timeout', '10m', ...sets]);
    kube(['-n', cfg.actorsNs, 'rollout', 'status', 'deploy/sigx-host', '--timeout=420s']);
    const mounted = kube(['-n', cfg.actorsNs, 'get', 'pod', '-l',
        'app.kubernetes.io/component=host', '-o',
        'jsonpath={.items[*].metadata.name}'], { quiet: true, allowFail: true })
        ?.split(/\s+/).filter(Boolean).length ?? 0;
    log(`  socket mounted on ${mounted} host pod(s)`);
}

/**
 * Run the WebSocket load Job and report the merged rows.
 *
 * Extra args are `wsLoadgen.*` chart values without the prefix, e.g.
 *   ws-load mode=hot ladder=1000,5000,10000 parallelism=2 durationS=60
 *
 * Commas are escaped for `--set` here rather than by the caller: `helm
 * --set` splits values on commas, so an unescaped ladder fails with
 * `key "5000" has no value` — a confusing error a long way from its cause.
 *
 * Every run prints the live deployment's shape — the same `INFRA_SHAPE`
 * string the recorded `ws-bench` path builds — and the image it resolved,
 * because a hand-run number without its shape is exactly how an A/B came
 * to span two images unnoticed (#224). `expect-shape='<string>'` (quoted —
 * a shape contains spaces) turns that into the guard `--compare` has:
 * refuse BEFORE the run when the live deployment is not the shape the
 * operator thinks they are measuring, and again after it if a rollout
 * changed the deployment mid-run.
 */
async function wsLoad(args) {
    // Values arrive as `key=value`, unprefixed (`mode=hot`). A token
    // without `=` would otherwise become `--set-string wsLoadgen.<token>=`
    // and change the run in a way that surfaces much later, if at all.
    const values = {};
    for (const kv of args) {
        const at = kv.indexOf('=');
        if (at <= 0) {
            console.error(`ws-load takes key=value arguments — got '${kv}'`);
            console.error('e.g. ws-load mode=hot ladder=1000,5000 parallelism=2');
            process.exit(1);
        }
        values[kv.slice(0, at)] = kv.slice(at + 1);
    }

    // The image is named ONCE. `image.tag=` is accepted here because
    // reusing what an earlier `up` built is the normal case — the git SHA
    // moves on every merge while the deployed image does not — but it is
    // lifted OUT of the values so it cannot also arrive as a chart value
    // and quietly win over the explicit one.
    // An EMPTY value counts as unset, not as a pin: `image.tag=` would
    // otherwise become the invalid image `repo:` while claiming explicitness.
    const tagDefaulted = !values['image.tag'];
    const imageTag = values['image.tag'] || gitSha();
    const imageRepository = values['image.repository']
        || `${cfg.acr}.azurecr.io/sigx-actors-test`;
    delete values['image.tag'];
    delete values['image.repository'];

    // Lifted out like the image keys: `expect-shape` is an instruction to
    // this verb, and left in `values` it would arrive at helm as a broken
    // `wsLoadgen.expect-shape` chart value.
    const expectShape = values['expect-shape'];
    delete values['expect-shape'];

    // ALWAYS visible, defaulted or not: the tag was silently `gitSha()` of
    // whatever the runner had checked out, which is how a hand-run A/B came
    // to span two images with nothing objecting (#224).
    step(`image: ${imageRepository}:${imageTag}` +
        (tagDefaulted ? ' (defaulted to git HEAD — pass image.tag=<tag> to pin)' : ''));
    // The Job's image is what the GENERATOR runs; the shape's `image=` is
    // what the HOSTS run. Both are named because they can differ, and a
    // comparison is only as good as the one that moved unnoticed.
    const shape = actorsShape();
    step(`shape: ${shape}`);
    if (expectShape !== undefined) {
        const mismatch = shapeMismatch(expectShape, shape);
        if (mismatch) {
            console.error(`✗ ${mismatch}`);
            console.error('  the live deployment is not the shape this run expects — nothing was run.');
            process.exit(1);
        }
        log('  expect-shape matches the live deployment');
    }

    let result;
    try {
        result = await runWsLoad({
            context: cfg.cluster,
            namespace: cfg.actorsNs,
            chartDir: join(here, 'chart'),
            imageRepository,
            imageTag,
            workload: cfg.workload,
            values,
            onLog: (message) => step(message)
        });
    } catch (error) {
        console.error(error.message);
        process.exit(1);
    }

    if (result.rows.length === 0) {
        for (const pod of result.pods.slice(0, 2)) {
            log(`--- ${pod} ---`);
            log(kube(['-n', cfg.actorsNs, 'logs', pod, '--tail=40'],
                { quiet: true, allowFail: true }) ?? '(no logs)');
        }
        console.error(`${result.job} produced no result rows`);
        process.exitCode = 1;
        return;
    }

    step(result.partial ? 'merged rows (PARTIAL — see the failure above)' : 'merged rows');
    for (const row of result.merged) log(JSON.stringify(row));

    step('peak concurrency, observed on the HOSTS');
    log(JSON.stringify({
        hosts: result.hosts,
        // The two numbers the runbook's TCP check needs, which this path
        // never printed (#223): hosts with tcp in their chain — short of
        // `hosts` on a TRANSPORT=tcp run means a fleet still partly on
        // HTTP — and whether the `cluster/*` deltas below saw every host.
        tcpHosts: result.tcpHosts,
        watchesTrustworthy: result.watchesTrustworthy,
        peakOpen: result.peakOpen,
        peakSubscriptions: result.peakSubscriptions,
        samples: result.samples,
        note: 'gauge sampled every ~5s while the job ran; a true peak between samples is missed'
    }));

    step('ops.sockets, summed across hosts');
    log(JSON.stringify({ hosts: result.hosts, delta: result.delta }));

    // The shape again, WITH the numbers it belongs to — so a pasted result
    // carries it — and re-read rather than reprinted: a rollout landing
    // mid-run means the rows above span two deployments, which is the
    // silent-A/B failure this guard exists for (#224).
    const shapeAfter = actorsShape();
    step(`shape: ${shapeAfter}`);
    if (shapeAfter !== shape) {
        console.error(`✗ the deployment changed mid-run — it was: ${shape}`);
        console.error('  the rows above span two shapes; the run is not valid');
        process.exitCode = 1;
    }

    // A partial run that exits 0 is a number nobody knows to distrust.
    if (result.partial) process.exitCode = 1;
    if ((result.delta.protocolBreaches ?? 0) > 0) {
        log('  ✗ protocol breaches — the client and the session disagree; the run is not valid');
        process.exitCode = 1;
    }
    // A fleet partly on HTTP, or a link that fell back to it, invalidates a
    // TRANSPORT=tcp measurement as thoroughly as a breach does, and just as
    // silently (#223). Judged against the shape the numbers belong to — the
    // one re-read above.
    const transport = transportGate(shapeAfter, result.delta, result);
    if (transport) {
        log(`  ${transport.valid ? '!' : '✗'} ${transport.message}`);
        if (!transport.valid) process.exitCode = 1;
    }
}


/**
 * The runtime knobs, read back off the LIVE Deployment.
 *
 * Every one of these changes the perf curve, and none of them changes the
 * image tag — so without them in the shape, `bench:infra:compare` happily
 * compares a rebalancing, activation-count deployment against a baseline
 * taken from a prefer-local one and reports the difference as a code
 * regression. Read from the container's env rather than from values.yaml
 * because a `helm --set` is exactly the case that would otherwise slip
 * through.
 */
/**
 * The socket-relevant env on the LIVE actors deployment.
 *
 * Separate from `liveKnobs()` because it reads a different release and a
 * different set of names — and because every one of these caps changes what
 * a connection-scale run can reach. A run under
 * `SOCKET_MAX_SUBSCRIPTIONS=1024` is not comparable with one under the
 * default 256, and without them in the shape the comparison is made anyway
 * and reported as a code change.
 *
 * `FETCH_CONNECTIONS` is here for the same reason despite not being a
 * socket knob: it is the host-to-host fetch pool, and every per-principal
 * cross-host watch stream PINS one pooled connection for the life of the
 * subscription — the pool size, not the runtime, was the 100–250 identity
 * cliff of #180 (#194). Two runs under different pool sizes measure
 * different deployments. (Adding it refuses comparison against artifacts
 * recorded before it was in the shape — deliberate: those runs' pool size
 * is unknown to the guard.)
 *
 * `TRANSPORT` joins it for the sharper version of the same argument (#203):
 * over TCP there is no pool to size, because every stream is multiplexed
 * onto one connection per peer. A TCP run and an HTTP run are not two
 * measurements of one deployment.
 */
const SOCKET_KNOBS = [
    'ENABLE_SOCKET',
    'ENABLE_SESSIONS',
    'FETCH_CONNECTIONS',
    // The host-to-host link (#203). Here for the same reason
    // `FETCH_CONNECTIONS` is: TCP multiplexes every cross-host watch stream
    // onto ONE connection per peer, so the pool arithmetic that bound the
    // identity ceiling does not apply to it at all. Two runs over different
    // transports measure different deployments.
    'TRANSPORT',
    'SOCKET_PATH',
    'SOCKET_ORIGIN',
    'SOCKET_MAX_CONCURRENT',
    'SOCKET_MAX_SUBSCRIPTIONS',
    'SOCKET_MAX_MESSAGE_BYTES',
    'SOCKET_PING_MS',
    'SOCKET_REVALIDATE_MS',
    'SOCKET_MAX_CONNECTION_MS'
];

function liveSocketKnobs(names = SOCKET_KNOBS) {
    const raw = kube(['-n', cfg.actorsNs, 'get', 'deploy', 'sigx-host', '-o',
        'jsonpath={range .spec.template.spec.containers[0].env[*]}{.name}={.value}{"\\n"}{end}'],
        { quiet: true, allowFail: true }) ?? '';
    const found = {};
    for (const line of raw.split('\n')) {
        const [name, ...rest] = line.split('=');
        if (names.includes(name) && rest.length > 0) found[name] = rest.join('=');
    }
    return found;
}

/**
 * The workflow engine's host knobs (#297), for the `wf` shape. Every one
 * moves the curve: the timer threshold decides which sleeps ride the
 * reminder shards at all, the tick is the wake-lag floor, and the
 * deactivate toggle is the difference between a fleet holding a million
 * sleeping runs and one holding a million activations. `FETCH_CONNECTIONS`
 * and `TRANSPORT` are here for the reason they are in `SOCKET_KNOBS`:
 * every child start and `childDone` is a cross-host call.
 */
const WORKFLOW_KNOBS = [
    'FETCH_CONNECTIONS',
    'TRANSPORT',
    'WF_TIMER_THRESHOLD_MS',
    'WF_REMINDER_TICK_MS',
    'WF_IDLE_AFTER_MS',
    'WF_DEACTIVATE_ON_SLEEP',
    'WF_STALE_WAKE_MS',
    'WF_CHILD_STALE_MS',
    'WF_STATS_SAVE_EVERY',
    'WF_STATS_RING',
    'WF_NOTIFY_RETRY_MS',
    'WF_COMPUTE_MAX_LOCAL',
    'WF_IO_MAX_LOCAL',
    'WF_CALL_TIMEOUT_MS'
];

/**
 * `INFRA_SHAPE` for a run against the ACTORS release (not chat): `ws` for
 * a socket run over `SOCKET_KNOBS`, `wf` for a workflow run over
 * `WORKFLOW_KNOBS`. The prefix is part of the string on purpose — the two
 * axes measure different nouns, and a socket baseline compared against a
 * workflow run must be refused as a shape mismatch, not read as a delta.
 */
function actorsShape(prefix = 'ws', knobNames = SOCKET_KNOBS) {
    const replicas = kube(['-n', cfg.actorsNs, 'get', 'deploy', 'sigx-host',
        '-o', 'jsonpath={.spec.replicas}'], { quiet: true, allowFail: true }) ?? '?';
    const nodes = kube(['-n', cfg.actorsNs, 'get', 'pods', '-l',
        'app.kubernetes.io/component=host', '-o',
        'jsonpath={range .items[*]}{.spec.nodeName}{"\\n"}{end}'],
        { quiet: true, allowFail: true }) ?? '';
    const distinct = new Set(nodes.split('\n').filter(Boolean)).size;
    const image = kube(['-n', cfg.actorsNs, 'get', 'deploy', 'sigx-host',
        '-o', 'jsonpath={.spec.template.spec.containers[0].image}'], { quiet: true });
    const knobs = Object.entries(liveSocketKnobs(knobNames))
        .sort(([a], [b]) => (a < b ? -1 : 1))
        .map(([k, v]) => `${k}=${v}`)
        .join(',');
    return (
        `${prefix} replicas=${replicas} nodes=${distinct} image=${image.split(':').pop()}` +
        (knobs ? ` knobs=${knobs}` : '')
    );
}

/**
 * Run the workflow-engine load Job and report the merged rows (#297).
 *
 * Extra args are `loadgen.*` chart values without the prefix — `sweep`,
 * `durationS`, `parallelism` — and the generator's `WF_*` knobs bare:
 *   wf-load WF_START_RATE=50 WF_MIX=order:100 WF_DELAY_MS=90000 durationS=60
 *   wf-load sweep=25,50,100 WF_DELAY_MS=2000
 *
 * Same discipline as `ws-load`: the image is named once, the live shape is
 * printed before and after (a rollout mid-run invalidates the rows), and
 * `expect-shape='…'` refuses a run against a deployment of another shape.
 */
async function wfLoad(args) {
    const values = {};
    for (const kv of args) {
        const at = kv.indexOf('=');
        if (at <= 0) {
            console.error(`wf-load takes key=value arguments — got '${kv}'`);
            console.error('e.g. wf-load WF_START_RATE=50 WF_MIX=order:100 durationS=60');
            process.exit(1);
        }
        values[kv.slice(0, at)] = kv.slice(at + 1);
    }
    const tagDefaulted = !values['image.tag'];
    const imageTag = values['image.tag'] || gitSha();
    const imageRepository = values['image.repository']
        || `${cfg.acr}.azurecr.io/sigx-actors-test`;
    delete values['image.tag'];
    delete values['image.repository'];
    const expectShape = values['expect-shape'];
    delete values['expect-shape'];

    step(`image: ${imageRepository}:${imageTag}` +
        (tagDefaulted ? ' (defaulted to git HEAD — pass image.tag=<tag> to pin)' : ''));
    const shape = actorsShape('wf', WORKFLOW_KNOBS);
    step(`shape: ${shape}`);
    if (expectShape !== undefined) {
        const mismatch = shapeMismatch(expectShape, shape);
        if (mismatch) {
            console.error(`✗ ${mismatch}`);
            console.error('  the live deployment is not the shape this run expects — nothing was run.');
            process.exit(1);
        }
        log('  expect-shape matches the live deployment');
    }

    let result;
    try {
        result = await runWfLoad({
            context: cfg.cluster,
            namespace: cfg.actorsNs,
            chartDir: join(here, 'chart'),
            imageRepository,
            imageTag,
            workload: cfg.workload,
            values,
            onLog: (message) => step(message)
        });
    } catch (error) {
        console.error(error.message);
        process.exit(1);
    }

    if (result.rows.length === 0) {
        for (const pod of result.pods.slice(0, 2)) {
            log(`--- ${pod} ---`);
            log(kube(['-n', cfg.actorsNs, 'logs', pod, '--tail=40'],
                { quiet: true, allowFail: true }) ?? '(no logs)');
        }
        console.error(`${result.job} produced no result rows`);
        process.exitCode = 1;
        return;
    }

    step(result.partial ? 'merged rows (PARTIAL — see the failure above)' : 'merged rows');
    for (const row of result.merged) log(JSON.stringify(row));

    step('peak activations, observed on the HOSTS');
    log(JSON.stringify({
        hosts: result.hosts,
        peakActivations: result.peakActivations,
        queuedAfter: result.queuedAfter,
        samples: result.samples,
        note: 'gauge sampled every ~5s while the job ran; a true peak between samples is missed'
    }));

    step(result.countersTrustworthy
        ? 'ops.workflow + cluster counters, summed across hosts'
        : 'ops.workflow: a snapshot missed a host — no delta reported');
    log(JSON.stringify({ hosts: result.hosts, delta: result.delta }));

    const shapeAfter = actorsShape('wf', WORKFLOW_KNOBS);
    step(`shape: ${shapeAfter}`);
    if (shapeAfter !== shape) {
        console.error(`✗ the deployment changed mid-run — it was: ${shape}`);
        console.error('  the rows above span two shapes; the run is not valid');
        process.exitCode = 1;
    }
    if (result.partial) process.exitCode = 1;
    const stuck = result.merged.reduce((a, row) => a + (row.stuck?.total ?? 0), 0);
    if (stuck > 0) {
        log(`  ✗ ${stuck} run(s) stuck at the end of the drain — see the rows' \`stuck\` breakdown`);
        process.exitCode = 1;
    }
}

/**
 * The recorded workflow run (#297): the `workflow/*` scenarios with the
 * live release's `wf` shape attached.
 *
 *   wf-bench                       # raw run
 *   wf-bench --save-baseline       # record THIS shape
 *   wf-bench --compare             # against the recorded one
 */
async function wfBench(args) {
    const image = kube(['-n', cfg.actorsNs, 'get', 'deploy', 'sigx-host', '-o',
        'jsonpath={.spec.template.spec.containers[0].image}'], { quiet: true });
    const [repository, tag] = [image.slice(0, image.lastIndexOf(':')), image.split(':').pop()];
    const shape = actorsShape('wf', WORKFLOW_KNOBS);
    step(`shape: ${shape}`);
    const env = {
        ...process.env,
        BENCH_WF: '1',
        INFRA_WF_CONTEXT: cfg.cluster,
        INFRA_WF_NS: cfg.actorsNs,
        INFRA_WF_IMAGE: repository,
        INFRA_WF_IMAGE_TAG: tag,
        INFRA_WF_WORKLOAD: cfg.workload,
        INFRA_SHAPE: shape
    };
    // One pass, no warmup, no GC-profile re-run: each of those is a whole
    // extra ladder on a paid cluster, and the two extra passes are how the
    // first recorded run spent an hour measuring its own backlog (#302).
    const code = spawnInherit('pnpm', ['bench:run', 'workflow/', '--runs=1', '--no-warmup', '--no-gc-profile', ...args], env);
    process.exitCode = code;
}

/**
 * The recorded socket run: assemble the env the `sockets/*` scenarios need
 * and hand off to the harness, so the numbers land in a baseline file with
 * a shape attached instead of on a terminal.
 *
 *   ws-bench                       # raw run
 *   ws-bench --save-baseline       # record THIS shape
 *   ws-bench --compare             # against the recorded one
 */
async function wsBench(args) {
    const image = kube(['-n', cfg.actorsNs, 'get', 'deploy', 'sigx-host', '-o',
        'jsonpath={.spec.template.spec.containers[0].image}'], { quiet: true });
    const [repository, tag] = [image.slice(0, image.lastIndexOf(':')), image.split(':').pop()];
    const shape = actorsShape();
    step(`shape: ${shape}`);
    // The image the CLUSTER is running, not `gitSha()`: the SHA moves on
    // every merge while the deployed image does not, and a tag the registry
    // has never seen is an ImagePullBackOff that looks like a cluster fault.
    const env = {
        ...process.env,
        BENCH_WS: '1',
        INFRA_WS_CONTEXT: cfg.cluster,
        INFRA_WS_NS: cfg.actorsNs,
        INFRA_WS_IMAGE: repository,
        INFRA_WS_IMAGE_TAG: tag,
        INFRA_WS_WORKLOAD: cfg.workload,
        INFRA_SHAPE: shape
    };
    const code = spawnInherit('pnpm', ['bench:run', 'sockets/', '--runs=1', ...args], env);
    process.exitCode = code;
}

const KNOBS = [
    'PLACEMENT',
    'PLACEMENT_REFRESH_MS',
    'REBALANCE',
    'REBALANCE_INTERVAL_MS',
    'REBALANCE_THRESHOLD',
    'REBALANCE_MIN_IDLE_MS',
    'REBALANCE_MAX_MOVES',
    'MAX_ACTIVATIONS',
    'SWEEP_INTERVAL_MS',
    'DIGEST_MAX_LOCAL',
    'DIGEST_ITERS',
    'MIGRATE_PERSIST'
];

/** `{ NAME: value }` for the knobs the live chat Deployment actually sets. */
function liveKnobs() {
    const raw = kube(['-n', cfg.chatNs, 'get', 'deploy', 'chat-host', '-o',
        'jsonpath={range .spec.template.spec.containers[0].env[*]}{.name}={.value}{"\\n"}{end}'],
        { quiet: true, allowFail: true }) ?? '';
    const out = {};
    for (const line of raw.split('\n')) {
        const eq = line.indexOf('=');
        if (eq <= 0) continue;
        const name = line.slice(0, eq);
        if (KNOBS.includes(name)) out[name] = line.slice(eq + 1);
    }
    return out;
}

/**
 * The deployment's identity for baseline purposes. Node spread is IN it on
 * purpose: replicas sharing a node add almost no capacity, and a report
 * cannot tell the two apart (#183). So are the runtime knobs — see above.
 */
function infraShape() {
    const replicas = kube(['-n', cfg.chatNs, 'get', 'deploy', 'chat-host',
        '-o', 'jsonpath={.spec.replicas}'], { quiet: true });
    const nodes = kube(['-n', cfg.chatNs, 'get', 'pods', '-l', 'app.kubernetes.io/component=host',
        '-o', 'jsonpath={range .items[*]}{.spec.nodeName}{"\\n"}{end}'], { quiet: true });
    const distinct = new Set(nodes.split('\n').filter(Boolean)).size;
    const image = kube(['-n', cfg.chatNs, 'get', 'deploy', 'chat-host',
        '-o', 'jsonpath={.spec.template.spec.containers[0].image}'], { quiet: true });
    // Sorted, so two identically-configured deployments produce the same
    // string whatever order the manifest happened to list the env in.
    const knobs = Object.entries(liveKnobs())
        .sort(([a], [b]) => (a < b ? -1 : 1))
        .map(([k, v]) => `${k}=${v}`)
        .join(',');
    return (
        `replicas=${replicas} nodes=${distinct} image=${image.split(':').pop()}` +
        (knobs ? ` knobs=${knobs}` : '')
    );
}

const opsSecret = () =>
    Buffer.from(
        kube(['-n', cfg.chatNs, 'get', 'secret', 'chat-secrets',
            '-o', 'jsonpath={.data.opsSecret}'], { quiet: true }),
        'base64'
    ).toString('utf8');

const authSecret = () =>
    Buffer.from(
        kube(['-n', cfg.chatNs, 'get', 'secret', 'chat-secrets',
            '-o', 'jsonpath={.data.authSecret}'], { quiet: true }),
        'base64'
    ).toString('utf8');

/**
 * Everything `test` and `bench` share: the same-region VM, the built dist,
 * the ops port-forward, and the `INFRA_*` environment read off the live
 * release. Returns `{ env, done }` — call `done()` when finished — or null
 * (with exitCode set) when the forward never came up.
 */
async function infraSession() {
    // Tier 3 drives its load from the VM (a laptop's WAN jitter is +/-50%,
    // which cannot detect a regression), so the VM is part of the session.
    await loadVmUp();
    // The harness measures the BUILT dist; without it the scenario registry
    // cannot even load (it imports every tier).
    if (!existsSync(join(repoRoot, 'packages/actors/dist/index.prod.js'))) {
        step('building (the bench harness needs dist/*.prod.js)');
        sh('pnpm', ['build'], { quiet: true });
    }
    // The ops checks read the INTERNAL listener, which is deliberately not
    // routable from outside — so do what an operator does: port-forward it.
    const forwardPort = Number(process.env.INFRA_OPS_PORT ?? 7399);
    // kubectl is a real executable everywhere, so this needs no rewriting
    // today — routed through `spawnable` anyway so a machine that installs
    // it as a shim (scoop, npm, a wrapper) does not silently break `test`
    // while every other verb keeps working.
    const fwd = spawnable('kubectl', ['--context', cfg.cluster, '-n', cfg.chatNs,
        'port-forward', `svc/${'chat'}-host`, `${forwardPort}:${cfg.ports.internal}`]);
    const forward = spawn(fwd.command, fwd.args, { stdio: 'ignore', ...fwd.options });
    // VERIFY it, rather than sleeping and hoping. A forward binds to one
    // pod, so it dies whenever that pod is replaced — which a rolling
    // restart guarantees, and a leftover forward from an earlier session
    // then holds the port while refusing every connection. Unchecked, all
    // of that surfaces as a handful of ops-reading FEATURE tests failing,
    // which is the most misleading shape this suite can fail in.
    const opsUp = await (async () => {
        for (let i = 0; i < 30; i++) {
            await new Promise((r) => setTimeout(r, 1_000));
            const res = await fetch(`http://127.0.0.1:${forwardPort}/_sigx/health`)
                .catch(() => null);
            if (res?.ok) return true;
        }
        return false;
    })();
    if (!opsUp) {
        forward.kill();
        log(`✗ the ops port-forward never came up on :${forwardPort}.`);
        log(`  Something else may be holding it — check for a stray`);
        log(`  \`kubectl port-forward\` from an earlier run, or set INFRA_OPS_PORT.`);
        process.exitCode = 1;
        return null;
    }
    const env = {
        ...process.env,
        INFRA_URL: `https://${cfg.chatHost}`,
        INFRA_AUTH_SECRET: authSecret(),
        INFRA_OPS_SECRET: opsSecret(),
        INFRA_NS: cfg.chatNs,
        INFRA_CONTEXT: cfg.cluster,
        INFRA_SHAPE: infraShape(),
        INFRA_OPS_URL: `http://127.0.0.1:${forwardPort}`,
        INFRA_VM_RG: cfg.loadRg,
        INFRA_VM_NAME: cfg.loadVm,
        // The knobs the suites gate on. Each is read back off the live
        // Deployment, so a suite asserting a feature runs exactly when the
        // deployment was actually brought up with it.
        ...knobEnv()
    };
    log(`  shape: ${env.INFRA_SHAPE}`);
    return { env, done: () => forward.kill() };
}

/**
 * The whole point of the exercise: assert, then measure, then say one
 * thing. Extra args go to vitest (`test -t "sealed"`), and `INFRA_CHAOS=1`
 * adds the destructive scenarios.
 */
async function test(args) {
    const session = await infraSession();
    if (!session) return;
    const { env, done } = session;
    step('assertions (vitest)');
    const assertions = spawnInherit('pnpm', ['test:infra', ...args], env);
    // Perf costs a VM and ~10 minutes of it. Measuring a deployment whose
    // assertions just failed spends that on a number nobody can trust, so
    // stop — unless the operator is specifically chasing the perf half.
    const forcePerf = process.env.INFRA_FORCE_PERF === '1';
    let perf = 0;
    if (assertions !== 0 && !forcePerf) {
        log('\n✗ assertions failed — skipping perf (INFRA_FORCE_PERF=1 runs it anyway)');
        perf = 1;
    } else {
        step('perf (bench tier 3, compared against the recorded baseline)');
        perf = spawnInherit('pnpm', ['bench:infra:compare'], env);
    }
    done();
    log(`\n${assertions === 0 ? '✓' : '✗'} assertions   ${perf === 0 ? '✓' : '✗'} perf`);
    if (assertions !== 0 || perf !== 0) process.exitCode = 1;
}

/**
 * The raw Tier-3 run alone: same environment as `test`, but the extra args
 * go straight to the bench harness (`--json=…`, `--runs=…`, a scenario
 * filter) and no baseline is consulted. This is the verb CI dispatches —
 * `test`'s comparison is a judgement against a locally recorded baseline,
 * where a JSON artifact is a measurement anyone can diff later.
 */
async function bench(args) {
    const session = await infraSession();
    if (!session) return;
    const { env, done } = session;
    step('perf (bench tier 3, raw run)');
    const perf = spawnInherit('pnpm', ['bench:infra', ...args], env);
    done();
    if (perf !== 0) process.exitCode = 1;
}

/** The live knobs, as the `INFRA_*` names the suites and scenarios read. */
function knobEnv() {
    const live = liveKnobs();
    const out = {};
    for (const [name, value] of Object.entries(live)) {
        if (value !== '') out[`INFRA_${name}`] = value;
    }
    return out;
}

/** Record the perf baseline FOR THE CURRENT SHAPE. */
async function baseline() {
    await loadVmUp();
    const env = {
        ...process.env,
        INFRA_URL: `https://${cfg.chatHost}`,
        INFRA_AUTH_SECRET: authSecret(),
        INFRA_SHAPE: infraShape(),
        INFRA_VM_RG: cfg.loadRg,
        INFRA_VM_NAME: cfg.loadVm
    };
    log(`  shape: ${env.INFRA_SHAPE}`);
    // Propagate failure: automation that reads exit 0 as "baseline recorded"
    // would compare against a stale one forever.
    if (spawnInherit('pnpm', ['bench:infra:baseline'], env) !== 0) {
        log('✗ baseline FAILED — nothing was recorded');
        process.exitCode = 1;
    }
}

/**
 * The one assertion that structurally needs TWO deploys: `migrateState`
 * only runs on a record the PREVIOUS image wrote.
 *
 *   node testenv.mjs migrate-check [<old-tag>]
 *
 * Write a room against whatever is deployed now, roll the release forward
 * to HEAD, then assert the room reads back whole at the new version. No
 * fixture and no test-only endpoint: a v1 record is simply what the older
 * image writes, which is the same thing that happens to every real room
 * during the deploy this is imitating.
 *
 * Default `<old-tag>` is the tag already running, so the normal sequence is
 * `up` on an older commit, then `migrate-check` on the newer one.
 */
async function migrateCheck(args) {
    const target = `https://${cfg.chatHost}`;
    const secret = authSecret();
    const name = 'migrator';
    const sig = createHmac('sha256', secret).update(name).digest('hex');
    const cookie = `user=${encodeURIComponent(`${name}.${sig}`)}`;
    const room = `migrated-${Date.now().toString(36)}`;

    const before = kube(['-n', cfg.chatNs, 'get', 'deploy', 'chat-host',
        '-o', 'jsonpath={.spec.template.spec.containers[0].image}'], { quiet: true });
    const oldTag = args[0] ?? before.split(':').pop();
    step(`writing ${room} against the OLD image (${oldTag})`);
    if (args[0]) {
        release('chat', 'perf/app/deploy/chart', cfg.chatNs, [
            '--set', `image.repository=${cfg.acr}.azurecr.io/sigx-chat`,
            '--set', `image.tag=${oldTag}`,
            '--set', `ingress.host=${cfg.chatHost}`,
            '--set', `nodeSelector.workload=${cfg.workload}`
        ]);
        kube(['-n', cfg.chatNs, 'rollout', 'status', 'deploy/chat-host', '--timeout=420s']);
    }
    for (let i = 0; i < 3; i++) {
        const res = await fetch(`${target}/_sigx/fn/${postMessageFnId()}`, {
            method: 'POST',
            headers: { 'content-type': 'application/json', origin: target, cookie },
            body: JSON.stringify({ args: [{ room, text: `pre-migration ${i}` }] })
        });
        if (!res.ok) {
            log(`✗ could not seed ${room}: ${res.status}`);
            process.exitCode = 1;
            return;
        }
    }

    const tag = gitSha();
    step(`rolling forward to ${tag}`);
    buildImage('sigx-chat', 'perf/app/Dockerfile', tag);
    release('chat', 'perf/app/deploy/chart', cfg.chatNs, [
        '--set', `image.repository=${cfg.acr}.azurecr.io/sigx-chat`,
        '--set', `image.tag=${tag}`,
        '--set', `ingress.host=${cfg.chatHost}`,
        '--set', `nodeSelector.workload=${cfg.workload}`
    ]);
    kube(['-n', cfg.chatNs, 'rollout', 'status', 'deploy/chat-host', '--timeout=420s']);

    step('asserting the migration');
    const code = spawnInherit('pnpm', ['test:infra', '-t', 'migrateState'], {
        ...process.env,
        INFRA_URL: target,
        INFRA_AUTH_SECRET: secret,
        INFRA_MIGRATED_ROOM: room
    });
    log(`\n${code === 0 ? '✓' : '✗'} migrateState (${room}, written by ${oldTag})`);
    if (code !== 0) process.exitCode = 1;
}

function spawnInherit(cmd, args, env) {
    const run = spawnable(cmd, args);
    try {
        execFileSync(run.command, run.args, {
            stdio: 'inherit',
            cwd: repoRoot,
            env,
            ...run.options
        });
        return 0;
    } catch {
        return 1;
    }
}

/** A signed session cookie, minted the way the chat app's guard verifies. */
function mintCookie() {
    const secret = Buffer.from(
        kube(['-n', cfg.chatNs, 'get', 'secret', 'chat-secrets',
            '-o', 'jsonpath={.data.authSecret}'], { quiet: true }),
        'base64'
    ).toString('utf8');
    const name = process.env.LOAD_USER ?? 'loadtest';
    const sig = createHmac('sha256', secret).update(name).digest('hex');
    return `user=${encodeURIComponent(`${name}.${sig}`)}`;
}

async function down() {
    step('deleting releases');
    for (const [name, ns] of [['chat', cfg.chatNs], ['sigx', cfg.actorsNs]]) {
        helm(['uninstall', name, '-n', ns], { quiet: true, allowFail: true });
        kube(['delete', 'namespace', ns, '--wait=false'], { quiet: true, allowFail: true });
    }
    step(`removing dns ${cfg.chatHost}`);
    az(['network', 'dns', 'record-set', 'a', 'delete', '-g', cfg.dnsRg, '-z', cfg.dnsZone,
        '-n', dnsName, '-y'], { quiet: true, allowFail: true });
    step(`deleting load vm resource group ${cfg.loadRg}`);
    az(['group', 'delete', '-n', cfg.loadRg, '-y', '--no-wait'], { quiet: true, allowFail: true });
    step(`deleting node pool ${cfg.pool}`);
    az(['aks', 'nodepool', 'delete', '-g', cfg.rg, '--cluster-name', cfg.cluster,
        '--name', cfg.pool, '--no-wait'], { allowFail: true });
    log('\n✓ down (pool and vm deletions continue in the background)');
}

const [verb = 'status', ...rest] = process.argv.slice(2);
const verbs = {
    up,
    down,
    status,
    test: () => test(rest),
    baseline,
    bench: () => bench(rest),
    load: () => load(rest),
    'ws-up': () => wsUp(rest),
    'ws-load': () => wsLoad(rest),
    'wf-load': () => wfLoad(rest),
    'wf-bench': () => wfBench(rest),
    'ws-bench': () => wsBench(rest),
    'migrate-check': () => migrateCheck(rest),
    'vm-up': loadVmUp
};

/**
 * Which identity env vars each verb actually reaches for — checked before
 * anything runs, so a missing name fails here with its env var named
 * rather than three az calls in with a 404 on somebody's default.
 */
const ENV_KEY = {
    RG: 'rg', CLUSTER: 'cluster', ACR: 'acr', LOCATION: 'location',
    CHAT_HOST: 'chatHost', DNS_ZONE: 'dnsZone', DNS_RG: 'dnsRg',
    LOAD_RG: 'loadRg', LOAD_VM: 'loadVm'
};
const NEEDS = {
    up: ['RG', 'CLUSTER', 'ACR', 'CHAT_HOST', 'DNS_ZONE', 'DNS_RG'],
    status: ['RG', 'CLUSTER', 'CHAT_HOST', 'LOAD_RG', 'LOAD_VM'],
    test: ['RG', 'CLUSTER', 'CHAT_HOST', 'LOCATION', 'LOAD_RG', 'LOAD_VM'],
    baseline: ['RG', 'CLUSTER', 'CHAT_HOST', 'LOCATION', 'LOAD_RG', 'LOAD_VM'],
    bench: ['RG', 'CLUSTER', 'CHAT_HOST', 'LOCATION', 'LOAD_RG', 'LOAD_VM'],
    load: ['RG', 'CLUSTER', 'CHAT_HOST', 'LOCATION', 'LOAD_RG', 'LOAD_VM'],
    // In-cluster only: the WebSocket Job talks to the actors Service, so
    // it needs neither the public chat host nor the load VM.
    'ws-up': ['RG', 'CLUSTER'],
    'ws-load': ['RG', 'CLUSTER', 'ACR'],
    'ws-bench': ['RG', 'CLUSTER'],
    // The workflow Job is in-cluster too (#297).
    'wf-load': ['RG', 'CLUSTER', 'ACR'],
    'wf-bench': ['RG', 'CLUSTER'],
    'migrate-check': ['RG', 'CLUSTER', 'ACR', 'CHAT_HOST'],
    down: ['RG', 'CLUSTER', 'CHAT_HOST', 'DNS_ZONE', 'DNS_RG', 'LOAD_RG'],
    'vm-up': ['LOCATION', 'LOAD_RG', 'LOAD_VM']
};
/** Verbs that reach kubectl/helm, and so need a kubeconfig first. */
const KUBE_VERBS = new Set(['up', 'status', 'test', 'baseline', 'bench', 'load', 'ws-up', 'ws-load', 'ws-bench', 'wf-load', 'wf-bench', 'migrate-check', 'down']);

if (!verbs[verb]) {
    log(`usage: node testenv.mjs <${Object.keys(verbs).join('|')}>`);
    process.exit(1);
}
const missing = NEEDS[verb].filter((name) => !cfg[ENV_KEY[name]]);
if (missing.length) {
    console.error(`${verb} needs ${missing.join(', ')} set in the environment.`);
    console.error('These name your Azure estate and are deliberately not defaulted');
    console.error('here — see deploy/RUNBOOK.md. Export them once locally; in CI');
    console.error('they come from Actions secrets.');
    process.exit(1);
}
// A fresh CI runner has no kubeconfig at all, so every kube-touching verb
// fetches credentials — not just `up`.
if (KUBE_VERBS.has(verb)) ensureCredentials();
await verbs[verb]();
