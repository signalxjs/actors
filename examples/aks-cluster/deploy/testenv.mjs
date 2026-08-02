#!/usr/bin/env node
/**
 * The Azure test environment, as one command per verb.
 *
 *   node testenv.mjs up            # node pool + both releases + DNS
 *   node testenv.mjs status        # what exists, what it costs
 *   node testenv.mjs load [args]   # run the edge ladder from a same-region VM
 *   node testenv.mjs down          # delete everything this script created
 *
 * Why a script and not a README section: every measurement in scenario (l)
 * of the RUNBOOK depends on a specific shape (hosts spread one-per-node, a
 * load client in the same region, an ingress that does not buffer), and a
 * shape assembled by hand is a shape nobody can reproduce or bill
 * correctly. `down` is as important as `up` — this pool costs money idle.
 *
 * Nothing is hardcoded: every value below is an env var with a default,
 * and `up` is idempotent, so re-running it converges rather than
 * duplicating. Requires az (logged in), kubectl and helm on PATH.
 */
import { execFileSync, spawn } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';
import { existsSync, readFileSync } from 'node:fs';
import { createHmac } from 'node:crypto';

const here = dirname(fileURLToPath(import.meta.url));
const repoRoot = join(here, '..', '..', '..');

const cfg = {
    subscription: process.env.SUBSCRIPTION ?? '',          // '' = az default
    rg: process.env.RG ?? 'OmniaFlowCore',
    cluster: process.env.CLUSTER ?? 'omniaflow-core',
    acr: process.env.ACR ?? 'omniaflowtestacr',
    location: process.env.LOCATION ?? 'westeurope',
    pool: process.env.POOL ?? 'sigxactors',
    poolSize: process.env.POOL_SIZE ?? 'Standard_D2ls_v6',
    poolCount: process.env.POOL_COUNT ?? '4',
    poolMax: process.env.POOL_MAX ?? '8',
    // The taint/label pair every chart in here selects on.
    workload: process.env.WORKLOAD ?? 'sigx-actors-test',
    actorsNs: process.env.ACTORS_NS ?? 'sigx-actors-test',
    chatNs: process.env.CHAT_NS ?? 'sigx-chat',
    chatHost: process.env.CHAT_HOST ?? 'chat.omniaflowtest.net',
    dnsZone: process.env.DNS_ZONE ?? 'omniaflowtest.net',
    dnsRg: process.env.DNS_RG ?? 'omniaflowcore',
    // The load client: its own resource group so `down` is one delete.
    loadRg: process.env.LOAD_RG ?? 'sigx-loadtest',
    loadVm: process.env.LOAD_VM ?? 'loadvm',
    loadVmSize: process.env.LOAD_VM_SIZE ?? 'Standard_D4s_v5',
    ingressNs: process.env.INGRESS_NS ?? 'ingress-nginx',
    // Mirrors the chart's ports.internal — where health and ops live.
    ports: { internal: Number(process.env.INTERNAL_PORT ?? 7311) }
};

// Suffix-anchored, and fatal if it does not match: a loose replace() on a
// host that merely CONTAINS the zone would compute the wrong record-set
// name, and ensureDns() would then happily rewrite somebody else's record.
const dnsSuffix = `.${cfg.dnsZone}`;
if (!cfg.chatHost.endsWith(dnsSuffix)) {
    console.error(`CHAT_HOST (${cfg.chatHost}) must be a name inside DNS_ZONE (${cfg.dnsZone})`);
    process.exit(1);
}
const dnsName = cfg.chatHost.slice(0, -dnsSuffix.length);

// ---------------------------------------------------------------------------

const log = (...a) => console.log(...a);
const step = (s) => log(`\n▸ ${s}`);

function sh(cmd, args, { quiet = false, allowFail = false } = {}) {
    try {
        const out = execFileSync(cmd, args, {
            encoding: 'utf8',
            stdio: quiet ? ['ignore', 'pipe', 'pipe'] : ['ignore', 'pipe', 'inherit'],
            cwd: repoRoot
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
    }
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

function buildImage(name, dockerfile, tag) {
    step(`building ${name}:${tag} in ${cfg.acr}`);
    az(['acr', 'build', '--registry', cfg.acr, '--image', `${name}:${tag}`,
        '--platform', 'linux/amd64', '--file', dockerfile, '.']);
}

function release(name, chart, ns, extra) {
    const exists = helm(['list', '-n', ns, '-q'], { quiet: true, allowFail: true })?.split('\n').includes(name);
    kube(['create', 'namespace', ns], { quiet: true, allowFail: true });
    // Preflight against the REAL API server: admission webhooks reject
    // things `helm lint` and `helm template` render happily (an nginx
    // Ingress path containing `$` under pathType Prefix, for one).
    const dry = helm(['upgrade', '--install', name, chart, '-n', ns, '--dry-run=server', ...extra],
        { quiet: true, allowFail: true });
    if (dry === null) {
        log(`✗ ${name}: the API server rejected this chart — running it again for the error:`);
        helm(['upgrade', '--install', name, chart, '-n', ns, '--dry-run=server', ...extra]);
    }
    step(`${exists ? 'upgrading' : 'installing'} ${name} in ${ns}`);
    helm([exists ? 'upgrade' : 'install', name, chart, '-n', ns,
        // reset-then-reuse (not --reuse-values): replaying old values breaks
        // the moment a chart gains a key, and every value we care about is
        // passed explicitly below anyway.
        ...(exists ? ['--reset-then-reuse-values'] : []), ...extra]);
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
    kube(['-n', cfg.ingressNs, 'get', 'svc', 'ingress-nginx-controller',
        '-o', 'jsonpath={.status.loadBalancer.ingress[0].ip}'], { quiet: true });

// ---------------------------------------------------------------------------

async function up() {
    const tag = gitSha();
    ensureCredentials();
    ensurePool();
    buildImage('sigx-actors-test', 'examples/aks-cluster/Dockerfile', tag);
    buildImage('sigx-chat', 'examples/chat/Dockerfile', tag);

    release('sigx', 'examples/aks-cluster/deploy/chart', cfg.actorsNs, [
        '--set', `image.tag=${tag}`,
        '--set', `nodeSelector.workload=${cfg.workload}`
    ]);
    release('chat', 'examples/chat/deploy/chart', cfg.chatNs, [
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

function runOnVm(script) {
    return az(['vm', 'run-command', 'invoke', '-g', cfg.loadRg, '-n', cfg.loadVm,
        '--command-id', 'RunShellScript', '--scripts', script,
        '--query', 'value[0].message', '-o', 'tsv'], { quiet: true });
}

/**
 * Run the edge ladder from the VM. Extra args become env for the ladder,
 * e.g. `load LADDER=64,128,256 MIX=0.2 DURATION_MS=30000`.
 */
async function load(args) {
    await loadVmUp();
    const cookie = mintCookie();
    const b64 = Buffer.from(readFileSync(join(here, 'edge-ladder.mjs'))).toString('base64');
    const env = ['ROOMS=64', 'WORKERS=4', 'DURATION_MS=20000', 'LADDER=32,64,128,256,512',
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

/**
 * The deployment's identity for baseline purposes. Node spread is IN it on
 * purpose: replicas sharing a node add almost no capacity, and a report
 * cannot tell the two apart (#183).
 */
function infraShape() {
    const replicas = kube(['-n', cfg.chatNs, 'get', 'deploy', 'chat-host',
        '-o', 'jsonpath={.spec.replicas}'], { quiet: true });
    const nodes = kube(['-n', cfg.chatNs, 'get', 'pods', '-l', 'app.kubernetes.io/component=host',
        '-o', 'jsonpath={range .items[*]}{.spec.nodeName}{"\\n"}{end}'], { quiet: true });
    const distinct = new Set(nodes.split('\n').filter(Boolean)).size;
    const image = kube(['-n', cfg.chatNs, 'get', 'deploy', 'chat-host',
        '-o', 'jsonpath={.spec.template.spec.containers[0].image}'], { quiet: true });
    return `replicas=${replicas} nodes=${distinct} image=${image.split(':').pop()}`;
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
 * The whole point of the exercise: assert, then measure, then say one
 * thing. Extra args go to vitest (`test -t "sealed"`), and `INFRA_CHAOS=1`
 * adds the destructive scenarios.
 */
async function test(args) {
    // Tier 3 drives its load from the VM (a laptop's WAN jitter is +/-50%,
    // which cannot detect a regression), so the VM is part of `test`.
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
    const forward = spawn('kubectl', ['--context', cfg.cluster, '-n', cfg.chatNs,
        'port-forward', `svc/${'chat'}-host`, `${forwardPort}:${cfg.ports.internal}`],
        { stdio: 'ignore' });
    await new Promise((r) => setTimeout(r, 3_000));
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
        INFRA_VM_NAME: cfg.loadVm
    };
    log(`  shape: ${env.INFRA_SHAPE}`);
    step('assertions (vitest)');
    const assertions = spawnInherit('pnpm', ['test:infra', ...args], env);
    step('perf (bench tier 3, compared against the recorded baseline)');
    const perf = spawnInherit('pnpm', ['bench:infra:compare'], env);
    forward.kill();
    log(`\n${assertions === 0 ? '✓' : '✗'} assertions   ${perf === 0 ? '✓' : '✗'} perf`);
    if (assertions !== 0 || perf !== 0) process.exitCode = 1;
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

function spawnInherit(cmd, args, env) {
    try {
        execFileSync(cmd, args, { stdio: 'inherit', cwd: repoRoot, env });
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
    load: () => load(rest),
    'vm-up': loadVmUp
};
if (!verbs[verb]) {
    log(`usage: node testenv.mjs <${Object.keys(verbs).join('|')}>`);
    process.exit(1);
}
await verbs[verb]();
